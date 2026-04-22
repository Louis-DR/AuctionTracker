"""Transport router: picks the right transport for each website.

Handles fallback logic — if the primary transport fails with a
blocking error, the router can automatically retry with the
fallback transport (if configured).
"""

from __future__ import annotations

import asyncio
import logging

from auction_tracker.config import AppConfig, TransportKind, WebsiteConfig
from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
)
from auction_tracker.transport.browser import BrowserTransport
from auction_tracker.transport.camoufox import CamoufoxTransport
from auction_tracker.transport.http import HttpTransport

logger = logging.getLogger(__name__)


class TransportRouter:
  """Routes fetch requests to the correct transport per website.

  Usage::

      async with TransportRouter(config) as router:
          result = await router.fetch("ebay", "https://www.ebay.com/itm/12345")
  """

  def __init__(self, config: AppConfig) -> None:
    self._config = config
    self._http: HttpTransport | None = None
    self._browser: BrowserTransport | None = None
    self._camoufox: CamoufoxTransport | None = None
    self._browser_lock = asyncio.Lock()
    self._camoufox_lock = asyncio.Lock()

  def _needs_transport(self, kind: TransportKind) -> bool:
    """Check whether any enabled website uses the given transport."""
    for name in self._config.websites:
      website_config = self._config.website(name)
      if not website_config.enabled:
        continue
      if website_config.transport == kind or website_config.fallback_transport == kind:
        return True
    return False

  async def start(self) -> None:
    transport_config = self._config.transport
    self._http = HttpTransport(
      impersonation=transport_config.impersonation,
      request_delay=transport_config.default_request_delay,
      timeout=transport_config.default_timeout,
      max_retries=transport_config.max_retries,
      retry_backoff_factor=transport_config.retry_backoff_factor,
    )
    await self._http.start()

    # Eagerly start browser-based transports if any enabled website
    # needs them. This avoids lazy-init races when multiple workers
    # all try to start the same transport concurrently.
    if self._needs_transport(TransportKind.BROWSER):
      await self._ensure_browser()
    if self._needs_transport(TransportKind.CAMOUFOX):
      await self._ensure_camoufox()

  async def _ensure_browser(self) -> BrowserTransport:
    """Start the browser transport on first use (lock-protected)."""
    if self._browser is not None:
      return self._browser
    async with self._browser_lock:
      if self._browser is not None:
        return self._browser
      transport_config = self._config.transport
      self._browser = BrowserTransport(
        headless=transport_config.browser_headless,
        max_pages=transport_config.browser_page_limit,
        timeout=transport_config.default_timeout,
      )
      await self._browser.start()
      return self._browser

  async def _ensure_camoufox(self) -> CamoufoxTransport:
    """Start the Camoufox transport on first use (lock-protected)."""
    if self._camoufox is not None:
      return self._camoufox
    async with self._camoufox_lock:
      if self._camoufox is not None:
        return self._camoufox
      transport_config = self._config.transport
      self._camoufox = CamoufoxTransport(
        timeout=transport_config.default_timeout,
        request_delay=transport_config.default_request_delay,
        max_pages=transport_config.browser_page_limit,
        profile_directory=self._config.database.path.parent / "browser_profiles",
      )
      await self._camoufox.start()
      return self._camoufox

  async def stop(self) -> None:
    if self._http is not None:
      await self._http.stop()
      self._http = None
    if self._browser is not None:
      await self._browser.stop()
      self._browser = None
    if self._camoufox is not None:
      await self._camoufox.stop()
      self._camoufox = None

  async def __aenter__(self):
    await self.start()
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.stop()
    return False

  def _get_transport(self, kind: TransportKind) -> Transport:
    if kind == TransportKind.HTTP:
      if self._http is None:
        raise RuntimeError("HTTP transport not initialized")
      return self._http
    raise ValueError(f"Unknown transport kind: {kind}")

  async def _get_transport_async(self, kind: TransportKind) -> Transport:
    """Return the transport for kind, starting browser/camoufox lazily if needed."""
    if kind == TransportKind.BROWSER:
      return await self._ensure_browser()
    if kind == TransportKind.CAMOUFOX:
      return await self._ensure_camoufox()
    return self._get_transport(kind)

  async def fetch(self, website_name: str, url: str, **kwargs) -> FetchResult:
    """Fetch a URL using the transport configured for the given website.

    The browser transport is started lazily on first use, so HTTP-only
    commands (e.g. fetching an eBay listing) never launch a browser.

    If the primary transport raises TransportBlocked and a fallback
    is configured, retries with the fallback transport.
    """
    website_config = self._config.website(website_name)
    primary = await self._get_transport_async(website_config.transport)

    # Enable warm-up (homepage visit to establish cookies) for transports
    # configured to need it. Both browser and HTTP transports support the
    # warm_up kwarg; the HTTP version tracks already-warmed domains so
    # the cost is paid only once per process lifetime.
    if website_config.transport in (TransportKind.BROWSER, TransportKind.CAMOUFOX) or website_config.http_warm_up:
      kwargs.setdefault("warm_up", True)

    try:
      return await primary.fetch(url, **kwargs)
    except (TransportBlocked, TransportError) as exc:
      # 404 Not Found and 410 Gone are both definitive "the server
      # knows this URL does not exist / has been removed" responses —
      # the fallback transport would receive the same reply, so there
      # is no point retrying and no point downloading the body (which
      # is often a generic "listing not available" page that the
      # parser would then misinterpret).
      if getattr(exc, "status_code", None) in (404, 410):
        raise
      fallback = await self._resolve_fallback(website_config, primary, url, website_name)
      if fallback is not None:
        return await fallback.fetch(url, **kwargs)
      raise

  async def _resolve_fallback(
    self,
    website_config: WebsiteConfig,
    primary: Transport,
    url: str,
    website_name: str,
  ) -> Transport | None:
    """Return the fallback transport, or None if no useful fallback exists.

    Avoids retrying on the same transport instance (e.g. when a
    config.yaml override sets both primary and fallback to the same
    kind).
    """
    if website_config.fallback_transport is None:
      return None
    if website_config.fallback_transport == website_config.transport:
      logger.debug(
        "Skipping fallback for %s on %s: primary and fallback are both %s",
        url, website_name, website_config.transport.value,
      )
      return None
    fallback = await self._get_transport_async(website_config.fallback_transport)
    if fallback is primary:
      logger.debug(
        "Skipping fallback for %s on %s: resolved to the same transport instance",
        url, website_name,
      )
      return None
    logger.warning(
      "Primary transport (%s) failed for %s on %s, falling back to %s",
      primary.name, url, website_name, website_config.fallback_transport.value,
    )
    return fallback
