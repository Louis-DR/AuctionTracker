"""Transport router: picks the right transport for each website.

Handles fallback logic — if the primary transport fails with a
blocking error, the router can automatically retry with the
fallback transport (if configured).
"""

from __future__ import annotations

import logging

from auction_tracker.config import AppConfig, TransportKind
from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
)
from auction_tracker.transport.browser import BrowserTransport
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
    # Browser transport is started lazily on first use.

  async def _ensure_browser(self) -> BrowserTransport:
    """Start the browser transport on first use."""
    if self._browser is None:
      transport_config = self._config.transport
      self._browser = BrowserTransport(
        headless=transport_config.browser_headless,
        max_pages=transport_config.browser_page_limit,
        timeout=transport_config.default_timeout,
      )
      await self._browser.start()
    return self._browser

  async def stop(self) -> None:
    if self._http is not None:
      await self._http.stop()
      self._http = None
    if self._browser is not None:
      await self._browser.stop()
      self._browser = None

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
    """Return the transport for kind, starting the browser lazily if needed."""
    if kind == TransportKind.BROWSER:
      return await self._ensure_browser()
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

    # When the primary transport is browser, enable warm-up (homepage
    # visit to establish cookies) unless the caller opts out.
    if website_config.transport == TransportKind.BROWSER:
      kwargs.setdefault("warm_up", True)

    try:
      return await primary.fetch(url, **kwargs)
    except TransportBlocked:
      if website_config.fallback_transport is not None:
        logger.warning(
          "Primary transport (%s) blocked for %s on %s, "
          "falling back to %s",
          primary.name, url, website_name,
          website_config.fallback_transport.value,
        )
        fallback = await self._get_transport_async(website_config.fallback_transport)
        return await fallback.fetch(url, **kwargs)
      raise
    except TransportError:
      if website_config.fallback_transport is not None:
        logger.warning(
          "Primary transport (%s) failed for %s on %s, "
          "falling back to %s",
          primary.name, url, website_name,
          website_config.fallback_transport.value,
        )
        fallback = await self._get_transport_async(website_config.fallback_transport)
        return await fallback.fetch(url, **kwargs)
      raise
