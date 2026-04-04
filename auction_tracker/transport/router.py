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

    needs_browser = any(
      website_config.transport == TransportKind.BROWSER
      or website_config.fallback_transport == TransportKind.BROWSER
      for website_config in self._config.websites.values()
      if website_config.enabled
    )
    if needs_browser:
      self._browser = BrowserTransport(
        max_pages=transport_config.browser_page_limit,
        timeout=transport_config.default_timeout,
      )
      await self._browser.start()

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
    if kind == TransportKind.BROWSER:
      if self._browser is None:
        raise RuntimeError(
          "Browser transport not initialized. "
          "Install playwright: pip install 'auction-tracker[browser]'"
        )
      return self._browser
    raise ValueError(f"Unknown transport kind: {kind}")

  async def fetch(self, website_name: str, url: str, **kwargs) -> FetchResult:
    """Fetch a URL using the transport configured for the given website.

    If the primary transport raises TransportBlocked and a fallback
    is configured, retries with the fallback transport.
    """
    website_config = self._config.website(website_name)
    primary = self._get_transport(website_config.transport)

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
        fallback = self._get_transport(website_config.fallback_transport)
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
        fallback = self._get_transport(website_config.fallback_transport)
        return await fallback.fetch(url, **kwargs)
      raise
