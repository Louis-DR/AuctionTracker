"""Browser transport using Playwright async API.

Designed to be simple and robust:
- Pure async, no threading
- Single browser instance with a tab semaphore
- Hard timeouts on every operation
- Clean startup and shutdown
"""

from __future__ import annotations

import asyncio
import logging
import time

from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
  TransportTimeout,
)

logger = logging.getLogger(__name__)


class BrowserTransport(Transport):
  """Async browser transport using Playwright.

  Limits concurrent pages with a semaphore and enforces a hard
  timeout on every navigation. The browser is launched lazily on
  first use and shut down on stop().
  """

  def __init__(
    self,
    headless: bool = True,
    timeout: float = 30.0,
    max_pages: int = 3,
    request_delay: float = 2.0,
  ) -> None:
    self._headless = headless
    self._timeout_ms = int(timeout * 1000)
    self._max_pages = max_pages
    self._request_delay = request_delay
    self._playwright = None
    self._browser = None
    self._context = None
    self._semaphore: asyncio.Semaphore | None = None
    self._rate_limit_lock = asyncio.Lock()
    self._last_request_time: float = 0.0

  @property
  def name(self) -> str:
    return "browser"

  async def start(self) -> None:
    try:
      from playwright.async_api import async_playwright
    except ImportError as error:
      raise ImportError(
        "Playwright is required for browser transport. "
        "Install it with: pip install 'auction-tracker[browser]'"
      ) from error

    self._playwright = await async_playwright().start()
    self._browser = await self._playwright.chromium.launch(headless=self._headless)
    self._context = await self._browser.new_context(
      user_agent=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
      ),
      viewport={"width": 1920, "height": 1080},
      locale="fr-FR",
    )
    self._semaphore = asyncio.Semaphore(self._max_pages)
    logger.info(
      "Browser transport started (headless=%s, max_pages=%d)",
      self._headless, self._max_pages,
    )

  async def stop(self) -> None:
    if self._context is not None:
      await self._context.close()
      self._context = None
    if self._browser is not None:
      await self._browser.close()
      self._browser = None
    if self._playwright is not None:
      await self._playwright.stop()
      self._playwright = None
    logger.info("Browser transport stopped")

  async def _enforce_rate_limit(self) -> None:
    async with self._rate_limit_lock:
      now = time.monotonic()
      elapsed = now - self._last_request_time
      if elapsed < self._request_delay:
        await asyncio.sleep(self._request_delay - elapsed)
      self._last_request_time = time.monotonic()

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._context is None:
      await self.start()

    wait_until = kwargs.get("wait_until", "domcontentloaded")

    await self._enforce_rate_limit()

    async with self._semaphore:
      page = await self._context.new_page()
      start_time = time.monotonic()
      try:
        response = await page.goto(
          url,
          wait_until=wait_until,
          timeout=self._timeout_ms,
        )

        status_code = response.status if response else 0
        if status_code in (403, 429, 503):
          raise TransportBlocked(
            f"Browser blocked by {url} (HTTP {status_code})",
            url=url,
            status_code=status_code,
          )

        html = await page.content()
        elapsed = time.monotonic() - start_time
        final_url = page.url

        logger.debug(
          "Browser fetched %s (%d bytes, %.1fs)",
          url, len(html), elapsed,
        )
        return FetchResult(
          html=html,
          url=url,
          status_code=status_code,
          redirected_url=final_url if final_url != url else None,
          elapsed_seconds=elapsed,
          transport_name=self.name,
        )

      except TransportBlocked:
        raise
      except Exception as error:
        elapsed = time.monotonic() - start_time
        error_name = type(error).__name__
        if "timeout" in error_name.lower() or "Timeout" in str(error):
          raise TransportTimeout(
            f"Browser timeout after {elapsed:.1f}s for {url}",
            url=url,
          ) from error
        raise TransportError(
          f"Browser error for {url}: {error}",
          url=url,
        ) from error
      finally:
        await page.close()
