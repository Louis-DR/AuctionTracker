"""Browser transport using Playwright async API.

Designed to be simple and robust:
- Pure async, no threading
- Single browser instance with a tab semaphore
- Hard timeouts on every operation
- Clean startup and shutdown
- Stealth patches to evade basic bot detection
- DataDome challenge detection and cookie consent dismissal
"""

from __future__ import annotations

import asyncio
import contextlib
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

# DataDome challenge markers (present in short captcha pages).
_DATADOME_MARKERS = (
  "geo.captcha-delivery.com",
  "dd.js",
)


class BrowserTransport(Transport):
  """Async browser transport using Playwright.

  Limits concurrent pages with a semaphore and enforces a hard
  timeout on every navigation. The browser is launched lazily on
  first use and shut down on stop().

  Anti-bot features:
  - playwright-stealth patches (navigator.webdriver, plugins, etc.)
  - DataDome challenge detection and wait-for-resolution
  - Automatic Didomi cookie consent dismissal
  - Per-domain homepage warm-up to establish cookie sessions
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
    self._warmed_up_domains: set[str] = set()
    self._stealth_available = False

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

    # Check for stealth support.
    try:
      from playwright_stealth import stealth_async  # noqa: F401
      self._stealth_available = True
    except ImportError:
      logger.info(
        "playwright-stealth not installed; browser stealth patches disabled. "
        "Install with: pip install playwright-stealth"
      )

    self._playwright = await async_playwright().start()
    self._browser = await self._playwright.chromium.launch(
      headless=self._headless,
    )
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
      "Browser transport started (headless=%s, max_pages=%d, stealth=%s)",
      self._headless, self._max_pages, self._stealth_available,
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
    self._warmed_up_domains.clear()
    logger.info("Browser transport stopped")

  async def _enforce_rate_limit(self) -> None:
    async with self._rate_limit_lock:
      now = time.monotonic()
      elapsed = now - self._last_request_time
      if elapsed < self._request_delay:
        await asyncio.sleep(self._request_delay - elapsed)
      self._last_request_time = time.monotonic()

  async def _apply_stealth(self, page) -> None:
    """Apply stealth patches to a page to evade bot detection."""
    if not self._stealth_available:
      return
    try:
      from playwright_stealth import stealth_async
      await stealth_async(page)
    except Exception as error:
      logger.debug("Failed to apply stealth patches: %s", error)

  async def _warm_up_domain(self, page, domain: str) -> None:
    """Visit the domain homepage to establish cookies and sessions.

    Only runs once per domain per browser session. This helps with
    anti-bot systems that expect an initial homepage visit before
    navigating to deep pages.
    """
    if domain in self._warmed_up_domains:
      return

    homepage = f"https://www.{domain}/"
    logger.info("Warming up browser session for %s", domain)
    try:
      await page.goto(homepage, wait_until="domcontentloaded", timeout=15_000)
      await asyncio.sleep(1.5)
      await self._dismiss_cookie_consent(page)
      self._warmed_up_domains.add(domain)
    except Exception as error:
      logger.warning("Homepage warm-up for %s failed: %s", domain, error)
      self._warmed_up_domains.add(domain)

  async def _dismiss_cookie_consent(self, page) -> None:
    """Click common cookie consent buttons if present.

    Covers Didomi (LeBonCoin) and other common consent frameworks.
    """
    consent_selectors = [
      "#didomi-notice-agree-button",
      "button[aria-label='Accepter & Fermer']",
      "button:has-text('Tout accepter')",
      "button:has-text('Accepter')",
      "#consent-page button",
      "button:has-text('Accept all')",
      "button:has-text('Accept')",
    ]
    for selector in consent_selectors:
      try:
        button = page.locator(selector).first
        if await button.is_visible(timeout=500):
          await asyncio.sleep(0.5)
          await button.click()
          logger.debug("Dismissed cookie consent via %s", selector)
          await asyncio.sleep(0.5)
          return
      except Exception:
        continue

  async def _wait_for_datadome(self, page, max_wait: float = 15.0) -> None:
    """Detect DataDome challenge pages and wait for auto-resolution.

    DataDome interstitials are short pages (< 10 KB) containing
    captcha-delivery.com or dd.js markers. When the browser solves
    the challenge automatically (e.g. via JavaScript), the page
    reloads with the real content.
    """
    if not await self._is_datadome_challenge(page):
      return

    logger.info("DataDome challenge detected, waiting for resolution...")
    start = time.monotonic()
    while time.monotonic() - start < max_wait:
      await asyncio.sleep(2.0)
      if not await self._is_datadome_challenge(page):
        elapsed = time.monotonic() - start
        logger.info("DataDome challenge resolved after %.1fs", elapsed)
        return

    logger.warning("DataDome challenge did not resolve within %.0fs", max_wait)

  @staticmethod
  async def _is_datadome_challenge(page) -> bool:
    """Return True if the current page is a DataDome challenge."""
    try:
      content = await page.content()
      if len(content) > 10_000:
        return False
      lower_prefix = content[:5000].lower()
      return any(
        marker in lower_prefix for marker in _DATADOME_MARKERS
      ) or "datadome" in lower_prefix[:3000]
    except Exception:
      return False

  def _extract_domain(self, url: str) -> str | None:
    """Extract the bare domain (e.g. 'leboncoin.fr') from a URL."""
    try:
      from urllib.parse import urlparse
      parsed = urlparse(url)
      host = parsed.hostname or ""
      # Strip www. prefix.
      if host.startswith("www."):
        host = host[4:]
      return host if host else None
    except Exception:
      return None

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._context is None:
      await self.start()

    wait_until = kwargs.get("wait_until", "domcontentloaded")
    warm_up = kwargs.get("warm_up", False)

    await self._enforce_rate_limit()

    async with self._semaphore:
      page = await self._context.new_page()
      start_time = time.monotonic()
      try:
        await self._apply_stealth(page)

        # Optionally warm up the domain on first visit.
        if warm_up:
          domain = self._extract_domain(url)
          if domain:
            await self._warm_up_domain(page, domain)

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

        # Wait for network to settle (helps with JS-rendered content).
        with contextlib.suppress(Exception):
          await page.wait_for_load_state("networkidle", timeout=5000)

        # Handle DataDome challenge if present.
        await self._wait_for_datadome(page)

        # Dismiss cookie consent after page loads.
        await self._dismiss_cookie_consent(page)

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
