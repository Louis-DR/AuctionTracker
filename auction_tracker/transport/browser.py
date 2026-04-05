"""Browser transport using Playwright async API.

Designed to be simple and robust:
- Pure async, no threading
- Single browser instance with a tab semaphore
- Hard timeouts on every operation
- Clean startup and shutdown
- Stealth patches to evade basic bot detection
- Human-like mouse/scroll behavior to bypass DataDome fingerprinting
- DataDome challenge detection and cookie consent dismissal
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from urllib.parse import urlparse

from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
  TransportTimeout,
)

logger = logging.getLogger(__name__)

# DataDome challenge markers (present in short captcha interstitials).
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
  - playwright-stealth patches applied to the entire browser context
    (navigator.webdriver, missing plugins, CDP exposure, etc.)
  - --disable-blink-features=AutomationControlled Chrome flag
  - DataDome challenge detection: waits up to 15 s for auto-resolution
    before giving up (handles both HTTP 200 and 403 interstitials)
  - Automatic Didomi / common cookie consent dismissal
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

    self._playwright = await async_playwright().start()
    self._browser = await self._playwright.chromium.launch(
      headless=self._headless,
      args=[
        # Hide the automation flag that Chrome exposes by default.
        "--disable-blink-features=AutomationControlled",
      ],
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

    # Apply stealth patches to the whole context so that every page
    # inherits them. The v2 API applies init scripts at context level.
    self._stealth_available = await self._apply_stealth_to_context()

    self._semaphore = asyncio.Semaphore(self._max_pages)
    logger.info(
      "Browser transport started (headless=%s, max_pages=%d, stealth=%s)",
      self._headless, self._max_pages, self._stealth_available,
    )

  async def _apply_stealth_to_context(self) -> bool:
    """Apply playwright-stealth patches to the browser context.

    Uses the v2 API: ``Stealth().apply_stealth_async(context)``.
    Patches are injected as init scripts so every page opened from
    this context is covered automatically.

    Returns True if stealth was successfully applied.
    """
    try:
      from playwright_stealth import Stealth
      stealth = Stealth(
        # Override navigator.languages to match the fr-FR locale we set.
        navigator_languages_override=("fr-FR", "fr"),
      )
      await stealth.apply_stealth_async(self._context)
      logger.debug("playwright-stealth v2 patches applied to browser context")
      return True
    except ImportError:
      logger.info(
        "playwright-stealth not installed; stealth patches disabled. "
        "Install with: pip install playwright-stealth"
      )
    except Exception as error:
      logger.warning("Failed to apply stealth patches: %s", error)
    return False

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

  async def _warm_up_domain(self, page, domain: str) -> None:
    """Visit the domain homepage to establish cookies and sessions.

    Only runs once per domain per browser session. Anti-bot systems
    like DataDome track navigation patterns and expect an initial
    homepage visit before accessing deep pages.
    """
    if domain in self._warmed_up_domains:
      return

    homepage = f"https://www.{domain}/"
    logger.info("Warming up browser session for %s", domain)
    try:
      await page.goto(homepage, wait_until="domcontentloaded", timeout=15_000)
      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=5000)
      await self._simulate_human_behavior(page)
      await self._dismiss_cookie_consent(page)
      self._warmed_up_domains.add(domain)
      logger.debug("Warm-up complete for %s", domain)
    except Exception as error:
      logger.warning("Homepage warm-up for %s failed: %s", domain, error)
      # Mark as done anyway so we don't keep retrying on every fetch.
      self._warmed_up_domains.add(domain)

  @staticmethod
  async def _simulate_human_behavior(page) -> None:
    """Simulate realistic mouse movements and scrolling on the page.

    DataDome and similar anti-bot systems analyse mouse movement
    patterns, scroll behaviour, and interaction timing. A browser
    that navigates without any pointer activity is a strong signal
    of automation. This simulates a real user casually scanning a
    page.
    """
    try:
      await asyncio.sleep(random.uniform(0.3, 1.0))

      for _ in range(random.randint(2, 4)):
        target_x = random.randint(100, 900)
        target_y = random.randint(100, 500)
        await page.mouse.move(target_x, target_y, steps=random.randint(5, 15))
        await asyncio.sleep(random.uniform(0.15, 0.4))

      total_scroll = random.randint(200, 600)
      scroll_steps = random.randint(2, 4)
      for _ in range(scroll_steps):
        scroll_amount = total_scroll // scroll_steps + random.randint(-30, 30)
        await page.mouse.wheel(0, scroll_amount)
        await asyncio.sleep(random.uniform(0.2, 0.5))

      if random.random() > 0.4:
        await page.mouse.wheel(0, -random.randint(50, 150))
        await asyncio.sleep(random.uniform(0.1, 0.3))

      await page.mouse.move(
        random.randint(200, 700),
        random.randint(150, 400),
        steps=random.randint(5, 10),
      )
    except Exception:
      pass

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
      with contextlib.suppress(Exception):
        button = page.locator(selector).first
        if await button.is_visible(timeout=500):
          await asyncio.sleep(0.5)
          await button.click()
          logger.debug("Dismissed cookie consent via %s", selector)
          await asyncio.sleep(0.5)
          return

  async def _wait_for_datadome(self, page, max_wait: float = 15.0) -> None:
    """Detect DataDome challenge pages and wait for auto-resolution.

    DataDome interstitials are short pages (< 10 KB) containing
    captcha-delivery.com or dd.js markers. When the browser solves
    the challenge automatically, the page reloads with real content.
    This can happen even on HTTP 403 responses — the 403 page carries
    DataDome JS that resolves and redirects.
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
    with contextlib.suppress(Exception):
      content = await page.content()
      if len(content) > 10_000:
        return False
      lower_prefix = content[:5000].lower()
      return (
        any(marker in lower_prefix for marker in _DATADOME_MARKERS)
        or "datadome" in lower_prefix[:3000]
      )
    return False

  @staticmethod
  def _extract_domain(url: str) -> str | None:
    """Extract the bare domain (e.g. 'leboncoin.fr') from a URL."""
    with contextlib.suppress(Exception):
      host = urlparse(url).hostname or ""
      if host.startswith("www."):
        host = host[4:]
      return host if host else None
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

        # Wait for network to settle (helps with JS-rendered content).
        with contextlib.suppress(Exception):
          await page.wait_for_load_state("networkidle", timeout=5000)

        # Simulate human interaction before anti-bot checks run their
        # analysis. DataDome monitors pointer and scroll events, so a
        # page without any mouse activity is flagged as automation.
        await self._simulate_human_behavior(page)

        # Handle DataDome challenge regardless of HTTP status code.
        # A 403 from DataDome still delivers HTML that auto-resolves.
        await self._wait_for_datadome(page)

        # Dismiss cookie consent after any challenge has resolved.
        await self._dismiss_cookie_consent(page)

        html = await page.content()
        elapsed = time.monotonic() - start_time
        final_url = page.url

        # Raise a persistent block only if the page is still a 403/429/503
        # AND it did not resolve into real content.
        if status_code in (403, 429, 503) and page.url == url:
          # Check whether we actually got useful content.
          if len(html) < 5000:
            raise TransportBlocked(
              f"Browser blocked by {url} (HTTP {status_code})",
              url=url,
              status_code=status_code,
            )
          logger.info(
            "HTTP %d from %s resolved to %d bytes of content",
            status_code, url, len(html),
          )

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
