"""Browser transport using Playwright async API.

Prefers ``rebrowser-playwright`` (a patched fork that fixes Chrome
DevTools Protocol leak vectors exploited by DataDome, Cloudflare, and
similar anti-bot systems) and falls back to vanilla ``playwright`` when
the patched version is not installed.

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
import platform
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

# Chrome launch arguments that reduce automation fingerprint leaks.
_STEALTH_CHROME_ARGS = [
  "--disable-blink-features=AutomationControlled",
  "--disable-features=AutomationControlled",
  "--disable-infobars",
  "--no-first-run",
  "--no-default-browser-check",
  "--disable-background-networking",
  "--disable-component-update",
  "--disable-domain-reliability",
  "--disable-sync",
  "--metrics-recording-only",
  "--no-service-autorun",
]


def _build_user_agent() -> str:
  """Build a Chrome user-agent string matching the host OS.

  DataDome and similar systems correlate the UA OS with low-level
  platform signals (navigator.platform, canvas font metrics, etc.).
  Claiming macOS while running on Windows is an instant red flag.
  """
  system = platform.system().lower()
  if system == "darwin":
    platform_token = "Macintosh; Intel Mac OS X 10_15_7"
  elif system == "linux":
    platform_token = "X11; Linux x86_64"
  else:
    platform_token = "Windows NT 10.0; Win64; x64"
  return (
    f"Mozilla/5.0 ({platform_token}) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
  )


def _import_async_playwright():
  """Import async_playwright, preferring the rebrowser-patched fork.

  ``rebrowser-playwright`` patches Chromium DevTools Protocol leak
  vectors (Runtime.enable, sourceURL injection, main-world execution
  context tracking) that DataDome and Cloudflare use to detect
  automation.  It is a strict superset of the official Playwright API,
  so all existing code works unchanged.
  """
  try:
    from rebrowser_playwright.async_api import async_playwright
    logger.info("Using rebrowser-playwright (CDP leak patches active)")
    return async_playwright
  except ImportError:
    pass

  try:
    from playwright.async_api import async_playwright
    logger.info(
      "rebrowser-playwright not installed; using vanilla playwright. "
      "Install rebrowser-playwright for stronger anti-detection."
    )
    return async_playwright
  except ImportError:
    raise ImportError(
      "Neither rebrowser-playwright nor playwright is installed. "
      "Install with: pip install 'auction-tracker[browser]'"
    )


class BrowserTransport(Transport):
  """Async browser transport using Playwright.

  Limits concurrent pages with a semaphore and enforces a hard
  timeout on every navigation. The browser is launched lazily on
  first use and shut down on stop().

  Anti-bot features:
  - rebrowser-playwright patches (CDP leak vectors fixed)
  - playwright-stealth patches applied to the entire browser context
    (navigator.webdriver, missing plugins, CDP exposure, etc.)
  - Extensive Chrome anti-automation flags
  - OS-appropriate user-agent to match platform fingerprint signals
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
    async_playwright = _import_async_playwright()

    self._playwright = await async_playwright().start()
    self._browser = await self._playwright.chromium.launch(
      headless=self._headless,
      args=_STEALTH_CHROME_ARGS,
    )

    user_agent = _build_user_agent()
    self._context = await self._browser.new_context(
      user_agent=user_agent,
      viewport={"width": 1920, "height": 1080},
      locale="fr-FR",
    )

    self._stealth_available = await self._apply_stealth_to_context()

    self._semaphore = asyncio.Semaphore(self._max_pages)
    logger.info(
      "Browser transport started (headless=%s, max_pages=%d, stealth=%s, ua=%s)",
      self._headless, self._max_pages, self._stealth_available,
      "rebrowser" if "rebrowser" in str(type(self._playwright)) else "vanilla",
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
      await page.goto(homepage, wait_until="domcontentloaded", timeout=20_000)
      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=8000)
      await asyncio.sleep(random.uniform(1.0, 2.0))
      await self._simulate_human_behavior(page)
      await self._dismiss_cookie_consent(page)
      await asyncio.sleep(random.uniform(0.5, 1.0))
      self._warmed_up_domains.add(domain)
      logger.debug("Warm-up complete for %s", domain)
    except Exception as error:
      logger.warning("Homepage warm-up for %s failed: %s", domain, error)
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
          await asyncio.sleep(random.uniform(0.3, 0.8))
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

        # Small random pre-navigation delay to vary timing.
        await asyncio.sleep(random.uniform(0.3, 1.0))

        response = await page.goto(
          url,
          wait_until=wait_until,
          timeout=self._timeout_ms,
        )

        status_code = response.status if response else 0

        with contextlib.suppress(Exception):
          await page.wait_for_load_state("networkidle", timeout=8000)

        await self._simulate_human_behavior(page)
        await self._wait_for_datadome(page)
        await self._dismiss_cookie_consent(page)

        # Post-navigation settle time.
        await asyncio.sleep(random.uniform(0.5, 1.5))

        # DataDome's JS can close or navigate the page during the
        # challenge wait. Treat a dead page as a block signal.
        if page.is_closed():
          raise TransportBlocked(
            f"Page closed during DataDome handling for {url}",
            url=url,
            status_code=403,
          )

        html = await page.content()
        elapsed = time.monotonic() - start_time
        final_url = page.url

        if status_code in (403, 429, 503) and page.url == url:
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
        error_str = str(error)
        if "timeout" in error_name.lower() or "Timeout" in error_str:
          raise TransportTimeout(
            f"Browser timeout after {elapsed:.1f}s for {url}",
            url=url,
          ) from error
        # TargetClosedError means the page/context was destroyed
        # (e.g. by DataDome redirect). Surface as TransportBlocked
        # so the router can try the fallback transport.
        if "TargetClosedError" in error_name or "Target closed" in error_str:
          raise TransportBlocked(
            f"Browser page closed (likely anti-bot) for {url}",
            url=url,
            status_code=403,
          ) from error
        raise TransportError(
          f"Browser error for {url}: {error}",
          url=url,
        ) from error
      finally:
        with contextlib.suppress(Exception):
          if not page.is_closed():
            await page.close()
