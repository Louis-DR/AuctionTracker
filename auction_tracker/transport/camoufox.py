"""Camoufox browser transport for anti-bot-heavy websites.

Camoufox is a Firefox fork that masks fingerprints at the C++ level,
making it far more effective against DataDome, Cloudflare, and similar
systems than Playwright Chromium with JS-level stealth patches.

Key features used here:
- ``humanize=True``: built-in realistic mouse movement and scrolling
- ``geoip=True``: automatic timezone/locale/geo matching
- Persistent browser profile to accumulate cookies across sessions
- Non-headless by default (strongest anti-detection posture)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import platform
import random
import time
from pathlib import Path
from urllib.parse import urlparse

from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
  TransportTimeout,
)

logger = logging.getLogger(__name__)

_DATADOME_MARKERS = (
  "geo.captcha-delivery.com",
  "dd.js",
)

_OS_MAP = {"darwin": "macos", "linux": "linux", "windows": "windows"}


class CamoufoxTransport(Transport):
  """Async Camoufox-based browser transport.

  Launches a Camoufox (Firefox) browser with anti-fingerprinting and
  human-like behavior built in. Designed for websites with aggressive
  anti-bot systems like DataDome (LeBonCoin, etc.).
  """

  def __init__(
    self,
    timeout: float = 30.0,
    request_delay: float = 3.0,
    profile_directory: Path | None = None,
  ) -> None:
    self._timeout_ms = int(timeout * 1000)
    self._request_delay = request_delay
    self._profile_directory = profile_directory or Path("data/browser_profiles")
    self._camoufox = None
    self._context = None
    self._page = None
    self._rate_limit_lock = asyncio.Lock()
    self._last_request_time: float = 0.0
    self._warmed_up_domains: set[str] = set()

  @property
  def name(self) -> str:
    return "camoufox"

  async def start(self) -> None:
    try:
      from camoufox.async_api import AsyncCamoufox
    except ImportError as error:
      raise ImportError(
        "Camoufox is required for this transport. "
        "Install it with: pip install 'camoufox[geoip]'"
      ) from error

    target_os = _OS_MAP.get(platform.system().lower(), "windows")
    profile_path = self._profile_directory / "camoufox"
    profile_path.mkdir(parents=True, exist_ok=True)

    self._camoufox = AsyncCamoufox(
      persistent_context=True,
      user_data_dir=str(profile_path),
      headless=False,
      humanize=True,
      os=target_os,
      locale="fr-FR",
      geoip=True,
      enable_cache=True,
    )
    self._context = await self._camoufox.__aenter__()
    self._context.set_default_timeout(self._timeout_ms)

    if self._context.pages:
      self._page = self._context.pages[0]
    else:
      self._page = await self._context.new_page()

    logger.info(
      "Camoufox transport started (profile=%s, os=%s)",
      profile_path, target_os,
    )

  async def stop(self) -> None:
    if self._camoufox is not None:
      with contextlib.suppress(Exception):
        await self._camoufox.__aexit__(None, None, None)
      self._camoufox = None
      self._context = None
      self._page = None
    self._warmed_up_domains.clear()
    logger.info("Camoufox transport stopped")

  async def _enforce_rate_limit(self) -> None:
    async with self._rate_limit_lock:
      now = time.monotonic()
      elapsed = now - self._last_request_time
      if elapsed < self._request_delay:
        await asyncio.sleep(self._request_delay - elapsed)
      self._last_request_time = time.monotonic()

  async def _ensure_page(self):
    """Return a working page, creating one if the existing page crashed."""
    if self._page is None or self._page.is_closed():
      self._page = await self._context.new_page()
    return self._page

  @staticmethod
  def _extract_domain(url: str) -> str | None:
    with contextlib.suppress(Exception):
      host = urlparse(url).hostname or ""
      if host.startswith("www."):
        host = host[4:]
      return host if host else None
    return None

  async def _warm_up_domain(self, page, domain: str) -> None:
    """Visit the homepage to establish cookies, simulate human browsing."""
    if domain in self._warmed_up_domains:
      return

    homepage = f"https://www.{domain}/"
    logger.info("Warming up Camoufox session for %s", domain)
    try:
      await page.goto(homepage, wait_until="domcontentloaded", timeout=20_000)
      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=8000)

      # Let the page settle and Camoufox's humanize do its thing.
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
    """Supplement Camoufox's built-in humanize with explicit interactions.

    While Camoufox's ``humanize=True`` handles cursor movement
    patterns at a low level, explicit mouse.move and scroll calls
    generate the DOM events that DataDome's JS listener needs to see.
    """
    try:
      await asyncio.sleep(random.uniform(0.3, 0.8))

      for _ in range(random.randint(2, 4)):
        target_x = random.randint(100, 900)
        target_y = random.randint(100, 500)
        await page.mouse.move(target_x, target_y)
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
      )
    except Exception:
      pass

  async def _dismiss_cookie_consent(self, page) -> None:
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

  @staticmethod
  async def _is_datadome_challenge(page) -> bool:
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

  async def _wait_for_datadome(self, page, max_wait: float = 20.0) -> None:
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

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._context is None:
      await self.start()

    warm_up = kwargs.get("warm_up", True)

    await self._enforce_rate_limit()

    page = await self._ensure_page()
    start_time = time.monotonic()
    try:
      if warm_up:
        domain = self._extract_domain(url)
        if domain:
          await self._warm_up_domain(page, domain)

      # Small random pre-navigation delay.
      await asyncio.sleep(random.uniform(0.3, 1.0))

      await page.goto(
        url,
        wait_until="domcontentloaded",
        timeout=self._timeout_ms,
      )

      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=8000)

      await self._simulate_human_behavior(page)
      await self._wait_for_datadome(page)
      await self._dismiss_cookie_consent(page)

      # Post-navigation settle time.
      await asyncio.sleep(random.uniform(0.5, 1.5))

      html = await page.content()
      elapsed = time.monotonic() - start_time
      final_url = page.url

      # Only raise blocked if the page is still a tiny challenge page.
      if len(html) < 5000 and await self._is_datadome_challenge(page):
        raise TransportBlocked(
          f"Camoufox blocked by DataDome on {url}",
          url=url,
          status_code=403,
        )

      logger.debug(
        "Camoufox fetched %s (%d bytes, %.1fs)",
        url, len(html), elapsed,
      )
      return FetchResult(
        html=html,
        url=url,
        status_code=200,
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
          f"Camoufox timeout after {elapsed:.1f}s for {url}",
          url=url,
        ) from error
      raise TransportError(
        f"Camoufox error for {url}: {error}",
        url=url,
      ) from error
