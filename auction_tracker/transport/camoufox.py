"""Camoufox browser transport for anti-bot-heavy websites.

Camoufox is a Firefox fork that masks fingerprints at the C++ level,
making it far more effective against DataDome, Cloudflare, and similar
systems than Playwright Chromium with JS-level stealth patches.

Key features used here:
- ``humanize=True``: built-in realistic mouse movement and scrolling
- ``geoip=True``: automatic timezone/locale/geo matching
- ``block_webrtc=True``: prevents WebRTC IP leaks
- ``disable_coop=True``: allows clicking DataDome captcha iframes
- Persistent browser profile to accumulate cookies across sessions
- Non-headless by default (strongest anti-detection posture)
- Firefox lock file cleanup to prevent stale locks from corrupting
  the profile

Concurrency model: a **single page** is shared across all workers.
An ``asyncio.Lock`` serialises all ``fetch()`` calls so only one
navigation happens at a time. This is the only reliable model on
Windows because:

- Camoufox deadlocks when multiple pages are created or used
  concurrently (https://github.com/daijro/camoufox/issues/279).
- Firefox freezes background windows even with occlusion-tracking
  prefs disabled (https://github.com/daijro/camoufox/issues/418).

The lock is held for the entire duration of each fetch (including
warm-up, navigation, human simulation, and content extraction).
A hard ``asyncio`` timeout wraps each fetch to prevent a frozen
browser from blocking the lock forever.
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

# Stale Firefox lock files that prevent profile reuse after a crash.
_FIREFOX_LOCK_FILES = ("parent.lock", "lock", ".parentlock")

# Hard ceiling on a single fetch, including warm-up, navigation,
# human-behaviour simulation, and DataDome wait. If the browser is
# frozen, the asyncio timeout fires and the page is recycled.
_HARD_FETCH_TIMEOUT = 120.0

# After this many seconds in a fetch, log a stall warning.
_STALL_WARNING_THRESHOLD = 45.0

# Firefox prefs that prevent the browser from freezing when its
# window is occluded or unfocused on Windows.
_ANTI_THROTTLE_PREFS: dict[str, object] = {
  "widget.windows.window_occlusion_tracking.enabled": False,
  "dom.timeout.enable_budget_timer_throttling": False,
  "dom.min_background_timeout_value": 4,
  "dom.min_background_timeout_value_without_budget_throttling": 4,
}


def _clean_firefox_locks(profile_path: Path) -> None:
  """Remove stale Firefox lock files from a profile directory.

  When Camoufox (or Firefox) crashes or is killed without proper
  shutdown, it leaves lock files behind. These prevent subsequent
  launches from reusing the persistent profile.
  """
  if not profile_path.exists():
    return
  for lock_name in _FIREFOX_LOCK_FILES:
    lock_file = profile_path / lock_name
    if not lock_file.exists():
      continue
    for attempt in range(5):
      try:
        lock_file.unlink()
        logger.info("Removed stale Firefox lock file: %s", lock_file)
        break
      except OSError:
        if attempt < 4:
          time.sleep(1)
        else:
          logger.warning(
            "Could not remove %s after 5 attempts — profile may fail to open",
            lock_file,
          )


class CamoufoxTransport(Transport):
  """Async Camoufox-based browser transport.

  Launches a single Camoufox (Firefox) browser with a single page.
  All workers share this one page sequentially via an asyncio lock.
  """

  def __init__(
    self,
    timeout: float = 30.0,
    request_delay: float = 3.0,
    max_pages: int = 4,
    profile_directory: Path | None = None,
  ) -> None:
    self._timeout_ms = int(timeout * 1000)
    self._request_delay = request_delay
    self._profile_directory = profile_directory or Path("data/browser_profiles")
    self._camoufox = None
    self._context = None
    self._page = None
    self._fetch_lock = asyncio.Lock()
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

    _clean_firefox_locks(profile_path)

    self._camoufox = AsyncCamoufox(
      persistent_context=True,
      user_data_dir=str(profile_path),
      headless=False,
      humanize=True,
      os=target_os,
      locale="fr-FR",
      geoip=True,
      enable_cache=True,
      block_webrtc=True,
      disable_coop=True,
      i_know_what_im_doing=True,
      firefox_user_prefs=_ANTI_THROTTLE_PREFS,
    )
    self._context = await self._camoufox.__aenter__()
    self._context.set_default_timeout(self._timeout_ms)

    # Persistent context always opens with one default page — use it.
    if self._context.pages:
      self._page = self._context.pages[0]
    else:
      self._page = await self._context.new_page()

    # Close any extra pages the persistent context may have restored.
    for extra_page in self._context.pages:
      if extra_page is not self._page:
        with contextlib.suppress(Exception):
          await extra_page.close()

    logger.info(
      "Camoufox transport started (profile=%s, os=%s, single-page serial mode)",
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

  async def _ensure_page(self):
    """Return the working page, creating a replacement if it crashed."""
    if self._page is None or self._page.is_closed():
      logger.warning("Camoufox page was closed, creating a replacement")
      self._page = await self._context.new_page()
    return self._page

  async def _enforce_rate_limit(self) -> None:
    now = time.monotonic()
    elapsed = now - self._last_request_time
    if elapsed < self._request_delay:
      await asyncio.sleep(self._request_delay - elapsed)
    self._last_request_time = time.monotonic()

  @staticmethod
  def _extract_domain(url: str) -> str | None:
    with contextlib.suppress(Exception):
      host = urlparse(url).hostname or ""
      if host.startswith("www."):
        host = host[4:]
      return host if host else None
    return None

  async def _warm_up_domain(self, page, domain: str) -> None:
    """Visit the homepage to establish cookies, simulate human browsing.

    Only runs once per domain per transport lifetime.
    """
    if domain in self._warmed_up_domains:
      return

    homepage = f"https://www.{domain}/"
    logger.info("Warming up Camoufox session for %s", domain)
    try:
      await page.goto(homepage, wait_until="domcontentloaded", timeout=30_000)
      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=10_000)

      await asyncio.sleep(random.uniform(1.5, 3.0))
      await self._simulate_human_behavior(page)
      await self._dismiss_cookie_consent(page)
      await asyncio.sleep(random.uniform(0.5, 1.5))

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

  async def _wait_for_datadome(self, page, max_wait: float = 30.0) -> None:
    """Wait for a DataDome challenge to resolve.

    DataDome sometimes presents a slider captcha that the user must
    solve manually (when running headful). The 30 s window gives
    enough time for manual intervention in headful mode.
    """
    if not await self._is_datadome_challenge(page):
      return

    logger.info("DataDome challenge detected, waiting up to %.0fs for resolution...", max_wait)
    start = time.monotonic()
    while time.monotonic() - start < max_wait:
      await asyncio.sleep(2.0)
      if not await self._is_datadome_challenge(page):
        elapsed = time.monotonic() - start
        logger.info("DataDome challenge resolved after %.1fs", elapsed)
        return

    logger.warning("DataDome challenge did not resolve within %.0fs", max_wait)

  async def _do_fetch(self, url: str, warm_up: bool) -> FetchResult:
    """Core fetch logic. Must be called while holding _fetch_lock."""
    await self._enforce_rate_limit()

    page = await self._ensure_page()
    start_time = time.monotonic()

    if warm_up:
      domain = self._extract_domain(url)
      if domain:
        await self._warm_up_domain(page, domain)

    await asyncio.sleep(random.uniform(0.5, 1.5))

    # Intercept JSON API responses issued by the page's JavaScript via
    # XHR / fetch.  SPAs like Vinted load all data dynamically after
    # page load, so page.content() alone returns only the HTML shell.
    # page.route() is synchronous from Playwright's point of view: the
    # handler MUST resolve before the browser receives the response, so
    # there is no race condition with page.on("response") async tasks.
    import json as _json
    _captured_api: dict[str, str] = {}

    async def _route_api(route) -> None:
      url_r = route.request.url
      try:
        response = await route.fetch()
        with contextlib.suppress(Exception):
          if len(_captured_api) < 20:
            ct = (response.headers or {}).get("content-type", "")
            if "json" in ct:
              body = await response.body()
              _captured_api[url_r] = body.decode("utf-8", errors="replace")
        await route.fulfill(response=response)
      except Exception:
        with contextlib.suppress(Exception):
          await route.continue_()

    await page.route("**/api/**", _route_api)
    try:
      logger.debug("Camoufox navigating to %s", url)
      await page.goto(
        url,
        wait_until="domcontentloaded",
        timeout=self._timeout_ms,
      )

      with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=10_000)

      elapsed = time.monotonic() - start_time
      if elapsed > _STALL_WARNING_THRESHOLD:
        logger.warning(
          "Camoufox fetch for %s has been running for %.0fs (navigation phase)",
          url, elapsed,
        )

      await self._simulate_human_behavior(page)
      await self._wait_for_datadome(page)
      await self._dismiss_cookie_consent(page)

      await asyncio.sleep(random.uniform(0.5, 2.0))

      if page.is_closed():
        raise TransportBlocked(
          f"Page closed during DataDome handling for {url}",
          url=url,
          status_code=403,
        )

      html = await page.content()
      elapsed = time.monotonic() - start_time
      final_url = page.url

      if len(html) < 5000 and await self._is_datadome_challenge(page):
        raise TransportBlocked(
          f"Camoufox blocked by DataDome on {url}",
          url=url,
          status_code=403,
        )

      # Inject captured API responses as a parseable script tag so that
      # SPA parsers (e.g. Vinted) can extract the XHR data they need.
      if _captured_api:
        payload = _json.dumps(_captured_api)
        inject = (
          f'<script id="__CAMOUFOX_CAPTURED_API__"'
          f' type="application/json">{payload}</script>'
        )
        if "</body>" in html:
          html = html.replace("</body>", inject + "\n</body>", 1)
        else:
          html += "\n" + inject
        logger.debug(
          "Camoufox injected %d captured API response(s) for %s",
          len(_captured_api), url,
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

    finally:
      with contextlib.suppress(Exception):
        await page.unroute("**/api/**", _route_api)

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._context is None:
      raise RuntimeError(
        "CamoufoxTransport.fetch() called before start(). "
        "The router must call start() before handing out the transport."
      )

    warm_up = kwargs.get("warm_up", True)

    async with self._fetch_lock:
      try:
        return await asyncio.wait_for(
          self._do_fetch(url, warm_up),
          timeout=_HARD_FETCH_TIMEOUT,
        )

      except asyncio.TimeoutError:
        logger.error(
          "Camoufox hard timeout after %.0fs for %s — browser may be frozen",
          _HARD_FETCH_TIMEOUT, url,
        )
        raise TransportTimeout(
          f"Camoufox hard timeout after {_HARD_FETCH_TIMEOUT:.0f}s for {url} "
          "(browser likely frozen/suspended)",
          url=url,
        )

      except TransportBlocked:
        raise
      except Exception as error:
        error_name = type(error).__name__
        error_str = str(error)
        if "timeout" in error_name.lower() or "Timeout" in error_str:
          raise TransportTimeout(
            f"Camoufox timeout for {url}: {error}",
            url=url,
          ) from error
        if "TargetClosedError" in error_name or "Target closed" in error_str:
          raise TransportBlocked(
            f"Camoufox page closed (likely anti-bot) for {url}",
            url=url,
            status_code=403,
          ) from error
        raise TransportError(
          f"Camoufox error for {url}: {error}",
          url=url,
        ) from error
