"""Abstract base class for website scrapers.

Each supported website gets its own scraper subclass.  The base class
defines a uniform interface so the monitoring loop and CLI can drive
any scraper without knowing its internals.
"""

from __future__ import annotations

import logging
import random
import time
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, Sequence

import requests

from auction_tracker.config import ScrapingConfig
from auction_tracker.database.models import (
  ItemCondition,
  ListingStatus,
  ListingType,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Data-transfer objects returned by scrapers
# ------------------------------------------------------------------

@dataclass
class ScraperCapabilities:
  """Declares what a scraper can do.

  The monitoring loop uses this to decide which methods to call.

  ``monitoring_strategy`` tells the smart monitor how to schedule
  checks for listings from this website:

  * ``"full"`` – aggressive polling with extension detection
    (Catawiki: bids near the close extend the auction).
  * ``"snapshot"`` – periodic price snapshots during the auction
    plus aggressive end-time polling, but **no** extension handling
    (eBay: fixed end time).  Bid history is fetched when accessible.
  * ``"post_auction"`` – no polling during the auction; just fetch
    once for metadata, then check after the auction ends to record
    the final price (Drouot, Invaluable, LiveAuctioneers, Interenchères).
  """
  can_search: bool = True
  can_fetch_listing: bool = True
  can_fetch_bids: bool = False
  can_fetch_seller: bool = False
  has_bid_history: bool = False
  has_watcher_count: bool = False
  has_view_count: bool = False
  has_buy_now: bool = False
  has_estimates: bool = False
  has_reserve_price: bool = False
  has_lot_numbers: bool = False
  has_auction_house_info: bool = False
  can_search_history: bool = False
  monitoring_strategy: str = "full"
  exclude_from_discover: bool = False   # True = skip in run_all_searches
  is_historical_only: bool = False      # True = skip in smart monitor watch loop


@dataclass
class ScrapedImage:
  """An image URL discovered by the scraper."""
  source_url: str
  position: int = 0


@dataclass
class ScrapedSeller:
  """Seller information extracted from a listing page."""
  external_id: str
  username: str
  display_name: Optional[str] = None
  country: Optional[str] = None
  rating: Optional[float] = None
  feedback_count: Optional[int] = None
  member_since: Optional[str] = None
  profile_url: Optional[str] = None


@dataclass
class ScrapedBid:
  """A single bid extracted from a listing's bid history."""
  amount: Decimal
  currency: str
  bid_time: datetime
  bidder_username: Optional[str] = None
  bidder_country: Optional[str] = None
  is_automatic: bool = False


@dataclass
class ScrapedListing:
  """All data the scraper could extract for a single listing.

  Fields left as ``None`` simply were not available on the source page.
  """
  external_id: str
  url: str
  title: str

  description: Optional[str] = None
  listing_type: ListingType = ListingType.AUCTION
  condition: ItemCondition = ItemCondition.UNKNOWN
  currency: str = "EUR"

  starting_price: Optional[Decimal] = None
  reserve_price: Optional[Decimal] = None
  estimate_low: Optional[Decimal] = None
  estimate_high: Optional[Decimal] = None
  buy_now_price: Optional[Decimal] = None
  current_price: Optional[Decimal] = None
  final_price: Optional[Decimal] = None

  buyer_premium_percent: Optional[Decimal] = None
  buyer_premium_fixed: Optional[Decimal] = None

  shipping_cost: Optional[Decimal] = None
  shipping_from_country: Optional[str] = None
  ships_internationally: Optional[bool] = None

  start_time: Optional[datetime] = None
  end_time: Optional[datetime] = None
  status: ListingStatus = ListingStatus.UNKNOWN

  bid_count: int = 0
  watcher_count: Optional[int] = None
  view_count: Optional[int] = None

  lot_number: Optional[str] = None
  auction_house_name: Optional[str] = None
  sale_name: Optional[str] = None
  sale_date: Optional[str] = None

  seller: Optional[ScrapedSeller] = None
  images: list[ScrapedImage] = field(default_factory=list)
  bids: list[ScrapedBid] = field(default_factory=list)

  # Free-form attributes (brand, model, nib size, …).
  attributes: dict[str, str] = field(default_factory=dict)


@dataclass
class SearchResult:
  """A single entry from a search results page."""
  external_id: str
  url: str
  title: str
  current_price: Optional[Decimal] = None
  currency: str = "EUR"
  image_url: Optional[str] = None
  end_time: Optional[datetime] = None
  listing_type: ListingType = ListingType.AUCTION
  status: ListingStatus = ListingStatus.UNKNOWN


# ------------------------------------------------------------------
# Abstract base scraper
# ------------------------------------------------------------------

class BaseScraper(ABC):
  """Base class that every website scraper must inherit from.

  Subclasses **must** implement at least :meth:`search` and
  :meth:`fetch_listing`.  They may also override :meth:`fetch_bids`
  and :meth:`fetch_seller` if the website exposes that data
  separately from the listing page.
  """

  def __init__(self, config: ScrapingConfig) -> None:
    self.config = config

    if config.use_impersonation:
      try:
        from curl_cffi import requests as cffi_requests
        self._session = cffi_requests.Session(impersonate="chrome120")
        logger.info("Using curl_cffi session (impersonate='chrome120')")
      except ImportError:
        logger.warning("curl_cffi not installed, falling back to standard requests")
        self._session = requests.Session()
    else:
      self._session = requests.Session()
      self._session.headers["User-Agent"] = config.user_agent

    if config.cookies:
      self._session.cookies.update(config.cookies)
    self._last_request_time: Optional[float] = None

    # Browser state (lazily initialised by _ensure_browser).
    self._camoufox = None
    self._browser_context = None
    self._browser_page = None
    self._browser_executor = None

    # Register atexit cleanup so the browser is properly shut down
    # when the Python process exits, preventing EPIPE from the
    # Playwright Node.js driver.
    import atexit
    import weakref
    _ref = weakref.ref(self)
    def _cleanup_browser():
      scraper = _ref()
      if scraper is not None:
        try:
          scraper.stop_browser()
        except Exception:
          pass
    atexit.register(_cleanup_browser)

  # ------------------------------------------------------------------
  # Metadata that subclasses should override
  # ------------------------------------------------------------------

  @property
  @abstractmethod
  def website_name(self) -> str:
    """Human-readable name (e.g. ``"Catawiki"``)."""
    ...

  @property
  @abstractmethod
  def website_base_url(self) -> str:
    """Base URL (e.g. ``"https://www.catawiki.com"``)."""
    ...

  @property
  def capabilities(self) -> ScraperCapabilities:
    """What this scraper supports.  Override to adjust."""
    return ScraperCapabilities()

  # ------------------------------------------------------------------
  # Abstract methods
  # ------------------------------------------------------------------

  @abstractmethod
  def search(
    self,
    query: str,
    *,
    category: Optional[str] = None,
    page: int = 1,
  ) -> Sequence[SearchResult]:
    """Run a keyword search and return summarised results."""
    ...

  def search_past(
    self,
    query: str,
    *,
    limit: int = 50,
  ) -> Sequence[SearchResult]:
    """Run a search for historical/past results."""
    raise NotImplementedError(
      f"{self.website_name} scraper does not support historical search."
    )

  @abstractmethod
  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch the full details of a single listing."""
    ...

  def fetch_bids(self, url_or_external_id: str) -> Sequence[ScrapedBid]:
    """Fetch the bid history of a listing (optional)."""
    raise NotImplementedError(
      f"{self.website_name} scraper does not support bid history fetching."
    )

  def fetch_seller(self, seller_external_id: str) -> ScrapedSeller:
    """Fetch standalone seller information (optional)."""
    raise NotImplementedError(
      f"{self.website_name} scraper does not support standalone seller fetching."
    )

  # ------------------------------------------------------------------
  # HTTP helpers
  # ------------------------------------------------------------------

  def _get(self, url: str, **kwargs) -> requests.Response:
    """Perform a rate-limited GET request."""
    self._rate_limit()
    logger.debug("GET %s", url)
    response = self._session.get(url, timeout=self.config.timeout, **kwargs)
    response.raise_for_status()
    return response

  def _post(self, url: str, **kwargs) -> requests.Response:
    """Perform a rate-limited POST request."""
    self._rate_limit()
    logger.debug("POST %s", url)
    response = self._session.post(url, timeout=self.config.timeout, **kwargs)
    response.raise_for_status()
    return response

  def _rate_limit(self) -> None:
    """Sleep if the minimum delay between requests has not elapsed."""
    if self._last_request_time is not None:
      elapsed = time.time() - self._last_request_time
      remaining = self.config.request_delay - elapsed
      if remaining > 0:
        time.sleep(remaining)
    self._last_request_time = time.time()

  # ------------------------------------------------------------------
  # Shared browser support (Camoufox)
  #
  # Playwright/Camoufox uses greenlets that are pinned to a single
  # thread.  The smart monitor dispatches fetch calls from a
  # ThreadPoolExecutor, so we proxy ALL Playwright operations through
  # a dedicated single-thread executor per scraper instance.  This
  # guarantees the browser is always used from the thread that created
  # it.
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_key(name: str) -> str:
    """Normalise a name for config-key matching (lowercase, no accents, underscores)."""
    import unicodedata
    # NFD decomposes accented chars, then we strip combining marks.
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_only.lower().replace(" ", "_")

  @property
  def _browser_enabled(self) -> bool:
    """Return True if browser mode is enabled for this scraper."""
    scraper_key = self._normalise_key(self.website_name)
    mapping = self.config.per_scraper_browser
    if not mapping:
      return False
    # Try normalised match.
    for key, value in mapping.items():
      if self._normalise_key(key) == scraper_key:
        return bool(value)
    return False

  @property
  def _browser_locale(self) -> Optional[str]:
    """Locale for the browser fingerprint.  Override in subclasses.

    Return ``None`` to let Camoufox auto-detect from GeoIP.
    French scrapers should return ``"fr-FR"``.
    """
    return None

  def _ensure_browser_thread(self) -> None:
    """Ensure the dedicated browser thread executor exists.

    The browser thread MUST NOT have an asyncio event loop attached,
    otherwise Playwright's Sync API refuses to run with:
      "It looks like you are using Playwright Sync API inside the asyncio loop."
    We use a thread initializer to clear any inherited asyncio state.
    """
    if self._browser_executor is None:
      import asyncio
      from concurrent.futures import ThreadPoolExecutor

      def _init_browser_thread():
        """Clear any asyncio loop from this thread so Playwright works."""
        try:
          asyncio.set_event_loop(None)
        except Exception:
          pass

      self._browser_executor = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix=f"browser-{self._normalise_key(self.website_name)}",
        initializer=_init_browser_thread,
      )

  def _ensure_browser(self) -> None:
    """Lazily start a persistent Camoufox browser session.

    MUST be called from the dedicated browser thread (via _run_on_browser_thread).
    The browser is created once and reused for the lifetime of the scraper.
    """
    if self._browser_context is not None:
      return

    if not self.config.browser_profile:
      raise RuntimeError(
        f"{self.website_name}: browser mode is enabled but "
        f"'browser_profile' is not set in scraping config."
      )

    import pathlib
    import platform
    scraper_key = self._normalise_key(self.website_name)
    profile_path = pathlib.Path(self.config.browser_profile) / scraper_key

    # Remove stale Firefox lock files.
    if profile_path.exists():
      import time as _time
      for lock_name in ("parent.lock", "lock", ".parentlock"):
        lock_file = profile_path / lock_name
        if lock_file.exists():
          for attempt in range(5):
            try:
              lock_file.unlink()
              logger.info("Removed lock file: %s", lock_file)
              break
            except OSError:
              if attempt < 4:
                _time.sleep(1)
              else:
                logger.warning("Could not remove %s after 5 attempts", lock_file)

    logger.info(
      "Starting Camoufox browser for %s (profile: %s)",
      self.website_name, profile_path,
    )

    try:
      from camoufox.sync_api import Camoufox
    except ImportError:
      raise ImportError(
        "Browser mode requires camoufox. "
        "Please run: pip install 'camoufox[geoip]'"
      )

    os_map = {"windows": "windows", "linux": "linux", "darwin": "macos"}
    target_os = os_map.get(platform.system().lower(), "windows")

    self._camoufox = Camoufox(
      user_data_dir=str(profile_path),
      headless=False,
      humanize=True,
      os=target_os,
      locale=self._browser_locale,
      geoip=True,
      enable_cache=True,
      persistent_context=True,
    )
    try:
      # Playwright's Sync API refuses to start if it detects an asyncio
      # event loop on the current thread.  The executor thread can inherit
      # one from the parent context, so clear it before entering.
      import asyncio as _aio
      try:
        _aio.set_event_loop(None)
      except Exception:
        pass
      self._browser_context = self._camoufox.__enter__()
    except Exception:
      self._camoufox = None
      raise

    if self._browser_context.pages:
      self._browser_page = self._browser_context.pages[0]
    else:
      self._browser_page = self._browser_context.new_page()

    # Safety: cap any single Playwright call at 30s so nothing hangs forever.
    self._browser_context.set_default_timeout(30_000)

  def _run_on_browser_thread(self, fn, *args, **kwargs):
    """Submit *fn* to the dedicated browser thread and wait for the result."""
    self._ensure_browser_thread()
    future = self._browser_executor.submit(fn, *args, **kwargs)
    try:
      return future.result(timeout=self.config.browser_thread_timeout)
    except TimeoutError:
      logger.warning(
        "%s: browser operation timed out after %.0fs",
        self.website_name, self.config.browser_thread_timeout,
      )
      raise

  def _get_html_via_browser(self, url: str) -> str:
    """Navigate the persistent browser to *url* and return the page HTML.

    Thread-safe: proxies all Playwright calls through the dedicated
    browser thread.
    """
    return self._run_on_browser_thread(self._browser_fetch, url)

  def _browser_fetch(self, url: str) -> str:
    """Internal: perform the actual browser fetch (runs on browser thread).

    Reuses the same page across calls (just navigates to a new URL).
    If the page has crashed or been closed, opens a fresh one.
    """
    self._ensure_browser()

    page = self._browser_page
    if not page or page.is_closed():
      page = self._browser_context.new_page()
      self._browser_page = page

    logger.debug("Browser navigating to %s …", url)

    nav_timeout = int(self.config.browser_nav_timeout * 1000)
    pre_min, pre_max = self.config.browser_pre_nav_delay
    post_min, post_max = self.config.browser_post_nav_delay

    # Small random pre-navigation delay (varies to avoid patterns).
    time.sleep(random.uniform(pre_min, pre_max))

    try:
      page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
    except Exception as exc:
      # Page may have crashed — try once with a fresh page.
      if page.is_closed():
        logger.warning("Page crashed, opening a fresh one for %s", url)
        page = self._browser_context.new_page()
        self._browser_page = page
        page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
      else:
        logger.error("Browser navigation failed for %s: %s", url, exc)
        raise

    # Wait for dynamic content (short timeout, don't block on slow assets).
    try:
      page.wait_for_load_state(
        "networkidle",
        timeout=int(self.config.browser_idle_timeout * 1000),
      )
    except Exception:
      pass   # DOM is ready, proceed anyway.

    # Simulate realistic human behaviour (if enabled).
    if self.config.browser_human_behavior:
      self._browser_human_behavior(page)

    # Post-navigation delay for JS hydration.
    time.sleep(random.uniform(post_min, post_max))
    self._last_request_time = time.time()

    return page.content()

  def _browser_human_behavior(self, page) -> None:
    """Simulate realistic human-like interaction on the page.

    DataDome and similar systems analyse mouse movement patterns,
    scroll behaviour, and interaction timing.  This simulates a
    real user casually reading a page.
    """
    try:
      # 1. Initial pause (humans don't interact immediately).
      time.sleep(random.uniform(0.3, 1.0))

      # 2. Random mouse movements across the page (2-3 movements).
      for _ in range(random.randint(2, 3)):
        x = random.randint(100, 900)
        y = random.randint(100, 500)
        page.mouse.move(x, y)
        time.sleep(random.uniform(0.2, 0.5))

      # 3. Scroll down naturally (in steps, not all at once).
      total_scroll = random.randint(200, 600)
      steps = random.randint(2, 3)
      for _ in range(steps):
        scroll_amount = total_scroll // steps + random.randint(-30, 30)
        page.mouse.wheel(0, scroll_amount)
        time.sleep(random.uniform(0.2, 0.5))

      # 4. Slight scroll back up (mimics reading).
      if random.random() > 0.4:
        page.mouse.wheel(0, -random.randint(50, 150))
        time.sleep(random.uniform(0.1, 0.3))

      # 5. One more mouse move.
      page.mouse.move(
        random.randint(200, 700),
        random.randint(150, 400),
      )
    except Exception:
      pass  # Best-effort, don't crash on interaction failures.

  def stop_browser(self) -> None:
    """Stop the persistent browser session (cleanup)."""
    camoufox = self._camoufox
    if camoufox is None:
      return

    logger.info("Stopping browser session for %s.", self.website_name)

    self._camoufox = None
    self._browser_context = None
    self._browser_page = None

    try:
      camoufox.__exit__(None, None, None)
    except Exception:
      pass

    executor = self._browser_executor
    if executor:
      self._browser_executor = None
      try:
        executor.shutdown(wait=False, cancel_futures=True)
      except TypeError:
        executor.shutdown(wait=False)


