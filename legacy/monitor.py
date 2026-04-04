"""Monitoring loop – periodically polls active listings and searches.

This module ties together the scraper, repository, and image downloader
into a single workflow that can be run from the CLI or scheduled
externally.

Requests to different websites are **interleaved** so that we make
progress on all sites concurrently instead of exhausting one website's
queue before touching the next.  A shared :class:`WebsiteThrottle`
enforces per-site delays while allowing other sites to be served
during the wait.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Sequence, Tuple

from pathlib import Path

from auction_tracker.config import AppConfig
from auction_tracker.currency import CurrencyConverter
from auction_tracker.database.engine import session_scope, thread_safe_session_scope
from auction_tracker.database.models import ListingStatus
from auction_tracker.database.repository import (
  add_listing_image,
  add_listing_search_source,
  get_active_listings,
  get_active_search_queries,
  get_or_create_listing,
  get_or_create_seller,
  get_or_create_website,
  mark_listing_sold,
  mark_listing_unsold,
  record_bid,
  set_listing_attribute,
  take_price_snapshot,
  update_listing_price,
)
from auction_tracker.images.downloader import ImageDownloader
from auction_tracker.scrapers.base import BaseScraper, ScrapedListing
from auction_tracker.scrapers.registry import ScraperRegistry
from auction_tracker.classifier import get_classifier

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Per-website throttle
# ------------------------------------------------------------------

class WebsiteThrottle:
  """Enforces a minimum delay between operations on the same website.

  Unlike the per-instance ``_rate_limit`` in each scraper (which
  throttles individual HTTP requests), this throttle works at the
  *operation* level – ensuring that consecutive search/fetch calls
  to the same site are spaced apart, and providing the information
  needed to interleave work across multiple sites.

  All public methods are **thread-safe**: an internal lock protects
  the shared timing dictionary so that multiple per-website worker
  threads can call these methods concurrently.
  """

  def __init__(self, delay: float = 2.0) -> None:
    self._delay = delay
    self._last_times: Dict[str, float] = {}
    self._lock = threading.Lock()

  def time_until_ready(self, website: str) -> float:
    """Return seconds until *website* can be requested (0 = ready now)."""
    with self._lock:
      last = self._last_times.get(website)
    if last is None:
      return 0.0
    return max(0.0, self._delay - (time.time() - last))

  def is_ready(self, website: str) -> bool:
    """Return ``True`` if *website* can be requested right now."""
    return self.time_until_ready(website) <= 0.0

  def wait_if_needed(self, website: str) -> float:
    """Block until *website* is ready.  Returns how long we waited."""
    remaining = self.time_until_ready(website)
    if remaining > 0:
      time.sleep(remaining)
    return remaining

  def mark_used(self, website: str) -> None:
    """Record that a request to *website* just completed."""
    with self._lock:
      self._last_times[website] = time.time()

  def pick_ready_from(self, candidates: Sequence[str]) -> Tuple[Optional[str], float]:
    """Pick the first ready website from *candidates*.

    Returns ``(website_name, 0.0)`` if one is ready now, or
    ``(None, shortest_wait)`` if all are throttled.
    """
    with self._lock:
      shortest_wait = float("inf")
      for name in candidates:
        last = self._last_times.get(name)
        if last is None:
          return name, 0.0
        wait = max(0.0, self._delay - (time.time() - last))
        if wait <= 0.0:
          return name, 0.0
        shortest_wait = min(shortest_wait, wait)
      return None, shortest_wait


def _fmt_price(price: Optional[Decimal], currency: str = "EUR") -> str:
  """Format a price for log messages, or return '–' when missing."""
  if price is None:
    return "–"
  return f"{price} {currency}"


def _fmt_duration(seconds: float) -> str:
  """Human-friendly duration string."""
  seconds = abs(seconds)
  if seconds < 60:
    return f"{seconds:.0f}s"
  if seconds < 3600:
    return f"{seconds / 60:.0f}m"
  if seconds < 86400:
    return f"{seconds / 3600:.1f}h"
  return f"{seconds / 86400:.1f}d"


# ------------------------------------------------------------------
# Listing status FSM
# ------------------------------------------------------------------

# Terminal states — once a listing reaches one of these, the scraper
# alone cannot move it out.  UNSOLD is included because a genuinely
# relisted item gets a new external_id on the platform.
_TERMINAL_STATUSES = frozenset({
  ListingStatus.SOLD,
  ListingStatus.UNSOLD,
  ListingStatus.CANCELLED,
})


def _apply_status_transition(
  session,
  listing,
  scraped_status: ListingStatus,
  final_price: Optional[Decimal],
  final_price_eur: Optional[Decimal],
) -> None:
  """Apply a status transition respecting the listing status FSM.

  Rules
  -----
  * ``UNKNOWN`` from the scraper means "no information" — the
    listing's current status is left unchanged.
  * ``SOLD`` and ``CANCELLED`` are **terminal** — the scraper cannot
    override them.  (CANCELLED is set by the vision classifier and is
    permanent.  SOLD means the item changed hands.)
  * ``UNSOLD`` is **terminal** — a genuinely relisted item gets a new
    ``external_id`` on the platform, creating a separate listing.
  * All other forward transitions are allowed (``UNKNOWN`` → anything,
    ``UPCOMING`` → ``ACTIVE``, ``ACTIVE`` → ``SOLD``/``UNSOLD``, etc.)
  """
  current = listing.status
  new = scraped_status

  # The scraper has no status information — leave the listing as-is.
  if new == ListingStatus.UNKNOWN:
    return

  # No change needed.
  if current == new:
    return

  # Terminal states cannot be overridden by the scraper.
  if current in _TERMINAL_STATUSES:
    return

  # Apply the transition.
  if new == ListingStatus.SOLD:
    effective_price = final_price or listing.current_price
    if effective_price is not None:
      mark_listing_sold(session, listing, final_price=effective_price)
    else:
      listing.status = ListingStatus.SOLD
    listing.final_price_eur = final_price_eur
  elif new == ListingStatus.UNSOLD:
    mark_listing_unsold(session, listing)
  else:
    # ACTIVE, UPCOMING, CANCELLED (e.g. listing removed by seller),
    # RELISTED.
    listing.status = new
    logger.debug(
      "  Status transition: %s → %s for listing #%d",
      current.value, new.value, listing.id,
    )


class Monitor:
  """Orchestrates scraping, database updates, and image downloads."""

  def __init__(self, config: AppConfig, scrapers: Optional[list[str]] = None) -> None:
    self.config = config
    self.scrapers = scrapers
    self.image_downloader = ImageDownloader(
      config.images,
      user_agent=config.scraping.user_agent,
    )
    # Currency converter for EUR-equivalent prices.
    cache_path = config.database.resolved_path.parent / "exchange_rates.json"
    self.currency_converter = CurrencyConverter(cache_path=cache_path)

    # Per-website throttle for interleaved scheduling.
    self.throttle = WebsiteThrottle(delay=config.scraping.request_delay)

    # Cache scraper instances so we reuse them (preserving their
    # internal per-request rate limiter across operations).
    self._scraper_cache: Dict[str, BaseScraper] = {}

    # CLIP classifier for filtering non-writing-instrument listings.
    # May be None if classification is disabled in config.
    self._classifier = get_classifier(config.classifier)

  def get_scraper(self, name: str) -> BaseScraper:
    """Return a (cached) scraper instance for *name*."""
    if name not in self._scraper_cache:
      self._scraper_cache[name] = ScraperRegistry.create(
        name, self.config.scraping,
      )
    return self._scraper_cache[name]

  # -----------------------------------------------------------------
  # High-level workflows
  # -----------------------------------------------------------------

  def run_search(self, scraper: BaseScraper, query: str, *, category: Optional[str] = None) -> int:
    """Execute a search and ingest every result into the database.

    Returns the number of newly created listings.
    """
    logger.info("Searching %s for '%s' …", scraper.website_name, query)
    t0 = time.time()
    results = scraper.search(query, category=category)
    elapsed = time.time() - t0
    logger.info(
      "  Found %d result(s) on %s (%.1fs).",
      len(results), scraper.website_name, elapsed,
    )

    created_count = 0
    with thread_safe_session_scope() as session:
      website = get_or_create_website(
        session,
        name=scraper.website_name,
        base_url=scraper.website_base_url,
      )

      for result in results:
        _listing, was_created = get_or_create_listing(
          session,
          website_id=website.id,
          external_id=result.external_id,
          defaults={
            "url": result.url,
            "title": result.title,
            "currency": result.currency,
            "current_price": result.current_price,
            "listing_type": result.listing_type,
            "status": ListingStatus.UNKNOWN,
            "end_time": result.end_time,
            "is_fully_fetched": False,
          },
        )

        # Record which search query discovered this listing.
        add_listing_search_source(
          session,
          listing_id=_listing.id,
          search_query_text=query,
        )

        if was_created:
          created_count += 1
          logger.info(
            "    + NEW  %s — %s",
            result.external_id,
            (result.title[:70] if result.title else "?"),
          )
        else:
          logger.debug(
            "    = known %s — %s",
            result.external_id,
            (result.title[:70] if result.title else "?"),
          )

    logger.info(
      "  Search '%s': %d new, %d already known.",
      query, created_count, len(results) - created_count,
    )
    return created_count

  def search_and_fetch(
    self,
    scraper: BaseScraper,
    query: str,
    *,
    category: Optional[str] = None,
    on_progress: Optional[callable] = None,
  ) -> Tuple[int, int]:
    """Search and immediately fetch full details for every result.

    This combines ``run_search`` and ``ingest_listing`` in a single
    workflow -- useful when you want complete listing data right away.

    Parameters
    ----------
    scraper:
      The scraper to use.
    query:
      The search text.
    category:
      Optional category filter passed to the scraper.
    on_progress:
      Optional callback ``(index, total, url, success)`` called after
      each listing is fetched.

    Returns
    -------
    (search_count, fetch_count):
      Number of search results found and number of listings
      successfully fetched with full details.  The difference
      ``search_count - fetch_count`` gives the number of failures.
    """
    logger.info("Search+fetch on %s for '%s' …", scraper.website_name, query)
    results = scraper.search(query, category=category)
    total = len(results)
    logger.info("  Found %d result(s) – fetching details …", total)

    # Store basic search results first.
    with session_scope() as session:
      website = get_or_create_website(
        session,
        name=scraper.website_name,
        base_url=scraper.website_base_url,
      )
      for result in results:
        _listing, _was_created = get_or_create_listing(
          session,
          website_id=website.id,
          external_id=result.external_id,
          defaults={
            "url": result.url,
            "title": result.title,
            "currency": result.currency,
            "current_price": result.current_price,
            "listing_type": result.listing_type,
            "status": ListingStatus.UNKNOWN,
            "end_time": result.end_time,
            "is_fully_fetched": False,
          },
        )

        # Record which search query discovered this listing.
        add_listing_search_source(
          session,
          listing_id=_listing.id,
          search_query_text=query,
        )

    # Now fetch full details for every result.
    website_name = scraper.website_name
    fetch_count = 0
    failed_count = 0
    t0 = time.time()
    for idx, result in enumerate(results, 1):
      self.throttle.wait_if_needed(website_name)
      try:
        self.ingest_listing(scraper, result.url)
        fetch_count += 1
        if on_progress:
          on_progress(idx, total, result.url, True)
      except Exception:
        failed_count += 1
        logger.exception("  Failed to fetch [%d/%d] %s", idx, total, result.url)
        if on_progress:
          on_progress(idx, total, result.url, False)
      self.throttle.mark_used(website_name)

    elapsed = time.time() - t0
    logger.info(
      "Search+fetch complete: %d found, %d fetched, %d failed in %s.",
      total, fetch_count, failed_count, _fmt_duration(elapsed),
    )
    return total, fetch_count

  def ingest_listing(self, scraper: BaseScraper, url_or_id: str) -> int:
    """Fetch a single listing's full details and store everything.

    Returns the database ID of the listing.
    """
    scraped = scraper.fetch_listing(url_or_id)
    return self._store_scraped_listing(scraper, scraped)

  def poll_active_listings(self, scraper: BaseScraper) -> Tuple[int, int]:
    """Re-fetch every active listing for this website and update prices.

    Returns a tuple of ``(updated_count, failed_count)``.
    """
    updated_count = 0
    failed_count = 0
    with session_scope() as session:
      website = get_or_create_website(
        session,
        name=scraper.website_name,
        base_url=scraper.website_base_url,
      )
      active = get_active_listings(session, website_id=website.id)
      total = len(active)
      logger.info(
        "Polling %d active listing(s) on %s …",
        total, scraper.website_name,
      )

    if total == 0:
      return 0, 0

    # Fetch outside the session to avoid long-held transactions.
    t0 = time.time()
    for idx, listing in enumerate(active, 1):
      try:
        t_item = time.time()
        scraped = scraper.fetch_listing(listing.url)
        self._store_scraped_listing(scraper, scraped)
        updated_count += 1
        elapsed_item = time.time() - t_item

        # Build a concise summary for this listing.
        price_str = _fmt_price(
          scraped.current_price or scraped.final_price,
          scraped.currency,
        )
        bid_str = f"{len(scraped.bids)} bid(s)" if scraped.bids else "no bids"
        status_str = scraped.status.value if scraped.status else "?"

        logger.info(
          "  [%d/%d] %-8s %s — %s, %s (%.1fs)",
          idx, total,
          status_str,
          (scraped.title[:55] if scraped.title else listing.external_id),
          price_str,
          bid_str,
          elapsed_item,
        )
      except Exception:
        failed_count += 1
        logger.exception(
          "  [%d/%d] FAILED  %s",
          idx, total, listing.url,
        )

    elapsed = time.time() - t0
    logger.info(
      "Poll complete: %d updated, %d failed out of %d (%.1fs total).",
      updated_count, failed_count, total, elapsed,
    )
    return updated_count, failed_count

  def run_all_searches(self) -> Tuple[int, int]:
    """Run every active search query with round-robin interleaving.

    Queries are grouped by website and processed in round-robin order
    so that all websites make progress concurrently instead of
    exhausting one website's queue before touching the next.

    Returns a tuple of ``(new_listing_count, failed_count)``.
    """
    with session_scope() as session:
      queries = get_active_search_queries(session)

    if not queries:
      logger.info("No saved searches to run.")
      return 0, 0

    # Group (query_text, category) by website name.
    from auction_tracker.database.models import Website as WebsiteModel
    website_queues: Dict[str, deque] = {}

    for query in queries:
      website_name = None
      if query.website_id is not None:
        with session_scope() as session:
          website = session.get(WebsiteModel, query.website_id)
          if website:
            website_name = website.name

      if website_name is None:
        # Run on every registered scraper (filtered by self.scrapers if set),
        # but skip scrapers that opt out of periodic discovery (e.g.
        # historical-only scrapers like Gazette Drouot).
        scrapers_to_use = self.scrapers if self.scrapers else ScraperRegistry.list_registered()
        for name in scrapers_to_use:
          try:
            scraper_instance = self.get_scraper(name)
            if scraper_instance.capabilities.exclude_from_discover:
              continue
          except Exception:
            pass  # will fail again at search time and be logged there
          website_queues.setdefault(name, deque()).append(
            (query.query_text, query.category),
          )
      else:
        # Check if this website is in the allowed list.
        if self.scrapers is None or website_name in self.scrapers:
          website_queues.setdefault(website_name, deque()).append(
            (query.query_text, query.category),
          )

    total_tasks = sum(len(queue) for queue in website_queues.values())
    logger.info(
      "Running %d search(es) across %d website(s) (interleaved) …",
      total_tasks, len(website_queues),
    )

    total = 0
    failed = 0
    website_order = list(website_queues.keys())

    # Round-robin: take one query from each website in turn.
    while any(website_queues.values()):
      for website_name in website_order:
        task_queue = website_queues.get(website_name)
        if not task_queue:
          continue

        query_text, category = task_queue.popleft()
        self.throttle.wait_if_needed(website_name)

        try:
          scraper = self.get_scraper(website_name)
          total += self.run_search(
            scraper, query_text, category=category,
          )
        except Exception:
          failed += 1
          logger.exception(
            "Search '%s' failed on %s", query_text, website_name,
          )

        self.throttle.mark_used(website_name)

    logger.info(
      "All searches done — %d new listing(s) total, %d failed.",
      total, failed,
    )
    return total, failed

  def poll_all_websites_interleaved(self) -> Tuple[int, int]:
    """Re-fetch every active listing across all websites, interleaved.

    Instead of processing all listings on website A then all on B,
    this method collects active listings per website and round-robins
    through them so every website makes progress concurrently.

    Returns a tuple of ``(updated_count, failed_count)``.
    """
    # Collect active listings grouped by website / scraper name.
    registered_scrapers = set(ScraperRegistry.list_registered())
    allowed_scrapers = set(self.scrapers) if self.scrapers else registered_scrapers
    website_queues: Dict[str, deque] = {}
    total_count = 0

    with session_scope() as session:
      active_listings = get_active_listings(session)

      for listing in active_listings:
        if not listing.website:
          continue
        scraper_name = listing.website.name.lower()
        if scraper_name not in registered_scrapers:
          scraper_name = scraper_name.replace(" ", "_")
          if scraper_name not in registered_scrapers:
            continue
        # Filter by allowed scrapers.
        if scraper_name not in allowed_scrapers:
          continue
        website_queues.setdefault(scraper_name, deque()).append(
          (listing.external_id, listing.url),
        )
        total_count += 1

    if total_count == 0:
      logger.info("No active listings to poll.")
      return 0, 0

    logger.info(
      "Polling %d active listing(s) across %d website(s) (interleaved) …",
      total_count, len(website_queues),
    )

    updated = 0
    failed = 0
    processed = 0
    website_order = list(website_queues.keys())
    t0 = time.time()

    # Round-robin: take one listing from each website in turn.
    while any(website_queues.values()):
      for scraper_name in website_order:
        task_queue = website_queues.get(scraper_name)
        if not task_queue:
          continue

        external_id, url = task_queue.popleft()
        processed += 1
        self.throttle.wait_if_needed(scraper_name)

        try:
          t_item = time.time()
          scraper = self.get_scraper(scraper_name)
          scraped = scraper.fetch_listing(url)
          self._store_scraped_listing(scraper, scraped)
          updated += 1
          elapsed_item = time.time() - t_item

          price_str = _fmt_price(
            scraped.current_price or scraped.final_price,
            scraped.currency,
          )
          bid_str = (
            f"{len(scraped.bids)} bid(s)" if scraped.bids else "no bids"
          )
          status_str = scraped.status.value if scraped.status else "?"

          logger.info(
            "  [%d/%d] %-12s %-8s %s — %s, %s (%.1fs)",
            processed, total_count,
            scraper_name,
            status_str,
            (scraped.title[:50] if scraped.title else external_id),
            price_str,
            bid_str,
            elapsed_item,
          )
        except Exception:
          failed += 1
          logger.exception(
            "  [%d/%d] %-12s FAILED  %s",
            processed, total_count, scraper_name, url,
          )

        self.throttle.mark_used(scraper_name)

    elapsed = time.time() - t0
    logger.info(
      "Poll complete: %d updated, %d failed out of %d (%.1fs total).",
      updated, failed, total_count, elapsed,
    )
    return updated, failed

  def snapshot_active_listings(self) -> int:
    """Take a price snapshot of every active listing across all websites."""
    count = 0
    with session_scope() as session:
      active = get_active_listings(session)
      for listing in active:
        price_eur = self._to_eur(
          listing.current_price, listing.currency,
        )
        snapshot = take_price_snapshot(
          session, listing, price_eur=price_eur,
        )
        if snapshot is not None:
          count += 1
    logger.info("Took %d price snapshot(s) of %d active listing(s).", count, len(active))
    return count

  def run_continuous(self) -> None:
    """Run the full monitoring loop indefinitely.

    Uses interleaved polling so all websites make progress
    concurrently.  Searches are also run with round-robin
    interleaving.
    """
    poll_interval = self.config.monitoring.poll_interval
    snapshot_interval = self.config.monitoring.snapshot_interval

    last_search_time = 0.0
    last_snapshot_time = 0.0
    cycle = 0

    logger.info(
      "Starting continuous monitoring loop "
      "(poll every %s, snapshots every %s).",
      _fmt_duration(poll_interval),
      _fmt_duration(snapshot_interval),
    )

    while True:
      cycle += 1
      cycle_failures = 0
      now = time.time()
      logger.info("━━━ Cycle %d ━━━", cycle)

      # Run searches periodically (using poll interval as cadence).
      if now - last_search_time >= poll_interval:
        try:
          _new, search_failures = self.run_all_searches()
          cycle_failures += search_failures
        except Exception:
          cycle_failures += 1
          logger.exception("Error during search run.")
        last_search_time = time.time()

      # Poll all active listings with interleaving across websites.
      try:
        _updated, poll_failures = self.poll_all_websites_interleaved()
        cycle_failures += poll_failures
      except Exception:
        cycle_failures += 1
        logger.exception("Error during interleaved poll.")

      # Snapshot prices.
      now = time.time()
      if now - last_snapshot_time >= snapshot_interval:
        try:
          self.snapshot_active_listings()
        except Exception:
          cycle_failures += 1
          logger.exception("Error during snapshots.")
        last_snapshot_time = time.time()

      if cycle_failures > 0:
        logger.warning(
          "Cycle %d complete with %d error(s). "
          "Sleeping %s until next cycle …",
          cycle, cycle_failures, _fmt_duration(poll_interval),
        )
      else:
        logger.info(
          "Cycle %d complete. Sleeping %s until next cycle …",
          cycle, _fmt_duration(poll_interval),
        )
      time.sleep(poll_interval)

  # -----------------------------------------------------------------
  # Internal
  # -----------------------------------------------------------------

  def _store_scraped_listing(self, scraper: BaseScraper, scraped: ScrapedListing) -> int:
    """Persist a ``ScrapedListing`` and all its related objects.

    Uses :func:`thread_safe_session_scope` so this method can be
    called from multiple per-website threads concurrently.

    Returns the database ID of the listing.
    """
    # Pre-compute EUR conversions for the listing prices.
    # Use the listing's end_time (or now) as the reference date for
    # the exchange rate.
    conversion_date = scraped.end_time or datetime.now(timezone.utc)
    current_price_eur = self._to_eur(
      scraped.current_price, scraped.currency, conversion_date,
    )
    final_price_eur = self._to_eur(
      scraped.final_price, scraped.currency, conversion_date,
    )

    with thread_safe_session_scope() as session:
      website = get_or_create_website(
        session,
        name=scraper.website_name,
        base_url=scraper.website_base_url,
      )

      # Seller
      seller_id = None
      if scraped.seller is not None:
        seller = get_or_create_seller(
          session,
          website_id=website.id,
          external_id=scraped.seller.external_id,
          username=scraped.seller.username,
          display_name=scraped.seller.display_name,
          country=scraped.seller.country,
          rating=scraped.seller.rating,
          feedback_count=scraped.seller.feedback_count,
          profile_url=scraped.seller.profile_url,
        )
        seller_id = seller.id

      # Listing
      listing, was_created = get_or_create_listing(
        session,
        website_id=website.id,
        external_id=scraped.external_id,
        defaults={
          "url": scraped.url,
          "title": scraped.title,
          "description": scraped.description,
          "listing_type": scraped.listing_type,
          "condition": scraped.condition,
          "currency": scraped.currency,
          "starting_price": scraped.starting_price,
          "reserve_price": scraped.reserve_price,
          "estimate_low": scraped.estimate_low,
          "estimate_high": scraped.estimate_high,
          "buy_now_price": scraped.buy_now_price,
          "current_price": scraped.current_price,
          "final_price": scraped.final_price,
          "current_price_eur": current_price_eur,
          "final_price_eur": final_price_eur,
          "buyer_premium_percent": scraped.buyer_premium_percent,
          "buyer_premium_fixed": scraped.buyer_premium_fixed,
          "shipping_cost": scraped.shipping_cost,
          "shipping_from_country": scraped.shipping_from_country,
          "ships_internationally": scraped.ships_internationally,
          "start_time": scraped.start_time,
          "end_time": scraped.end_time,
          "status": scraped.status,
          "bid_count": scraped.bid_count,
          "watcher_count": scraped.watcher_count,
          "view_count": scraped.view_count,
          "lot_number": scraped.lot_number,
          "auction_house_name": scraped.auction_house_name,
          "sale_name": scraped.sale_name,
          "seller_id": seller_id,
        },
      )

      # Mark the listing as fully fetched regardless of whether it was
      # just created or already existed (a search may have created it
      # with is_fully_fetched=False).
      listing.is_fully_fetched = True

      if was_created:
        listing.last_checked_at = datetime.now(timezone.utc)
        logger.debug(
          "  Created listing #%d: %s",
          listing.id, scraped.title[:60] if scraped.title else "?",
        )
      else:
        # Update mutable fields.
        if scraped.current_price is not None:
          update_listing_price(
            session, listing,
            price=scraped.current_price,
            bid_count=scraped.bid_count,
          )
          listing.current_price_eur = current_price_eur
        # Always update bid_count even if price hasn't changed.
        elif scraped.bid_count is not None:
          listing.bid_count = scraped.bid_count

        # Update listing status using proper FSM transition rules.
        # Terminal states (SOLD, UNSOLD, CANCELLED) are never overridden.
        # UNKNOWN from the scraper is ignored (no information).

        # KEY FIX: Explicitly check if the listing was rejected by classifier.
        # Although CANCELLED is terminal, some scrapers might return valid data (like SOLD)
        # for a rejected item. We must prioritize the rejection.
        is_rejected = False
        if listing.status == ListingStatus.CANCELLED:
             for attr in listing.attributes:
                 if attr.attribute_name == "rejected_by_classifier" and attr.attribute_value == "true":
                     is_rejected = True
                     break

        if not is_rejected:
            _apply_status_transition(
              session, listing, scraped.status,
              scraped.final_price, final_price_eur,
            )
        else:
            logger.debug(
                "  Skipping status update for #%d (REJECTED by classifier). Kept as CANCELLED.",
                listing.id,
            )
        listing.last_checked_at = datetime.now(timezone.utc)

        # Always-update fields: these change on every fetch and must
        # always reflect the latest scraped value.
        if scraped.end_time is not None:
          listing.end_time = scraped.end_time
        if scraped.start_time is not None:
          listing.start_time = scraped.start_time
        if scraped.watcher_count is not None:
          listing.watcher_count = scraped.watcher_count
        if scraped.view_count is not None:
          listing.view_count = scraped.view_count

        # Back-fill fields that were missing on the initial insert
        # (e.g. the listing was first seen from a search, then fully
        # fetched from the detail page).
        # For the condition enum, also replace UNKNOWN with a real value.
        from auction_tracker.database.models import ItemCondition
        if (
          scraped.condition != ItemCondition.UNKNOWN
          and listing.condition == ItemCondition.UNKNOWN
        ):
          listing.condition = scraped.condition

        _backfill_fields = {
          "description": scraped.description,
          "starting_price": scraped.starting_price,
          "reserve_price": scraped.reserve_price,
          "estimate_low": scraped.estimate_low,
          "estimate_high": scraped.estimate_high,
          "buy_now_price": scraped.buy_now_price,
          "buyer_premium_percent": scraped.buyer_premium_percent,
          "buyer_premium_fixed": scraped.buyer_premium_fixed,
          "shipping_cost": scraped.shipping_cost,
          "shipping_from_country": scraped.shipping_from_country,
          "ships_internationally": scraped.ships_internationally,
          "lot_number": scraped.lot_number,
          "auction_house_name": scraped.auction_house_name,
          "sale_name": scraped.sale_name,
          "seller_id": seller_id,
        }
        backfilled = []
        for field_name, new_value in _backfill_fields.items():
          if new_value is not None and getattr(listing, field_name, None) is None:
            setattr(listing, field_name, new_value)
            backfilled.append(field_name)
        if backfilled:
          logger.debug(
            "  Back-filled %d field(s) on listing #%d: %s",
            len(backfilled), listing.id, ", ".join(backfilled),
          )
        session.flush()

      # Bids
      new_bid_count = 0
      for bid in scraped.bids:
        bid_amount_eur = self._to_eur(
          bid.amount, bid.currency, bid.bid_time,
        )
        bid_record = record_bid(
          session,
          listing_id=listing.id,
          amount=bid.amount,
          currency=bid.currency,
          amount_eur=bid_amount_eur,
          bid_time=bid.bid_time,
          bidder_username=bid.bidder_username,
          bidder_country=bid.bidder_country,
          is_automatic=bid.is_automatic,
        )
        # record_bid returns the existing record if it's a duplicate;
        # we can detect new bids by checking if the id was just assigned.
        if bid_record and bid_record.id and session.is_modified(bid_record):
          new_bid_count += 1
      if new_bid_count > 0:
        logger.debug(
          "  Recorded %d new bid(s) for listing #%d (total scraped: %d).",
          new_bid_count, listing.id, len(scraped.bids),
        )

      # Images – download up to N images for classification, store the rest
      # as remote URLs only.
      #
      # If classification is enabled, we run CLIP on the first N downloaded
      # images and reject the listing if none show a "writing instrument"
      # above the configured threshold.

      classifier_config = self.config.classifier
      images_to_classify = (
        classifier_config.images_to_classify if self._classifier else 1
      )
      # Track downloaded images as (full_path, image_record) tuples.
      downloaded_images: list[tuple[str, object]] = []

      for image in scraped.images:
        image_record = add_listing_image(
          session,
          listing_id=listing.id,
          source_url=image.source_url,
          position=image.position,
        )

        # Download images up to the classification limit.
        should_download = (
          image.position < images_to_classify
          and image_record.local_path is None
        )
        if should_download:
          result = self.image_downloader.download(
            image.source_url,
            listing.id,
            position=image.position,
          )
          if result is not None:
            image_record.local_path = result["local_path"]
            image_record.width = result["width"]
            image_record.height = result["height"]
            image_record.file_size_bytes = result["file_size_bytes"]
            image_record.downloaded_at = result["downloaded_at"]

            # Build absolute path for classifier.
            full_path = str(
              self.config.images.resolved_directory / result["local_path"]
            )
            downloaded_images.append((full_path, image_record))

      # Run classification if enabled and we have images.
      downloaded_paths = [path for path, _ in downloaded_images]
      if self._classifier and downloaded_paths:
        is_relevant, max_score, top_classes = self._classifier.classify_listing_images(
          downloaded_paths,
          threshold=classifier_config.writing_instrument_threshold,
        )

        # Save top 3 classes for all listings (useful for analysis).
        for i, (label, score) in enumerate(top_classes, 1):
          set_listing_attribute(
            session,
            listing_id=listing.id,
            attribute_name=f"classifier_top_{i}",
            attribute_value=f"{label}: {score:.1%}",
          )

        # Save writing instrument score for all listings.
        set_listing_attribute(
          session,
          listing_id=listing.id,
          attribute_name="classifier_max_score",
          attribute_value=f"{max_score:.4f}",
        )

        if not is_relevant:
          # Mark listing as rejected by classifier.
          logger.info(
            "  REJECTED by classifier: %s (max score: %.1f%%, threshold: %.1f%%)",
            scraped.title[:60] if scraped.title else scraped.external_id,
            max_score * 100,
            classifier_config.writing_instrument_threshold * 100,
          )
          listing.status = ListingStatus.CANCELLED
          set_listing_attribute(
            session,
            listing_id=listing.id,
            attribute_name="rejected_by_classifier",
            attribute_value="true",
          )

          # Delete downloaded images and clear local_path so UI uses remote URLs.
          for path, img_record in downloaded_images:
            try:
              Path(path).unlink(missing_ok=True)
            except Exception:
              logger.warning("Failed to delete image: %s", path)
            img_record.local_path = None

          session.flush()

          # Still return the listing ID (it's saved, just marked rejected).
          listing_id = listing.id
          self.currency_converter.save_cache()
          return listing_id
        else:
          logger.debug(
            "  Passed classification: %s (max score: %.1f%%)",
            scraped.title[:50] if scraped.title else scraped.external_id,
            max_score * 100,
          )

      # Attributes
      for attribute_name, attribute_value in scraped.attributes.items():
        set_listing_attribute(
          session,
          listing_id=listing.id,
          attribute_name=attribute_name,
          attribute_value=attribute_value,
        )

      # Always take a price snapshot so we build up a price history
      # over time.  For sites without bid history (eBay, Yahoo Japan,
      # LeBonCoin, etc.) this is the main way to track prices.
      take_price_snapshot(
        session, listing, price_eur=current_price_eur,
      )

      listing_id = listing.id

    # Persist the exchange rate cache after each listing so we don't
    # lose fetched rates on crash.
    self.currency_converter.save_cache()

    return listing_id

  def _to_eur(
    self,
    amount: Optional[Decimal],
    currency: str,
    at_date: Optional[datetime] = None,
  ) -> Optional[Decimal]:
    """Convert an amount to EUR, returning None if not possible."""
    if amount is None:
      return None
    if currency.upper() == "EUR":
      return amount
    try:
      return self.currency_converter.to_eur(amount, currency, at_date)
    except Exception as error:
      logger.debug("Currency conversion failed: %s", error)
      return None
