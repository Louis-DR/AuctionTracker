"""Watch loop: monitors active listings and updates their status.

The watcher maintains a priority queue of listings ordered by their
next check time. On each tick, it pops due listings, fetches them,
processes the result (detect extensions, status changes, etc.), and
reschedules them.

Key design decisions:
- The scheduler (pure logic) is separated from the watcher (I/O).
- Each fetch is independent — one failure does not block others.
- Terminal listings are removed from the queue, not re-checked.
- Extension detection only happens for FULL strategy websites.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from sqlalchemy.orm import Session

from auction_tracker.config import AppConfig, MonitoringStrategy
from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.models import Listing, ListingStatus
from auction_tracker.database.repository import Repository
from auction_tracker.orchestrator.ingest import Ingest
from auction_tracker.orchestrator.scheduler import (
  CheckQueue,
  Phase,
  Scheduler,
  TrackedListing,
)
from auction_tracker.orchestrator.utils import fetch_and_parse_listing
from auction_tracker.parsing.base import ParserBlocked, ParserRegistry
from auction_tracker.transport.base import TransportError
from auction_tracker.transport.router import TransportRouter

logger = logging.getLogger(__name__)


@dataclass
class WatchStats:
  """Statistics from a watch cycle."""
  checks_performed: int = 0
  listings_updated: int = 0
  listings_completed: int = 0
  extensions_detected: int = 0
  errors: int = 0


class Watcher:
  """Monitors active listings using a priority-queue scheduler.

  The watcher is designed to run as a long-lived async loop::

      watcher = Watcher(config, db, router, repository)
      await watcher.load_active_listings()
      await watcher.run_forever()

  Or for a single pass (useful for CLI)::

      stats = await watcher.run_once()
  """

  def __init__(
    self,
    config: AppConfig,
    database: DatabaseEngine,
    router: TransportRouter,
    repository: Repository,
    metrics=None,
    live=None,
    converter=None,
  ) -> None:
    self._config = config
    self._database = database
    self._router = router
    self._repo = repository
    self._metrics = metrics
    self._live = live
    self._ingest = Ingest(repository, converter=converter)
    self._scheduler = Scheduler(config.scheduler)
    self._queue = CheckQueue()

  @property
  def queue_size(self) -> int:
    return len(self._queue)

  def load_active_listings(self, session: Session) -> int:
    """Load all non-terminal listings from the database into the
    check queue. Returns the number of listings loaded.
    """
    listings = self._repo.get_active_listings(session)
    count = 0
    for listing in listings:
      self._enqueue_listing(listing)
      count += 1
    logger.info("Loaded %d active listings into watch queue", count)
    return count

  def enqueue_listing(self, listing: Listing) -> None:
    """Add a single listing to the watch queue (e.g. after discovery)."""
    self._enqueue_listing(listing)

  def _enqueue_listing(self, listing: Listing) -> None:
    """Build a TrackedListing from a database Listing and schedule it."""
    website_name = listing.website.name
    website_config = self._config.website(website_name)

    if website_config.historical_only:
      return

    tracked = TrackedListing(
      listing_id=listing.id,
      website_name=website_name,
      external_id=listing.external_id,
      url=listing.url,
      strategy=website_config.monitoring_strategy,
      end_time=listing.end_time.timestamp() if listing.end_time else None,
      last_fetched_at=listing.last_checked_at.timestamp() if listing.last_checked_at else 0.0,
      is_terminal=listing.is_terminal,
    )

    schedule = self._scheduler.compute_next_check(tracked)
    tracked.next_check_at = schedule.next_check_at
    tracked.phase = schedule.phase

    self._queue.add_or_update(tracked)

  async def run_once(self) -> WatchStats:
    """Process all listings that are currently due for a check.

    Due listings are sorted by urgency before processing: IMMINENT
    listings (ending within minutes) are checked before ENDING,
    APPROACHING, and ROUTINE ones. Within the same phase, those with
    the earliest end time go first.

    Returns statistics about what happened.
    """
    stats = WatchStats()
    now = time.time()
    due_listings = self._queue.pop_due(now)

    if not due_listings:
      return stats

    # Higher number = lower urgency.
    phase_priority = {
      Phase.IMMINENT: 0,
      Phase.ENDING: 1,
      Phase.APPROACHING: 2,
      Phase.ROUTINE: 3,
      Phase.DONE: 4,
    }
    due_listings.sort(key=lambda tracked: (
      phase_priority.get(tracked.phase, 99),
      tracked.end_time if tracked.end_time is not None else float("inf"),
    ))

    logger.info("Processing %d due listings", len(due_listings))

    if self._live:
      self._live.watch_started(len(due_listings), self.queue_size + len(due_listings))

    for check_index, tracked in enumerate(due_listings):
      if self._live:
        self._live.watch_progress(check_index, tracked.external_id, tracked.website_name)
      try:
        await self._check_listing(tracked, stats)
        if self._metrics:
          self._metrics.watch_check(tracked.website_name, tracked.external_id)
        if self._live:
          self._live.increment("watch_checks")
      except Exception as error:
        stats.errors += 1
        tracked.consecutive_failures += 1
        logger.error(
          "Error checking listing %s [%s]: %s",
          tracked.external_id, tracked.website_name, error,
          exc_info=True,
        )
        if self._metrics:
          self._metrics.error("watch", str(error), website_name=tracked.website_name)
        if self._live:
          self._live.increment("errors")

      # Reschedule (even after errors, with backoff).
      schedule = self._scheduler.compute_next_check(tracked)
      tracked.next_check_at = schedule.next_check_at
      tracked.phase = schedule.phase

      if tracked.phase == Phase.DONE:
        self._queue.remove(tracked.listing_id)
        stats.listings_completed += 1
        if self._live:
          self._live.increment("watch_completed")
      else:
        self._queue.add_or_update(tracked)
        if stats.listings_updated > 0 and self._live:
          self._live.increment("watch_updated")

    if self._live:
      self._live.watch_idle(self.queue_size)

    logger.info(
      "Watch cycle: %d checked, %d updated, %d completed, %d errors",
      stats.checks_performed, stats.listings_updated,
      stats.listings_completed, stats.errors,
    )
    if self._metrics and stats.checks_performed > 0:
      self._metrics.watch_cycle(
        stats.checks_performed, stats.listings_updated,
        stats.listings_completed, stats.extensions_detected,
        stats.errors,
      )
    return stats

  async def _check_listing(
    self,
    tracked: TrackedListing,
    stats: WatchStats,
  ) -> None:
    """Fetch and process a single listing."""
    website_name = tracked.website_name
    if not ParserRegistry.has(website_name):
      logger.warning("No parser for %s, removing from queue", website_name)
      tracked.is_terminal = True
      return

    parser = ParserRegistry.get(website_name)

    try:
      _result, scraped = await fetch_and_parse_listing(
        self._router, parser, website_name, tracked.url,
      )
    except (TransportError, ParserBlocked):
      tracked.consecutive_failures += 1
      raise
    stats.checks_performed += 1
    tracked.consecutive_failures = 0
    tracked.last_fetched_at = time.time()

    with self._database.session() as session:
      listing, _ = self._ingest.ingest_listing(
        session, _website_id_for(session, self._repo, website_name), scraped,
      )
      stats.listings_updated += 1

      self._process_scraped_result(tracked, listing, scraped, stats)

      session.commit()

  def _process_scraped_result(
    self,
    tracked: TrackedListing,
    listing: Listing,
    scraped,
    stats: WatchStats,
  ) -> None:
    """Update tracking state based on the scrape result."""
    # Detect end time changes (extensions for FULL strategy).
    if scraped.end_time is not None:
      new_end = scraped.end_time.timestamp()
      if (
        tracked.end_time is not None
        and new_end > tracked.end_time + 60
        and tracked.strategy == MonitoringStrategy.FULL
      ):
          tracked.extension_count += 1
          stats.extensions_detected += 1
          logger.info(
            "Extension detected for %s [%s] (#%d)",
            tracked.external_id, tracked.website_name,
            tracked.extension_count,
          )
      tracked.end_time = new_end

    # Detect terminal status.
    if listing.is_terminal:
      tracked.is_terminal = True
      logger.info(
        "Listing %s [%s] reached terminal status: %s",
        tracked.external_id, tracked.website_name,
        listing.status.value,
      )

    # Detect stuck ENDING phase for full/snapshot strategies.
    if (
      tracked.phase == Phase.ENDING
      and not tracked.is_terminal
      and tracked.end_time is not None
    ):
      tracked.post_end_checks += 1
      time_since_end = time.time() - tracked.end_time

      max_wait = self._scheduler.ending_max_wait(tracked.strategy)
      if time_since_end > max_wait:
        # See WebsiteWorker._process_watch_result for the same fix:
        # default to SOLD when the auction received any bids, only
        # fall back to UNSOLD for genuinely zero-bid closures.
        guessed_status = (
          ListingStatus.SOLD
          if (listing.bid_count or 0) > 0
          else ListingStatus.UNSOLD
        )
        logger.warning(
          "Listing %s [%s] stuck in ENDING for %.0fs (bids=%d) — marking %s",
          tracked.external_id, tracked.website_name, time_since_end,
          listing.bid_count or 0, guessed_status.value,
        )
        tracked.is_terminal = True
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, tracked.listing_id, guessed_status,
          )
          session.commit()

  async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
    """Run the watch loop continuously until stopped.

    Sleeps between cycles, waking when the next listing is due.
    Pass an asyncio.Event to stop gracefully.
    """
    if stop_event is None:
      stop_event = asyncio.Event()

    logger.info("Watch loop started with %d listings", self.queue_size)

    while not stop_event.is_set():
      await self.run_once()

      next_time = self._queue.peek_next_time()
      if next_time is None:
        logger.info("No more listings to watch, waiting for new work")
        if self._live:
          self._live.watch_sleeping(self.queue_size, 60.0)
        with contextlib.suppress(TimeoutError):
          await asyncio.wait_for(stop_event.wait(), timeout=60.0)
        continue

      sleep_duration = max(0.0, min(next_time - time.time(), 60.0))
      if sleep_duration > 0:
        if self._live:
          self._live.watch_sleeping(self.queue_size, sleep_duration)
        with contextlib.suppress(TimeoutError):
          await asyncio.wait_for(stop_event.wait(), timeout=sleep_duration)

    logger.info("Watch loop stopped")

  def get_queue_status(self) -> list[dict]:
    """Return a snapshot of the queue for diagnostics."""
    entries = self._queue.get_all()
    now = time.time()
    return [
      {
        "listing_id": entry.listing_id,
        "website": entry.website_name,
        "external_id": entry.external_id,
        "strategy": entry.strategy.value,
        "phase": entry.phase.value,
        "next_check_in": max(0.0, entry.next_check_at - now),
        "consecutive_failures": entry.consecutive_failures,
        "extensions": entry.extension_count,
      }
      for entry in sorted(entries, key=lambda entry: entry.next_check_at)
    ]


def _website_id_for(session: Session, repo: Repository, name: str) -> int:
  """Look up a website's database ID by name."""
  website = repo.get_website_by_name(session, name)
  if website is None:
    raise ValueError(f"Website '{name}' not found in database")
  return website.id
