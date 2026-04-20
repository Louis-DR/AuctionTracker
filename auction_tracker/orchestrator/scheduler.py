"""Priority-queue based scheduler for listing checks.

The scheduler maintains a priority queue of (next_check_time, listing_id)
entries. It does not perform any fetching or database operations itself —
it only decides *when* each listing should be checked next, based on
the monitoring strategy and the listing's current state.

All times are POSIX timestamps (float seconds since epoch) so that
comparisons and arithmetic are simple.
"""

from __future__ import annotations

import heapq
import logging
import time
from dataclasses import dataclass
from enum import StrEnum

from auction_tracker.config import (
  MonitoringStrategy,
  SchedulerConfig,
)

logger = logging.getLogger(__name__)


class Phase(StrEnum):
  """Where a listing is in its monitoring lifecycle."""
  ROUTINE = "routine"
  APPROACHING = "approaching"
  IMMINENT = "imminent"
  ENDING = "ending"
  WAITING = "waiting"
  DONE = "done"


@dataclass
class TrackedListing:
  """In-memory state for a listing being monitored.

  This is separate from the database Listing model to keep the
  scheduler fast and memory-light. Only the fields needed for
  scheduling decisions are stored here.
  """
  listing_id: int
  website_name: str
  external_id: str
  url: str
  strategy: MonitoringStrategy

  end_time: float | None = None
  phase: Phase = Phase.ROUTINE
  next_check_at: float = 0.0
  last_fetched_at: float = 0.0
  consecutive_failures: int = 0
  post_end_checks: int = 0
  extension_count: int = 0
  is_terminal: bool = False

  # POSIX timestamp of when the listing was first published (website's
  # own publication date, or discovery time as a fallback).  Used by
  # the age-based watch-interval logic for open-ended listings (those
  # without a fixed end time, e.g. classified ads).
  published_at: float | None = None

  # Per-website age-based schedule: ordered list of
  # (max_age_seconds, interval_seconds) pairs.  The last entry's
  # max_age should be None (unlimited) to serve as a catch-all.
  # Populated from WebsiteConfig.age_watch_schedule at enqueue time.
  # Only consulted when end_time is None.
  age_watch_schedule: list[tuple[float | None, float]] | None = None

  def __lt__(self, other: TrackedListing) -> bool:
    """Comparison for the priority queue (earliest check first)."""
    return self.next_check_at < other.next_check_at


@dataclass
class CheckSchedule:
  """Result of a scheduling decision."""
  next_check_at: float
  phase: Phase


def _age_based_interval(
  age_watch_schedule: list[tuple[float | None, float]] | None,
  published_at: float | None,
  fallback: float,
  now: float,
) -> float:
  """Return the watch interval appropriate for a listing's current age.

  Walks the age-band list in order and returns the first band whose
  ``max_age`` is ``None`` (catch-all) or is >= the listing's age.
  Falls back to ``fallback`` when no schedule is configured or the
  listing's publication time is unknown.
  """
  if not age_watch_schedule or published_at is None:
    return fallback
  age = now - published_at
  for max_age, interval in age_watch_schedule:
    if max_age is None or age <= max_age:
      return interval
  # All bands exhausted without a catch-all — use the last band's interval.
  return age_watch_schedule[-1][1]


class Scheduler:
  """Decides when each listing should be checked next.

  The scheduler is deterministic: given the same state, it always
  produces the same schedule. This makes it easy to test.

  Usage::

      scheduler = Scheduler(config.scheduler)
      tracked = TrackedListing(...)
      schedule = scheduler.compute_next_check(tracked)
      tracked.next_check_at = schedule.next_check_at
      tracked.phase = schedule.phase
  """

  def __init__(self, config: SchedulerConfig) -> None:
    self._config = config

  def compute_next_check(
    self,
    tracked: TrackedListing,
    now: float | None = None,
  ) -> CheckSchedule:
    """Compute the next check time for a listing.

    Dispatches to the strategy-specific method based on the
    listing's monitoring strategy.
    """
    if now is None:
      now = time.time()

    if tracked.is_terminal:
      return CheckSchedule(next_check_at=float("inf"), phase=Phase.DONE)

    if tracked.consecutive_failures >= self._config.consecutive_failure_threshold:
      return CheckSchedule(
        next_check_at=now + self._config.failure_cooldown,
        phase=tracked.phase,
      )

    if tracked.strategy == MonitoringStrategy.FULL:
      return self._schedule_full(tracked, now)
    elif tracked.strategy == MonitoringStrategy.SNAPSHOT:
      return self._schedule_snapshot(tracked, now)
    elif tracked.strategy == MonitoringStrategy.POST_AUCTION:
      return self._schedule_post_auction(tracked, now)
    else:
      raise ValueError(f"Unknown strategy: {tracked.strategy}")

  def _schedule_full(self, tracked: TrackedListing, now: float) -> CheckSchedule:
    """Schedule for FULL strategy (e.g. Catawiki).

    Aggressive polling near auction end with extension detection.
    Phases tighten as end time approaches.
    """
    config = self._config.full

    if tracked.end_time is None:
      return CheckSchedule(
        next_check_at=now + self._config.daily_refresh_interval,
        phase=Phase.ROUTINE,
      )

    remaining = tracked.end_time - now

    if remaining <= 0:
      return CheckSchedule(
        next_check_at=now + config.ending_poll_interval,
        phase=Phase.ENDING,
      )

    if remaining <= config.imminent_threshold:
      return CheckSchedule(
        next_check_at=now + config.imminent_interval,
        phase=Phase.IMMINENT,
      )

    if remaining <= config.approaching_threshold:
      next_time = min(
        now + config.approaching_interval,
        tracked.end_time - config.imminent_threshold,
      )
      return CheckSchedule(
        next_check_at=next_time,
        phase=Phase.APPROACHING,
      )

    next_time = min(
      now + self._config.daily_refresh_interval,
      tracked.end_time - config.approaching_threshold,
    )
    return CheckSchedule(
      next_check_at=next_time,
      phase=Phase.ROUTINE,
    )

  def _schedule_snapshot(self, tracked: TrackedListing, now: float) -> CheckSchedule:
    """Schedule for SNAPSHOT strategy (e.g. eBay, Yahoo Japan, classifieds).

    Periodic snapshots that tighten near the end. No extension
    detection — if end time changes by more than a threshold, the
    schedule adjusts but does not treat it as an extension.

    For listings without a fixed end time (classified ads), the
    routine interval is taken from the per-website age-based schedule
    (``tracked.age_watch_schedule``) when configured, so that stale
    listings are checked less frequently than fresh ones.
    """
    config = self._config.snapshot

    if tracked.end_time is None:
      interval = _age_based_interval(
        tracked.age_watch_schedule,
        tracked.published_at,
        config.routine_interval,
        now,
      )
      return CheckSchedule(
        next_check_at=now + interval,
        phase=Phase.ROUTINE,
      )

    remaining = tracked.end_time - now

    if remaining <= 0:
      return CheckSchedule(
        next_check_at=now + config.ending_poll_interval,
        phase=Phase.ENDING,
      )

    if remaining <= config.imminent_threshold:
      return CheckSchedule(
        next_check_at=now + config.imminent_interval,
        phase=Phase.IMMINENT,
      )

    if remaining <= config.approaching_threshold:
      next_time = min(
        now + config.approaching_interval,
        tracked.end_time - config.imminent_threshold,
      )
      return CheckSchedule(
        next_check_at=next_time,
        phase=Phase.APPROACHING,
      )

    next_time = min(
      now + config.routine_interval,
      tracked.end_time - config.approaching_threshold,
    )
    return CheckSchedule(
      next_check_at=next_time,
      phase=Phase.ROUTINE,
    )

  def ending_max_wait(self, strategy: MonitoringStrategy) -> float:
    """Maximum seconds a listing may stay in ENDING before being marked UNSOLD."""
    if strategy == MonitoringStrategy.FULL:
      return self._config.full.ending_max_wait
    if strategy == MonitoringStrategy.SNAPSHOT:
      return self._config.snapshot.ending_max_wait
    if strategy == MonitoringStrategy.POST_AUCTION:
      return self._config.post_auction.max_wait
    return 3600.0

  def _schedule_post_auction(self, tracked: TrackedListing, now: float) -> CheckSchedule:
    """Schedule for POST_AUCTION strategy (e.g. Drouot, Invaluable).

    Don't poll during the auction — just check once before and then
    wait until after the end to look for results.
    """
    config = self._config.post_auction

    if tracked.end_time is None:
      return CheckSchedule(
        next_check_at=now + self._config.daily_refresh_interval,
        phase=Phase.ROUTINE,
      )

    remaining = tracked.end_time - now

    if remaining > 0:
      if tracked.last_fetched_at == 0:
        return CheckSchedule(next_check_at=now, phase=Phase.ROUTINE)
      return CheckSchedule(
        next_check_at=tracked.end_time + config.delay_after_end,
        phase=Phase.WAITING,
      )

    # Auction has ended — check for results.
    if tracked.last_fetched_at == 0:
      return CheckSchedule(next_check_at=now, phase=Phase.ENDING)

    if tracked.post_end_checks == 0:
      time_since_end = now - tracked.end_time
      if time_since_end < config.delay_after_end:
        return CheckSchedule(
          next_check_at=tracked.end_time + config.delay_after_end,
          phase=Phase.ENDING,
        )

    if tracked.post_end_checks >= config.max_recheck_count:
      return CheckSchedule(next_check_at=float("inf"), phase=Phase.DONE)

    return CheckSchedule(
      next_check_at=now + config.recheck_interval,
      phase=Phase.ENDING,
    )


class CheckQueue:
  """Priority queue of listings ordered by next check time.

  Thread-safe for the single-threaded async context we run in.
  Provides efficient peek/pop of the next due listing.
  """

  def __init__(self) -> None:
    self._heap: list[TrackedListing] = []
    self._entries: dict[int, TrackedListing] = {}

  def __len__(self) -> int:
    return len(self._entries)

  def __contains__(self, listing_id: int) -> bool:
    return listing_id in self._entries

  def add_or_update(self, tracked: TrackedListing) -> None:
    """Add a listing or update its schedule.

    If the listing is already in the queue, the old entry becomes
    stale and is lazily discarded on pop.
    """
    self._entries[tracked.listing_id] = tracked
    heapq.heappush(self._heap, tracked)

  def remove(self, listing_id: int) -> None:
    """Mark a listing as removed. Lazily cleaned up on pop."""
    self._entries.pop(listing_id, None)

  def peek_next_time(self) -> float | None:
    """Return the next check time without removing the entry."""
    self._clean_stale()
    if not self._heap:
      return None
    return self._heap[0].next_check_at

  def pop_due(self, now: float | None = None) -> list[TrackedListing]:
    """Pop all listings whose next_check_at <= now."""
    if now is None:
      now = time.time()
    due: list[TrackedListing] = []
    while self._heap:
      self._clean_stale()
      if not self._heap:
        break
      if self._heap[0].next_check_at > now:
        break
      tracked = heapq.heappop(self._heap)
      if tracked.listing_id in self._entries and self._entries[tracked.listing_id] is tracked:
        due.append(tracked)
    return due

  def get_all(self) -> list[TrackedListing]:
    """Return all tracked listings (for diagnostics)."""
    return list(self._entries.values())

  def get(self, listing_id: int) -> TrackedListing | None:
    return self._entries.get(listing_id)

  def _clean_stale(self) -> None:
    """Remove entries from heap top that are no longer in the active set."""
    while self._heap:
      top = self._heap[0]
      if top.listing_id in self._entries and self._entries[top.listing_id] is top:
        break
      heapq.heappop(self._heap)
