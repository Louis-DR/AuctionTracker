"""Tests for the scheduling logic.

The scheduler is the heart of the monitoring system, so it gets
thorough testing. All tests are deterministic — they pass explicit
timestamps and verify the output.
"""

from __future__ import annotations

from auction_tracker.config import (
  FullStrategyConfig,
  MonitoringStrategy,
  PostAuctionStrategyConfig,
  SchedulerConfig,
  SnapshotStrategyConfig,
)
from auction_tracker.orchestrator.scheduler import (
  CheckQueue,
  Phase,
  Scheduler,
  TrackedListing,
)


def make_config(**overrides) -> SchedulerConfig:
  return SchedulerConfig(**overrides)


def make_tracked(
  listing_id: int = 1,
  strategy: MonitoringStrategy = MonitoringStrategy.SNAPSHOT,
  end_time: float | None = None,
  **kwargs,
) -> TrackedListing:
  return TrackedListing(
    listing_id=listing_id,
    website_name="test",
    external_id=f"test-{listing_id}",
    url=f"https://example.com/{listing_id}",
    strategy=strategy,
    end_time=end_time,
    **kwargs,
  )


class TestSchedulerFull:
  """Tests for the FULL monitoring strategy (e.g. Catawiki)."""

  def test_no_end_time_returns_routine(self):
    scheduler = Scheduler(make_config())
    tracked = make_tracked(strategy=MonitoringStrategy.FULL, end_time=None)
    now = 1000000.0

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ROUTINE
    assert result.next_check_at == now + scheduler._config.daily_refresh_interval

  def test_far_from_end_returns_routine(self):
    scheduler = Scheduler(make_config())
    now = 1000000.0
    end_time = now + 86400  # 1 day away.
    tracked = make_tracked(strategy=MonitoringStrategy.FULL, end_time=end_time)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ROUTINE
    # Should be capped at end_time - approaching_threshold.
    assert result.next_check_at <= end_time - scheduler._config.full.approaching_threshold

  def test_approaching_end_tightens_interval(self):
    config = make_config(full=FullStrategyConfig(
      approaching_threshold=3600.0,
      approaching_interval=300.0,
      imminent_threshold=300.0,
    ))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now + 1800  # 30 minutes away (within approaching_threshold).
    tracked = make_tracked(strategy=MonitoringStrategy.FULL, end_time=end_time)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.APPROACHING
    assert result.next_check_at <= now + 300.0

  def test_imminent_polls_aggressively(self):
    config = make_config(full=FullStrategyConfig(
      imminent_threshold=300.0,
      imminent_interval=20.0,
    ))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now + 60  # 1 minute away.
    tracked = make_tracked(strategy=MonitoringStrategy.FULL, end_time=end_time)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.IMMINENT
    assert result.next_check_at == now + 20.0

  def test_past_end_enters_ending(self):
    config = make_config(full=FullStrategyConfig(ending_poll_interval=15.0))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now - 60  # Already ended.
    tracked = make_tracked(strategy=MonitoringStrategy.FULL, end_time=end_time)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ENDING
    assert result.next_check_at == now + 15.0


class TestSchedulerSnapshot:
  """Tests for the SNAPSHOT monitoring strategy (e.g. eBay)."""

  def test_no_end_time_uses_routine_interval(self):
    config = make_config(snapshot=SnapshotStrategyConfig(routine_interval=21600.0))
    scheduler = Scheduler(config)
    tracked = make_tracked(strategy=MonitoringStrategy.SNAPSHOT, end_time=None)
    now = 1000000.0

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ROUTINE
    assert result.next_check_at == now + 21600.0

  def test_past_end_enters_ending(self):
    config = make_config(snapshot=SnapshotStrategyConfig(ending_poll_interval=120.0))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now - 30
    tracked = make_tracked(strategy=MonitoringStrategy.SNAPSHOT, end_time=end_time)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ENDING
    assert result.next_check_at == now + 120.0


class TestSchedulerPostAuction:
  """Tests for the POST_AUCTION monitoring strategy (e.g. Drouot)."""

  def test_before_end_never_fetched_checks_now(self):
    scheduler = Scheduler(make_config())
    now = 1000000.0
    end_time = now + 3600
    tracked = make_tracked(
      strategy=MonitoringStrategy.POST_AUCTION,
      end_time=end_time,
      last_fetched_at=0,
    )

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ROUTINE
    assert result.next_check_at == now

  def test_before_end_already_fetched_waits(self):
    config = make_config(post_auction=PostAuctionStrategyConfig(delay_after_end=900.0))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now + 3600
    tracked = make_tracked(
      strategy=MonitoringStrategy.POST_AUCTION,
      end_time=end_time,
      last_fetched_at=now - 100,
    )

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.WAITING
    assert result.next_check_at == end_time + 900.0

  def test_after_end_checks_for_results(self):
    config = make_config(post_auction=PostAuctionStrategyConfig(recheck_interval=3600.0))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now - 7200  # Ended 2 hours ago.
    tracked = make_tracked(
      strategy=MonitoringStrategy.POST_AUCTION,
      end_time=end_time,
      last_fetched_at=now - 3600,
      post_end_checks=1,
    )

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.ENDING
    assert result.next_check_at == now + 3600.0

  def test_gives_up_after_max_rechecks(self):
    config = make_config(post_auction=PostAuctionStrategyConfig(max_recheck_count=5))
    scheduler = Scheduler(config)
    now = 1000000.0
    end_time = now - 86400
    tracked = make_tracked(
      strategy=MonitoringStrategy.POST_AUCTION,
      end_time=end_time,
      last_fetched_at=now - 3600,
      post_end_checks=5,
    )

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.phase == Phase.DONE


class TestSchedulerFailures:
  """Tests for failure handling."""

  def test_terminal_listing_returns_done(self):
    scheduler = Scheduler(make_config())
    tracked = make_tracked(is_terminal=True)

    result = scheduler.compute_next_check(tracked)

    assert result.phase == Phase.DONE
    assert result.next_check_at == float("inf")

  def test_consecutive_failures_trigger_cooldown(self):
    config = make_config(
      consecutive_failure_threshold=3,
      failure_cooldown=300.0,
    )
    scheduler = Scheduler(config)
    now = 1000000.0
    tracked = make_tracked(consecutive_failures=3)

    result = scheduler.compute_next_check(tracked, now=now)

    assert result.next_check_at == now + 300.0


class TestCheckQueue:
  """Tests for the priority queue."""

  def test_add_and_pop(self):
    queue = CheckQueue()
    tracked = make_tracked(listing_id=1)
    tracked.next_check_at = 100.0

    queue.add_or_update(tracked)

    assert len(queue) == 1
    assert 1 in queue

    due = queue.pop_due(now=200.0)
    assert len(due) == 1
    assert due[0].listing_id == 1

  def test_ordering(self):
    queue = CheckQueue()
    early = make_tracked(listing_id=1)
    early.next_check_at = 100.0
    late = make_tracked(listing_id=2)
    late.next_check_at = 200.0

    queue.add_or_update(late)
    queue.add_or_update(early)

    due = queue.pop_due(now=150.0)
    assert len(due) == 1
    assert due[0].listing_id == 1

  def test_remove(self):
    queue = CheckQueue()
    tracked = make_tracked(listing_id=1)
    tracked.next_check_at = 100.0
    queue.add_or_update(tracked)

    queue.remove(1)

    assert len(queue) == 0
    assert queue.pop_due(now=200.0) == []

  def test_update_replaces_old_entry(self):
    queue = CheckQueue()
    tracked = make_tracked(listing_id=1)
    tracked.next_check_at = 100.0
    queue.add_or_update(tracked)

    # Update with a later time.
    tracked_updated = make_tracked(listing_id=1)
    tracked_updated.next_check_at = 300.0
    queue.add_or_update(tracked_updated)

    assert len(queue) == 1
    due = queue.pop_due(now=200.0)
    assert len(due) == 0

    due = queue.pop_due(now=400.0)
    assert len(due) == 1
    assert due[0].next_check_at == 300.0
