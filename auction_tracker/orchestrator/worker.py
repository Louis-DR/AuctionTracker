"""Per-website worker pipeline.

Instead of three global loops (search, fetch, watch) that process all
websites sequentially, each website gets its own async worker that
interleaves all three operations within that website's rate budget.

Architecture::

  Pipeline
    +-- WebsiteWorker("ebay")     -> async task
    +-- WebsiteWorker("catawiki") -> async task
    +-- WebsiteWorker("drouot")   -> async task
    +-- ...

Each worker's tick loop:

  1. Pop the single most urgent due watch check -> execute
  2. Else: continue a search cycle (one query per tick)
  3. Else: fetch one unfetched listing from the backlog
  4. Else: start a new search cycle (if interval elapsed)
  5. Else: sleep until the next event

Between every request the worker sleeps for the website's
``request_delay``, ensuring bot-detection rate limits are respected.
Since the HTTP transport already uses per-domain rate limiting,
multiple workers sharing one TransportRouter do not interfere with
each other.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import random
import time
from dataclasses import dataclass

from sqlalchemy.orm import Session

from datetime import UTC, datetime

from auction_tracker.config import (
  AppConfig,
  MonitoringStrategy,
  TransportKind,
  WebsiteConfig,
)
from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.models import Listing, ListingStatus, SearchQuery
from auction_tracker.database.repository import Repository
from auction_tracker.orchestrator.ingest import Ingest
from auction_tracker.orchestrator.scheduler import (
  CheckQueue,
  Phase,
  Scheduler,
  TrackedListing,
)
from auction_tracker.orchestrator.utils import fetch_and_parse_listing
from auction_tracker.parsing.base import ListingGone, Parser, ParserBlocked, ParserRegistry
from auction_tracker.transport.base import TransportBlocked, TransportError
from auction_tracker.transport.router import TransportRouter

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_FETCH_ERRORS = 3
_UTILIZATION_EMIT_INTERVAL = 60.0

# Interval for heartbeat log lines. If more than this many seconds
# pass without any log output, the worker is likely frozen.
_HEARTBEAT_INTERVAL = 300.0

_PHASE_PRIORITY = {
  Phase.IMMINENT: 0,
  Phase.ENDING: 1,
  Phase.APPROACHING: 2,
  Phase.ROUTINE: 3,
  Phase.DONE: 4,
}


@dataclass
class _FetchItem:
  """A listing stub discovered via search, pending full-detail fetch."""
  listing_id: int
  website_id: int
  url: str
  external_id: str


@dataclass
class WorkerStats:
  """Cumulative statistics for a single website worker."""
  watch_checks: int = 0
  watch_updated: int = 0
  watch_completed: int = 0
  extensions_detected: int = 0
  searches_run: int = 0
  search_results: int = 0
  new_listings: int = 0
  listings_fetched: int = 0
  listings_classified: int = 0
  listings_rejected: int = 0
  errors: int = 0


# ======================================================================
# WebsiteWorker
# ======================================================================


class WebsiteWorker:
  """Autonomous worker for a single website.

  Runs in its own async task.  On each tick it picks the single
  highest-priority task and executes it, then sleeps for the
  website's ``request_delay`` before the next tick.

  Task priority (highest first):

  1. Due watch check (IMMINENT > ENDING > APPROACHING > ROUTINE)
  2. Pending search query (mid-cycle, one query per tick)
  3. Unfetched listing from the discovery backlog
  4. Start a new search cycle (if interval has elapsed)
  """

  def __init__(
    self,
    website_name: str,
    config: AppConfig,
    router: TransportRouter,
    database: DatabaseEngine,
    repository: Repository,
    search_interval: float,
    classify: bool,
    metrics=None,
    live=None,
    converter=None,
  ) -> None:
    self._name = website_name
    self._config = config
    self._website_config: WebsiteConfig = config.website(website_name)
    self._router = router
    self._database = database
    self._repo = repository
    self._ingest = Ingest(repository, converter=converter)
    self._scheduler = Scheduler(config.scheduler)
    self._parser: Parser = ParserRegistry.get(website_name)

    self._request_delay: float = self._website_config.request_delay
    self._search_interval: float = search_interval
    self._classify: bool = classify
    self._uses_camoufox: bool = (
      self._website_config.transport == TransportKind.CAMOUFOX
    )

    # Watch state
    self._watch_queue = CheckQueue()

    # Fetch state
    self._fetch_queue: collections.deque[_FetchItem] = collections.deque()
    self._fetch_queued_ids: set[int] = set()
    self._fetch_error_counts: dict[int, int] = {}

    # Search state: a cycle is represented by a deque of pending
    # queries.  Each tick pops one query, so watch checks and fetches
    # can be interleaved between search queries.
    self._search_queries: list[SearchQuery] = []
    self._pending_searches: collections.deque[SearchQuery] = collections.deque()
    self._last_search_at: float = 0.0

    # Cached database id for this website (resolved on first use).
    self._website_id: int | None = None

    self._stats = WorkerStats()
    self._metrics = metrics
    self._live = live

    # Utilization tracking: accumulate wall-clock seconds attributed to
    # active (inside _tick) vs idle (between ticks / sleeping).
    # _flush_utilization() is called at natural checkpoints (tick
    # boundaries and before/after each network call) to keep the
    # accumulators current and emit a DB event every ~60 s.
    self._active_accumulator: float = 0.0
    self._idle_accumulator: float = 0.0
    self._last_utilization_emit: float = 0.0
    self._segment_start: float = 0.0
    self._in_tick: bool = False
    self._last_heartbeat: float = 0.0
    self._ticks_since_heartbeat: int = 0

  @property
  def name(self) -> str:
    return self._name

  @property
  def stats(self) -> WorkerStats:
    return self._stats

  @property
  def watch_queue_size(self) -> int:
    return len(self._watch_queue)

  @property
  def fetch_queue_size(self) -> int:
    return len(self._fetch_queue)

  # ------------------------------------------------------------------
  # Initialisation
  # ------------------------------------------------------------------

  def load_initial_state(self, session: Session) -> None:
    """Populate internal queues from the database.

    Must be called (inside an open session) before ``run()``.
    """
    website_obj = self._repo.get_website_by_name(session, self._name)
    if website_obj is not None:
      self._website_id = website_obj.id

    if not self._website_config.historical_only:
      for listing in self._repo.get_active_listings(
        session, website_name=self._name,
      ):
        self._enqueue_watch(listing)

    if self._parser.capabilities.can_parse_listing:
      for listing in self._repo.get_listings_needing_fetch(
        session, website_name=self._name,
      ):
        self._enqueue_fetch(listing)

    can_search = (
      self._parser.capabilities.can_search
      and not self._website_config.exclude_from_discovery
    )
    if can_search:
      self._search_queries = list(
        self._repo.get_active_searches(session),
      )

    logger.info(
      "[%s] Loaded %d watch, %d fetch, %d search queries",
      self._name, len(self._watch_queue),
      len(self._fetch_queue), len(self._search_queries),
    )

  # ------------------------------------------------------------------
  # Main loop
  # ------------------------------------------------------------------

  async def run(self, stop_event: asyncio.Event) -> None:
    """Run continuously until *stop_event* is set."""
    logger.info("[%s] Worker started", self._name)
    now = time.time()
    self._segment_start = now
    self._last_utilization_emit = now
    self._last_heartbeat = now
    self._ticks_since_heartbeat = 0
    self._active_accumulator = 0.0
    self._idle_accumulator = 0.0
    self._in_tick = False

    while not stop_event.is_set():
      # Flush the idle segment that elapsed since the last tick ended,
      # then switch to active before doing real work.
      self._flush_utilization()
      self._in_tick = True
      try:
        executed = await self._tick()
      except Exception as exc:
        self._stats.errors += 1
        logger.error(
          "[%s] Tick error: %s", self._name, exc, exc_info=True,
        )
        executed = True
      # Flush the active segment, then switch back to idle for the sleep.
      self._flush_utilization()
      self._in_tick = False

      self._ticks_since_heartbeat += 1
      self._emit_heartbeat_if_due()

      if not executed:
        self._report_idle()
      if not executed:
        delay = self._idle_sleep_duration()
      elif self._uses_camoufox:
        # Camoufox websites share a single browser page behind an
        # asyncio lock. The lock wait already provides natural spacing
        # between requests to the same website (~4x the page load time
        # when 4 websites share the browser). Adding a full inter-tick
        # sleep would only further starve the watch/fetch queues.
        delay = random.uniform(0.05, 0.15)
      else:
        delay = self._request_delay + random.uniform(0, 0.5)
      with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=delay)

    # Final flush so the last slice of time is recorded.
    self._flush_utilization(force_emit=True)
    logger.info("[%s] Worker stopped", self._name)

  async def run_once(self) -> WorkerStats:
    """Single pass: search, drain fetch queue, process due watches."""
    if self._search_queries:
      self._pending_searches = collections.deque(self._search_queries)
      while self._pending_searches:
        await self._execute_one_search()
        await asyncio.sleep(self._request_delay)

    while self._fetch_queue:
      await self._execute_fetch()
      await asyncio.sleep(self._request_delay)

    due = self._watch_queue.pop_due(time.time())
    due.sort(key=lambda tracked: (
      _PHASE_PRIORITY.get(tracked.phase, 99),
      tracked.end_time if tracked.end_time is not None else float("inf"),
    ))
    for tracked in due:
      await self._execute_watch(tracked)
      await asyncio.sleep(self._request_delay)

    return self._stats

  # ------------------------------------------------------------------
  # Tick: pick the single highest-priority task
  # ------------------------------------------------------------------

  async def _tick(self) -> bool:
    """Execute one unit of work. Returns True if a request was made."""
    now = time.time()

    # Priority 1: most-urgent due watch check.
    due = self._watch_queue.pop_due(now)
    if due:
      due.sort(key=lambda tracked: (
        _PHASE_PRIORITY.get(tracked.phase, 99),
        tracked.end_time if tracked.end_time is not None else float("inf"),
      ))
      tracked = due[0]
      for other in due[1:]:
        self._watch_queue.add_or_update(other)
      self._report_activity("watch", tracked.external_id)
      await self._execute_watch(tracked)
      return True

    # Priority 2: continue a multi-query search cycle.
    if self._pending_searches:
      query = self._pending_searches[0]
      self._report_activity("search", query.query_text)
      await self._execute_one_search()
      return True

    # Priority 3: fetch one unfetched listing.
    if self._fetch_queue:
      item = self._fetch_queue[0]
      self._report_activity("fetch", item.external_id)
      await self._execute_fetch()
      return True

    # Priority 4: start a new search cycle.
    if self._should_start_search():
      self._begin_search_cycle()
      if self._pending_searches:
        query = self._pending_searches[0]
        self._report_activity("search", query.query_text)
        await self._execute_one_search()
        return True

    return False

  def _report_activity(self, task_type: str, detail: str) -> None:
    if self._live:
      self._live.worker_activity(
        self._name, task_type, detail,
        watch_queue=len(self._watch_queue),
        fetch_queue=len(self._fetch_queue),
        search_queue=len(self._pending_searches),
      )

  def _emit_heartbeat_if_due(self) -> None:
    now = time.time()
    if now - self._last_heartbeat < _HEARTBEAT_INTERVAL:
      return
    elapsed_minutes = (now - self._last_heartbeat) / 60.0
    logger.info(
      "[%s] Heartbeat: %d ticks in last %.0fmin | "
      "fetch_queue=%d watch_queue=%d search_queue=%d | "
      "stats: fetched=%d watched=%d errors=%d",
      self._name,
      self._ticks_since_heartbeat,
      elapsed_minutes,
      len(self._fetch_queue),
      len(self._watch_queue),
      len(self._pending_searches),
      self._stats.listings_fetched,
      self._stats.watch_checks,
      self._stats.errors,
    )
    self._last_heartbeat = now
    self._ticks_since_heartbeat = 0

  def _report_idle(self) -> None:
    if self._live:
      next_seconds, next_kind = self._next_event_info()
      self._live.worker_idle(
        self._name,
        watch_queue=len(self._watch_queue),
        fetch_queue=len(self._fetch_queue),
        search_queue=len(self._pending_searches),
        next_event_in=next_seconds,
        next_event_kind=next_kind,
      )

  def _flush_utilization(self, *, force_emit: bool = False) -> None:
    """Attribute elapsed wall time to idle or active and emit if due.

    Called at tick boundaries (idle <-> active transitions) *and*
    before / after each network call inside ``_execute_fetch``,
    ``_execute_watch``, and ``_execute_one_search`` so that even
    long Camoufox operations produce regular utilization events.
    """
    now = time.time()
    dt = now - self._segment_start
    self._segment_start = now
    if self._in_tick:
      self._active_accumulator += dt
    else:
      self._idle_accumulator += dt
    if force_emit or now - self._last_utilization_emit >= _UTILIZATION_EMIT_INTERVAL:
      if self._metrics:
        self._metrics.worker_utilization(
          self._name,
          idle_seconds=self._idle_accumulator,
          active_seconds=self._active_accumulator,
          fetch_queue=len(self._fetch_queue),
          watch_queue=len(self._watch_queue),
          search_queue=len(self._pending_searches),
        )
      self._active_accumulator = 0.0
      self._idle_accumulator = 0.0
      self._last_utilization_emit = now

  # ------------------------------------------------------------------
  # Task: watch check
  # ------------------------------------------------------------------

  async def _execute_watch(self, tracked: TrackedListing) -> None:
    """Fetch and process a single watch check."""
    # Guard: the classifier (or a parallel worker path) may have marked
    # this listing terminal since it was enqueued. Verify the DB status
    # before doing any network I/O to avoid overwriting a CANCELLED
    # verdict with the parser-returned "active".
    with self._database.session() as session:
      listing_check = session.get(Listing, tracked.listing_id)
      if listing_check is not None and listing_check.is_terminal:
        tracked.is_terminal = True
        logger.debug(
          "[%s] Skipping watch for %s — already terminal (%s)",
          self._name, tracked.external_id, listing_check.status.value,
        )
        self._reschedule_watch(tracked)
        return

    watch_delay = (
      time.time() - tracked.next_check_at
      if tracked.next_check_at > 0 else 0.0
    )
    try:
      self._flush_utilization()
      _result, scraped = await fetch_and_parse_listing(
        self._router, self._parser, self._name, tracked.url,
      )
      self._flush_utilization()
    except (TransportError, ParserBlocked) as exc:
      # A 404 means the listing page no longer exists on the server — treat
      # it as cancelled regardless of monitoring strategy or website.
      if isinstance(exc, TransportError) and exc.status_code == 404:
        logger.info(
          "[%s] Listing %s is gone (HTTP 404) — marking as cancelled",
          self._name, tracked.external_id,
        )
        tracked.is_terminal = True
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, tracked.listing_id, ListingStatus.CANCELLED,
          )
          session.commit()
        self._reschedule_watch(tracked)
        return

      # A persistent HTTP 403 on a snapshot (classifieds) listing means the
      # ad has been removed by the seller — treat it as sold rather than as
      # an error so the metric counters stay clean and the listing is retired.
      if (
        isinstance(exc, TransportBlocked)
        and exc.status_code == 403
        and tracked.strategy == MonitoringStrategy.SNAPSHOT
      ):
        logger.info(
          "[%s] Listing %s is gone (HTTP 403) — marking as sold",
          self._name, tracked.external_id,
        )
        tracked.is_terminal = True
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, tracked.listing_id, ListingStatus.SOLD,
          )
          session.commit()
        self._reschedule_watch(tracked)
        return

      tracked.consecutive_failures += 1
      self._reschedule_watch(tracked)
      self._stats.errors += 1
      logger.warning(
        "[%s] Watch transport error for %s: %s",
        self._name, tracked.external_id, exc,
      )
      if self._metrics:
        self._metrics.error(
          "watch", str(exc), website_name=self._name,
        )
      if self._live:
        self._live.increment("errors")
      return
    except ListingGone as exc:
      # The page returned a valid response but the listing content is
      # gone — the lot has been removed or the ad was taken down. Mark
      # it as sold (best guess for completed auctions and withdrawn
      # classifieds) without incrementing the error counter.
      logger.info(
        "[%s] Listing %s is gone — marking as sold: %s",
        self._name, tracked.external_id, exc,
      )
      tracked.is_terminal = True
      with self._database.session() as session:
        self._repo.mark_listing_status(
          session, tracked.listing_id, ListingStatus.SOLD,
        )
        session.commit()
      self._reschedule_watch(tracked)
      return
    except Exception as exc:
      tracked.consecutive_failures += 1
      self._reschedule_watch(tracked)
      self._stats.errors += 1
      logger.error(
        "[%s] Watch error for %s: %s",
        self._name, tracked.external_id, exc, exc_info=True,
      )
      if self._metrics:
        self._metrics.error(
          "watch", str(exc), website_name=self._name,
        )
      if self._live:
        self._live.increment("errors")
      return

    self._stats.watch_checks += 1
    tracked.consecutive_failures = 0
    tracked.last_fetched_at = time.time()
    logger.info(
      "[%s] Watched %s [%s] — %s %s",
      self._name,
      scraped.title[:60] if scraped.title else "?",
      tracked.external_id,
      scraped.current_price or "?",
      scraped.currency or "",
    )

    # Use a single session for all DB writes in this check.
    # _process_watch_result receives the session so that its
    # mark-as-UNSOLD path reuses the same connection instead of
    # opening a nested one (which would deadlock on the write lock).
    with self._database.session() as session:
      website_id = self._resolve_website_id(session)
      listing, _ = self._ingest.ingest_listing(
        session, website_id, scraped,
      )
      self._stats.watch_updated += 1
      self._process_watch_result(tracked, listing, scraped, session)
      session.commit()

    self._reschedule_watch(tracked)

    if self._metrics:
      self._metrics.watch_check(
        self._name, tracked.external_id,
        delay_seconds=watch_delay,
      )
    if self._live:
      self._live.increment("watch_checks")

  # ------------------------------------------------------------------
  # Task: fetch full listing details
  # ------------------------------------------------------------------

  async def _execute_fetch(self) -> None:
    """Fetch full details for one unfetched listing.

    The DB work is split into two short transactions so that the
    SQLite write lock is never held across an ``await`` (which would
    block other workers for the duration of image downloads).

    On failure the item is pushed to the **back** of the deque so
    other listings get a chance before a retry.  After
    ``_MAX_CONSECUTIVE_FETCH_ERRORS`` consecutive failures the item
    is parked (not re-queued) until the next search cycle resets all
    error counters.
    """
    item = self._fetch_queue.popleft()
    self._fetch_queued_ids.discard(item.listing_id)

    error_count = self._fetch_error_counts.get(item.listing_id, 0)
    if error_count >= _MAX_CONSECUTIVE_FETCH_ERRORS:
      logger.debug(
        "[%s] Skipping %s — %d consecutive errors (parked until next search cycle)",
        self._name, item.external_id, error_count,
      )
      return

    try:
      self._flush_utilization()
      _result, scraped = await fetch_and_parse_listing(
        self._router, self._parser, self._name, item.url,
      )
      self._flush_utilization()

      # Transaction 1: ingest the listing (fast, no network I/O).
      with self._database.session() as session:
        self._ingest.ingest_listing(
          session, item.website_id, scraped,
        )
        self._fetch_error_counts.pop(item.listing_id, None)
        session.commit()

      # Transaction 2: download images and classify (may await
      # network I/O, so this MUST be outside the ingest session).
      if self._classify and scraped.image_urls:
        self._flush_utilization()
        await self._classify_and_filter(
          item.listing_id, scraped,
        )
        self._flush_utilization()

      self._stats.listings_fetched += 1
      logger.info(
        "[%s] Fetched listing %s [%s]",
        self._name, scraped.title[:60] if scraped.title else "?",
        item.external_id,
      )
      if self._metrics:
        self._metrics.fetch_listing(self._name, item.external_id)
      if self._live:
        self._live.increment("fetched")

      # Transaction 3: re-read the listing to check terminal status
      # and enqueue for watching.
      with self._database.session() as session:
        listing = session.get(Listing, item.listing_id)
        if listing is not None and not listing.is_terminal:
          self._enqueue_watch(listing)

    except Exception as exc:
      # A 404 means the listing page no longer exists — mark cancelled
      # immediately without counting as an error.
      if isinstance(exc, TransportError) and exc.status_code == 404:
        logger.info(
          "[%s] New listing %s not found (HTTP 404) — marking cancelled",
          self._name, item.external_id,
        )
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, item.listing_id, ListingStatus.CANCELLED,
          )
          session.commit()
        return

      # A 403 on a snapshot listing means the ad was removed before we
      # could fetch its details — mark cancelled and skip without counting
      # as an error (not a tool failure, just a deleted listing).
      if (
        isinstance(exc, TransportBlocked)
        and exc.status_code == 403
        and self._parser.capabilities.has_buy_now
        and not self._parser.capabilities.has_bid_history
      ):
        logger.info(
          "[%s] New listing %s is already gone (HTTP 403) — marking cancelled",
          self._name, item.external_id,
        )
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, item.listing_id, ListingStatus.CANCELLED,
          )
          session.commit()
        return

      if isinstance(exc, ListingGone):
        logger.info(
          "[%s] New listing %s is gone — marking cancelled: %s",
          self._name, item.external_id, exc,
        )
        with self._database.session() as session:
          self._repo.mark_listing_status(
            session, item.listing_id, ListingStatus.CANCELLED,
          )
          session.commit()
        return

      self._fetch_error_counts[item.listing_id] = error_count + 1
      new_count = error_count + 1
      if new_count >= _MAX_CONSECUTIVE_FETCH_ERRORS:
        logger.warning(
          "[%s] %s failed %d times — parked until next search cycle: %s",
          self._name, item.external_id, new_count, exc,
        )
      else:
        logger.error(
          "[%s] Fetch error for %s (attempt %d/%d): %s",
          self._name, item.external_id, new_count,
          _MAX_CONSECUTIVE_FETCH_ERRORS, exc,
        )
        # Re-enqueue at the back so other items are tried first.
        self._fetch_queue.append(item)
        self._fetch_queued_ids.add(item.listing_id)
      self._stats.errors += 1
      if self._metrics:
        self._metrics.error(
          "fetch", str(exc), website_name=self._name,
        )
      if self._live:
        self._live.increment("errors")

  # ------------------------------------------------------------------
  # Task: run one search query from the current cycle
  # ------------------------------------------------------------------

  async def _execute_one_search(self) -> None:
    """Pop one query from the pending deque and run it.

    The network fetch happens *before* any DB session is opened so
    that the SQLite write lock is never held across an ``await``.
    """
    if not self._pending_searches:
      return

    query = self._pending_searches.popleft()

    try:
      search_results = await self._fetch_search_results(query)
      if search_results is None:
        return

      website_id = self._resolve_website_id_cached()

      with self._database.session() as session:
        new_count = 0
        for scraped_result in search_results:
          listing, is_new = self._ingest.ingest_search_result(
            session, website_id, scraped_result,
            query_text=query.query_text,
          )
          if is_new:
            new_count += 1
            self._enqueue_fetch(listing)

        merged_query = session.merge(query)
        merged_query.last_run_at = datetime.now(UTC).replace(
          tzinfo=None,
        )
        merged_query.result_count = len(search_results)
        session.commit()

      result_count = len(search_results)
      self._stats.searches_run += 1
      self._stats.search_results += result_count
      self._stats.new_listings += new_count

      if self._metrics:
        self._metrics.search_run(
          self._name, query.query_text,
          result_count, new_count,
        )
      if self._live:
        self._live.increment("searches_run")
        self._live.increment("search_results_found", result_count)
        self._live.increment("new_listings", new_count)

    except TransportError as exc:
      # Some search endpoints (e.g. Buyee/Yahoo Japan) return HTTP 404
      # when a query finds no results instead of a 200 with an empty
      # list. Treat this as zero results rather than an error.
      if exc.status_code == 404:
        logger.debug(
          "[%s] Search returned 404 for '%s' — treating as empty",
          self._name, query.query_text,
        )
      else:
        self._stats.errors += 1
        logger.error(
          "[%s] Search error for '%s': %s",
          self._name, query.query_text, exc, exc_info=True,
        )
        if self._metrics:
          self._metrics.error(
            "search", str(exc), website_name=self._name,
          )
        if self._live:
          self._live.increment("errors")
    except Exception as exc:
      self._stats.errors += 1
      logger.error(
        "[%s] Search error for '%s': %s",
        self._name, query.query_text, exc, exc_info=True,
      )
      if self._metrics:
        self._metrics.error(
          "search", str(exc), website_name=self._name,
        )
      if self._live:
        self._live.increment("errors")

    if not self._pending_searches:
      self._last_search_at = time.time()
      logger.info("[%s] Search cycle complete", self._name)

  async def _fetch_search_results(self, query: SearchQuery):
    """Fetch and parse a search page (network I/O, no DB session).

    Returns the parsed results list, or None on blocked/failure.
    """
    search_kwargs: dict = {}
    if self._website_config.preferred_domain:
      search_kwargs["domain"] = self._website_config.preferred_domain
    search_url = self._parser.build_search_url(
      query.query_text, **search_kwargs,
    )

    logger.info(
      "[%s] Searching '%s': %s",
      self._name, query.query_text, search_url,
    )

    self._flush_utilization()
    result = await self._router.fetch(self._name, search_url)
    self._flush_utilization()
    try:
      search_results = self._parser.parse_search_results(
        result.html, url=search_url,
      )
    except ParserBlocked as blocked:
      search_results = await self._retry_blocked_search(blocked)

    if search_results is not None:
      logger.info(
        "[%s] Parsed %d search results",
        self._name, len(search_results),
      )

    return search_results

  async def _retry_blocked_search(self, blocked: ParserBlocked):
    """Try fallback URLs when a search page is blocked."""
    for fallback_url in blocked.fallback_urls:
      logger.info(
        "[%s] Retrying blocked search with fallback: %s",
        self._name, fallback_url,
      )
      try:
        result = await self._router.fetch(self._name, fallback_url)
        return self._parser.parse_search_results(
          result.html, url=fallback_url,
        )
      except ParserBlocked:
        continue
    logger.warning(
      "[%s] All search fallbacks exhausted", self._name,
    )
    return None

  # ------------------------------------------------------------------
  # Watch result processing (ported from watcher.py)
  # ------------------------------------------------------------------

  def _process_watch_result(
    self,
    tracked: TrackedListing,
    listing: Listing,
    scraped,
    session: Session,
  ) -> None:
    """Update tracking state after a successful watch fetch.

    The caller's *session* is reused for any writes (e.g. marking
    a stuck listing as UNSOLD) to avoid opening a nested connection
    that would deadlock on the SQLite write lock.
    """
    if scraped.end_time is not None:
      new_end = scraped.end_time.timestamp()
      if (
        tracked.end_time is not None
        and new_end > tracked.end_time + 60
        and tracked.strategy == MonitoringStrategy.FULL
      ):
        tracked.extension_count += 1
        self._stats.extensions_detected += 1
        logger.info(
          "[%s] Extension #%d for %s",
          self._name, tracked.extension_count,
          tracked.external_id,
        )
      tracked.end_time = new_end

    if listing.is_terminal:
      tracked.is_terminal = True
      logger.info(
        "[%s] %s reached terminal: %s",
        self._name, tracked.external_id, listing.status.value,
      )

    # Detect listings stuck in ENDING phase.
    if (
      tracked.phase == Phase.ENDING
      and not tracked.is_terminal
      and tracked.end_time is not None
    ):
      tracked.post_end_checks += 1
      time_since_end = time.time() - tracked.end_time
      max_wait = self._scheduler.ending_max_wait(tracked.strategy)
      if time_since_end > max_wait:
        logger.warning(
          "[%s] %s stuck in ENDING for %.0fs, marking UNSOLD",
          self._name, tracked.external_id, time_since_end,
        )
        tracked.is_terminal = True
        self._repo.mark_listing_status(
          session, tracked.listing_id, ListingStatus.UNSOLD,
        )

  # ------------------------------------------------------------------
  # Classification
  # ------------------------------------------------------------------

  async def _classify_and_filter(
    self,
    listing_id: int,
    scraped,
  ) -> None:
    """Download images, classify, and reject if irrelevant.

    Opens its own short-lived DB session for the classifier
    verdict so that no write lock is held during the (potentially
    slow) image downloads.
    """
    from auction_tracker.orchestrator.images import (
      classify_listing as run_classification,
      download_listing_images,
    )

    # Network I/O — no DB session held.
    image_paths = await download_listing_images(
      scraped.image_urls, listing_id, self._config.classifier,
    )
    if not image_paths:
      return

    # CPU-bound classification (synchronous, fast).
    is_relevant, score, top_classes = run_classification(
      image_paths, self._config.classifier,
    )
    self._stats.listings_classified += 1
    if self._metrics:
      self._metrics.classification(
        self._name, scraped.external_id, is_relevant, score,
      )
    if self._live:
      self._live.increment("classified")

    # Short DB transaction to persist the verdict.
    with self._database.session() as session:
      self._repo.upsert_listing_attribute(
        session, listing_id, "classifier_score", f"{score:.4f}",
      )
      self._repo.upsert_listing_attribute(
        session, listing_id,
        "classifier_accepted", "1" if is_relevant else "0",
      )
      if top_classes:
        self._repo.upsert_listing_attribute(
          session, listing_id,
          "classifier_top_class", top_classes[0][0],
        )

      if not is_relevant:
        self._stats.listings_rejected += 1
        if self._live:
          self._live.increment("rejected")
        self._repo.mark_listing_status(
          session, listing_id, ListingStatus.CANCELLED,
        )
        top_labels = ", ".join(
          f"{label} ({prob:.0%})" for label, prob in top_classes
        )
        logger.info(
          "[%s] Rejected %s (score=%.0f%%): %s",
          self._name, scraped.external_id, score * 100, top_labels,
        )

      session.commit()

  # ------------------------------------------------------------------
  # Queue helpers
  # ------------------------------------------------------------------

  def _enqueue_watch(self, listing: Listing) -> None:
    """Build a TrackedListing from a DB Listing and schedule it."""
    if self._website_config.historical_only:
      return
    tracked = TrackedListing(
      listing_id=listing.id,
      website_name=self._name,
      external_id=listing.external_id,
      url=listing.url,
      strategy=self._website_config.monitoring_strategy,
      end_time=(
        listing.end_time.timestamp() if listing.end_time else None
      ),
      last_fetched_at=(
        listing.last_checked_at.timestamp()
        if listing.last_checked_at else 0.0
      ),
      is_terminal=listing.is_terminal,
    )
    schedule = self._scheduler.compute_next_check(tracked)
    tracked.next_check_at = schedule.next_check_at
    tracked.phase = schedule.phase
    if tracked.phase == Phase.DONE:
      return
    self._watch_queue.add_or_update(tracked)

  def _enqueue_fetch(self, listing: Listing) -> None:
    """Add a listing to the fetch queue, skipping duplicates."""
    if listing.id in self._fetch_queued_ids:
      return
    self._fetch_queue.append(_FetchItem(
      listing_id=listing.id,
      website_id=listing.website_id,
      url=listing.url,
      external_id=listing.external_id,
    ))
    self._fetch_queued_ids.add(listing.id)

  def _reschedule_watch(self, tracked: TrackedListing) -> None:
    """Compute next check time and re-insert or remove from queue."""
    schedule = self._scheduler.compute_next_check(tracked)
    tracked.next_check_at = schedule.next_check_at
    tracked.phase = schedule.phase
    if tracked.phase == Phase.DONE:
      self._watch_queue.remove(tracked.listing_id)
      self._stats.watch_completed += 1
      if self._live:
        self._live.increment("watch_completed")
    else:
      self._watch_queue.add_or_update(tracked)

  def _resolve_website_id(self, session: Session) -> int:
    """Return the cached website DB id, refreshing from *session*
    if not yet resolved.
    """
    if self._website_id is not None:
      return self._website_id
    website = self._repo.get_website_by_name(session, self._name)
    if website is None:
      raise ValueError(f"Website '{self._name}' not found in database")
    self._website_id = website.id
    return self._website_id

  def _resolve_website_id_cached(self) -> int:
    """Return the cached website DB id, opening a throwaway session
    to resolve it if needed.
    """
    if self._website_id is not None:
      return self._website_id
    with self._database.session() as session:
      return self._resolve_website_id(session)

  def _should_start_search(self) -> bool:
    if not self._search_queries:
      return False
    if self._pending_searches:
      return False
    return time.time() - self._last_search_at >= self._search_interval

  def _begin_search_cycle(self) -> None:
    """Refresh queries from DB and populate the pending deque.

    Also resets consecutive-error counters so that listings parked
    after ``_MAX_CONSECUTIVE_FETCH_ERRORS`` get another round of
    attempts, and re-enqueues unfetched listings from the DB that
    may have been dropped from the in-memory deque.
    """
    with self._database.session() as session:
      self._search_queries = list(
        self._repo.get_active_searches(session),
      )
      # Clear error counters — the new cycle is a fresh start.
      parked_count = sum(
        1 for count in self._fetch_error_counts.values()
        if count >= _MAX_CONSECUTIVE_FETCH_ERRORS
      )
      self._fetch_error_counts.clear()

      # Re-enqueue any DB rows still needing fetch (includes items
      # that were parked due to repeated failures).
      if self._parser.capabilities.can_parse_listing:
        for listing in self._repo.get_listings_needing_fetch(
          session, website_name=self._name,
        ):
          self._enqueue_fetch(listing)
      if parked_count:
        logger.info(
          "[%s] Unparked %d previously-failed fetch items",
          self._name, parked_count,
        )

    self._pending_searches = collections.deque(self._search_queries)
    logger.info(
      "[%s] Starting search cycle (%d queries, %d pending fetches)",
      self._name, len(self._pending_searches), len(self._fetch_queue),
    )

  def _next_event_info(self) -> tuple[float, str]:
    """Return (seconds_until, kind) for the soonest upcoming event."""
    now = time.time()
    best_seconds = 30.0
    best_kind = "sleep"

    next_watch = self._watch_queue.peek_next_time()
    if next_watch is not None:
      delta = max(0.5, next_watch - now)
      if delta < best_seconds:
        best_seconds = delta
        best_kind = "watch"

    if self._search_queries and not self._pending_searches:
      delta = max(0.5, self._last_search_at + self._search_interval - now)
      if delta < best_seconds:
        best_seconds = delta
        best_kind = "search"

    return best_seconds, best_kind

  def _idle_sleep_duration(self) -> float:
    """Seconds to sleep when no immediate work is available."""
    return self._next_event_info()[0]


# ======================================================================
# Pipeline: manages all website workers
# ======================================================================


class Pipeline:
  """Orchestrates per-website workers for the continuous pipeline.

  Creates one ``WebsiteWorker`` per enabled website that has a
  registered parser, then runs them all as concurrent async tasks
  sharing a single ``TransportRouter``.

  Usage::

      pipeline = Pipeline(config, database, repository, ...)
      await pipeline.run()        # continuous
      stats = await pipeline.run_once()  # single pass
  """

  def __init__(
    self,
    config: AppConfig,
    database: DatabaseEngine,
    repository: Repository,
    search_interval: float,
    classify: bool,
    website_filter: str | None = None,
    metrics=None,
    live=None,
  ) -> None:
    from auction_tracker.currency import CurrencyConverter

    self._config = config
    self._database = database
    self._repo = repository
    self._search_interval = search_interval
    self._classify = classify
    self._website_filter = website_filter
    self._metrics = metrics
    self._live = live
    self._workers: dict[str, WebsiteWorker] = {}
    cache_path = config.database.path.parent / "exchange_rates.json"
    self._converter = CurrencyConverter(cache_path=cache_path)

  # ------------------------------------------------------------------
  # Worker construction
  # ------------------------------------------------------------------

  def _build_workers(self, router: TransportRouter) -> None:
    """Create one worker per eligible website."""
    from auction_tracker.logging_setup import add_website_log_handler

    log_dir = self._config.logging.log_dir

    for name in self._config.websites:
      website_config = self._config.website(name)
      if not website_config.enabled:
        continue
      if self._website_filter is not None and name != self._website_filter:
        continue
      if not ParserRegistry.has(name):
        continue

      if log_dir is not None:
        add_website_log_handler(
          name,
          log_dir,
          max_bytes=self._config.logging.max_bytes,
          backup_count=self._config.logging.backup_count,
        )

      self._workers[name] = WebsiteWorker(
        website_name=name,
        config=self._config,
        router=router,
        database=self._database,
        repository=self._repo,
        search_interval=self._search_interval,
        classify=self._classify,
        metrics=self._metrics,
        live=self._live,
        converter=self._converter,
      )

  def _load_state(self) -> None:
    """Load initial queues for every worker from the database."""
    with self._database.session() as session:
      for worker in self._workers.values():
        worker.load_initial_state(session)

  # ------------------------------------------------------------------
  # Continuous mode
  # ------------------------------------------------------------------

  async def run(self, stop_event: asyncio.Event | None = None) -> None:
    """Start all workers and run until stopped (Ctrl+C / stop_event)."""
    if stop_event is None:
      stop_event = asyncio.Event()

    async with TransportRouter(self._config) as router:
      self._build_workers(router)
      if not self._workers:
        logger.warning(
          "No enabled websites with parsers — nothing to do",
        )
        return

      self._load_state()

      logger.info(
        "Pipeline started with %d workers: %s",
        len(self._workers), ", ".join(self._workers),
      )

      tasks = {
        name: asyncio.create_task(
          self._run_worker_safe(worker, stop_event),
          name=f"worker-{name}",
        )
        for name, worker in self._workers.items()
      }

      try:
        await asyncio.gather(*tasks.values())
      except (KeyboardInterrupt, asyncio.CancelledError):
        pass
      finally:
        stop_event.set()
        for task in tasks.values():
          task.cancel()
        with contextlib.suppress(Exception):
          await asyncio.gather(
            *tasks.values(), return_exceptions=True,
          )

    self._log_summary()

  # ------------------------------------------------------------------
  # Single-pass mode
  # ------------------------------------------------------------------

  async def run_once(self) -> dict[str, WorkerStats]:
    """Run a single search / fetch / watch pass for every website.

    Workers run concurrently.  Returns per-website stats.
    """
    async with TransportRouter(self._config) as router:
      self._build_workers(router)
      if not self._workers:
        return {}

      self._load_state()

      results = await asyncio.gather(
        *(worker.run_once() for worker in self._workers.values()),
        return_exceptions=True,
      )

      return {
        name: (
          result if isinstance(result, WorkerStats)
          else WorkerStats(errors=1)
        )
        for name, result in zip(self._workers, results)
      }

  # ------------------------------------------------------------------
  # Internals
  # ------------------------------------------------------------------

  @staticmethod
  async def _run_worker_safe(
    worker: WebsiteWorker,
    stop_event: asyncio.Event,
  ) -> None:
    """Run a worker, catching unexpected crashes."""
    try:
      await worker.run(stop_event)
    except asyncio.CancelledError:
      pass
    except Exception as exc:
      logger.error(
        "Worker %s crashed: %s", worker.name, exc, exc_info=True,
      )

  def _log_summary(self) -> None:
    """Log aggregate statistics across all workers."""
    total = WorkerStats()
    for worker in self._workers.values():
      worker_stats = worker.stats
      total.watch_checks += worker_stats.watch_checks
      total.watch_updated += worker_stats.watch_updated
      total.watch_completed += worker_stats.watch_completed
      total.extensions_detected += worker_stats.extensions_detected
      total.searches_run += worker_stats.searches_run
      total.search_results += worker_stats.search_results
      total.new_listings += worker_stats.new_listings
      total.listings_fetched += worker_stats.listings_fetched
      total.listings_classified += worker_stats.listings_classified
      total.listings_rejected += worker_stats.listings_rejected
      total.errors += worker_stats.errors

    logger.info(
      "Pipeline summary: %d searched (%d results, %d new), "
      "%d fetched, %d watched (%d completed), %d errors",
      total.searches_run, total.search_results, total.new_listings,
      total.listings_fetched, total.watch_checks,
      total.watch_completed, total.errors,
    )
