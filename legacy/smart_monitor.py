"""Two independent loops for auction tracking.

The system is split into two commands designed to run as **separate
processes** concurrently:

1. **discover** — :class:`DiscoveryLoop` runs saved searches and
   fetches full details (images, bids, attributes) for newly found
   listings.  This loop can be slow (10+ minutes per cycle when many
   listings need fetching) without affecting monitoring timing.

2. **watch** — :class:`WatchLoop` monitors active listings with
   timing-aware, strategy-based scheduling.  Because it runs
   independently of the discovery/fetch work, it can maintain tight
   polling intervals (every 20 seconds for imminent auctions).

Both processes share the same SQLite database.  WAL journal mode
allows concurrent readers alongside a single writer, and a 30-second
busy timeout handles inter-process write contention.  Intra-process
thread contention is handled by :data:`database_write_lock`.

Each loop distributes per-website work across threads using a
:class:`~concurrent.futures.ThreadPoolExecutor`.  A configurable
**per-website phase timeout** (default 10 minutes) ensures that a
slow website cannot block the entire cycle.

The :class:`WatchLoop` adapts its behaviour to each website's
characteristics via the scraper's ``monitoring_strategy``:

**"full"** (Catawiki)
  Aggressive, phase-based scheduling with extension detection.

**"snapshot"** (eBay, Yahoo Japan)
  Periodic price snapshots with progressive intervals near the end.

**"post_auction"** (Drouot, Invaluable, LiveAuctioneers, Interenchères)
  Single fetch, then wait until after the auction to check results.

All thresholds and intervals are configurable in the
``smart_monitoring`` section of ``config.yaml``.
"""

from __future__ import annotations

import heapq
import logging
import signal
import threading
import time
import unicodedata
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.progress import (
  BarColumn,
  MofNCompleteColumn,
  Progress,
  ProgressColumn,
  SpinnerColumn,
  Task,
  TextColumn,
  TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from auction_tracker.config import AppConfig
from auction_tracker.database.engine import (
  session_scope,
  thread_safe_session_scope,
)
from auction_tracker.database.models import Listing, ListingStatus
from auction_tracker.database.repository import (
  get_active_listings,
  get_unfetched_active_listings,
)
from auction_tracker.monitor import Monitor, _fmt_price, _fmt_duration
from auction_tracker.scrapers.registry import ScraperRegistry

logger = logging.getLogger(__name__)
console = Console()


# ------------------------------------------------------------------
# Website failure tracking
# ------------------------------------------------------------------

class WebsiteFailureTracker:
  """Tracks consecutive failures per website and puts websites in timeout
  mode when they exceed the failure threshold.

  Websites in timeout mode are limited to one query per cycle to avoid
  slowing down other websites. They exit timeout mode on success, or
  stay in timeout mode on failure.
  """

  def __init__(self, failure_threshold: int, disable_duration: float) -> None:
    """Initialize the tracker.

    Args:
      failure_threshold: Number of consecutive failures before timeout mode.
      disable_duration: How long (in seconds) to stay in timeout mode.
    """
    self._failure_threshold = failure_threshold
    self._disable_duration = disable_duration
    # Track last N operations per website (True = success, False = failure)
    self._failure_history: Dict[str, deque] = {}
    # Track when each website entered timeout mode (timestamp)
    self._timeout_until: Dict[str, float] = {}
    # Track which websites have used their timeout query this cycle
    self._timeout_queries_this_cycle: Set[str] = set()
    self._lock = threading.RLock()

  def start_cycle(self) -> None:
    """Call at the start of each cycle to reset per-cycle tracking."""
    with self._lock:
      self._timeout_queries_this_cycle.clear()

  def record_success(self, website_name: str) -> None:
    """Record a successful operation for a website."""
    with self._lock:
      history = self._failure_history.setdefault(website_name, deque(maxlen=self._failure_threshold))
      history.append(True)
      # If we had a success, exit timeout mode
      if website_name in self._timeout_until:
        del self._timeout_until[website_name]
        logger.info("Website %s exited timeout mode after successful operation", website_name)

  def record_failure(self, website_name: str) -> None:
    """Record a failed operation for a website."""
    with self._lock:
      history = self._failure_history.setdefault(website_name, deque(maxlen=self._failure_threshold))
      history.append(False)

      # Check if we've hit the threshold
      if len(history) >= self._failure_threshold:
        # Check if all recent operations were failures
        if all(not success for success in history):
          now = time.time()
          timeout_until = now + self._disable_duration
          self._timeout_until[website_name] = timeout_until
          logger.warning(
            "Website %s entered timeout mode for %s due to %d consecutive failures",
            website_name,
            _fmt_duration(self._disable_duration),
            self._failure_threshold,
          )

  def is_in_timeout(self, website_name: str) -> bool:
    """Check if a website is currently in timeout mode."""
    with self._lock:
      if website_name not in self._timeout_until:
        return False

      # Check if timeout period has expired
      timeout_until = self._timeout_until[website_name]
      if time.time() >= timeout_until:
        del self._timeout_until[website_name]
        logger.info("Website %s exited timeout mode after timeout period expired", website_name)
        return False

      return True

  def can_make_timeout_query(self, website_name: str) -> bool:
    """Check if a website in timeout mode can make its one query this cycle.

    Returns False if the website is not in timeout mode or has already
    used its timeout query this cycle.
    """
    with self._lock:
      if not self.is_in_timeout(website_name):
        return False
      return website_name not in self._timeout_queries_this_cycle

  def mark_timeout_query_used(self, website_name: str) -> None:
    """Mark that a website in timeout mode has used its query this cycle."""
    with self._lock:
      self._timeout_queries_this_cycle.add(website_name)

  def get_timeout_websites(self) -> List[Tuple[str, float]]:
    """Get list of websites currently in timeout mode and when they'll exit.

    Returns:
      List of (website_name, timeout_until_timestamp) tuples.
    """
    with self._lock:
      now = time.time()
      timeout_list = []
      expired = []

      for website_name, timeout_until in self._timeout_until.items():
        if now >= timeout_until:
          expired.append(website_name)
        else:
          timeout_list.append((website_name, timeout_until))

      # Clean up expired entries
      for website_name in expired:
        del self._timeout_until[website_name]

      return timeout_list

  def get_failure_count(self, website_name: str) -> int:
    """Get the current consecutive failure count for a website."""
    with self._lock:
      history = self._failure_history.get(website_name, deque())
      if not history:
        return 0

      # Count consecutive failures from the end
      count = 0
      for success in reversed(history):
        if not success:
          count += 1
        else:
          break

      return count


# ------------------------------------------------------------------
# Custom progress columns
# ------------------------------------------------------------------

class ConditionalTimeColumn(ProgressColumn):
  """Shows elapsed time only for top-level tasks, not sub-tasks.

  Sub-tasks are identified by descriptions that start with spaces
  (e.g., "  ebay", "  catawiki").
  """

  def render(self, task: Task) -> Text:
    """Render the elapsed time for top-level tasks only."""
    # Check if this is a sub-task (description starts with spaces).
    if task.description.startswith("  "):
      return Text("")

    # For top-level tasks, show elapsed time.
    elapsed = task.finished_time if task.finished else task.elapsed
    if elapsed is None:
      return Text("--:--", style="progress.elapsed")

    minutes, seconds = divmod(int(elapsed), 60)
    if minutes > 0:
      return Text(f"{minutes}:{seconds:02d}", style="progress.elapsed")
    return Text(f"{seconds}s", style="progress.elapsed")


class ErrorCountColumn(ProgressColumn):
  """Shows error count for sub-tasks (per-website progress bars).

  Only displays errors for sub-tasks (those starting with spaces).
  Shows nothing for top-level tasks to keep them clean.
  """

  def render(self, task: Task) -> Text:
    """Render the error count in red if > 0, empty otherwise."""
    # Only show errors for sub-tasks (website-specific bars).
    if not task.description.startswith("  "):
      return Text("")

    # Get error count from task fields (default to 0).
    error_count = task.fields.get("errors", 0)
    if error_count > 0:
      return Text(f"{error_count} error(s)", style="red")
    return Text("")


# ------------------------------------------------------------------
# Monitoring strategies (must match ScraperCapabilities values)
# ------------------------------------------------------------------

STRATEGY_FULL = "full"
STRATEGY_SNAPSHOT = "snapshot"
STRATEGY_POST_AUCTION = "post_auction"

# ------------------------------------------------------------------
# Monitoring phases
# ------------------------------------------------------------------

PHASE_ROUTINE = "routine"
PHASE_APPROACHING = "approaching"
PHASE_IMMINENT = "imminent"
PHASE_ENDING = "ending"
PHASE_WAITING = "waiting"  # post_auction: idle until end time

_PHASE_SYMBOLS = {
  PHASE_ROUTINE: "🕐",
  PHASE_APPROACHING: "⏳",
  PHASE_IMMINENT: "🔥",
  PHASE_ENDING: "🏁",
  PHASE_WAITING: "💤",
}

_STRATEGY_LABELS = {
  STRATEGY_FULL: "full",
  STRATEGY_SNAPSHOT: "snap",
  STRATEGY_POST_AUCTION: "post",
}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _to_timestamp(value: Optional[datetime]) -> Optional[float]:
  """Convert a datetime to a Unix timestamp.

  Naive datetimes are assumed to be UTC.  Returns ``None`` when
  *value* is ``None``.
  """
  if value is None:
    return None
  if value.tzinfo is not None:
    return value.timestamp()
  return value.replace(tzinfo=timezone.utc).timestamp()


def _normalize_website_name(name: str) -> str:
  """Normalize a website name for scraper matching.

  Removes accents and converts to lowercase.  This ensures that
  database website names like "Interenchères" match registered
  scraper names like "interencheres".
  """
  # Decompose accented characters into base + combining marks.
  nfd = unicodedata.normalize("NFD", name)
  # Filter out combining marks (accents).
  without_accents = "".join(
    char for char in nfd
    if unicodedata.category(char) != "Mn"
  )
  return without_accents.lower()


def _resolve_strategy(scraper_name: str) -> str:
  """Look up the monitoring strategy for a registered scraper."""
  try:
    from auction_tracker.config import ScrapingConfig
    scraper = ScraperRegistry.create(scraper_name, ScrapingConfig())
    return scraper.capabilities.monitoring_strategy
  except Exception:
    return STRATEGY_FULL


# ------------------------------------------------------------------
# Tracked listing state
# ------------------------------------------------------------------

@dataclass
class TrackedListing:
  """In-memory state for a single listing being watched."""

  listing_id: int
  external_id: str
  url: str
  title: str
  scraper_name: str
  end_time: Optional[datetime]
  strategy: str = STRATEGY_FULL

  last_fetched_at: float = 0.0
  phase: str = PHASE_ROUTINE
  original_end_time: Optional[datetime] = None
  extension_count: int = 0
  last_price: Optional[str] = None  # e.g. "120 EUR" for display
  post_end_checks: int = 0  # number of checks done after end time


# ------------------------------------------------------------------
# Base loop — shared setup, signal handling, and progress helpers
# ------------------------------------------------------------------

class _BaseLoop:
  """Shared infrastructure for :class:`DiscoveryLoop` and
  :class:`WatchLoop`.

  Handles SIGINT/graceful shutdown, Rich progress bar construction,
  and interruptible sleep.
  """

  def __init__(
    self,
    config: AppConfig,
    *,
    scrapers: Optional[list[str]] = None,
    verbose: bool = False,
  ) -> None:
    self.config = config
    self.monitor = Monitor(config, scrapers=scrapers)
    self.scrapers = scrapers
    self.verbose = verbose

    # Website failure tracker for temporarily disabling failing websites.
    self.failure_tracker = WebsiteFailureTracker(
      failure_threshold=config.smart_monitoring.failure_threshold,
      disable_duration=config.smart_monitoring.disable_duration,
    )

    # Graceful shutdown flag.
    self._shutdown_requested = False

    # Lock protecting Rich progress bar updates from worker threads.
    self._progress_lock = threading.Lock()

  # ----------------------------------------------------------------
  # Progress helpers
  # ----------------------------------------------------------------

  @staticmethod
  def _make_progress(**kwargs) -> Progress:
    """Create a Rich progress bar with aligned columns.

    Progress numbers are right-aligned for clean vertical alignment
    even when websites have different digit counts (e.g., 5/10 vs
    123/1000). The timer shows elapsed time only for the overall
    task (not per website) to avoid redundancy. Error counts are
    shown in red at the end of each website's progress bar.
    """
    from rich.progress import TaskProgressColumn

    return Progress(
      SpinnerColumn(),
      TextColumn("[bold]{task.description}"),
      BarColumn(bar_width=30),
      TaskProgressColumn("{task.percentage:>3.0f}%"),
      TextColumn("•"),
      TextColumn("{task.completed:>4}/{task.total}"),
      TextColumn("•"),
      ConditionalTimeColumn(),
      ErrorCountColumn(),
      console=console,
      expand=False,
      **kwargs,
    )

  def _advance_progress(self, progress, *task_ids):
    """Thread-safe wrapper to advance one or more progress tasks."""
    with self._progress_lock:
      for task_id in task_ids:
        progress.advance(task_id)

  # ----------------------------------------------------------------
  # Interruptible sleep
  # ----------------------------------------------------------------

  def _interruptible_sleep(self, seconds: float) -> None:
    """Sleep for *seconds*, waking early if shutdown was requested.

    Polls the shutdown flag in short intervals so the loop reacts
    promptly to Ctrl+C even during a long sleep.
    """
    end_time = time.time() + seconds
    while time.time() < end_time and not self._shutdown_requested:
      remaining = end_time - time.time()
      time.sleep(min(remaining, 1.0))

  # ----------------------------------------------------------------
  # Entry point with signal handling
  # ----------------------------------------------------------------

  def run(self) -> None:
    """Install SIGINT handler and run the loop.

    The first Ctrl+C sets the shutdown flag so the current operation
    finishes cleanly.  A second Ctrl+C forces an immediate exit.
    """
    if self.verbose:
      logging.getLogger().setLevel(logging.DEBUG)
      for handler in logging.getLogger().handlers:
        handler.setLevel(logging.DEBUG)
      logging.getLogger("auction_tracker").setLevel(logging.DEBUG)
    else:
      from auction_tracker.config import suppress_console_logging
      suppress_console_logging()

    original_sigint = signal.getsignal(signal.SIGINT)

    def _sigint_handler(signum, frame):
      if self._shutdown_requested:
        signal.signal(signal.SIGINT, original_sigint)
        console.print("\n[red]Forced shutdown.[/red]")
        raise KeyboardInterrupt
      self._shutdown_requested = True
      console.print(
        "\n[yellow]Shutdown requested — finishing current operation. "
        "Press Ctrl+C again to force quit.[/yellow]",
      )

    signal.signal(signal.SIGINT, _sigint_handler)

    try:
      self._run_loop()
    finally:
      signal.signal(signal.SIGINT, original_sigint)

  def _run_loop(self) -> None:
    """Subclasses implement the main loop here."""
    raise NotImplementedError


# ==================================================================
# Discovery loop — search + initial fetch
# ==================================================================

class DiscoveryLoop(_BaseLoop):
  """Search for new listings and fetch their full details.

  Runs two phases in each cycle, each parallelised across websites:

  1. **Discovery** — run saved searches (one thread per website).
  2. **Initial fetch** — fetch full details for unfetched listings
     (one thread per website).

  Designed to run as a separate process alongside :class:`WatchLoop`.
  """

  def __init__(
    self,
    config: AppConfig,
    *,
    run_searches: bool = True,
    scrapers: Optional[list[str]] = None,
    verbose: bool = False,
  ) -> None:
    super().__init__(config, scrapers=scrapers, verbose=verbose)
    self.run_searches = run_searches

  # ----------------------------------------------------------------
  # Main loop
  # ----------------------------------------------------------------

  def _run_loop(self) -> None:
    console.rule("[bold]Discovery Loop[/bold]")
    console.print(
      f"[dim]Phase timeout: "
      f"{_fmt_duration(self.config.smart_monitoring.phase_timeout)}[/dim]",
    )

    last_search_time = 0.0
    search_interval = self.config.smart_monitoring.discovery_interval
    cycle = 0

    while not self._shutdown_requested:
      cycle += 1
      cycle_start = time.time()
      cycle_failures = 0
      now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      console.rule(f"[bold]Cycle {cycle}[/bold] — {now_str}")
      logger.debug("Discovery cycle %d started at %s", cycle, now_str)

      # Reset per-cycle timeout query tracking
      self.failure_tracker.start_cycle()

      # ---- Phase 1: Discovery (saved searches) ------------------

      search_due = (
        self.run_searches
        and time.time() - last_search_time >= search_interval
      )
      if search_due and not self._shutdown_requested:
        logger.debug("Phase 1: Discovery (search interval elapsed)")
        console.print("[bold cyan]Phase 1: Discovery[/bold cyan]")
        try:
          _new_from_search, search_failures = self._phase_discover()
          cycle_failures += search_failures
          logger.debug(
            "Phase 1 complete: %d new from search, %d failure(s)",
            _new_from_search, search_failures,
          )
        except Exception:
          cycle_failures += 1
          logger.exception("Error in discovery phase.")
        last_search_time = time.time()
      elif not self._shutdown_requested:
        remaining = search_interval - (time.time() - last_search_time)
        logger.debug(
          "Phase 1: Discovery skipped — next in %s",
          _fmt_duration(remaining),
        )
        console.print(
          f"[dim]Phase 1: Discovery — next in "
          f"{_fmt_duration(remaining)}[/dim]",
        )

      # ---- Phase 2: Initial fetch (unfetched listings) ----------

      has_deferred_listings = False
      if not self._shutdown_requested:
        logger.debug("Phase 2: Initial fetch starting")
        console.print("[bold cyan]Phase 2: Initial fetch[/bold cyan]")
        try:
          _fetched, fetch_failures, deferred = (
            self._phase_initial_fetch()
          )
          cycle_failures += fetch_failures
          has_deferred_listings = deferred > 0
          logger.debug(
            "Phase 2 complete: %d fetched, %d failure(s), %d deferred",
            _fetched, fetch_failures, deferred,
          )
        except Exception:
          cycle_failures += 1
          logger.exception("Error in initial fetch phase.")

      # ---- Cycle summary ----------------------------------------

      cycle_elapsed = time.time() - cycle_start
      parts = [f"Cycle {cycle} done in {_fmt_duration(cycle_elapsed)}"]
      if cycle_failures > 0:
        parts.append(f"[red]{cycle_failures} error(s)[/red]")

      # Check for timeout websites
      timeout_list = self.failure_tracker.get_timeout_websites()
      if timeout_list:
        timeout_names = [name for name, _ in timeout_list]
        parts.append(f"[yellow]{len(timeout_names)} website(s) in timeout[/yellow]")

      console.print(f"[dim]{' — '.join(parts)}[/dim]")

      if self._shutdown_requested:
        break

      # ---- Sleep until the next event ----------------------------

      if has_deferred_listings:
        console.print(
          "[dim]Deferred listings remain — starting next cycle "
          "immediately…[/dim]\n",
        )
        self._interruptible_sleep(1.0)
      else:
        next_search_time = last_search_time + search_interval
        # Also check for unfetched listings periodically (they may
        # have been added by external CLI commands).
        next_check = time.time() + 60.0
        wake_at = min(next_search_time, next_check)
        sleep_seconds = max(1.0, wake_at - time.time())
        console.print(
          f"[dim]Sleeping {_fmt_duration(sleep_seconds)} until "
          f"next event…[/dim]\n",
        )
        self._interruptible_sleep(sleep_seconds)

    console.print("\n[yellow]Shutting down gracefully.[/yellow]")
    console.print(
      "[dim]The database is in a consistent state. "
      "Unfetched listings will be picked up on the next run.[/dim]",
    )

  # ----------------------------------------------------------------
  # Phase 1 — Discovery (saved searches, parallel per website)
  # ----------------------------------------------------------------

  def _phase_discover(self) -> Tuple[int, int]:
    """Run all saved searches with one thread per website.

    Each website's searches run sequentially within its dedicated
    thread, but all websites run in parallel.  A per-website phase
    timeout limits how long each website can take.

    Returns a tuple of ``(new_listing_count, failed_count)``.
    """
    from auction_tracker.database.models import Website as WebsiteModel
    from auction_tracker.database.repository import get_active_search_queries

    with session_scope() as session:
      queries = get_active_search_queries(session)

    if not queries:
      console.print("  [dim]No saved searches configured.[/dim]")
      return 0, 0

    # Group (query_text, category) by website name.
    website_queues: Dict[str, deque] = {}
    disabled_websites = []
    for query_item in queries:
      website_name = None
      if query_item.website_id is not None:
        with session_scope() as session:
          website = session.get(WebsiteModel, query_item.website_id)
          if website:
            website_name = website.name

      if website_name is None:
        scrapers_to_use = (
          self.scrapers
          if self.scrapers
          else ScraperRegistry.list_registered()
        )
        for name in scrapers_to_use:
          # Skip scrapers that opt out of discovery (e.g. historical-only).
          try:
            scraper = self.monitor.get_scraper(name)
            if scraper.capabilities.exclude_from_discover:
              continue
          except Exception:
            pass

          # Limit timeout websites to one query per cycle
          if self.failure_tracker.is_in_timeout(name):
            if name not in disabled_websites:
              disabled_websites.append(name)
            if self.failure_tracker.can_make_timeout_query(name):
              website_queues.setdefault(name, deque()).append(
                (query_item.query_text, query_item.category),
              )
            continue
          website_queues.setdefault(name, deque()).append(
            (query_item.query_text, query_item.category),
          )
      else:
        if self.scrapers is None or website_name in self.scrapers:
          # Skip scrapers that opt out of discovery
          try:
            scraper = self.monitor.get_scraper(website_name)
            if scraper.capabilities.exclude_from_discover:
              continue
          except Exception:
            pass

          # Limit timeout websites to one query per cycle
          if self.failure_tracker.is_in_timeout(website_name):
            if website_name not in disabled_websites:
              disabled_websites.append(website_name)
            if self.failure_tracker.can_make_timeout_query(website_name):
              website_queues.setdefault(website_name, deque()).append(
                (query_item.query_text, query_item.category),
              )
            continue
          website_queues.setdefault(website_name, deque()).append(
            (query_item.query_text, query_item.category),
          )

    # Show warning for timeout websites
    if disabled_websites:
      timeout_info = []
      for website_name in disabled_websites:
        timeout_list = self.failure_tracker.get_timeout_websites()
        for timeout_name, timeout_until in timeout_list:
          if timeout_name == website_name:
            remaining = timeout_until - time.time()
            timeout_info.append(
              f"{website_name} (exits timeout in {_fmt_duration(remaining)})"
            )
            break
      console.print(
        f"  [yellow]⚠ Warning: {len(disabled_websites)} website(s) "
        f"in timeout mode (limited to 1 query/cycle): {', '.join(timeout_info)}[/yellow]"
      )

    total_searches = sum(len(q) for q in website_queues.values())
    if total_searches == 0:
      console.print("  [dim]No searches to run.[/dim]")
      return 0, 0

    phase_timeout = self.config.smart_monitoring.phase_timeout
    throttle = self.monitor.throttle

    # Results aggregated from worker threads.
    total_new = 0
    failed_count = 0
    deferred_count = 0
    new_per_website: Dict[str, int] = {}
    website_order = list(website_queues.keys())

    # --- Per-website worker function -----------------------------

    def _discover_worker(
      website_name: str,
      search_queue: list,
      progress: Progress,
      overall_task_id,
      website_task_id,
    ) -> Tuple[str, int, int, int]:
      """Run all searches for one website.  Returns
      ``(website_name, new_count, failed, deferred)``.
      """
      deadline = time.time() + phase_timeout
      new_count = 0
      failed = 0
      deferred = 0

      for query_text, category in search_queue:
        if self._shutdown_requested or time.time() > deadline:
          logger.debug(
            "Deferred search '%s' on %s — phase timeout",
            query_text[:40] if query_text else "?", website_name,
          )
          deferred += 1
          continue

        throttle.wait_if_needed(website_name)

        # Mark timeout query as used when we start processing
        if self.failure_tracker.is_in_timeout(website_name):
          self.failure_tracker.mark_timeout_query_used(website_name)

        try:
          scraper = self.monitor.get_scraper(website_name)
          count = self.monitor.run_search(
            scraper, query_text, category=category,
          )
          new_count += count
          # Record success if we got results or no error occurred
          self.failure_tracker.record_success(website_name)
        except Exception:
          failed += 1
          self.failure_tracker.record_failure(website_name)
          logger.exception(
            "Search '%s' failed on %s", query_text, website_name,
          )
          with self._progress_lock:
            progress.update(website_task_id, errors=failed)

        throttle.mark_used(website_name)
        self._advance_progress(progress, overall_task_id, website_task_id)

      return website_name, new_count, failed, deferred

    # --- Run workers in parallel ---------------------------------

    with self._make_progress() as progress:
      overall_task = progress.add_task(
        "Searches", total=total_searches,
      )
      website_tasks: Dict[str, int] = {}
      for website_name in website_order:
        count = len(website_queues[website_name])
        task_id = progress.add_task(
          f"  {website_name}", total=count, errors=0,
        )
        website_tasks[website_name] = task_id

      with ThreadPoolExecutor(
        max_workers=len(website_order),
        thread_name_prefix="discover",
      ) as executor:
        futures = {
          executor.submit(
            _discover_worker,
            name,
            list(website_queues[name]),
            progress,
            overall_task,
            website_tasks[name],
          ): name
          for name in website_order
        }

        for future in as_completed(futures):
          website_name = futures[future]
          try:
            _name, new_count, fails, deferred = future.result()
            total_new += new_count
            failed_count += fails
            deferred_count += deferred
            if new_count > 0:
              new_per_website[website_name] = new_count
          except Exception:
            logger.exception(
              "Discovery worker for %s raised an exception",
              website_name,
            )
            failed_count += 1

    # Summary line.
    parts = []
    for website_name in website_order:
      new_count = new_per_website.get(website_name, 0)
      if new_count > 0:
        parts.append(f"{website_name} +{new_count}")
    if parts:
      console.print(
        f"  [green]Discovery: {total_new} new listing(s) "
        f"({', '.join(parts)})[/green]",
      )
    else:
      console.print("  [dim]Discovery: no new listings.[/dim]")
    if failed_count > 0:
      console.print(
        f"  [red]{failed_count} search(es) failed — see log for details.[/red]",
      )
    if deferred_count > 0:
      console.print(
        f"  [dim]{deferred_count} search(es) deferred (timeout).[/dim]",
      )

    return total_new, failed_count

  # ----------------------------------------------------------------
  # Phase 2 — Initial fetch (parallel per website)
  # ----------------------------------------------------------------

  def _phase_initial_fetch(self) -> Tuple[int, int, int]:
    """Fetch full details for listings discovered by search but
    never fully fetched.

    Each website's listings are processed in a dedicated thread.
    A per-website phase timeout limits how long each website can
    take; remaining listings are deferred to the next cycle.

    Returns ``(fetched_count, failed_count, deferred_count)``.
    """
    registered_scrapers = set(ScraperRegistry.list_registered())
    allowed_scrapers = (
      set(self.scrapers) if self.scrapers else registered_scrapers
    )

    # Identify disabled scrapers (e.g. historical-only).
    excluded_scrapers = set()
    for name in registered_scrapers:
      try:
        scraper = self.monitor.get_scraper(name)
        if scraper.capabilities.exclude_from_discover:
          excluded_scrapers.add(name)
      except Exception:
        pass

    website_queues: Dict[str, deque] = {}
    total_count = 0

    with session_scope() as session:
      unfetched = get_unfetched_active_listings(session)

      disabled_websites = []
      for listing in unfetched:
        if not listing.website:
          continue
        scraper_name = _normalize_website_name(listing.website.name)
        if scraper_name not in registered_scrapers:
          scraper_name = scraper_name.replace(" ", "_")
          if scraper_name not in registered_scrapers:
            continue
        if scraper_name not in allowed_scrapers:
          continue
        if scraper_name in excluded_scrapers:
          continue
        # Limit timeout websites to one query per cycle
        if self.failure_tracker.is_in_timeout(scraper_name):
          if scraper_name not in disabled_websites:
            disabled_websites.append(scraper_name)
          if self.failure_tracker.can_make_timeout_query(scraper_name):
            website_queues.setdefault(scraper_name, deque()).append(
              (listing.external_id, listing.url),
            )
            total_count += 1
          continue
        website_queues.setdefault(scraper_name, deque()).append(
          (listing.external_id, listing.url),
        )
        total_count += 1

      # Show warning for timeout websites
      if disabled_websites:
        timeout_info = []
        for website_name in disabled_websites:
          timeout_list = self.failure_tracker.get_timeout_websites()
          for timeout_name, timeout_until in timeout_list:
            if timeout_name == website_name:
              remaining = timeout_until - time.time()
              timeout_info.append(
                f"{website_name} (exits timeout in {_fmt_duration(remaining)})"
              )
              break
        console.print(
          f"  [yellow]⚠ Warning: {len(disabled_websites)} website(s) "
          f"in timeout mode (limited to 1 query/cycle): {', '.join(timeout_info)}[/yellow]"
        )

    if total_count == 0:
      console.print("  [dim]No unfetched listings to process.[/dim]")
      return 0, 0, 0

    phase_timeout = self.config.smart_monitoring.phase_timeout
    website_order = list(website_queues.keys())
    throttle = self.monitor.throttle

    console.print(
      f"  Fetching details for [bold]{total_count}[/bold] listing(s) "
      f"across {len(website_queues)} website(s) "
      f"(timeout {_fmt_duration(phase_timeout)})…",
    )

    # Shared counters (protected by a lock).
    counters_lock = threading.Lock()
    counters = {"fetched": 0, "failed": 0, "rejected": 0, "deferred": 0}

    # --- Per-website worker function -----------------------------

    def _fetch_worker(
      website_name: str,
      listings_queue: list,
      progress: Progress,
      overall_task_id,
      website_task_id,
    ) -> None:
      """Fetch all unfetched listings for one website."""
      deadline = time.time() + phase_timeout
      website_errors = 0

      for external_id, url in listings_queue:
        if self._shutdown_requested or time.time() > deadline:
          logger.debug(
            "Deferred fetch %s on %s — phase timeout",
            external_id, website_name,
          )
          with counters_lock:
            counters["deferred"] += 1
          continue

        throttle.wait_if_needed(website_name)

        # Mark timeout query as used when we start processing
        if self.failure_tracker.is_in_timeout(website_name):
          self.failure_tracker.mark_timeout_query_used(website_name)

        try:
          listing_id = self.monitor.ingest_listing(
            self.monitor.get_scraper(website_name), url,
          )
          with counters_lock:
            counters["fetched"] += 1
          # Record success
          self.failure_tracker.record_success(website_name)
          # Check if the listing was rejected by the classifier.
          with thread_safe_session_scope() as session:
            listing = session.get(Listing, listing_id)
            if listing and listing.status == ListingStatus.CANCELLED:
              with counters_lock:
                counters["rejected"] += 1
        except Exception as exception:
          website_errors += 1
          with counters_lock:
            counters["failed"] += 1
          # Record failure
          self.failure_tracker.record_failure(website_name)
          logger.exception(
            "  Failed to fetch %s (%s)", external_id, website_name,
          )
          with self._progress_lock:
            progress.update(website_task_id, errors=website_errors)

          # Mark listings that are not found as cancelled to prevent
          # retrying them indefinitely.
          if "not found" in str(exception).lower():
            try:
              with thread_safe_session_scope() as session:
                listing = (
                  session.query(Listing)
                  .filter(Listing.url == url)
                  .first()
                )
                if listing and not listing.is_fully_fetched:
                  listing.status = ListingStatus.CANCELLED
                  listing.is_fully_fetched = True
                  session.commit()
                  logger.info(
                    "  Marked listing %s (%s) as CANCELLED "
                    "due to 'not found' error",
                    external_id, website_name,
                  )
            except Exception:
              logger.exception(
                "  Failed to mark listing %s (%s) as cancelled",
                external_id, website_name,
              )

        throttle.mark_used(website_name)
        self._advance_progress(progress, overall_task_id, website_task_id)

    # --- Run workers in parallel ---------------------------------

    with self._make_progress() as progress:
      overall_task = progress.add_task(
        "Initial fetch", total=total_count,
      )
      website_tasks: Dict[str, int] = {}
      for website_name in website_order:
        count = len(website_queues[website_name])
        task_id = progress.add_task(
          f"  {website_name}", total=count, errors=0,
        )
        website_tasks[website_name] = task_id

      with ThreadPoolExecutor(
        max_workers=len(website_order),
        thread_name_prefix="fetch",
      ) as executor:
        futures = {
          executor.submit(
            _fetch_worker,
            name,
            list(website_queues[name]),
            progress,
            overall_task,
            website_tasks[name],
          ): name
          for name in website_order
        }

        for future in as_completed(futures):
          website_name = futures[future]
          try:
            future.result()
          except Exception:
            logger.exception(
              "Fetch worker for %s raised an exception",
              website_name,
            )

    # Summary.
    fetched = counters["fetched"]
    failed = counters["failed"]
    rejected = counters["rejected"]
    deferred_count = counters["deferred"]

    parts = [f"{fetched} fetched"]
    if rejected:
      parts.append(f"{rejected} rejected")
    if deferred_count:
      parts.append(f"{deferred_count} deferred")
    console.print(f"  [green]Initial fetch: {', '.join(parts)}[/green]")
    if failed:
      console.print(
        f"  [red]{failed} fetch(es) failed — see log for details.[/red]",
      )

    return fetched, failed, deferred_count


# ==================================================================
# Watch loop — timing-aware monitoring of active listings
# ==================================================================

class WatchLoop(_BaseLoop):
  """Monitor active listings with timing-aware, strategy-based
  scheduling.

  Periodically scans the database for newly fetched listings (added
  by :class:`DiscoveryLoop`) and schedules them for monitoring.  Due
  tasks are grouped by website and processed in parallel.

  Designed to run as a separate process alongside
  :class:`DiscoveryLoop`.
  """

  # How often to check the database for newly fetched listings that
  # were added by the DiscoveryLoop process (seconds).
  _DB_RELOAD_INTERVAL = 30.0

  def __init__(
    self,
    config: AppConfig,
    *,
    scrapers: Optional[list[str]] = None,
    verbose: bool = False,
  ) -> None:
    super().__init__(config, scrapers=scrapers, verbose=verbose)

    # Tracking state (accessed from the main thread only; per-website
    # worker threads return results but never mutate these directly).
    self.tracked: Dict[int, TrackedListing] = {}
    self.task_queue: list = []
    self._counter = 0
    self.finished_ids: Set[int] = set()

    # Cache the strategy for each scraper name so we only look it up
    # once per scraper rather than once per listing.
    self._strategy_cache: Dict[str, str] = {}

    # Lock protecting the shared priority queue and its counter
    # from concurrent _push_task / _collect_due_tasks calls across
    # per-website worker threads.
    self._queue_lock = threading.Lock()

  def _get_strategy(self, scraper_name: str) -> str:
    """Return the monitoring strategy for *scraper_name* (cached)."""
    if scraper_name not in self._strategy_cache:
      self._strategy_cache[scraper_name] = _resolve_strategy(scraper_name)
    return self._strategy_cache[scraper_name]

  # ----------------------------------------------------------------
  # Scheduling helpers
  # ----------------------------------------------------------------

  def _push_task(self, listing_id: int, run_at: float) -> None:
    """Push a check task onto the priority queue (thread-safe)."""
    with self._queue_lock:
      self._counter += 1
      heapq.heappush(self.task_queue, (run_at, self._counter, listing_id))

  def _compute_next_check(self, tracked: TrackedListing) -> float:
    """Decide when to next check *tracked* and update its phase.

    Returns a Unix timestamp for the next check.
    """
    config = self.config.smart_monitoring
    now = time.time()
    end_timestamp = _to_timestamp(tracked.end_time)
    strategy = tracked.strategy

    if strategy == STRATEGY_POST_AUCTION:
      return self._schedule_post_auction(tracked, config, now, end_timestamp)
    if strategy == STRATEGY_SNAPSHOT:
      return self._schedule_snapshot(tracked, config, now, end_timestamp)
    return self._schedule_full(tracked, config, now, end_timestamp)

  # ---------- schedule helpers per strategy --------------------

  def _schedule_full(self, tracked, config, now, end_timestamp):
    """Schedule for FULL strategy (Catawiki): aggressive + extensions."""
    if end_timestamp is None:
      tracked.phase = PHASE_ROUTINE
      return now + config.daily_refresh_interval

    remaining = end_timestamp - now

    if remaining <= 0:
      tracked.phase = PHASE_ENDING
      return now + config.full_ending_poll_interval

    if remaining <= config.full_imminent_threshold:
      tracked.phase = PHASE_IMMINENT
      return now + config.full_imminent_interval

    if remaining <= config.full_approaching_threshold:
      tracked.phase = PHASE_APPROACHING
      return min(
        now + config.full_approaching_interval,
        end_timestamp - config.full_imminent_threshold,
      )

    tracked.phase = PHASE_ROUTINE
    return min(
      now + config.daily_refresh_interval,
      end_timestamp - config.full_approaching_threshold,
    )

  def _schedule_snapshot(self, tracked, config, now, end_timestamp):
    """Schedule for SNAPSHOT strategy (eBay, Yahoo Japan): periodic snapshots."""
    if end_timestamp is None:
      tracked.phase = PHASE_ROUTINE
      return now + config.snapshot_interval

    remaining = end_timestamp - now

    if remaining <= 0:
      tracked.phase = PHASE_ENDING
      return now + config.snapshot_ending_poll_interval

    if remaining <= config.snapshot_imminent_threshold:
      tracked.phase = PHASE_IMMINENT
      return now + config.snapshot_imminent_interval

    if remaining <= config.snapshot_approaching_threshold:
      tracked.phase = PHASE_APPROACHING
      return min(
        now + config.snapshot_approaching_interval,
        end_timestamp - config.snapshot_imminent_threshold,
      )

    tracked.phase = PHASE_ROUTINE
    return min(
      now + config.snapshot_interval,
      end_timestamp - config.snapshot_approaching_threshold,
    )

  def _schedule_post_auction(self, tracked, config, now, end_timestamp):
    """Schedule for POST-AUCTION strategy (Drouot, etc.)."""
    if end_timestamp is None:
      tracked.phase = PHASE_ROUTINE
      return now + config.daily_refresh_interval

    remaining = end_timestamp - now

    if remaining > 0:
      if tracked.last_fetched_at == 0:
        tracked.phase = PHASE_ROUTINE
        return now
      tracked.phase = PHASE_WAITING
      return end_timestamp + config.post_auction_delay

    tracked.phase = PHASE_ENDING
    if tracked.last_fetched_at == 0:
      return now
    if tracked.post_end_checks == 0:
      time_since_end = now - end_timestamp
      if time_since_end < config.post_auction_delay:
        return end_timestamp + config.post_auction_delay
    return now + config.post_auction_recheck

  # ----------------------------------------------------------------
  # Loading listings from the database
  # ----------------------------------------------------------------

  def _load_tracked_listings(self) -> int:
    """Scan the database for active/upcoming *fully-fetched* listings
    and start tracking any that are new.

    Returns the number of newly tracked listings.
    """
    registered_scrapers = set(ScraperRegistry.list_registered())
    new_count = 0

    allowed_scrapers = set(self.scrapers) if self.scrapers else registered_scrapers

    with session_scope() as session:
      active_listings = get_active_listings(
        session, join_website=True, include_unknown=True,
      )
    total_active = len(active_listings)
    logger.debug(
      "_load_tracked_listings: %d active/upcoming listing(s) in database",
      total_active,
    )

    for listing in active_listings:
      if listing.id in self.tracked or listing.id in self.finished_ids:
        if listing.id in self.tracked and listing.end_time:
          tracked = self.tracked[listing.id]
          tracked.end_time = listing.end_time
          next_check = self._compute_next_check(tracked)
          if next_check <= time.time() + 60:
            self._push_task(listing.id, next_check)
            logger.debug(
              "Re-scheduled listing #%d (end_time updated), next_check in %.0fs",
              listing.id, next_check - time.time(),
            )
        continue

      if not listing.is_fully_fetched and listing.status != ListingStatus.UNKNOWN:
        logger.debug("Skipping listing #%d (not fully fetched)", listing.id)
        continue

      scraper_name = None
      if listing.website:
        candidate = _normalize_website_name(listing.website.name)
        for registered_name in registered_scrapers:
          if _normalize_website_name(registered_name) == candidate:
            scraper_name = registered_name
            break
        if scraper_name is None:
          candidate_underscore = candidate.replace(" ", "_")
          for registered_name in registered_scrapers:
            if _normalize_website_name(registered_name) == candidate_underscore:
              scraper_name = registered_name
              break

      if scraper_name is None:
        continue

      if scraper_name not in allowed_scrapers:
        continue

      # Skip historical-only scrapers (Gazette Drouot, etc.)
      # These maintain static archives and don't need real-time monitoring.
      try:
        scraper_instance = self.monitor.get_scraper(scraper_name)
        if scraper_instance.capabilities.is_historical_only:
          continue
      except Exception:
        # If we can't load the scraper, we can't monitor it anyway.
        continue

      strategy = self._get_strategy(scraper_name)

      tracked = TrackedListing(
        listing_id=listing.id,
        external_id=listing.external_id,
        url=listing.url,
        title=(listing.title[:60] if listing.title else "?"),
        scraper_name=scraper_name,
        end_time=listing.end_time,
        original_end_time=listing.end_time,
        strategy=strategy,
        last_fetched_at=_to_timestamp(listing.last_checked_at) or 0.0,
      )
      self.tracked[listing.id] = tracked
      next_check = self._compute_next_check(tracked)
      self._push_task(listing.id, next_check)
      logger.debug(
        "Tracking listing #%d [%s], next_check in %.0fs",
        listing.id, strategy, next_check - time.time(),
      )
      new_count += 1

    return new_count

  # ----------------------------------------------------------------
  # Processing a single listing
  # ----------------------------------------------------------------

  def _process_listing(self, listing_id: int) -> bool:
    """Fetch the latest data for a listing, update the database, and
    either reschedule or remove it from tracking.

    Returns ``True`` on success, ``False`` on failure.
    """
    if listing_id not in self.tracked:
      return True

    tracked = self.tracked[listing_id]
    symbol = _PHASE_SYMBOLS.get(tracked.phase, "?")
    strat_label = _STRATEGY_LABELS.get(tracked.strategy, tracked.strategy)

    remaining_label = ""
    end_timestamp = _to_timestamp(tracked.end_time)
    if end_timestamp is not None:
      remaining_seconds = end_timestamp - time.time()
      if remaining_seconds > 0:
        remaining_label = f" — ends in {_fmt_duration(remaining_seconds)}"
      else:
        remaining_label = f" — {_fmt_duration(abs(remaining_seconds))} past end"

    logger.info(
      "%s [%s/%s] #%d: %s%s",
      symbol,
      tracked.phase.upper(),
      strat_label,
      listing_id,
      tracked.title,
      remaining_label,
    )

    try:
      t0 = time.time()
      scraper = self.monitor.get_scraper(tracked.scraper_name)
      scraped = scraper.fetch_listing(tracked.url)
      tracked.last_fetched_at = time.time()
      elapsed = tracked.last_fetched_at - t0

      self.monitor._store_scraped_listing(scraper, scraped)

      price_str = _fmt_price(
        scraped.current_price or scraped.final_price,
        scraped.currency,
      )
      tracked.last_price = price_str
      bid_str = f"{len(scraped.bids)} bid(s)" if scraped.bids else "no bids"

      logger.info(
        "   → %s, %s, status=%s (%.1fs)",
        price_str, bid_str, scraped.status.value, elapsed,
      )

      terminal_statuses = (
        ListingStatus.SOLD,
        ListingStatus.UNSOLD,
        ListingStatus.CANCELLED,
      )
      if scraped.status in terminal_statuses:
        self._handle_ended(listing_id, scraped, tracked)
        return True

      if tracked.strategy == STRATEGY_FULL:
        self._post_fetch_full(tracked, scraped, listing_id)
      elif tracked.strategy == STRATEGY_SNAPSHOT:
        self._post_fetch_snapshot(tracked, scraped, listing_id)
      elif tracked.strategy == STRATEGY_POST_AUCTION:
        self._post_fetch_post_auction(tracked, scraped, listing_id)
      else:
        self._post_fetch_full(tracked, scraped, listing_id)

      return True

    except Exception:
      logger.exception("   ✗ Failed to check listing #%d", listing_id)
      self._push_task(listing_id, time.time() + 60)
      return False

  # ---- Strategy-specific post-fetch handlers --------------------

  def _post_fetch_full(self, tracked, scraped, listing_id):
    """Post-fetch logic for the FULL strategy (Catawiki)."""
    config = self.config.smart_monitoring

    if scraped.end_time is not None and tracked.end_time is not None:
      old_end = _to_timestamp(tracked.end_time)
      new_end = _to_timestamp(scraped.end_time)
      if new_end is not None and old_end is not None and new_end > old_end + 5:
        tracked.extension_count += 1
        logger.info(
          "   ⏰ EXTENDED! %s → %s (extension #%d, +%s)",
          tracked.end_time.strftime("%H:%M:%S"),
          scraped.end_time.strftime("%H:%M:%S"),
          tracked.extension_count,
          _fmt_duration(new_end - old_end),
        )
        tracked.end_time = scraped.end_time
    elif scraped.end_time is not None and tracked.end_time is None:
      tracked.end_time = scraped.end_time
      tracked.original_end_time = scraped.end_time
      logger.info(
        "   End time discovered: %s",
        scraped.end_time.strftime("%Y-%m-%d %H:%M:%S"),
      )

    if tracked.phase == PHASE_ENDING and tracked.end_time is not None:
      current_end = _to_timestamp(tracked.end_time)
      if current_end is not None:
        time_past_end = time.time() - current_end
        max_wait = config.full_ending_max_wait
        # Clean up
        if time_past_end > max_wait:
          logger.warning(
            "   Giving up on listing #%d: %s past end time (max wait %s)",
            listing_id, _fmt_duration(time_past_end), _fmt_duration(max_wait),
          )

          # Set a final status in the DB since the scraper never returned
          # a terminal status and we're giving up.
          with thread_safe_session_scope() as session:
            listing = session.get(Listing, listing_id)
            if listing and listing.status not in (
              ListingStatus.SOLD, ListingStatus.UNSOLD,
              ListingStatus.CANCELLED,
            ):
              # If we have a price and bids, it likely sold but we missed the
              # specific "SOLD" status. Otherwise assume UNSOLD.
              if scraped.current_price and scraped.bid_count and scraped.bid_count > 0:
                   listing.status = ListingStatus.SOLD
                   listing.final_price = scraped.current_price
              else:
                   listing.status = ListingStatus.UNSOLD

              listing.last_checked_at = datetime.now(timezone.utc)
              session.flush()
              logger.info(
                "   Marked listing #%d as %s (gave up waiting for result).",
                listing_id,
                listing.status.value,
              )

          self.finished_ids.add(listing_id)
          del self.tracked[listing_id]
          return

    self._reschedule(tracked, listing_id)

  def _post_fetch_snapshot(self, tracked, scraped, listing_id):
    """Post-fetch logic for the SNAPSHOT strategy (eBay)."""
    config = self.config.smart_monitoring

    if scraped.end_time is not None:
      if tracked.end_time is None:
        tracked.end_time = scraped.end_time
        tracked.original_end_time = scraped.end_time
        logger.info(
          "   End time discovered: %s",
          scraped.end_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
      elif tracked.end_time:
        # If the end time changed significantly, update our tracking.
        delta = abs(_to_timestamp(scraped.end_time) - _to_timestamp(tracked.end_time))
        if delta > 600:  # > 10 minutes change
          logger.info(
            "   Listing #%d end time changed: %s -> %s",
            listing_id, tracked.end_time, scraped.end_time,
          )
          tracked.end_time = scraped.end_time
          # Reset phase to routine if we're not close to the new end time
          if tracked.phase != PHASE_ROUTINE:
              time_to_end = (tracked.end_time - datetime.now(timezone.utc)).total_seconds()
              if time_to_end > 3600:
                  tracked.phase = PHASE_ROUTINE

    if tracked.phase == PHASE_ENDING and tracked.end_time is not None:
      current_end = _to_timestamp(tracked.end_time)
      if current_end is not None:
        time_past_end = time.time() - current_end
        max_wait = config.snapshot_ending_max_wait
        if time_past_end > max_wait:
          logger.warning(
            "   ⚠ Listing #%d: %s past end time (fixed), "
            "marking as finished.",
            listing_id,
            _fmt_duration(time_past_end),
          )
          # Set a final status in the DB since the scraper never returned
          # a terminal status and we're giving up.
          with thread_safe_session_scope() as session:
            listing = session.get(Listing, listing_id)
            if listing and listing.status not in (
              ListingStatus.SOLD, ListingStatus.UNSOLD,
              ListingStatus.CANCELLED,
            ):
              listing.status = ListingStatus.UNSOLD
              listing.last_checked_at = datetime.now(timezone.utc)
              session.flush()
              logger.info("   Marked listing #%d as UNSOLD (expired).", listing_id)

          self.finished_ids.add(listing_id)
          del self.tracked[listing_id]
          return

    self._reschedule(tracked, listing_id)

  def _post_fetch_post_auction(self, tracked, scraped, listing_id):
    """Post-fetch logic for the POST-AUCTION strategy."""
    config = self.config.smart_monitoring

    if scraped.end_time is not None and tracked.end_time is None:
      tracked.end_time = scraped.end_time
      tracked.original_end_time = scraped.end_time
      logger.info(
        "   End time discovered: %s",
        scraped.end_time.strftime("%Y-%m-%d %H:%M:%S"),
      )

    end_timestamp = _to_timestamp(tracked.end_time)
    if end_timestamp is not None and time.time() > end_timestamp:
      tracked.post_end_checks += 1

    if tracked.phase == PHASE_ENDING and tracked.end_time is not None:
      if end_timestamp is not None:
        time_past_end = time.time() - end_timestamp
        max_wait = config.post_auction_max_wait
        if time_past_end > max_wait:
          logger.warning(
            "   ⚠ Listing #%d: %s past end time, no result "
            "after %d check(s). Giving up.",
            listing_id,
            _fmt_duration(time_past_end),
            tracked.post_end_checks,
          )
          # Set a final status in the DB since the scraper never returned
          # a terminal status and we're giving up.
          with thread_safe_session_scope() as session:
            listing = session.get(Listing, listing_id)
            if listing and listing.status not in (
              ListingStatus.SOLD, ListingStatus.UNSOLD,
              ListingStatus.CANCELLED,
            ):
              listing.status = ListingStatus.UNSOLD
              listing.last_checked_at = datetime.now(timezone.utc)
              session.flush()
              logger.info("   Marked listing #%d as UNSOLD (post-auction give up).", listing_id)

          self.finished_ids.add(listing_id)
          del self.tracked[listing_id]
          return

    self._reschedule(tracked, listing_id)

  # ---- Shared reschedule helper ----------------------------------

  def _reschedule(self, tracked, listing_id):
    """Compute and push the next check for *tracked*."""
    next_check = self._compute_next_check(tracked)
    self._push_task(listing_id, next_check)

    sleep_for = max(0, next_check - time.time())
    logger.debug(
      "   Next check in %s [%s/%s]",
      _fmt_duration(sleep_for),
      tracked.phase,
      _STRATEGY_LABELS.get(tracked.strategy, tracked.strategy),
    )

  # ----------------------------------------------------------------
  # Handling ended auctions
  # ----------------------------------------------------------------

  def _handle_ended(
    self,
    listing_id: int,
    scraped,
    tracked: TrackedListing,
  ) -> None:
    """Log and clean up when a listing reaches a terminal status."""
    final_price = scraped.final_price or scraped.current_price
    price_str = _fmt_price(final_price, scraped.currency)

    logger.info(
      "   🏆 ENDED — status: %s | final: %s | bids: %d",
      scraped.status.value,
      price_str,
      len(scraped.bids),
    )
    if tracked.extension_count > 0:
      original = (
        tracked.original_end_time.strftime("%H:%M:%S")
        if tracked.original_end_time else "?"
      )
      final = (
        tracked.end_time.strftime("%H:%M:%S")
        if tracked.end_time else "?"
      )
      logger.info(
        "   Extended %d time(s): %s → %s",
        tracked.extension_count, original, final,
      )

    self.finished_ids.add(listing_id)
    del self.tracked[listing_id]

  # ----------------------------------------------------------------
  # Status report
  # ----------------------------------------------------------------

  def _log_status(self) -> None:
    """Log a summary of the current tracking state."""
    if not self.tracked:
      logger.info("No listings being tracked. Finished: %d.", len(self.finished_ids))
      return

    phase_counts: Dict[str, int] = {}
    strategy_counts: Dict[str, int] = {}
    for tracked in self.tracked.values():
      phase_counts[tracked.phase] = phase_counts.get(tracked.phase, 0) + 1
      strategy_counts[tracked.strategy] = strategy_counts.get(tracked.strategy, 0) + 1

    phase_parts = []
    for phase in (PHASE_ROUTINE, PHASE_APPROACHING, PHASE_IMMINENT, PHASE_ENDING, PHASE_WAITING):
      count = phase_counts.get(phase, 0)
      if count > 0:
        symbol = _PHASE_SYMBOLS.get(phase, "")
        phase_parts.append(f"{symbol} {count} {phase}")

    strat_parts = []
    for strat in (STRATEGY_FULL, STRATEGY_SNAPSHOT, STRATEGY_POST_AUCTION):
      count = strategy_counts.get(strat, 0)
      if count > 0:
        strat_parts.append(f"{_STRATEGY_LABELS[strat]}={count}")

    logger.info(
      "━━━ Status: tracking %d listing(s) [%s] strategies=[%s] — finished: %d ━━━",
      len(self.tracked),
      ", ".join(phase_parts),
      ", ".join(strat_parts),
      len(self.finished_ids),
    )

    upcoming = []
    now = time.time()
    for run_at, _counter, lid in sorted(self.task_queue)[:5]:
      if lid in self.tracked:
        t = self.tracked[lid]
        strat = _STRATEGY_LABELS.get(t.strategy, t.strategy)
        upcoming.append(
          f"  #{lid} [{t.phase}/{strat}] in {_fmt_duration(run_at - now)}: "
          f"{t.title} ({t.last_price or '–'})"
        )
    if upcoming:
      logger.info("Next checks:\n%s", "\n".join(upcoming))

  # ----------------------------------------------------------------
  # Monitoring phase (process due tasks, parallel per website)
  # ----------------------------------------------------------------

  def _phase_monitor(self) -> Tuple[int, int]:
    """Process all due monitoring tasks from the priority queue.

    Tasks are grouped by website and each website's batch runs in a
    dedicated thread.  A per-website phase timeout limits how long
    each website can take.

    Returns a tuple of ``(tasks_processed, failures)``.
    """
    due_tasks = self._collect_due_tasks()
    if not due_tasks:
      logger.debug("No monitoring tasks due right now (queue size: %d)", len(self.task_queue))
      console.print("  [dim]No monitoring tasks due right now.[/dim]")
      return 0, 0

    website_task_lists: Dict[str, List[int]] = {}
    timeout_websites = []
    for listing_id in due_tasks:
      tracked = self.tracked.get(listing_id)
      if tracked:
        # Limit timeout websites to one query per cycle
        if self.failure_tracker.is_in_timeout(tracked.scraper_name):
          if tracked.scraper_name not in timeout_websites:
            timeout_websites.append(tracked.scraper_name)
          if self.failure_tracker.can_make_timeout_query(tracked.scraper_name):
            website_task_lists.setdefault(tracked.scraper_name, []).append(
              listing_id,
            )
          else:
            # Reschedule for later (already used timeout query this cycle)
            self._push_task(listing_id, time.time() + 300.0)  # Try again in 5 minutes
          continue
        website_task_lists.setdefault(tracked.scraper_name, []).append(
          listing_id,
        )

    # Show warning for timeout websites
    if timeout_websites:
      timeout_info = []
      for website_name in timeout_websites:
        timeout_list = self.failure_tracker.get_timeout_websites()
        for timeout_name, timeout_until in timeout_list:
          if timeout_name == website_name:
            remaining = timeout_until - time.time()
            timeout_info.append(
              f"{website_name} (exits timeout in {_fmt_duration(remaining)})"
            )
            break
      console.print(
        f"  [yellow]⚠ Warning: {len(timeout_websites)} website(s) "
        f"in timeout mode (limited to 1 query/cycle): {', '.join(timeout_info)}[/yellow]"
      )

    count_parts = ", ".join(
      f"{name} {len(ids)}" for name, ids in website_task_lists.items()
    )
    console.print(
      f"  Processing [bold]{len(due_tasks)}[/bold] due task(s) "
      f"({count_parts})…",
    )

    ended_count = len(self.finished_ids)
    phase_timeout = self.config.smart_monitoring.phase_timeout
    throttle = self.monitor.throttle
    website_order = list(website_task_lists.keys())

    failure_lock = threading.Lock()
    failure_count = [0]
    deferred_count = [0]

    def _monitor_worker(
      website_name: str,
      listing_ids: list,
      progress: Progress,
      overall_task_id,
      website_task_id,
    ) -> None:
      """Process all due monitoring tasks for one website."""
      deadline = time.time() + phase_timeout
      website_errors = 0

      for listing_id in listing_ids:
        if self._shutdown_requested or time.time() > deadline:
          if listing_id in self.tracked:
            self._push_task(listing_id, time.time())
          logger.debug(
            "Deferred listing #%d (%s) — phase timeout",
            listing_id, website_name,
          )
          with failure_lock:
            deferred_count[0] += 1
          continue

        throttle.wait_if_needed(website_name)

        # Mark timeout query as used when we start processing
        if self.failure_tracker.is_in_timeout(website_name):
          self.failure_tracker.mark_timeout_query_used(website_name)

        success = self._process_listing(listing_id)
        if success:
          # Record success
          self.failure_tracker.record_success(website_name)
        else:
          website_errors += 1
          # Record failure
          self.failure_tracker.record_failure(website_name)
          with failure_lock:
            failure_count[0] += 1
          with self._progress_lock:
            progress.update(website_task_id, errors=website_errors)

        throttle.mark_used(website_name)
        self._advance_progress(progress, overall_task_id, website_task_id)

    with self._make_progress() as progress:
      overall_task = progress.add_task(
        "Monitoring", total=len(due_tasks),
      )
      website_tasks: Dict[str, int] = {}
      for website_name in website_order:
        count = len(website_task_lists[website_name])
        task_id = progress.add_task(
          f"  {website_name}", total=count, errors=0,
        )
        website_tasks[website_name] = task_id

      with ThreadPoolExecutor(
        max_workers=len(website_task_lists),
        thread_name_prefix="monitor",
      ) as executor:
        futures = {
          executor.submit(
            _monitor_worker,
            name,
            listing_ids,
            progress,
            overall_task,
            website_tasks[name],
          ): name
          for name, listing_ids in website_task_lists.items()
        }

        for future in as_completed(futures):
          website_name = futures[future]
          try:
            future.result()
          except Exception:
            logger.exception(
              "Monitor worker for %s raised an exception",
              website_name,
            )

    failed = failure_count[0]
    deferred = deferred_count[0]
    newly_ended = len(self.finished_ids) - ended_count

    summary_parts = []
    if newly_ended > 0:
      summary_parts.append(
        f"[yellow]{newly_ended} auction(s) ended this cycle.[/yellow]",
      )
    if deferred > 0:
      summary_parts.append(
        f"[dim]{deferred} task(s) deferred (timeout).[/dim]",
      )
    if failed > 0:
      summary_parts.append(
        f"[red]{failed} check(s) failed — see log for details.[/red]",
      )
    for part in summary_parts:
      console.print(f"  {part}")

    return len(due_tasks), failed

  # ----------------------------------------------------------------
  # Task collection
  # ----------------------------------------------------------------

  def _collect_due_tasks(self) -> List[int]:
    """Pop all tasks from the queue whose run_at time has passed."""
    due: List[int] = []
    with self._queue_lock:
      while self.task_queue and self.task_queue[0][0] <= time.time():
        _timestamp, _counter, listing_id = heapq.heappop(self.task_queue)
        if listing_id in self.tracked:
          due.append(listing_id)
    if due:
      logger.debug(
        "_collect_due_tasks: %d task(s) due (sample IDs: %s)",
        len(due), due[:10] if len(due) > 10 else due,
      )
    return due

  # ----------------------------------------------------------------
  # Main loop
  # ----------------------------------------------------------------

  def _run_loop(self) -> None:
    console.rule("[bold]Watch Loop[/bold]")
    console.print(
      f"[dim]Phase timeout: "
      f"{_fmt_duration(self.config.smart_monitoring.phase_timeout)}[/dim]",
    )

    count = self._load_tracked_listings()
    console.print(
      f"Loaded [bold]{count}[/bold] active listing(s) from the database.",
    )
    self._print_tracking_summary()

    last_reload = time.time()
    cycle = 0

    while not self._shutdown_requested:
      cycle += 1
      cycle_start = time.time()
      cycle_failures = 0
      now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      console.rule(f"[bold]Cycle {cycle}[/bold] — {now_str}")

      # Reset per-cycle timeout query tracking
      self.failure_tracker.start_cycle()

      # Periodically reload the database to pick up newly fetched
      # listings from the DiscoveryLoop process.
      if time.time() - last_reload >= self._DB_RELOAD_INTERVAL:
        logger.debug("Reloading tracked listings from database")
        new_count = self._load_tracked_listings()
        if new_count > 0:
          console.print(
            f"  [green]Picked up {new_count} newly fetched "
            f"listing(s) from the database.[/green]",
          )
          logger.debug("Reload: %d new listing(s) added to tracking", new_count)
        last_reload = time.time()

      # Process due monitoring tasks.
      if not self._shutdown_requested:
        try:
          _processed, monitor_failures = self._phase_monitor()
          cycle_failures += monitor_failures
        except Exception:
          cycle_failures += 1
          logger.exception("Error in monitoring phase.")

      # Cycle summary.
      cycle_elapsed = time.time() - cycle_start
      self._print_cycle_summary(cycle, cycle_elapsed, cycle_failures)

      if self._shutdown_requested:
        break

      # Sleep until next event.
      if self.task_queue:
        next_task_time = self.task_queue[0][0]
      else:
        next_task_time = time.time() + 60

      next_reload = last_reload + self._DB_RELOAD_INTERVAL
      wake_at = min(next_task_time, next_reload)
      sleep_seconds = max(0.5, wake_at - time.time())
      logger.debug(
        "Sleeping %s (next task: %.0fs, next reload: %.0fs)",
        _fmt_duration(sleep_seconds),
        next_task_time - time.time(),
        next_reload - time.time(),
      )
      console.print(
        f"[dim]Sleeping {_fmt_duration(sleep_seconds)} until "
        f"next event…[/dim]\n",
      )
      self._interruptible_sleep(sleep_seconds)

    console.print("\n[yellow]Shutting down gracefully.[/yellow]")
    console.print(
      "[dim]All data is safely persisted in the database.[/dim]",
    )

  # ----------------------------------------------------------------
  # Display helpers
  # ----------------------------------------------------------------

  def _print_tracking_summary(self) -> None:
    """Print a summary table of all tracked listings."""
    self._print_status_table(title="Tracked Listings")

  def _count_unfetched_by_website(self) -> Dict[str, int]:
    """Return a dict mapping website name → unfetched listing count."""
    registered_scrapers = set(ScraperRegistry.list_registered())
    allowed_scrapers = (
      set(self.scrapers) if self.scrapers else registered_scrapers
    )
    unfetched_counts: Dict[str, int] = {}

    with session_scope() as session:
      unfetched = get_unfetched_active_listings(session)
      for listing in unfetched:
        if not listing.website:
          continue
        scraper_name = listing.website.name.lower()
        if scraper_name not in registered_scrapers:
          scraper_name = scraper_name.replace(" ", "_")
          if scraper_name not in registered_scrapers:
            continue
        if scraper_name not in allowed_scrapers:
          continue
        unfetched_counts[scraper_name] = (
          unfetched_counts.get(scraper_name, 0) + 1
        )

    return unfetched_counts

  def _print_status_table(
    self,
    *,
    title: str = "Status",
    cycle: Optional[int] = None,
    elapsed: Optional[float] = None,
    total_failures: int = 0,
  ) -> None:
    """Print a status table showing tracked listings by website and
    monitoring phase, plus unfetched counts.
    """
    unfetched_counts = self._count_unfetched_by_website()

    all_websites: set[str] = set(unfetched_counts.keys())

    website_phase_counts: Dict[str, Dict[str, int]] = {}
    for tracked in self.tracked.values():
      website = tracked.scraper_name
      phase = tracked.phase
      all_websites.add(website)
      if website not in website_phase_counts:
        website_phase_counts[website] = {}
      website_phase_counts[website][phase] = (
        website_phase_counts[website].get(phase, 0) + 1
      )

    if not all_websites and not self.finished_ids:
      console.print("[dim]No listings being tracked.[/dim]")
      return

    if cycle is not None and elapsed is not None:
      header = f"{title} — Cycle {cycle} done in {_fmt_duration(elapsed)}"
      if total_failures > 0:
        header += f" — [red]{total_failures} error(s)[/red]"
      # Check for timeout websites
      timeout_list = self.failure_tracker.get_timeout_websites()
      if timeout_list:
        timeout_names = [name for name, _ in timeout_list]
        header += f" — [yellow]{len(timeout_names)} website(s) in timeout[/yellow]"
    else:
      header = title

    table = Table(title=header, show_lines=False)
    table.add_column("Website", style="cyan")
    table.add_column("Unfetched", justify="right", style="yellow")
    table.add_column("Tracked", justify="right", style="bold")
    for phase in (
      PHASE_ROUTINE, PHASE_APPROACHING, PHASE_IMMINENT,
      PHASE_ENDING, PHASE_WAITING,
    ):
      symbol = _PHASE_SYMBOLS.get(phase, "")
      table.add_column(f"{symbol} {phase}", justify="right")
    table.add_column("Finished", justify="right", style="green")

    total_unfetched = 0
    total_tracked = 0
    total_phase_counts: Dict[str, int] = {}

    for website_name in sorted(all_websites):
      phases = website_phase_counts.get(website_name, {})
      tracked_total = sum(phases.values())
      unfetched = unfetched_counts.get(website_name, 0)

      total_unfetched += unfetched
      total_tracked += tracked_total

      row = [
        website_name,
        str(unfetched) if unfetched > 0 else "–",
        str(tracked_total) if tracked_total > 0 else "–",
      ]
      for phase in (
        PHASE_ROUTINE, PHASE_APPROACHING, PHASE_IMMINENT,
        PHASE_ENDING, PHASE_WAITING,
      ):
        count = phases.get(phase, 0)
        total_phase_counts[phase] = (
          total_phase_counts.get(phase, 0) + count
        )
        row.append(str(count) if count > 0 else "–")

      row.append("–")
      table.add_row(*row)

    if len(all_websites) > 1:
      totals_row = [
        "[bold]Total[/bold]",
        f"[bold]{total_unfetched}[/bold]" if total_unfetched > 0 else "–",
        f"[bold]{total_tracked}[/bold]" if total_tracked > 0 else "–",
      ]
      for phase in (
        PHASE_ROUTINE, PHASE_APPROACHING, PHASE_IMMINENT,
        PHASE_ENDING, PHASE_WAITING,
      ):
        count = total_phase_counts.get(phase, 0)
        totals_row.append(
          f"[bold]{count}[/bold]" if count > 0 else "–"
        )
      totals_row.append(
        f"[bold green]{len(self.finished_ids)}[/bold green]"
        if self.finished_ids else "–"
      )
      table.add_row(*totals_row, end_section=True)
    elif self.finished_ids:
      console.print(table)
      console.print(
        f"  [green]{len(self.finished_ids)} auction(s) finished[/green]"
      )
      return

    console.print(table)

  def _print_cycle_summary(
    self, cycle: int, elapsed: float, total_failures: int = 0,
  ) -> None:
    """Print the status table at the end of a cycle."""
    self._print_status_table(
      title="Status",
      cycle=cycle,
      elapsed=elapsed,
      total_failures=total_failures,
    )
