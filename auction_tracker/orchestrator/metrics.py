"""Operational metrics collector and live status for the pipeline loops.

Two independent systems:

1. ``MetricsCollector`` — records timestamped events to the
   ``pipeline_events`` database table for the historical operations
   dashboard.

2. ``LiveStatus`` — maintains an in-memory snapshot of what each
   pipeline loop is doing *right now* and flushes it to a JSON file
   at ~1 Hz. The web frontend polls this file to display live
   progress bars.

Event types (MetricsCollector):
  search_run, fetch_batch, watch_cycle, watch_check,
  classification, error, pipeline_start, pipeline_stop.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.models import PipelineEvent

logger = logging.getLogger(__name__)


class MetricsCollector:
  """Writes operational events to the database.

  Thread-safe: each call creates its own session. Designed to be
  shared between the search, fetch, and watch loops.
  """

  def __init__(self, database: DatabaseEngine) -> None:
    self._database = database

  def _emit(
    self,
    event_type: str,
    website_name: str | None = None,
    details: dict | None = None,
  ) -> None:
    try:
      with self._database.session() as session:
        session.add(PipelineEvent(
          timestamp=datetime.now(UTC).replace(tzinfo=None),
          event_type=event_type,
          website_name=website_name,
          detail_json=json.dumps(details) if details else None,
        ))
        session.commit()
    except Exception:
      logger.debug("Failed to record pipeline event %s", event_type, exc_info=True)

  # --- Pipeline lifecycle ---

  def pipeline_started(self) -> None:
    self._emit("pipeline_start")

  def pipeline_stopped(self) -> None:
    self._emit("pipeline_stop")

  # --- Search loop ---

  def search_run(
    self,
    website_name: str,
    query: str,
    results_found: int,
    new_listings: int,
  ) -> None:
    self._emit("search_run", website_name, {
      "query": query,
      "results_found": results_found,
      "new_listings": new_listings,
    })

  # --- Fetch loop ---

  def fetch_batch(
    self,
    fetched: int,
    classified: int,
    rejected: int,
    errors: int,
  ) -> None:
    self._emit("fetch_batch", details={
      "fetched": fetched,
      "classified": classified,
      "rejected": rejected,
      "errors": errors,
    })

  def fetch_listing(self, website_name: str, external_id: str) -> None:
    self._emit("fetch_listing", website_name, {"external_id": external_id})

  def classification(
    self,
    website_name: str,
    external_id: str,
    accepted: bool,
    score: float,
  ) -> None:
    self._emit("classification", website_name, {
      "external_id": external_id,
      "accepted": accepted,
      "score": round(score, 4),
    })

  # --- Watch loop ---

  def watch_cycle(
    self,
    checks: int,
    updated: int,
    completed: int,
    extensions: int,
    errors: int,
  ) -> None:
    self._emit("watch_cycle", details={
      "checks": checks,
      "updated": updated,
      "completed": completed,
      "extensions": extensions,
      "errors": errors,
    })

  def watch_check(self, website_name: str, external_id: str) -> None:
    self._emit("watch_check", website_name, {"external_id": external_id})

  # --- Errors ---

  def error(
    self,
    source: str,
    message: str,
    website_name: str | None = None,
  ) -> None:
    self._emit("error", website_name, {"source": source, "message": message[:500]})


# ===================================================================
# Live status — ephemeral JSON file for ~1 Hz frontend polling
# ===================================================================


class LiveStatus:
  """In-memory snapshot of current pipeline activity.

  All three loops (search, fetch, watch) run in the same asyncio
  event loop so no locks are required.  A background task created by
  ``start()`` atomically writes the snapshot to *status_path* every
  second.  The web frontend reads this file to paint live progress
  bars.
  """

  def __init__(self, status_path: Path) -> None:
    self._path = status_path
    self._started_at = time.time()
    self._task: asyncio.Task | None = None

    # Per-loop state dicts — mutated directly by the loops.
    self._search: dict = self._idle_state()
    self._fetch: dict = self._idle_state()
    self._watch: dict = self._idle_state()

    # Cumulative session counters (survive across loop iterations).
    self._counters: dict = {
      "searches_run": 0,
      "search_results_found": 0,
      "new_listings": 0,
      "fetched": 0,
      "classified": 0,
      "rejected": 0,
      "watch_checks": 0,
      "watch_updated": 0,
      "watch_completed": 0,
      "watch_extensions": 0,
      "errors": 0,
    }

  @staticmethod
  def _idle_state() -> dict:
    return {"state": "idle"}

  # --- Serialization & flush ---

  def to_dict(self) -> dict:
    return {
      "running": True,
      "started_at": self._started_at,
      "uptime_seconds": round(time.time() - self._started_at, 1),
      "updated_at": time.time(),
      "search_loop": dict(self._search),
      "fetch_loop": dict(self._fetch),
      "watch_loop": dict(self._watch),
      "counters": dict(self._counters),
    }

  def _flush(self) -> None:
    """Atomically write snapshot to disk (tmp + rename)."""
    try:
      self._path.parent.mkdir(parents=True, exist_ok=True)
      descriptor, tmp_path = tempfile.mkstemp(
        dir=str(self._path.parent), suffix=".tmp",
      )
      try:
        with os.fdopen(descriptor, "w") as handle:
          json.dump(self.to_dict(), handle)
        os.replace(tmp_path, str(self._path))
      except Exception:
        with contextlib.suppress(OSError):
          os.unlink(tmp_path)
        raise
    except Exception:
      logger.debug("Failed to flush live status", exc_info=True)

  async def _flush_loop(self) -> None:
    """Background coroutine: flush at ~1 Hz until cancelled."""
    try:
      while True:
        self._flush()
        await asyncio.sleep(1.0)
    except asyncio.CancelledError:
      pass

  def start(self) -> None:
    """Start the background flush task (call from the running event loop)."""
    self._task = asyncio.create_task(self._flush_loop())

  def stop(self) -> None:
    """Cancel the flush task and remove the status file."""
    if self._task is not None:
      self._task.cancel()
      self._task = None
    with contextlib.suppress(OSError):
      self._path.unlink(missing_ok=True)

  # --- Search loop updates ---

  def search_started(
    self,
    total_queries: int,
    total_websites: int,
  ) -> None:
    self._search = {
      "state": "running",
      "query_index": 0,
      "query_total": total_queries,
      "query_text": "",
      "website_index": 0,
      "website_total": total_websites,
      "website_name": "",
    }

  def search_progress(
    self,
    query_index: int,
    query_text: str,
    website_index: int,
    website_name: str,
  ) -> None:
    self._search.update({
      "state": "running",
      "query_index": query_index,
      "query_text": query_text,
      "website_index": website_index,
      "website_name": website_name,
    })

  def search_idle(self) -> None:
    self._search = self._idle_state()

  def search_sleeping(self, resume_in_seconds: float) -> None:
    self._search = {"state": "sleeping", "resume_in_seconds": round(resume_in_seconds)}

  # --- Fetch loop updates ---

  def fetch_started(self, batch_total: int) -> None:
    self._fetch = {
      "state": "running",
      "batch_index": 0,
      "batch_total": batch_total,
      "current_listing": "",
      "current_website": "",
    }

  def fetch_progress(
    self,
    batch_index: int,
    listing_id: str,
    website_name: str,
  ) -> None:
    self._fetch.update({
      "state": "running",
      "batch_index": batch_index,
      "current_listing": listing_id,
      "current_website": website_name,
    })

  def fetch_idle(self) -> None:
    self._fetch = self._idle_state()

  def fetch_sleeping(self) -> None:
    self._fetch = {"state": "sleeping"}

  # --- Watch loop updates ---

  def watch_started(self, due_count: int, queue_size: int) -> None:
    self._watch = {
      "state": "running",
      "check_index": 0,
      "check_total": due_count,
      "queue_size": queue_size,
      "current_listing": "",
      "current_website": "",
    }

  def watch_progress(
    self,
    check_index: int,
    listing_id: str,
    website_name: str,
  ) -> None:
    self._watch.update({
      "state": "running",
      "check_index": check_index,
      "current_listing": listing_id,
      "current_website": website_name,
    })

  def watch_idle(self, queue_size: int) -> None:
    self._watch = {"state": "idle", "queue_size": queue_size}

  def watch_sleeping(self, queue_size: int, next_check_in: float) -> None:
    self._watch = {
      "state": "sleeping",
      "queue_size": queue_size,
      "next_check_in": round(next_check_in),
    }

  # --- Counter increments (called alongside MetricsCollector) ---

  def increment(self, key: str, amount: int = 1) -> None:
    if key in self._counters:
      self._counters[key] += amount
