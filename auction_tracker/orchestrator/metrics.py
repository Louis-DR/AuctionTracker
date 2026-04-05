"""Operational metrics collector for the pipeline loops.

Records timestamped events to the ``pipeline_events`` database table
so the web dashboard can visualize pipeline health, throughput, and
error rates.  Each method is a fire-and-forget call that opens its
own short-lived session so callers never need to manage transactions.

Event types:
  search_run      — One search query executed on one website.
  fetch_batch     — One batch of unfetched listings processed.
  watch_cycle     — One pass of the watcher loop completed.
  watch_check     — One individual listing checked by the watcher.
  classification  — One listing classified by the image model.
  error           — An error that occurred in any loop.
  pipeline_start  — The ``run`` command started.
  pipeline_stop   — The ``run`` command stopped.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

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
