"""Logging configuration with file rotation and rich console output.

Two layers of file logging are supported:

1. Combined log (``log_file``): all loggers write here, as before.
2. Split logs (``log_dir``): when a directory is provided, per-website
   rotating files are created inside it.  Each ``WebsiteWorker`` writes
   its own log by calling ``add_website_log_handler``.  A ``shared.log``
   file captures everything that does *not* belong to a specific website
   (orchestrator plumbing, web layer, DB, transport internals, etc.).
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
  level: str = "INFO",
  log_file: Path | None = None,
  log_dir: Path | None = None,
  max_bytes: int = 10 * 1024 * 1024,
  backup_count: int = 5,
) -> None:
  """Configure the root logger with console and optional file output.

  Console output uses a concise format. File output (when enabled)
  uses a detailed format with rotation to prevent unbounded growth.

  If ``log_dir`` is provided, a ``shared.log`` file is created inside
  it. Per-website log files are added later by each ``WebsiteWorker``
  via ``add_website_log_handler``.
  """
  root_logger = logging.getLogger()
  root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

  # Clear any existing handlers to allow re-initialization.
  root_logger.handlers.clear()

  console_format = logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
  )
  console_handler = logging.StreamHandler(sys.stderr)
  console_handler.setFormatter(console_format)
  root_logger.addHandler(console_handler)

  file_format = logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
  )

  if log_file is not None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
      log_file,
      maxBytes=max_bytes,
      backupCount=backup_count,
      encoding="utf-8",
    )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)

  if log_dir is not None:
    log_dir.mkdir(parents=True, exist_ok=True)
    shared_handler = RotatingFileHandler(
      log_dir / "shared.log",
      maxBytes=max_bytes,
      backupCount=backup_count,
      encoding="utf-8",
    )
    shared_handler.setFormatter(file_format)
    # Only forward to shared.log what is NOT handled by a website-specific
    # logger. We tag this handler so we can find it from worker code.
    shared_handler._is_shared_log = True  # type: ignore[attr-defined]
    shared_handler.addFilter(_NotWebsiteFilter())
    root_logger.addHandler(shared_handler)

  # Quiet down noisy libraries.
  logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("curl_cffi").setLevel(logging.WARNING)


def add_website_log_handler(
  website_name: str,
  log_dir: Path,
  max_bytes: int = 10 * 1024 * 1024,
  backup_count: int = 5,
) -> None:
  """Attach a rotating file handler for one website worker.

  All log records whose message contains ``[<website_name>]`` (the
  bracket-prefixed tag that every ``WebsiteWorker`` log line carries)
  are routed to ``data/logs/<website_name>.log``.

  This function is idempotent: calling it twice for the same website
  name is a no-op (the second handler is discarded).
  """
  log_dir.mkdir(parents=True, exist_ok=True)
  root_logger = logging.getLogger()

  # Avoid duplicate handlers across restarts / re-initialisation.
  for handler in root_logger.handlers:
    if getattr(handler, "_website_log_name", None) == website_name:
      return

  file_format = logging.Formatter(
    "%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
  )
  website_handler = RotatingFileHandler(
    log_dir / f"{website_name}.log",
    maxBytes=max_bytes,
    backupCount=backup_count,
    encoding="utf-8",
  )
  website_handler.setFormatter(file_format)
  website_handler._website_log_name = website_name  # type: ignore[attr-defined]
  website_handler.addFilter(_WebsiteFilter(website_name))
  root_logger.addHandler(website_handler)

  # Exclude this website's lines from the shared log so they don't
  # appear in both places.  Find the shared handler and update its
  # filter to keep the excluded set in sync.
  for handler in root_logger.handlers:
    if getattr(handler, "_is_shared_log", False):
      for log_filter in handler.filters:
        if isinstance(log_filter, _NotWebsiteFilter):
          log_filter.add_excluded(website_name)
      break


# ------------------------------------------------------------------
# Filters
# ------------------------------------------------------------------


class _WebsiteFilter(logging.Filter):
  """Accept only records that carry the ``[website_name]`` tag."""

  def __init__(self, website_name: str) -> None:
    super().__init__()
    self._tag = f"[{website_name}]"

  def filter(self, record: logging.LogRecord) -> bool:
    return self._tag in record.getMessage()


class _NotWebsiteFilter(logging.Filter):
  """Reject records that belong to any known website worker.

  Starts empty; call ``add_excluded(name)`` as each website worker
  registers its own handler.
  """

  def __init__(self) -> None:
    super().__init__()
    self._excluded_tags: set[str] = set()

  def add_excluded(self, website_name: str) -> None:
    self._excluded_tags.add(f"[{website_name}]")

  def filter(self, record: logging.LogRecord) -> bool:
    message = record.getMessage()
    return not any(tag in message for tag in self._excluded_tags)
