"""Logging configuration with file rotation and rich console output."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
  level: str = "INFO",
  log_file: Path | None = None,
  max_bytes: int = 10 * 1024 * 1024,
  backup_count: int = 5,
) -> None:
  """Configure the root logger with console and optional file output.

  Console output uses a concise format. File output (when enabled)
  uses a detailed format with rotation to prevent unbounded growth.
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

  if log_file is not None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_format = logging.Formatter(
      "%(asctime)s %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
      log_file,
      maxBytes=max_bytes,
      backupCount=backup_count,
      encoding="utf-8",
    )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)

  # Quiet down noisy libraries.
  logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
  logging.getLogger("urllib3").setLevel(logging.WARNING)
  logging.getLogger("curl_cffi").setLevel(logging.WARNING)
