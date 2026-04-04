"""Database engine and session management.

Provides a singleton engine and a context-managed session factory so
that all database access goes through a single connection pool.

A module-level :data:`database_write_lock` is provided for
multi-threaded callers that need to serialise write transactions on
SQLite.  The convenience context manager :func:`thread_safe_session_scope`
acquires the lock automatically.

When multiple processes (e.g., ``discover`` and ``watch``) share the
same database, SQLite's WAL mode and busy_timeout handle most
contention, but transient I/O errors can still occur.  All write
operations are automatically retried with exponential backoff.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm import Session, sessionmaker

from auction_tracker.database.models import Base

logger = logging.getLogger(__name__)

# Module-level singletons.
_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None

# Reentrant lock that serialises all database *write* transactions.
# SQLite only allows one writer at a time; this lock prevents
# "database is locked" errors when multiple threads try to write
# concurrently.  Read-only sessions can skip the lock when WAL
# mode is enabled (readers do not block writers in WAL).
#
# Note: This lock only works within a single process.  When multiple
# processes (discover + watch) share the database, SQLite's file-level
# locking + WAL mode + busy_timeout handle inter-process contention.
database_write_lock = threading.RLock()

# Maximum number of retries for database operations that fail due to
# inter-process contention (OperationalError).
_MAX_DB_RETRIES = 5

# Base delay (seconds) for exponential backoff retries.
_RETRY_BASE_DELAY = 0.1


def _enable_sqlite_pragmas(dbapi_connection, _connection_record):
  """Turn on foreign-key enforcement, WAL journal mode, and optimizations.

  WAL (Write-Ahead Logging) allows concurrent readers alongside a
  single writer, which is essential for the multi-threaded smart
  monitor.

  The busy timeout (60 seconds) tells SQLite to retry for up to 60 s
  when the database is locked by another **process**.  This is needed
  because the ``discover`` and ``watch`` commands run as separate
  processes sharing the same database file.  (Intra-process thread
  contention is handled separately by :data:`database_write_lock`.)

  Additional pragmas optimize for multi-process access:
  - synchronous=NORMAL: Faster writes, still safe with WAL
  - wal_autocheckpoint: Automatically checkpoint WAL file
  """
  cursor = dbapi_connection.cursor()
  cursor.execute("PRAGMA foreign_keys = ON")
  cursor.execute("PRAGMA journal_mode = WAL")
  cursor.execute("PRAGMA busy_timeout = 60000")  # 60 seconds
  # Optimize for multi-process access: faster writes, still safe with WAL.
  cursor.execute("PRAGMA synchronous = NORMAL")
  # Automatically checkpoint WAL file to prevent it from growing too large.
  cursor.execute("PRAGMA wal_autocheckpoint = 1000")
  cursor.close()


def _is_retryable_error(error: Exception) -> bool:
  """Check if a database error should be retried.

  Retries OperationalError (disk I/O, database locked, etc.) and
  PendingRollbackError (invalid transaction state) which can occur
  due to inter-process contention.
  """
  # PendingRollbackError occurs when a commit fails and the session
  # is left in an invalid state. We can retry after rolling back.
  if isinstance(error, PendingRollbackError):
    return True

  if isinstance(error, OperationalError):
    error_str = str(error).lower()
    # Retry on disk I/O errors, locked database, and similar transient issues.
    retryable_patterns = [
      "disk i/o error",
      "database is locked",
      "database lock",
      "unable to open database",
    ]
    return any(pattern in error_str for pattern in retryable_patterns)
  return False


def _retry_db_operation(operation, *args, **kwargs):
  """Execute a database operation with retry logic.

  Retries on OperationalError (disk I/O, locked database) with
  exponential backoff.  This handles transient inter-process
  contention when multiple processes share the same SQLite database.

  Args:
    operation: Callable that performs the database operation.
    *args, **kwargs: Arguments passed to *operation*.

  Returns:
    The result of calling *operation*.

  Raises:
    The last exception if all retries are exhausted.
  """
  last_error = None
  for attempt in range(_MAX_DB_RETRIES):
    try:
      return operation(*args, **kwargs)
    except OperationalError as error:
      last_error = error
      if not _is_retryable_error(error):
        # Not a retryable error — fail immediately.
        raise

      if attempt < _MAX_DB_RETRIES - 1:
        # Exponential backoff with jitter to reduce contention.
        delay = _RETRY_BASE_DELAY * (2 ** attempt)
        jitter = random.uniform(0, delay * 0.1)
        total_delay = delay + jitter

        logger.debug(
          "Database operation failed (attempt %d/%d): %s. "
          "Retrying in %.2fs...",
          attempt + 1,
          _MAX_DB_RETRIES,
          str(error)[:100],
          total_delay,
        )
        time.sleep(total_delay)
      else:
        logger.warning(
          "Database operation failed after %d attempts: %s",
          _MAX_DB_RETRIES,
          str(error)[:200],
        )
    except Exception:
      # Non-retryable error — fail immediately.
      raise

  # All retries exhausted.
  raise last_error


def get_engine(database_path: Optional[Path] = None) -> Engine:
  """Return the global SQLAlchemy engine, creating it on first call.

  *database_path* is only used on the very first invocation; subsequent
  calls return the already-created engine.
  """
  global _engine
  if _engine is not None:
    return _engine

  if database_path is None:
    # Fall back to an in-memory database for quick tests.
    url = "sqlite:///:memory:"
  else:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite:///{database_path}"

  logger.info("Creating database engine: %s", url)
  _engine = create_engine(url, echo=False, future=True)

  # Enable foreign keys and WAL mode for SQLite.
  event.listen(_engine, "connect", _enable_sqlite_pragmas)

  return _engine


def get_session() -> Session:
  """Open and return a new database session.

  The caller is responsible for committing or rolling back.  Prefer
  the :func:`session_scope` context manager instead.
  """
  global _session_factory
  if _session_factory is None:
    _session_factory = sessionmaker(bind=get_engine(), expire_on_commit=False)
  return _session_factory()


def _commit_with_retry(session: Session) -> None:
  """Commit a session with automatic retry and rollback on failure.

  If commit fails with a retryable error (OperationalError or
  PendingRollbackError), rolls back the session and retries the commit
  with exponential backoff.
  """
  last_error = None
  for attempt in range(_MAX_DB_RETRIES):
    try:
      session.commit()
      return  # Success!
    except (OperationalError, PendingRollbackError) as error:
      last_error = error
      if not _is_retryable_error(error):
        # Not a retryable error — fail immediately.
        raise

      if attempt < _MAX_DB_RETRIES - 1:
        # Rollback the session to clear invalid state before retrying.
        try:
          session.rollback()
        except Exception as rollback_error:
          # If rollback fails, the session is in a bad state.
          # Log and re-raise the original commit error.
          logger.warning(
            "Failed to rollback session after commit error: %s. "
            "Original error: %s",
            str(rollback_error)[:200],
            str(error)[:200],
          )
          raise error from rollback_error

        # Exponential backoff with jitter to reduce contention.
        delay = _RETRY_BASE_DELAY * (2 ** attempt)
        jitter = random.uniform(0, delay * 0.1)
        total_delay = delay + jitter

        logger.debug(
          "Database commit failed (attempt %d/%d): %s. "
          "Rolled back and retrying in %.2fs...",
          attempt + 1,
          _MAX_DB_RETRIES,
          str(error)[:100],
          total_delay,
        )
        time.sleep(total_delay)
      else:
        logger.warning(
          "Database commit failed after %d attempts: %s",
          _MAX_DB_RETRIES,
          str(error)[:200],
        )
    except Exception:
      # Non-retryable error — fail immediately.
      raise

  # All retries exhausted.
  raise last_error


@contextmanager
def session_scope() -> Generator[Session, None, None]:
  """Provide a transactional scope around a series of operations.

  Usage::

      with session_scope() as session:
          session.add(my_object)

  Catches ``BaseException`` (not just ``Exception``) so that
  ``KeyboardInterrupt`` also triggers an explicit rollback rather
  than relying on the implicit rollback from ``session.close()``.
  This prevents any partially-committed state.

  Database commits are automatically retried with exponential backoff
  if they fail due to inter-process contention.  The session is
  automatically rolled back before each retry attempt.
  """
  session = get_session()
  try:
    yield session
    # Retry commit on OperationalError or PendingRollbackError.
    _commit_with_retry(session)
  except BaseException:
    try:
      session.rollback()
    except Exception:
      # Rollback might fail if session is already closed/invalid.
      # Log but don't raise — we're already handling an exception.
      logger.debug("Rollback failed during exception handling (expected)")
    raise
  finally:
    session.close()


@contextmanager
def thread_safe_session_scope() -> Generator[Session, None, None]:
  """Like :func:`session_scope` but acquires :data:`database_write_lock`.

  Use this in multi-threaded contexts where several threads may try to
  write to the database concurrently.  The lock serialises the write
  transactions so SQLite never sees two writers at the same time.

  Database commits are automatically retried with exponential backoff
  if they fail due to inter-process contention (OperationalError).
  """
  with database_write_lock:
    with session_scope() as session:
      yield session


def retry_on_db_error(operation, *args, **kwargs):
  """Execute a database operation with automatic retry on contention errors.

  Wraps a database operation (typically a function that uses
  :func:`session_scope` or :func:`thread_safe_session_scope`) and
  retries it if it fails with an OperationalError due to inter-process
  contention.

  This is useful for operations that might fail at any point (not just
  during commit), such as complex queries or bulk operations.

  Example::

      def create_listing(session, data):
          listing = Listing(**data)
          session.add(listing)
          session.flush()

      with session_scope() as session:
          retry_on_db_error(create_listing, session, listing_data)

  Args:
    operation: Callable that performs the database operation.
    *args, **kwargs: Arguments passed to *operation*.

  Returns:
    The result of calling *operation*.

  Raises:
    The last exception if all retries are exhausted.
  """
  return _retry_db_operation(operation, *args, **kwargs)


def _run_migrations(engine: Engine) -> None:
  """Apply lightweight schema migrations for columns added after the
  initial ``create_all``.  Each migration is idempotent.
  """
  import sqlite3

  raw_url = str(engine.url)
  if raw_url.startswith("sqlite:///"):
    db_path = raw_url[len("sqlite:///"):]
  elif raw_url == "sqlite:///:memory:" or raw_url == "sqlite://":
    return  # in-memory; create_all already handles everything
  else:
    return

  con = sqlite3.connect(db_path)
  try:
    _migrate_table(con, "bid_events", {
      "bidder_country": "VARCHAR(2)",
      "amount_eur": "NUMERIC(14, 2)",
    })
    _migrate_table(con, "listings", {
      "current_price_eur": "NUMERIC(14, 2)",
      "final_price_eur": "NUMERIC(14, 2)",
      "is_fully_fetched": "BOOLEAN NOT NULL DEFAULT 1",
    })
    _migrate_table(con, "price_snapshots", {
      "price_eur": "NUMERIC(14, 2)",
    })
  finally:
    con.close()


def _migrate_table(
  con,
  table_name: str,
  new_columns: dict[str, str],
) -> None:
  """Add missing columns to a table.  Idempotent."""
  # Check that the table exists at all.
  exists = con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
    (table_name,),
  ).fetchone()
  if not exists:
    return

  cursor = con.execute(f"PRAGMA table_info({table_name})")
  existing_columns = {row[1] for row in cursor.fetchall()}

  for column_name, column_type in new_columns.items():
    if column_name not in existing_columns:
      con.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
      )
      con.commit()
      logger.info(
        "Migration: added %s.%s column.", table_name, column_name,
      )


def initialize_database(database_path: Optional[Path] = None) -> Engine:
  """Create all tables that do not yet exist and return the engine."""
  engine = get_engine(database_path)
  Base.metadata.create_all(engine)
  _run_migrations(engine)
  logger.info("Database tables created / verified.")
  return engine


def reset_engine() -> None:
  """Dispose of the current engine.

  Useful in tests or when switching databases at runtime.
  """
  global _engine, _session_factory
  if _engine is not None:
    _engine.dispose()
    _engine = None
  _session_factory = None
