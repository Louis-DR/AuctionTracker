"""Database engine management and session factory.

SQLite concurrency notes
~~~~~~~~~~~~~~~~~~~~~~~~

The ``run`` pipeline launches one async worker per website; all
workers share a single ``DatabaseEngine`` and open short-lived
sessions for each unit of work.  The Flask web view is yet another
process with its own engine.

To prevent ``database is locked`` errors:

1. **WAL mode** — allows concurrent readers alongside a single writer
   (much better than the default rollback journal).
2. **busy_timeout** — makes SQLite wait (up to 10 s) for a write lock
   instead of failing immediately when another connection holds it.
   SQLite's built-in busy handler uses exponential backoff internally.

Workers must never hold a session (and therefore a write lock) across
an ``await`` boundary, because that would block every other worker
until the awaited I/O completes and the session commits.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import ClassVar

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from auction_tracker.database.models import Base

logger = logging.getLogger(__name__)

# SQLite will wait this long for a write lock before raising
# "database is locked". 10 seconds handles typical contention from
# the concurrent search/fetch/watch loops and the Flask web view.
_BUSY_TIMEOUT_MS = 10_000


def _configure_sqlite_connection(dbapi_connection, _connection_record):
  """Set SQLite pragmas on every new connection.

  - WAL mode: concurrent reads + serialised writes.
  - busy_timeout: wait for a write lock instead of failing instantly.
  - foreign_keys: enforce referential integrity.
  """
  cursor = dbapi_connection.cursor()
  cursor.execute("PRAGMA journal_mode=WAL")
  cursor.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
  cursor.execute("PRAGMA foreign_keys=ON")
  cursor.close()


class DatabaseEngine:
  """Manages the SQLAlchemy engine and session lifecycle.

  Usage::

      db = DatabaseEngine(Path("data/auction_tracker.db"))
      db.initialize()

      with db.session() as session:
          listings = session.query(Listing).all()
  """

  def __init__(self, db_path: Path) -> None:
    self._db_path = db_path
    self._engine: Engine | None = None
    self._session_factory: sessionmaker[Session] | None = None

  @property
  def engine(self) -> Engine:
    if self._engine is None:
      raise RuntimeError("Database not initialized. Call initialize() first.")
    return self._engine

  def initialize(self) -> None:
    """Create the engine, configure SQLite pragmas, and ensure all
    tables exist.
    """
    self._db_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"sqlite:///{self._db_path}"
    self._engine = create_engine(url, echo=False)

    event.listen(self._engine, "connect", _configure_sqlite_connection)

    Base.metadata.create_all(self._engine)
    self._apply_migrations()
    self._session_factory = sessionmaker(bind=self._engine)
    logger.info("Database initialized at %s", self._db_path)

  # Column additions for existing databases.  Each entry is a
  # (table, column, column_type) tuple.  ``create_all`` handles
  # brand-new databases; these migrations cover upgrades.
  _COLUMN_MIGRATIONS: ClassVar[list[tuple[str, str, str]]] = [
    ("price_snapshots", "exchange_rate", "REAL"),
  ]

  def _apply_migrations(self) -> None:
    """Add columns that are present in the ORM models but missing
    from an older SQLite schema.
    """
    with self._engine.connect() as connection:
      for table, column, column_type in self._COLUMN_MIGRATIONS:
        try:
          connection.execute(
            text(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}"),
          )
          connection.commit()
          logger.info("Migration: added %s.%s (%s).", table, column, column_type)
        except Exception:
          connection.rollback()

  def session(self) -> Session:
    """Create a new session. Use as a context manager::

        with db.session() as session:
            ...
            session.commit()
    """
    if self._session_factory is None:
      raise RuntimeError("Database not initialized. Call initialize() first.")
    return self._session_factory()

  def dispose(self) -> None:
    """Dispose of the engine and release connections."""
    if self._engine is not None:
      self._engine.dispose()
      self._engine = None
      self._session_factory = None
