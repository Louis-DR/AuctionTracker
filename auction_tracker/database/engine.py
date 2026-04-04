"""Database engine management and session factory."""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from auction_tracker.database.models import Base

logger = logging.getLogger(__name__)


def _enable_wal_and_foreign_keys(dbapi_connection, _connection_record):
  """Enable WAL mode and foreign key enforcement for SQLite."""
  cursor = dbapi_connection.cursor()
  cursor.execute("PRAGMA journal_mode=WAL")
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

    event.listen(self._engine, "connect", _enable_wal_and_foreign_keys)

    Base.metadata.create_all(self._engine)
    self._session_factory = sessionmaker(bind=self._engine)
    logger.info("Database initialized at %s", self._db_path)

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
