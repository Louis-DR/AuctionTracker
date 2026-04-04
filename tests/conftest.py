"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from auction_tracker.config import AppConfig, DatabaseConfig
from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.repository import Repository

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
  """App config pointing to a temporary database."""
  return AppConfig(
    database=DatabaseConfig(path=tmp_path / "test.db"),
  )


@pytest.fixture
def database(app_config: AppConfig) -> DatabaseEngine:
  """Initialized in-memory database engine."""
  db = DatabaseEngine(app_config.database.path)
  db.initialize()
  yield db
  db.dispose()


@pytest.fixture
def repository() -> Repository:
  return Repository()


@pytest.fixture
def session(database: DatabaseEngine):
  """Database session that rolls back after each test."""
  with database.session() as session:
    yield session
    session.rollback()
