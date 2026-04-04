"""Tests for the database layer (models, engine, repository)."""

from __future__ import annotations

from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.models import ListingStatus
from auction_tracker.database.repository import Repository


class TestDatabaseEngine:

  def test_initialize_creates_tables(self, database: DatabaseEngine):
    """Tables should exist after initialization."""
    from sqlalchemy import inspect
    inspector = inspect(database.engine)
    table_names = inspector.get_table_names()
    assert "websites" in table_names
    assert "listings" in table_names
    assert "bid_events" in table_names
    assert "price_snapshots" in table_names
    assert "search_queries" in table_names


class TestRepository:

  def test_create_website(self, session, repository: Repository):
    website = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    assert website.id is not None
    assert website.name == "test_site"

  def test_get_or_create_website_is_idempotent(self, session, repository: Repository):
    first = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    second = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    assert first.id == second.id

  def test_upsert_listing_creates_new(self, session, repository: Repository):
    website = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    listing, is_new = repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="12345",
      url="https://test.com/item/12345",
      title="Test Fountain Pen",
    )
    assert is_new is True
    assert listing.external_id == "12345"

  def test_upsert_listing_updates_existing(self, session, repository: Repository):
    website = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    listing_1, _ = repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="12345",
      url="https://test.com/item/12345",
      title="Test Fountain Pen",
    )
    listing_2, is_new = repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="12345",
      url="https://test.com/item/12345",
      title="Updated Title",
      current_price=100.0,
    )
    assert is_new is False
    assert listing_2.id == listing_1.id
    assert listing_2.title == "Updated Title"

  def test_get_active_listings(self, session, repository: Repository):
    website = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="active-1",
      url="https://test.com/1",
      title="Active Pen",
      status=ListingStatus.ACTIVE,
    )
    repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="sold-1",
      url="https://test.com/2",
      title="Sold Pen",
      status=ListingStatus.SOLD,
    )

    active = repository.get_active_listings(session)
    assert len(active) == 1
    assert active[0].external_id == "active-1"

  def test_add_price_snapshot(self, session, repository: Repository):
    website = repository.get_or_create_website(
      session, name="test_site", base_url="https://test.com",
    )
    listing, _ = repository.upsert_listing(
      session,
      website_id=website.id,
      external_id="snap-1",
      url="https://test.com/1",
      title="Snap Pen",
    )
    snapshot = repository.add_price_snapshot(
      session,
      listing_id=listing.id,
      price=150.0,
      currency="EUR",
      bid_count=5,
    )
    assert snapshot.id is not None
    assert snapshot.price == 150.0
