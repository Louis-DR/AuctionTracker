"""Tests for the parsing layer (base classes and models)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from auction_tracker.parsing.base import Parser, ParserCapabilities, ParserRegistry
from auction_tracker.parsing.models import ScrapedListing, ScrapedSearchResult


class DummyParser(Parser):
  """A minimal parser for testing the registry."""

  @property
  def website_name(self) -> str:
    return "dummy"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(can_search=True, has_bid_history=True)

  def parse_search_results(self, html: str) -> list[ScrapedSearchResult]:
    return [
      ScrapedSearchResult(
        external_id="1", url="https://dummy.com/1", title="Test Item",
      )
    ]

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    return ScrapedListing(
      external_id="1", url="https://dummy.com/1", title="Test Item",
      current_price=Decimal("42.00"), currency="EUR",
    )


class TestScrapedModels:

  def test_search_result_minimal(self):
    result = ScrapedSearchResult(
      external_id="abc", url="https://example.com/abc", title="A Pen",
    )
    assert result.currency == "EUR"
    assert result.current_price is None

  def test_listing_with_all_fields(self):
    listing = ScrapedListing(
      external_id="xyz",
      url="https://example.com/xyz",
      title="Montblanc 149",
      current_price=Decimal("850.00"),
      currency="EUR",
      bid_count=12,
      image_urls=["https://img.com/1.jpg", "https://img.com/2.jpg"],
      attributes={"brand": "Montblanc", "nib_size": "M"},
    )
    assert listing.bid_count == 12
    assert len(listing.image_urls) == 2
    assert listing.attributes["brand"] == "Montblanc"


class TestParserRegistry:

  def setup_method(self):
    # Clear the registry between tests.
    ParserRegistry._parsers.clear()

  def test_register_and_get(self):
    ParserRegistry.register(DummyParser)
    parser = ParserRegistry.get("dummy")
    assert parser.website_name == "dummy"

  def test_get_unknown_raises(self):
    with pytest.raises(KeyError, match="No parser registered"):
      ParserRegistry.get("nonexistent")

  def test_list_registered(self):
    ParserRegistry.register(DummyParser)
    assert "dummy" in ParserRegistry.list_registered()

  def test_has(self):
    ParserRegistry.register(DummyParser)
    assert ParserRegistry.has("dummy")
    assert not ParserRegistry.has("nonexistent")

  def test_parser_capabilities(self):
    ParserRegistry.register(DummyParser)
    parser = ParserRegistry.get("dummy")
    assert parser.capabilities.can_search is True
    assert parser.capabilities.has_bid_history is True
    assert parser.capabilities.has_seller_info is False
