"""Tests for the Subito parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.subito import (
  SubitoParser,
  _find_listing_item_payload,
  _item_external_id,
  _load_next_data,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "subito"


def _read(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> SubitoParser:
  return SubitoParser()


class TestRegistration:
  def test_registry(self):
    assert ParserRegistry.has("subito")


class TestBuildSearchUrl:
  def test_page_one(self, parser: SubitoParser):
    url = parser.build_search_url("fountain pen")
    assert "q=fountain+pen" in url
    assert url.startswith("https://www.subito.it/annunci-italia/")

  def test_page_two_has_start(self, parser: SubitoParser):
    url = parser.build_search_url("pen", page=2)
    assert "start=30" in url


class TestExtractExternalId:
  def test_htm(self, parser: SubitoParser):
    url = "https://www.subito.it/annunci/lazio/vendita/usato/roma/12345678.htm"
    assert parser.extract_external_id(url) == "12345678"


class TestParseSearchResults:
  def test_count(self, parser: SubitoParser):
    results = parser.parse_search_results(_read("search_results.html"))
    assert len(results) == 2

  def test_prices(self, parser: SubitoParser):
    results = parser.parse_search_results(_read("search_results.html"))
    assert results[0].current_price == Decimal("89")
    assert results[1].current_price == Decimal("12")


class TestParseListing:
  def test_detail(self, parser: SubitoParser):
    listing = parser.parse_listing(
      _read("listing_detail.html"),
      url="https://www.subito.it/annunci/lazio/vendita/usato/roma/12345678.htm",
    )
    assert listing.external_id == "12345678"
    assert "Parker" in listing.title
    assert listing.current_price == Decimal("89")
    assert listing.seller is not None
    assert listing.seller.username == "Mario Rossi"
    assert "Roma" in (listing.attributes.get("location") or "")


class TestHelpers:
  def test_find_payload(self):
    data = _load_next_data(_read("listing_detail.html"))
    assert data is not None
    item = _find_listing_item_payload(data)
    assert item is not None
    assert _item_external_id(item) == "12345678"
