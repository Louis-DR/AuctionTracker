"""Tests for the Marktplaats parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.marktplaats import (
  MarktplaatsParser,
  _extract_braced_json_object,
  _extract_jsonld_product,
  _parse_nl_price_text,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "marktplaats"


def _read(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> MarktplaatsParser:
  return MarktplaatsParser()


class TestRegistration:
  def test_registry(self):
    assert ParserRegistry.has("marktplaats")
    assert isinstance(ParserRegistry.get("marktplaats"), MarktplaatsParser)


class TestBuildSearchUrl:
  def test_page_one(self, parser: MarktplaatsParser):
    assert parser.build_search_url("fountain pen") == (
      "https://www.marktplaats.nl/q/fountain+pen/"
    )

  def test_page_two(self, parser: MarktplaatsParser):
    assert parser.build_search_url("fountain pen", page=2) == (
      "https://www.marktplaats.nl/q/fountain+pen/p/2/"
    )


class TestExtractExternalId:
  def test_extract(self, parser: MarktplaatsParser):
    url = "https://www.marktplaats.nl/v/verzamelen/pennen/a1524398513-slug"
    assert parser.extract_external_id(url) == "1524398513"


class TestParseNlPrice:
  def test_with_comma(self):
    assert _parse_nl_price_text("€ 15,50") == Decimal("15.50")

  def test_nbsp(self):
    assert _parse_nl_price_text("€\xa02,00") == Decimal("2.00")


class TestExtractJson:
  def test_braced_config(self):
    html = _read("listing_detail.html")
    cfg = _extract_braced_json_object(html, "window.__CONFIG__ = ")
    assert cfg is not None
    assert cfg["listing"]["itemId"] == "a1524398513"

  def test_jsonld(self):
    html = _read("listing_detail.html")
    product = _extract_jsonld_product(html)
    assert product is not None
    assert "vulpen" in (product.get("description") or "").lower()


class TestParseSearchResults:
  def test_count(self, parser: MarktplaatsParser):
    results = parser.parse_search_results(_read("search_results.html"))
    assert len(results) == 2

  def test_first(self, parser: MarktplaatsParser):
    first = parser.parse_search_results(_read("search_results.html"))[0]
    assert first.external_id == "1524398513"
    assert first.title == "Vintage Fountain Pen"
    assert first.current_price == Decimal("2.00")
    assert first.currency == "EUR"


class TestParseListing:
  def test_listing(self, parser: MarktplaatsParser):
    listing = parser.parse_listing(
      _read("listing_detail.html"),
      url="https://www.marktplaats.nl/v/verzamelen/pennen/a1524398513-slug",
    )
    assert listing.external_id == "1524398513"
    assert listing.title == "Vintage Fountain Pen"
    assert listing.current_price == Decimal("2.00")
    assert listing.seller is not None
    assert listing.seller.username == "Jan Jansen"
    assert listing.image_urls[0].startswith("https://")
