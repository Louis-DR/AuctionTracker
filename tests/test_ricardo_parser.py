"""Tests for the Ricardo parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.ricardo import (
  RicardoParser,
  _parse_chf_amount,
  _parse_ricardo_card_prices,
  _slug_query,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "ricardo"


def _read(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> RicardoParser:
  return RicardoParser()


class TestRegistration:
  def test_registry(self):
    assert ParserRegistry.has("ricardo")


class TestBuildSearchUrl:
  def test_page_one(self, parser: RicardoParser):
    url = parser.build_search_url("fountain pen")
    assert "/de/s/fountain-pen/" in url

  def test_locale_fr(self, parser: RicardoParser):
    url = parser.build_search_url("montre", locale="fr")
    assert "/fr/s/montre/" in url

  def test_page_two(self, parser: RicardoParser):
    url = parser.build_search_url("pen", page=2)
    assert "page=2" in url


class TestSlugQuery:
  def test_spaces(self):
    assert _slug_query("Fountain Pen") == "fountain-pen"


class TestExtractExternalId:
  def test_from_url(self, parser: RicardoParser):
    url = (
      "https://www.ricardo.ch/de/a/"
      "spezial-pen-marlen-hippocrates-fuellfederhalter-1314415068/"
    )
    assert parser.extract_external_id(url) == "1314415068"


class TestChfParsing:
  def test_amount(self):
    assert _parse_chf_amount("250.00") == Decimal("250.00")
    assert _parse_chf_amount("1'250.50 CHF") == Decimal("1250.50")


class TestCardPrices:
  def test_auction_with_instant(self):
    chunk = "250.00 (0 Gebote) 350.00 Sofort kaufen"
    current, buy_now, kind = _parse_ricardo_card_prices(chunk)
    assert current == Decimal("250.00")
    assert buy_now == Decimal("350.00")
    assert kind == "auction"

  def test_fixed_chf(self):
    chunk = "Sofort kaufen 120.00 CHF"
    current, buy_now, kind = _parse_ricardo_card_prices(chunk)
    assert buy_now == Decimal("120.00")
    assert kind == "buy_now"


class TestParseSearchResults:
  def test_count(self, parser: RicardoParser):
    results = parser.parse_search_results(_read("search_results.html"))
    assert len(results) == 2

  def test_auction_prices(self, parser: RicardoParser):
    first = parser.parse_search_results(_read("search_results.html"))[0]
    assert first.external_id == "1314415068"
    assert first.current_price == Decimal("250.00")
    assert first.listing_type == "auction"
    assert first.bid_count == 0


class TestParseListing:
  def test_jsonld(self, parser: RicardoParser):
    url = (
      "https://www.ricardo.ch/de/a/"
      "spezial-pen-marlen-hippocrates-fuellfederhalter-1314415068/"
    )
    listing = parser.parse_listing(_read("listing_detail.html"), url=url)
    assert listing.external_id == "1314415068"
    assert listing.currency == "CHF"
    assert listing.current_price == Decimal("350.00")
    assert listing.seller is not None
    assert "SwissCollector" in (listing.seller.display_name or listing.seller.username)
