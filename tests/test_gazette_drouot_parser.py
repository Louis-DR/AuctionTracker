"""Tests for the Gazette Drouot parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.gazette_drouot import (
  GazetteDrouotParser,
  _clean_number,
  _derive_listing_status,
  _extract_auction_house,
  _extract_estimates,
  _extract_image_urls,
  _extract_lot_number,
  _parse_french_date,
  _parse_gazette_price,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "gazette_drouot"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> GazetteDrouotParser:
  return GazetteDrouotParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered(self):
    assert ParserRegistry.has("gazette_drouot")

  def test_website_name(self, parser: GazetteDrouotParser):
    assert parser.website_name == "gazette_drouot"

  def test_capabilities(self, parser: GazetteDrouotParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.has_estimates is True
    assert capabilities.has_bid_history is False
    assert capabilities.has_seller_info is False
    assert capabilities.has_auction_house_info is True


# ==================================================================
# URL helpers
# ==================================================================


class TestUrlHelpers:
  def test_build_search_url(self, parser: GazetteDrouotParser):
    url = parser.build_search_url("stylo plume")
    assert "recherche/lot" in url
    assert "stylo+plume" in url
    assert "type=result" in url

  def test_build_search_url_page_2(self, parser: GazetteDrouotParser):
    url = parser.build_search_url("test", page=2)
    assert "page=2" in url

  def test_extract_external_id(self, parser: GazetteDrouotParser):
    url = "https://www.gazette-drouot.com/lots/21211687-waterman"
    assert parser.extract_external_id(url) == "21211687"

  def test_extract_external_id_no_match(self, parser: GazetteDrouotParser):
    assert parser.extract_external_id("https://example.com") is None


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: GazetteDrouotParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert len(results) == 2

  def test_first_result(self, parser: GazetteDrouotParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "21211687"
    assert "WATERMAN" in first.title
    assert first.current_price == Decimal("150")
    assert first.currency == "EUR"
    assert first.listing_type == "auction"

  def test_image_url(self, parser: GazetteDrouotParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert results[0].image_url is not None
    assert "cdn.drouot.com" in results[0].image_url

  def test_end_time(self, parser: GazetteDrouotParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert results[0].end_time is not None
    assert results[0].end_time.year == 2025
    assert results[0].end_time.month == 3


# ==================================================================
# Sold lot
# ==================================================================


class TestSoldLot:
  def test_basic_fields(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687-waterman",
    )
    assert listing.external_id == "21211687"
    assert "Waterman" in listing.title or "WATERMAN" in listing.title
    assert listing.status == "sold"

  def test_price(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.current_price == Decimal("150")
    assert listing.final_price == Decimal("150")

  def test_estimates(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.estimate_low == Decimal("100")
    assert listing.estimate_high == Decimal("200")

  def test_images(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert len(listing.image_urls) == 2
    assert all("cdn.drouot.com" in url for url in listing.image_urls)

  def test_lot_number(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.lot_number == "233"

  def test_auction_house(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.auction_house_name == "Maison de Ventes Artcurial"

  def test_attributes(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.attributes["source"] == "gazette_drouot"
    assert listing.attributes["sale_type"] == "Live"
    assert listing.attributes["sale_name"] == "Tabac, Écriture & Coutellerie"

  def test_description(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211687",
    )
    assert listing.description is not None
    assert "Waterman" in listing.description


# ==================================================================
# Unsold lot
# ==================================================================


class TestUnsoldLot:
  def test_status(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211688",
    )
    assert listing.status == "unsold"

  def test_no_final_price(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211688",
    )
    assert listing.final_price is None

  def test_estimates_present(self, parser: GazetteDrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(
      html, url="https://www.gazette-drouot.com/lots/21211688",
    )
    assert listing.estimate_low == Decimal("200")
    assert listing.estimate_high == Decimal("400")


# ==================================================================
# Helper function tests
# ==================================================================


class TestParseFrenchDate:
  def test_standard(self):
    result = _parse_french_date("26 mars 2025")
    assert result is not None
    assert result.year == 2025
    assert result.month == 3
    assert result.day == 26

  def test_with_weekday(self):
    result = _parse_french_date("mercredi 26 mars 2025")
    assert result is not None
    assert result.month == 3

  def test_premier(self):
    result = _parse_french_date("1er avril 2025")
    assert result is not None
    assert result.day == 1
    assert result.month == 4

  def test_accent(self):
    result = _parse_french_date("15 février 2025")
    assert result is not None
    assert result.month == 2

  def test_slash_format(self):
    result = _parse_french_date("26/03/2025")
    assert result is not None
    assert result.month == 3

  def test_empty(self):
    assert _parse_french_date("") is None

  def test_invalid(self):
    assert _parse_french_date("not a date") is None


class TestParseGazettePrice:
  def test_simple_eur(self):
    price, currency = _parse_gazette_price("150 EUR")
    assert price == Decimal("150")
    assert currency == "EUR"

  def test_with_spaces(self):
    price, currency = _parse_gazette_price("4 000 EUR")
    assert price == Decimal("4000")
    assert currency == "EUR"

  def test_with_euro_symbol(self):
    price, currency = _parse_gazette_price("150 \u20ac")
    assert price == Decimal("150")
    assert currency == "EUR"

  def test_no_currency(self):
    price, currency = _parse_gazette_price("150")
    assert price == Decimal("150")
    assert currency == "EUR"

  def test_empty(self):
    price, currency = _parse_gazette_price("")
    assert price is None


class TestCleanNumber:
  def test_french_format(self):
    assert _clean_number("4 000,50") == "4000.50"

  def test_simple(self):
    assert _clean_number("150") == "150"

  def test_dots_and_comma(self):
    assert _clean_number("1.200,50") == "1200.50"


class TestExtractImageUrls:
  def test_from_openseadragon(self):
    html = """url: 'https://cdn.drouot.com/d/image/lot?path=img1.jpg'"""
    urls = _extract_image_urls(html)
    assert len(urls) == 1
    assert "cdn.drouot.com" in urls[0]

  def test_no_images(self):
    assert _extract_image_urls("<html>no images</html>") == []
