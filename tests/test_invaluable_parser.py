"""Tests for the Invaluable parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.invaluable import (
  InvaluableParser,
  _build_attributes,
  _build_lot_url,
  _decimal_or_none,
  _derive_lot_status,
  _derive_search_status,
  _extract_preloaded_state,
  _extract_search_image,
  _millis_to_datetime,
  _parse_auction_house,
  _parse_buyer_premium,
  _parse_condition,
  _parse_iso_datetime,
  _parse_lot_image_urls,
  _slugify,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "invaluable"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> InvaluableParser:
  return InvaluableParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered(self):
    assert ParserRegistry.has("invaluable")

  def test_website_name(self, parser: InvaluableParser):
    assert parser.website_name == "invaluable"

  def test_capabilities(self, parser: InvaluableParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.has_estimates is True
    assert capabilities.has_lot_numbers is True
    assert capabilities.has_watcher_count is True
    assert capabilities.has_bid_history is False


# ==================================================================
# URL helpers
# ==================================================================


class TestUrlHelpers:
  def test_build_search_url_page_1(self, parser: InvaluableParser):
    url = parser.build_search_url("fountain pen")
    assert "keyword=fountain pen" in url
    assert "page=0" in url

  def test_build_search_url_page_3(self, parser: InvaluableParser):
    url = parser.build_search_url("test", page=3)
    assert "page=2" in url

  def test_extract_external_id(self, parser: InvaluableParser):
    url = "https://www.invaluable.com/auction-lot/montblanc-149-a1b2c3d4e5"
    assert parser.extract_external_id(url) == "a1b2c3d4e5"

  def test_extract_external_id_bare(self, parser: InvaluableParser):
    url = "https://www.invaluable.com/auction-lot/-abc123def"
    assert parser.extract_external_id(url) == "abc123def"

  def test_build_lot_url_with_title(self):
    url = _build_lot_url("ABC123", "Test Pen")
    assert "auction-lot/test-pen-abc123" in url

  def test_build_lot_url_without_title(self):
    url = _build_lot_url("ABC123")
    assert "auction-lot/-abc123" in url

  def test_slugify(self):
    assert _slugify("Hello World! 123") == "hello-world-123"
    assert _slugify("  --test--  ") == "test"


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    assert len(results) == 3

  def test_first_result_active(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    first = results[0]
    assert first.external_id == "A1B2C3D4E5"
    assert "Montblanc" in first.title
    assert first.current_price == Decimal("850")
    assert first.currency == "USD"
    assert first.listing_type == "auction"
    assert first.image_url is not None

  def test_second_result_sold(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    second = results[1]
    assert second.external_id == "F6G7H8I9J0"
    assert second.currency == "EUR"

  def test_third_result_unsold(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    third = results[2]
    assert third.external_id == "K1L2M3N4O5"
    assert third.current_price == Decimal("180")

  def test_fallback_url_when_missing(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    second = results[1]
    assert "invaluable.com" in second.url

  def test_image_fallback_to_filename(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    second = results[1]
    assert "housePhotos" in second.image_url

  def test_no_image_returns_none(self, parser: InvaluableParser):
    raw = _read_fixture("search_results.json")
    results = parser.parse_search_results(raw)
    third = results[2]
    assert third.image_url is None

  def test_invalid_json_raises(self, parser: InvaluableParser):
    with pytest.raises(ValueError, match="JSON"):
      parser.parse_search_results("not json")


# ==================================================================
# Active lot
# ==================================================================


class TestActiveLot:
  def test_basic_fields(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.external_id == "A1B2C3D4E5"
    assert "Montblanc" in listing.title
    assert listing.status == "active"

  def test_pricing(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("850")
    assert listing.estimate_low == Decimal("600")
    assert listing.estimate_high == Decimal("1000")
    assert listing.final_price is None

  def test_bid_count(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.bid_count == 12

  def test_watcher_count(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.watcher_count == 35

  def test_condition(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "like_new"

  def test_seller(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.display_name == "Heritage Auctions"
    assert listing.seller.country == "US"
    assert "auction-house" in listing.seller.profile_url

  def test_buyer_premium(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_percent == Decimal("28.0")

  def test_images(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 2

  def test_lot_number(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.lot_number == "142"

  def test_sale_info(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.auction_house_name == "Heritage Auctions"
    assert "Writing Instruments" in listing.sale_name
    assert listing.start_time is not None

  def test_attributes(self, parser: InvaluableParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["sale_type"] == "live"
    assert listing.attributes["category"] == "Writing Instruments"
    assert listing.attributes["subcategory"] == "Fountain Pens"
    assert "buyer_premium_tiers" in listing.attributes

  def test_no_preloaded_state_raises(self, parser: InvaluableParser):
    with pytest.raises(ValueError, match="__PRELOADED_STATE__"):
      parser.parse_listing("<html><body>empty</body></html>")


# ==================================================================
# Sold lot
# ==================================================================


class TestSoldLot:
  def test_status(self, parser: InvaluableParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_final_price(self, parser: InvaluableParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price == Decimal("420")

  def test_condition_very_good(self, parser: InvaluableParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "very_good"

  def test_attributes_timed(self, parser: InvaluableParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["sale_type"] == "timed"
    assert listing.attributes.get("circa") == "2000"
    assert listing.attributes.get("medium") == "Resin and Gold"


# ==================================================================
# Helper function tests
# ==================================================================


class TestDeriveSearchStatus:
  def test_passed(self):
    assert _derive_search_status({"isPassed": True}) == "unsold"

  def test_results_posted_with_price(self):
    assert _derive_search_status(
      {"resultsPosted": True, "priceResult": 500.0},
    ) == "sold"

  def test_results_posted_no_price(self):
    assert _derive_search_status(
      {"resultsPosted": True, "priceResult": 0},
    ) == "unsold"

  def test_default_active(self):
    assert _derive_search_status({}) == "active"


class TestDeriveLotStatus:
  def test_sold(self):
    assert _derive_lot_status({"isLotSold": True}, {}) == "sold"

  def test_passed(self):
    assert _derive_lot_status({"isLotPassed": True}, {}) == "unsold"

  def test_in_progress(self):
    assert _derive_lot_status({"isLotInProgress": True}, {}) == "active"

  def test_default(self):
    assert _derive_lot_status({}, {}) == "active"


class TestParseBuyerPremium:
  def test_percentage_string(self):
    assert _parse_buyer_premium({"payableBP": "28.0%"}, {}) == Decimal("28.0")

  def test_tiers_fallback(self):
    result = _parse_buyer_premium(
      {}, {"buyersPremiums": [{"premium": 25}]},
    )
    assert result == Decimal("25")

  def test_no_premium(self):
    assert _parse_buyer_premium({}, {}) is None


class TestParseCondition:
  def test_excellent(self):
    assert _parse_condition("Excellent condition") == "like_new"

  def test_very_good(self):
    assert _parse_condition("In very good condition") == "very_good"

  def test_good(self):
    assert _parse_condition("Good condition overall") == "good"

  def test_empty(self):
    assert _parse_condition("") is None

  def test_unknown(self):
    assert _parse_condition("Some wear") is None


class TestDecimalOrNone:
  def test_positive(self):
    assert _decimal_or_none(42) == Decimal("42")

  def test_zero_is_none(self):
    assert _decimal_or_none(0) is None

  def test_none(self):
    assert _decimal_or_none(None) is None


class TestMillisToDatetime:
  def test_valid(self):
    result = _millis_to_datetime(1700000000000)
    assert result is not None
    assert result.year == 2023

  def test_zero(self):
    assert _millis_to_datetime(0) is None

  def test_none(self):
    assert _millis_to_datetime(None) is None


class TestParseIsoDatetime:
  def test_date_only(self):
    result = _parse_iso_datetime("2026-06-20")
    assert result is not None
    assert result.year == 2026

  def test_empty(self):
    assert _parse_iso_datetime("") is None


class TestExtractPreloadedState:
  def test_valid(self):
    html = _read_fixture("lot_active.html")
    result = _extract_preloaded_state(html)
    assert result is not None
    assert "pdp" in result

  def test_no_state(self):
    assert _extract_preloaded_state("<html>no state</html>") is None
