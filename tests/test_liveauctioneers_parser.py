"""Tests for the LiveAuctioneers parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.liveauctioneers import (
  LiveAuctioneersParser,
  _build_single_image_url,
  _decimal_or_none,
  _derive_status,
  _extract_item_id,
  _extract_window_data,
  _find_item,
  _get_current_price,
  _parse_seller,
  _timestamp_to_datetime,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "liveauctioneers"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> LiveAuctioneersParser:
  return LiveAuctioneersParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered(self):
    assert ParserRegistry.has("liveauctioneers")

  def test_website_name(self, parser: LiveAuctioneersParser):
    assert parser.website_name == "liveauctioneers"

  def test_capabilities(self, parser: LiveAuctioneersParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.has_buy_now is True
    assert capabilities.has_estimates is True
    assert capabilities.has_bid_history is False


# ==================================================================
# URL helpers
# ==================================================================


class TestUrlHelpers:
  def test_build_search_url(self, parser: LiveAuctioneersParser):
    url = parser.build_search_url("fountain pen")
    assert "keyword=fountain pen" in url
    assert "search" in url

  def test_build_search_url_page_2(self, parser: LiveAuctioneersParser):
    url = parser.build_search_url("test", page=2)
    assert "page=2" in url

  def test_extract_external_id(self, parser: LiveAuctioneersParser):
    url = "https://www.liveauctioneers.com/item/100001_montblanc"
    assert parser.extract_external_id(url) == "100001"

  def test_extract_external_id_no_slug(self, parser: LiveAuctioneersParser):
    url = "https://www.liveauctioneers.com/item/100001"
    assert parser.extract_external_id(url) == "100001"

  def test_extract_external_id_no_match(self, parser: LiveAuctioneersParser):
    assert parser.extract_external_id("https://example.com") is None


class TestExtractItemId:
  def test_with_slug(self):
    assert _extract_item_id("https://la.com/item/12345_pen") == "12345"

  def test_without_slug(self):
    assert _extract_item_id("https://la.com/item/12345") == "12345"

  def test_bare_number(self):
    assert _extract_item_id("12345") == "12345"


# ==================================================================
# window.__data extraction
# ==================================================================


class TestWindowDataExtraction:
  def test_extract_valid(self):
    html = _read_fixture("search_results.html")
    data = _extract_window_data(html)
    assert data is not None
    assert "search" in data
    assert "itemSummary" in data

  def test_extract_none(self):
    assert _extract_window_data("<html>no data</html>") is None

  def test_undefined_replaced(self):
    html = 'window.__data = {"x":undefined};window.__amplitude = {};'
    data = _extract_window_data(html)
    assert data == {"x": None}


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: LiveAuctioneersParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert len(results) == 2

  def test_first_result(self, parser: LiveAuctioneersParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "100001"
    assert "Montblanc" in first.title
    assert first.current_price == Decimal("750")
    assert first.currency == "USD"
    assert first.listing_type == "auction"

  def test_second_result_sold(self, parser: LiveAuctioneersParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    second = results[1]
    assert second.external_id == "100002"
    assert second.current_price == Decimal("380")

  def test_image_url_generated(self, parser: LiveAuctioneersParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert results[0].image_url is not None
    assert "p1.liveauctioneers.com" in results[0].image_url

  def test_no_data_raises(self, parser: LiveAuctioneersParser):
    with pytest.raises(ValueError, match="window.__data"):
      parser.parse_search_results("<html>empty</html>")


# ==================================================================
# Active listing
# ==================================================================


class TestActiveListing:
  def test_basic_fields(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.external_id == "100001"
    assert "Montblanc" in listing.title
    assert listing.status == "active"

  def test_pricing(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.current_price == Decimal("850")
    assert listing.starting_price == Decimal("500")
    assert listing.estimate_low == Decimal("600")
    assert listing.estimate_high == Decimal("1000")
    assert listing.final_price is None

  def test_bid_count(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.bid_count == 15

  def test_seller(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.seller is not None
    assert listing.seller.display_name == "Heritage Auctions"
    assert listing.seller.country == "US"
    assert "auctioneer" in listing.seller.profile_url

  def test_buyer_premium(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.buyer_premium_percent == Decimal("28")

  def test_images(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert len(listing.image_urls) == 3

  def test_lot_number(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.lot_number == "142"

  def test_description_from_detail(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.description is not None
    assert "magnificent" in listing.description.lower()

  def test_attributes(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100001")
    assert listing.attributes["auction_type"] == "live"
    assert listing.attributes["sale_city"] == "Dallas"
    assert "buyers_premium_low_pct" in listing.attributes


# ==================================================================
# Sold listing
# ==================================================================


class TestSoldListing:
  def test_status(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100002")
    assert listing.status == "sold"

  def test_final_price(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100002")
    assert listing.final_price == Decimal("380")

  def test_timed_auction(self, parser: LiveAuctioneersParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html, url="https://la.com/item/100002")
    assert listing.attributes["auction_type"] == "timed"


# ==================================================================
# Helper function tests
# ==================================================================


class TestDeriveStatus:
  def test_deleted(self):
    assert _derive_status({"isDeleted": True}) == "cancelled"

  def test_sold(self):
    assert _derive_status({"isSold": True}) == "sold"

  def test_passed(self):
    assert _derive_status({"isPassed": True}) == "unsold"

  def test_available(self):
    assert _derive_status({"isAvailable": True}) == "active"

  def test_default(self):
    assert _derive_status({}) == "active"


class TestGetCurrentPrice:
  def test_sale_price_wins(self):
    item = {"salePrice": 500, "leadingBid": 400, "startPrice": 100}
    assert _get_current_price(item) == Decimal("500")

  def test_leading_bid(self):
    item = {"salePrice": 0, "leadingBid": 400, "startPrice": 100}
    assert _get_current_price(item) == Decimal("400")

  def test_start_price_fallback(self):
    item = {"salePrice": 0, "leadingBid": 0, "startPrice": 100}
    assert _get_current_price(item) == Decimal("100")

  def test_no_price(self):
    assert _get_current_price({}) is None


class TestFindItem:
  def test_direct_lookup(self):
    items = {"123": {"itemId": 123, "title": "Test"}}
    assert _find_item(items, "123") == {"itemId": 123, "title": "Test"}

  def test_scan_by_item_id(self):
    items = {"abc": {"itemId": 123, "title": "Test"}}
    assert _find_item(items, "123") == {"itemId": 123, "title": "Test"}

  def test_single_item_fallback(self):
    items = {"abc": {"itemId": 999, "title": "Only"}}
    assert _find_item(items, None) == {"itemId": 999, "title": "Only"}

  def test_not_found(self):
    items = {"abc": {"itemId": 1}, "def": {"itemId": 2}}
    assert _find_item(items, "999") is None


class TestBuildImageUrl:
  def test_valid(self):
    item = {"sellerId": 10, "catalogId": 20, "itemId": 30, "imageVersion": 5}
    url = _build_single_image_url(item, photo_index=1)
    assert "10/20/30_1_x.jpg" in url
    assert "quality=95" in url

  def test_missing_ids(self):
    assert _build_single_image_url({}) is None


class TestParseSeller:
  def test_valid(self):
    item = {
      "sellerId": 5001,
      "sellerName": "Heritage",
      "sellerCountryCode": "US",
      "sellerLogoId": "logo1",
    }
    seller = _parse_seller(item)
    assert seller is not None
    assert seller.display_name == "Heritage"
    assert "auctioneer" in seller.profile_url

  def test_no_seller(self):
    assert _parse_seller({}) is None


class TestDecimalOrNone:
  def test_positive(self):
    assert _decimal_or_none(42) == Decimal("42")

  def test_zero_is_none(self):
    assert _decimal_or_none(0) is None

  def test_none(self):
    assert _decimal_or_none(None) is None


class TestTimestampToDatetime:
  def test_valid(self):
    result = _timestamp_to_datetime(1700000000)
    assert result is not None
    assert result.year == 2023

  def test_zero(self):
    assert _timestamp_to_datetime(0) is None

  def test_none(self):
    assert _timestamp_to_datetime(None) is None
