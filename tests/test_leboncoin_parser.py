"""Tests for the LeBonCoin parser."""

from __future__ import annotations

from datetime import UTC
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from auction_tracker.parsing.base import ParserBlocked, ParserRegistry
from auction_tracker.parsing.sites.leboncoin import (
  LeBonCoinParser,
  _ad_to_search_result,
  _build_attributes,
  _check_for_datadome,
  _decimal_or_none,
  _derive_status,
  _extract_buyer_fee,
  _extract_condition,
  _extract_image_urls,
  _extract_list_id,
  _extract_next_data,
  _extract_price,
  _extract_seller,
  _extract_watcher_count,
  _parse_leboncoin_datetime,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "leboncoin"
PARIS = ZoneInfo("Europe/Paris")


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> LeBonCoinParser:
  return LeBonCoinParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered_in_registry(self):
    assert ParserRegistry.has("leboncoin")

  def test_get_returns_instance(self):
    parser = ParserRegistry.get("leboncoin")
    assert isinstance(parser, LeBonCoinParser)

  def test_website_name(self, parser: LeBonCoinParser):
    assert parser.website_name == "leboncoin"


class TestCapabilities:
  def test_can_search(self, parser: LeBonCoinParser):
    assert parser.capabilities.can_search is True

  def test_can_parse_listing(self, parser: LeBonCoinParser):
    assert parser.capabilities.can_parse_listing is True

  def test_no_bid_history(self, parser: LeBonCoinParser):
    assert parser.capabilities.has_bid_history is False

  def test_has_seller_info(self, parser: LeBonCoinParser):
    assert parser.capabilities.has_seller_info is True

  def test_has_watcher_count(self, parser: LeBonCoinParser):
    assert parser.capabilities.has_watcher_count is True

  def test_has_buy_now(self, parser: LeBonCoinParser):
    assert parser.capabilities.has_buy_now is True

  def test_no_estimates(self, parser: LeBonCoinParser):
    assert parser.capabilities.has_estimates is False


# ==================================================================
# URL helpers
# ==================================================================


class TestBuildSearchUrl:
  def test_basic_query(self, parser: LeBonCoinParser):
    url = parser.build_search_url("stylo plume")
    assert "leboncoin.fr/recherche" in url
    assert "text=stylo+plume" in url

  def test_with_category(self, parser: LeBonCoinParser):
    url = parser.build_search_url("fountain pen", category="46")
    assert "category=46" in url

  def test_page_1_omitted(self, parser: LeBonCoinParser):
    url = parser.build_search_url("test", page=1)
    assert "page=" not in url

  def test_page_2_included(self, parser: LeBonCoinParser):
    url = parser.build_search_url("test", page=2)
    assert "page=2" in url


class TestExtractExternalId:
  def test_full_url(self, parser: LeBonCoinParser):
    url = "https://www.leboncoin.fr/ad/accessoires_bagagerie/2876543210.htm"
    assert parser.extract_external_id(url) == "2876543210"

  def test_bare_id(self, parser: LeBonCoinParser):
    assert parser.extract_external_id("2876543210") == "2876543210"

  def test_url_with_query(self, parser: LeBonCoinParser):
    url = "https://www.leboncoin.fr/ad/collection/2876543212.htm?ca=12_s"
    assert parser.extract_external_id(url) == "2876543212"

  def test_invalid_url(self, parser: LeBonCoinParser):
    assert parser.extract_external_id("https://www.leboncoin.fr/recherche") is None


# ==================================================================
# DataDome detection
# ==================================================================


class TestDataDomeDetection:
  def test_challenge_page_raises(self, parser: LeBonCoinParser):
    html = _read_fixture("datadome_challenge.html")
    with pytest.raises(ParserBlocked, match="DataDome"):
      parser.parse_listing(html)

  def test_challenge_page_in_search(self, parser: LeBonCoinParser):
    html = _read_fixture("datadome_challenge.html")
    with pytest.raises(ParserBlocked, match="DataDome"):
      parser.parse_search_results(html)

  def test_large_page_not_flagged(self):
    # A real page is much larger than 10 KB.
    html = "datadome" + "x" * 20_000
    # Should not raise.
    _check_for_datadome(html)

  def test_short_page_without_markers_not_flagged(self):
    html = "<html><body>Just a short page</body></html>"
    _check_for_datadome(html)


# ==================================================================
# __NEXT_DATA__ extraction
# ==================================================================


class TestExtractNextData:
  def test_standard_format(self):
    html = '<script id="__NEXT_DATA__" type="application/json">{"a": 1}</script>'
    assert _extract_next_data(html) == {"a": 1}

  def test_single_quote_format(self):
    html = "<script id='__NEXT_DATA__' type='application/json'>{\"b\": 2}</script>"
    assert _extract_next_data(html) == {"b": 2}

  def test_with_extra_attributes(self):
    html = '<script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">{"c": 3}</script>'
    assert _extract_next_data(html) == {"c": 3}

  def test_no_next_data_returns_none(self):
    html = "<html><body>No data here</body></html>"
    assert _extract_next_data(html) is None

  def test_invalid_json_returns_none(self):
    html = '<script id="__NEXT_DATA__" type="application/json">{invalid}</script>'
    assert _extract_next_data(html) is None


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert len(results) == 3

  def test_first_result_fields(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "2876543210"
    assert "Montblanc" in first.title
    assert first.current_price == Decimal("450")
    assert first.currency == "EUR"
    assert first.listing_type == "buy_now"

  def test_price_from_cents(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    # First ad has price_cents=45000 → 450.00
    assert results[0].current_price == Decimal("450")

  def test_relative_url_made_absolute(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert results[0].url.startswith("https://www.leboncoin.fr/")

  def test_absolute_url_preserved(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    # Third ad has absolute URL.
    assert results[2].url.startswith("https://www.leboncoin.fr/")

  def test_image_url_present(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert results[0].image_url is not None
    assert "leboncoin" in results[0].image_url

  def test_no_images_results_in_none(self, parser: LeBonCoinParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    # Third ad has empty images dict.
    assert results[2].image_url is None

  def test_no_next_data_raises(self, parser: LeBonCoinParser):
    with pytest.raises(ValueError, match="__NEXT_DATA__"):
      parser.parse_search_results("<html><body>empty</body></html>")


# ==================================================================
# Active listing
# ==================================================================


class TestActiveListing:
  def test_basic_fields(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.external_id == "2876543210"
    assert "Montblanc" in listing.title
    assert listing.listing_type == "buy_now"
    assert listing.status == "active"

  def test_price(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("450")
    assert listing.buy_now_price == Decimal("450")
    assert listing.currency == "EUR"

  def test_description(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.description is not None
    assert "Magnifique" in listing.description
    assert "18 carats" in listing.description

  def test_images(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    # Should prefer urls_large.
    assert len(listing.image_urls) == 3
    assert all("_large" in url for url in listing.image_urls)

  def test_seller(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.external_id == "store_98765"
    assert listing.seller.username == "PenCollector75"
    assert listing.seller.display_name == "PenCollector75 (private)"
    assert listing.seller.country == "FR"

  def test_seller_rating(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    # Rating 0.92 * 5 = 4.6
    assert listing.seller.rating == 4.6
    assert listing.seller.feedback_count == 47

  def test_seller_profile_url(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.profile_url is not None
    assert "a1b2c3d4-e5f6-7890-abcd-ef1234567890" in listing.seller.profile_url

  def test_condition(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "very_good"

  def test_watcher_count(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.watcher_count == 12

  def test_buyer_fee(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    # 2250 cents → 22.50 EUR
    assert listing.buyer_premium_fixed == Decimal("22.50")

  def test_start_time(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.start_time is not None
    # "2026-03-15 14:30:00" Paris → 13:30:00 UTC (CET = UTC+1 in March)
    assert listing.start_time.tzinfo == UTC
    assert listing.start_time.hour == 13
    assert listing.start_time.minute == 30

  def test_no_end_time(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.end_time is None

  def test_location_attributes(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["city"] == "Paris 8e"
    assert listing.attributes["region"] == "Île-de-France"
    assert listing.attributes["department"] == "Paris"

  def test_category_attributes(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["category"] == "Accessoires & Bagagerie"
    assert listing.attributes["category_id"] == "46"

  def test_brand_attribute(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["brand"] == "Montblanc"

  def test_negotiable_attribute(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["negotiable"] == "true"

  def test_seller_type_attribute(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["seller_type"] == "private"

  def test_ad_type_attribute(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["ad_type"] == "offer"

  def test_shipping_country(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_from_country == "FR"

  def test_ships_internationally(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    # shippable attribute is "true"
    assert listing.ships_internationally is True

  def test_url_made_absolute(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(html)
    assert listing.url.startswith("https://www.leboncoin.fr/")


# ==================================================================
# Sold / expired listing
# ==================================================================


class TestSoldListing:
  def test_status_expired_maps_to_sold(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_price(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("280")

  def test_seller_pro(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.display_name == "VintageWriter (pro)"

  def test_condition_good(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "good"

  def test_watcher_count_from_string(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    # favorites is "5" (string), should parse to int.
    assert listing.watcher_count == 5

  def test_no_buyer_fee(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_fixed is None


# ==================================================================
# Removed listing (ad is null)
# ==================================================================


class TestRemovedListing:
  def test_removed_listing(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_removed.html")
    url = "https://www.leboncoin.fr/ad/collection/2876543299.htm"
    listing = parser.parse_listing(html, url=url)
    assert listing.status == "sold"
    assert listing.external_id == "2876543299"
    assert "[Removed]" in listing.title

  def test_removed_listing_no_url(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_removed.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"
    assert listing.external_id == "unknown"


# ==================================================================
# Minimal listing (sparse data)
# ==================================================================


class TestMinimalListing:
  def test_basic_fields(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.external_id == "2876543230"
    assert listing.title == "Stylo plume ancien"
    assert listing.status == "active"

  def test_price_from_list(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    # No price_cents, falls back to price list.
    assert listing.current_price == Decimal("25")

  def test_no_images(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.image_urls == []

  def test_no_seller(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.seller is None

  def test_no_condition(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.condition is None

  def test_no_watcher_count(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.watcher_count is None

  def test_no_buyer_fee(self, parser: LeBonCoinParser):
    html = _read_fixture("listing_minimal.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_fixed is None


# ==================================================================
# Helper functions
# ==================================================================


class TestExtractPrice:
  def test_from_price_cents(self):
    ad = {"price_cents": 45000}
    assert _extract_price(ad) == Decimal("450")

  def test_from_price_list(self):
    ad = {"price": [320]}
    assert _extract_price(ad) == Decimal("320")

  def test_price_cents_preferred_over_list(self):
    ad = {"price_cents": 45000, "price": [450]}
    assert _extract_price(ad) == Decimal("450")

  def test_no_price(self):
    assert _extract_price({}) is None

  def test_empty_price_list(self):
    assert _extract_price({"price": []}) is None


class TestDeriveStatus:
  def test_active(self):
    assert _derive_status({"status": "active"}) == "active"

  def test_expired(self):
    assert _derive_status({"status": "expired"}) == "sold"

  def test_deleted(self):
    assert _derive_status({"status": "deleted"}) == "sold"

  def test_unknown_defaults_active(self):
    assert _derive_status({"status": "something_else"}) == "active"

  def test_missing_status_defaults_active(self):
    assert _derive_status({}) == "active"


class TestExtractImageUrls:
  def test_prefers_large(self):
    ad = {
      "images": {
        "urls": ["small1.jpg", "small2.jpg"],
        "urls_large": ["large1.jpg", "large2.jpg"],
      }
    }
    assert _extract_image_urls(ad) == ["large1.jpg", "large2.jpg"]

  def test_falls_back_to_regular(self):
    ad = {"images": {"urls": ["small1.jpg"]}}
    assert _extract_image_urls(ad) == ["small1.jpg"]

  def test_no_images(self):
    assert _extract_image_urls({}) == []
    assert _extract_image_urls({"images": {}}) == []


class TestExtractSeller:
  def test_full_seller(self):
    ad = {
      "owner": {
        "store_id": "store_123",
        "user_id": "uuid-abc",
        "name": "TestSeller",
        "type": "private",
      },
      "attributes": [
        {"key": "rating_score", "value": "0.8"},
        {"key": "rating_count", "value": "10"},
        {"key": "country_isocode3166", "value": "FR"},
      ],
    }
    seller = _extract_seller(ad)
    assert seller is not None
    assert seller.external_id == "store_123"
    assert seller.username == "TestSeller"
    assert seller.display_name == "TestSeller (private)"
    assert seller.rating == 4.0
    assert seller.feedback_count == 10
    assert seller.country == "FR"
    assert "uuid-abc" in seller.profile_url

  def test_fallback_to_user_id(self):
    ad = {
      "owner": {"user_id": "uuid-xyz", "name": "Seller"},
      "attributes": [],
    }
    seller = _extract_seller(ad)
    assert seller is not None
    assert seller.external_id == "uuid-xyz"

  def test_country_from_location(self):
    ad = {
      "owner": {"user_id": "x", "name": "S"},
      "attributes": [],
      "location": {"country_id": "BE"},
    }
    seller = _extract_seller(ad)
    assert seller is not None
    assert seller.country == "BE"

  def test_no_owner(self):
    assert _extract_seller({}) is None

  def test_empty_owner(self):
    ad = {"owner": {"name": "", "store_id": ""}, "attributes": []}
    assert _extract_seller(ad) is None


class TestExtractCondition:
  def test_new(self):
    ad = {"attributes": [{"key": "condition", "value": "etatneuf"}]}
    assert _extract_condition(ad) == "new"

  def test_very_good(self):
    ad = {"attributes": [{"key": "condition", "value": "tresbonetat"}]}
    assert _extract_condition(ad) == "very_good"

  def test_good(self):
    ad = {"attributes": [{"key": "condition", "value": "bonetat"}]}
    assert _extract_condition(ad) == "good"

  def test_fair(self):
    ad = {"attributes": [{"key": "condition", "value": "etatsatisfaisant"}]}
    assert _extract_condition(ad) == "fair"

  def test_unknown_returns_none(self):
    ad = {"attributes": [{"key": "condition", "value": "something"}]}
    assert _extract_condition(ad) is None

  def test_no_condition(self):
    assert _extract_condition({"attributes": []}) is None


class TestExtractBuyerFee:
  def test_fee_in_cents(self):
    ad = {"buyer_fee": {"amount": 2250, "currency": "EUR"}}
    assert _extract_buyer_fee(ad) == Decimal("22.50")

  def test_no_fee(self):
    assert _extract_buyer_fee({}) is None
    assert _extract_buyer_fee({"buyer_fee": {}}) is None
    assert _extract_buyer_fee({"buyer_fee": {"amount": None}}) is None


class TestExtractWatcherCount:
  def test_integer(self):
    assert _extract_watcher_count({"counters": {"favorites": 12}}) == 12

  def test_string(self):
    assert _extract_watcher_count({"counters": {"favorites": "5"}}) == 5

  def test_non_numeric_string(self):
    assert _extract_watcher_count({"counters": {"favorites": "abc"}}) is None

  def test_no_counters(self):
    assert _extract_watcher_count({}) is None
    assert _extract_watcher_count({"counters": {}}) is None


class TestExtractListId:
  def test_from_standard_url(self):
    assert _extract_list_id(
      "https://www.leboncoin.fr/ad/accessoires_bagagerie/2876543210.htm"
    ) == "2876543210"

  def test_from_bare_id(self):
    assert _extract_list_id("2876543210") == "2876543210"

  def test_from_url_with_query(self):
    assert _extract_list_id(
      "https://www.leboncoin.fr/ad/collection/2876543212.htm?ca=12_s"
    ) == "2876543212"

  def test_empty_string(self):
    assert _extract_list_id("") is None

  def test_no_id_found(self):
    assert _extract_list_id("https://www.leboncoin.fr/recherche") is None


class TestDecimalOrNone:
  def test_integer(self):
    assert _decimal_or_none(100) == Decimal("100")

  def test_string(self):
    assert _decimal_or_none("45.50") == Decimal("45.50")

  def test_with_divisor(self):
    assert _decimal_or_none(4500, divisor=100) == Decimal("45")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_invalid(self):
    assert _decimal_or_none("not_a_number") is None


class TestParseLeboncoinDatetime:
  def test_standard_format(self):
    result = _parse_leboncoin_datetime("2026-03-15 14:30:00")
    assert result is not None
    assert result.tzinfo == UTC
    # March 15 Paris is CET (UTC+1), so 14:30 Paris → 13:30 UTC
    assert result.hour == 13
    assert result.minute == 30

  def test_summer_time(self):
    # July is CEST (UTC+2), so 14:30 Paris → 12:30 UTC
    result = _parse_leboncoin_datetime("2026-07-15 14:30:00")
    assert result is not None
    assert result.hour == 12
    assert result.minute == 30

  def test_none_input(self):
    assert _parse_leboncoin_datetime(None) is None

  def test_empty_string(self):
    assert _parse_leboncoin_datetime("") is None

  def test_invalid_format(self):
    assert _parse_leboncoin_datetime("not a date") is None


class TestBuildAttributes:
  def test_location_fields(self):
    ad = {
      "category_name": "Test",
      "category_id": 1,
      "owner": {},
      "attributes": [],
    }
    attrs = _build_attributes(ad, "Paris", "IDF", "75")
    assert attrs["city"] == "Paris"
    assert attrs["region"] == "IDF"
    assert attrs["department"] == "75"

  def test_empty_location_omitted(self):
    ad = {"attributes": [], "owner": {}}
    attrs = _build_attributes(ad, "", "", "")
    assert "city" not in attrs

  def test_brand_collected(self):
    ad = {
      "attributes": [{"key": "brand", "value": "Pilot", "value_label": "Pilot"}],
      "owner": {},
    }
    attrs = _build_attributes(ad, "", "", "")
    assert attrs["brand"] == "Pilot"


class TestAdToSearchResult:
  def test_valid_ad(self):
    ad = {
      "list_id": 123456789,
      "subject": "Test Pen",
      "url": "/ad/test/123456789.htm",
      "price_cents": 5000,
      "images": {"urls": ["https://img.example.com/1.jpg"]},
    }
    result = _ad_to_search_result(ad)
    assert result is not None
    assert result.external_id == "123456789"
    assert result.title == "Test Pen"
    assert result.current_price == Decimal("50")
    assert result.listing_type == "buy_now"

  def test_missing_list_id(self):
    assert _ad_to_search_result({}) is None

  def test_relative_url(self):
    ad = {"list_id": 1, "subject": "X", "url": "/ad/test/1.htm"}
    result = _ad_to_search_result(ad)
    assert result is not None
    assert result.url.startswith("https://www.leboncoin.fr/")
