"""Tests for the Vinted parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.vinted import (
  VintedParser,
  _build_attributes,
  _decimal_or_none,
  _derive_status,
  _extract_condition,
  _extract_currency,
  _extract_image_urls,
  _extract_price,
  _extract_seller,
  _parse_json,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "vinted"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> VintedParser:
  return VintedParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered_in_registry(self):
    assert ParserRegistry.has("vinted")

  def test_get_returns_instance(self):
    instance = ParserRegistry.get("vinted")
    assert isinstance(instance, VintedParser)

  def test_website_name(self, parser: VintedParser):
    assert parser.website_name == "vinted"


class TestCapabilities:
  def test_can_search(self, parser: VintedParser):
    assert parser.capabilities.can_search is True

  def test_can_parse_listing(self, parser: VintedParser):
    assert parser.capabilities.can_parse_listing is True

  def test_has_seller_info(self, parser: VintedParser):
    assert parser.capabilities.has_seller_info is True

  def test_no_bid_history(self, parser: VintedParser):
    assert parser.capabilities.has_bid_history is False


# ==================================================================
# URL helpers
# ==================================================================


class TestBuildSearchUrl:
  def test_default_domain(self, parser: VintedParser):
    url = parser.build_search_url("stylo plume")
    assert "www.vinted.fr" in url
    assert "search_text=stylo+plume" in url
    assert "per_page=96" in url

  def test_custom_domain(self, parser: VintedParser):
    url = parser.build_search_url("fountain pen", domain="vinted.de")
    assert "www.vinted.de" in url
    assert "search_text=fountain+pen" in url

  def test_page_parameter(self, parser: VintedParser):
    url = parser.build_search_url("pen", page=3)
    assert "page=3" in url

  def test_page_one_omitted(self, parser: VintedParser):
    url = parser.build_search_url("pen", page=1)
    # per_page is always present; only a separate "page=N" should be absent.
    assert "&page=" not in url


class TestBuildFetchUrl:
  def test_public_url_transformed(self, parser: VintedParser):
    public_url = "https://www.vinted.fr/items/4812937650-stylo-plume"
    api_url = parser.build_fetch_url(public_url)
    assert api_url == "https://www.vinted.fr/api/v2/items/4812937650/details"

  def test_api_url_unchanged(self, parser: VintedParser):
    api_url = "https://www.vinted.fr/api/v2/items/4812937650/details"
    assert parser.build_fetch_url(api_url) == api_url

  def test_different_domain(self, parser: VintedParser):
    url = "https://www.vinted.de/items/1234-test"
    api_url = parser.build_fetch_url(url)
    assert "www.vinted.de" in api_url
    assert "/api/v2/items/1234/details" in api_url


class TestExtractExternalId:
  def test_from_public_url(self, parser: VintedParser):
    url = "https://www.vinted.fr/items/4812937650-stylo-plume"
    assert parser.extract_external_id(url) == "4812937650"

  def test_unrecognised_url(self, parser: VintedParser):
    assert parser.extract_external_id("https://www.vinted.fr/catalog") is None


# ==================================================================
# JSON parsing
# ==================================================================


class TestParseJson:
  def test_valid_json(self):
    result = _parse_json('{"items": []}')
    assert result == {"items": []}

  def test_empty_body_raises(self):
    with pytest.raises(ValueError, match="Empty response"):
      _parse_json("")

  def test_html_body_raises(self):
    with pytest.raises(ValueError, match="HTML instead of JSON"):
      _parse_json("<html><head></head><body>blocked</body></html>")

  def test_invalid_json_raises(self):
    with pytest.raises(ValueError, match="Failed to parse JSON"):
      _parse_json("{not valid json}")


# ==================================================================
# Price extraction
# ==================================================================


class TestExtractPrice:
  def test_dict_amount(self):
    assert _extract_price({"amount": "285.00"}) == Decimal("285.00")

  def test_string_amount(self):
    assert _extract_price("45.50") == Decimal("45.50")

  def test_none(self):
    assert _extract_price(None) is None


class TestExtractCurrency:
  def test_from_dict(self):
    assert _extract_currency({"currency_code": "GBP"}) == "GBP"

  def test_default_eur(self):
    assert _extract_currency(None) == "EUR"
    assert _extract_currency("45.00") == "EUR"


# ==================================================================
# Status and condition
# ==================================================================


class TestDeriveStatus:
  def test_active(self):
    assert _derive_status({"is_closed": False, "can_buy": True}) == "active"

  def test_sold_by_is_closed(self):
    assert _derive_status({"is_closed": True}) == "sold"

  def test_sold_by_can_buy_false(self):
    assert _derive_status({"is_closed": False, "can_buy": False}) == "sold"


class TestExtractCondition:
  def test_new_with_tags(self):
    assert _extract_condition({"status_id": 6}) == "new"

  def test_new_without_tags(self):
    assert _extract_condition({"status_id": 1}) == "like_new"

  def test_very_good(self):
    assert _extract_condition({"status_id": 2}) == "very_good"

  def test_good(self):
    assert _extract_condition({"status_id": 3}) == "good"

  def test_satisfactory(self):
    assert _extract_condition({"status_id": 4}) == "fair"

  def test_unknown_id(self):
    assert _extract_condition({"status_id": 99}) is None

  def test_missing_id(self):
    assert _extract_condition({}) is None


# ==================================================================
# Seller extraction
# ==================================================================


class TestExtractSeller:
  def test_full_seller(self):
    user = {
      "id": 55512340,
      "login": "pen_collector_75",
      "country_iso_code": "FR",
      "feedback_reputation": 4.8,
      "feedback_count": 47,
      "profile_url": "https://www.vinted.fr/member/55512340",
    }
    seller = _extract_seller({"user": user})
    assert seller is not None
    assert seller.external_id == "55512340"
    assert seller.username == "pen_collector_75"
    assert seller.country == "FR"
    # 4.8 * 20 = 96.0
    assert seller.rating == 96.0
    assert seller.feedback_count == 47
    assert seller.profile_url == "https://www.vinted.fr/member/55512340"

  def test_no_user(self):
    assert _extract_seller({}) is None


# ==================================================================
# Image extraction
# ==================================================================


class TestExtractImageUrls:
  def test_prefers_full_size(self):
    photos = [
      {"url": "small.jpg", "full_size_url": "big.jpg"},
      {"url": "small2.jpg"},
    ]
    urls = _extract_image_urls({"photos": photos})
    assert urls == ["big.jpg", "small2.jpg"]

  def test_empty_photos(self):
    assert _extract_image_urls({"photos": []}) == []
    assert _extract_image_urls({}) == []


# ==================================================================
# Attributes
# ==================================================================


class TestBuildAttributes:
  def test_brand_and_color(self):
    item = {"brand_title": "Montblanc", "color1": "Noir", "catalog_id": 1927}
    attrs = _build_attributes(item)
    assert attrs["brand"] == "Montblanc"
    assert attrs["color"] == "Noir"
    assert attrs["catalog_id"] == "1927"


# ==================================================================
# Search results (from fixture)
# ==================================================================


class TestParseSearchResults:
  def test_parse_search_fixture(self, parser: VintedParser):
    text = _read_fixture("search_results.json")
    url = "https://www.vinted.fr/api/v2/catalog/items?search_text=stylo+plume"
    results = parser.parse_search_results(text, url=url)
    assert len(results) == 3

    first = results[0]
    assert first.external_id == "4812937650"
    assert first.title == "Stylo plume Montblanc Meisterstück 149"
    assert first.current_price == Decimal("285.00")
    assert first.currency == "EUR"
    assert first.listing_type == "buy_now"
    assert "vinted.fr" in first.url
    assert first.image_url is not None

    third = results[2]
    assert third.external_id == "6234567890"
    assert third.current_price == Decimal("45.00")


# ==================================================================
# Item detail parsing (from fixtures)
# ==================================================================


class TestParseActiveItem:
  def test_parse_active_fixture(self, parser: VintedParser):
    text = _read_fixture("item_active.json")
    url = "https://www.vinted.fr/items/4812937650-stylo-plume"
    listing = parser.parse_listing(text, url=url)

    assert listing.external_id == "4812937650"
    assert listing.title == "Stylo plume Montblanc Meisterstück 149"
    assert listing.description is not None
    assert "Montblanc" in listing.description
    assert listing.listing_type == "buy_now"
    assert listing.status == "active"
    assert listing.buy_now_price == Decimal("285.00")
    assert listing.current_price == Decimal("285.00")
    assert listing.currency == "EUR"
    assert listing.condition == "good"
    assert listing.watcher_count == 12
    assert listing.view_count == 87
    assert len(listing.image_urls) == 2
    assert listing.image_urls[0].endswith("f1600/1712345678.jpeg")

  def test_seller(self, parser: VintedParser):
    text = _read_fixture("item_active.json")
    listing = parser.parse_listing(text)
    seller = listing.seller
    assert seller is not None
    assert seller.external_id == "55512340"
    assert seller.username == "pen_collector_75"
    assert seller.country == "FR"
    # 4.8 * 20 = 96.0
    assert seller.rating == 96.0
    assert seller.feedback_count == 47

  def test_shipping_cost(self, parser: VintedParser):
    text = _read_fixture("item_active.json")
    listing = parser.parse_listing(text)
    assert listing.shipping_cost == Decimal("6.45")

  def test_attributes(self, parser: VintedParser):
    text = _read_fixture("item_active.json")
    listing = parser.parse_listing(text)
    assert listing.attributes.get("brand") == "Montblanc"
    assert listing.attributes.get("color") == "Noir"
    assert listing.attributes.get("catalog_id") == "1927"


class TestParseSoldItem:
  def test_parse_sold_fixture(self, parser: VintedParser):
    text = _read_fixture("item_sold.json")
    url = "https://www.vinted.fr/items/3901827364-pelikan-souveran"
    listing = parser.parse_listing(text, url=url)

    assert listing.external_id == "3901827364"
    assert listing.status == "sold"
    assert listing.buy_now_price == Decimal("195.00")

  def test_sold_seller(self, parser: VintedParser):
    text = _read_fixture("item_sold.json")
    listing = parser.parse_listing(text)
    seller = listing.seller
    assert seller is not None
    assert seller.country == "DE"
    # 5.0 * 20 = 100.0
    assert seller.rating == 100.0
    assert seller.feedback_count == 112

  def test_sold_has_two_colors(self, parser: VintedParser):
    text = _read_fixture("item_sold.json")
    listing = parser.parse_listing(text)
    assert listing.attributes.get("color") == "Vert"
    assert listing.attributes.get("color2") == "Noir"


# ==================================================================
# Utility helpers
# ==================================================================


class TestDecimalOrNone:
  def test_string(self):
    assert _decimal_or_none("12.50") == Decimal("12.50")

  def test_int(self):
    assert _decimal_or_none(42) == Decimal("42")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_invalid(self):
    assert _decimal_or_none("not a number") is None
