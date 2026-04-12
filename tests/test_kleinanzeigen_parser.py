"""Tests for the Kleinanzeigen parser."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.kleinanzeigen import (
  KleinanzeigenParser,
  _classify_price_text,
  _decimal_or_none,
  _extract_ad_id_from_sidebar,
  _extract_attributes,
  _extract_belen_conf,
  _extract_description,
  _extract_image_urls,
  _extract_location,
  _extract_posting_date,
  _extract_price,
  _extract_seller,
  _extract_seller_rating,
  _extract_shipping_cost,
  _parse_german_price,
  _shipping_available,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "kleinanzeigen"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> KleinanzeigenParser:
  return KleinanzeigenParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered_in_registry(self):
    assert ParserRegistry.has("kleinanzeigen")

  def test_get_returns_instance(self):
    instance = ParserRegistry.get("kleinanzeigen")
    assert isinstance(instance, KleinanzeigenParser)

  def test_website_name(self, parser: KleinanzeigenParser):
    assert parser.website_name == "kleinanzeigen"


class TestCapabilities:
  def test_can_search(self, parser: KleinanzeigenParser):
    assert parser.capabilities.can_search is True

  def test_can_parse_listing(self, parser: KleinanzeigenParser):
    assert parser.capabilities.can_parse_listing is True

  def test_has_seller_info(self, parser: KleinanzeigenParser):
    assert parser.capabilities.has_seller_info is True

  def test_has_buy_now(self, parser: KleinanzeigenParser):
    assert parser.capabilities.has_buy_now is True

  def test_no_bid_history(self, parser: KleinanzeigenParser):
    assert parser.capabilities.has_bid_history is False

  def test_no_estimates(self, parser: KleinanzeigenParser):
    assert parser.capabilities.has_estimates is False

  def test_no_auction_house_info(self, parser: KleinanzeigenParser):
    assert parser.capabilities.has_auction_house_info is False


# ==================================================================
# URL helpers
# ==================================================================


class TestBuildSearchUrl:
  def test_page_one(self, parser: KleinanzeigenParser):
    url = parser.build_search_url("fountain pen")
    assert url == "https://www.kleinanzeigen.de/s-fountain%20pen/k0"

  def test_page_two(self, parser: KleinanzeigenParser):
    url = parser.build_search_url("fountain pen", page=2)
    assert url == "https://www.kleinanzeigen.de/s-seite:2/fountain%20pen/k0"

  def test_page_one_explicit(self, parser: KleinanzeigenParser):
    url = parser.build_search_url("laptop", page=1)
    assert url == "https://www.kleinanzeigen.de/s-laptop/k0"

  def test_special_characters(self, parser: KleinanzeigenParser):
    url = parser.build_search_url("Füller & Tinte")
    assert "F%C3%BCller" in url
    assert "%26" in url


class TestExtractExternalId:
  def test_standard_url(self, parser: KleinanzeigenParser):
    url = "https://www.kleinanzeigen.de/s-anzeige/bmw-116i/3232063424-216-1914"
    assert parser.extract_external_id(url) == "3232063424"

  def test_three_segment_id(self, parser: KleinanzeigenParser):
    url = "/s-anzeige/pilot-pen/3376469369-93-1055"
    assert parser.extract_external_id(url) == "3376469369"

  def test_no_id(self, parser: KleinanzeigenParser):
    assert parser.extract_external_id("https://www.kleinanzeigen.de/") is None

  def test_short_numbers_ignored(self, parser: KleinanzeigenParser):
    # Numbers shorter than 6 digits in fallback are not matched.
    assert parser.extract_external_id("/s-anzeige/item/123") is None


# ==================================================================
# German price parsing
# ==================================================================


class TestParseGermanPrice:
  def test_simple_integer(self):
    assert _parse_german_price("420 €") == Decimal("420")

  def test_thousands_separator(self):
    assert _parse_german_price("6.500 €") == Decimal("6500")

  def test_thousands_and_decimals(self):
    assert _parse_german_price("1.299,99 €") == Decimal("1299.99")

  def test_with_vb_marker(self):
    assert _parse_german_price("6.500 € VB") == Decimal("6500")

  def test_just_number(self):
    assert _parse_german_price("35") == Decimal("35")

  def test_decimal_comma(self):
    assert _parse_german_price("7,69 €") == Decimal("7.69")

  def test_empty_string(self):
    assert _parse_german_price("") is None

  def test_none_input(self):
    assert _parse_german_price(None) is None

  def test_zu_verschenken(self):
    assert _parse_german_price("Zu verschenken") is None

  def test_auf_anfrage(self):
    assert _parse_german_price("Auf Anfrage") is None

  def test_large_price(self):
    assert _parse_german_price("12.345.678 €") == Decimal("12345678")


class TestDecimalOrNone:
  def test_valid_string(self):
    assert _decimal_or_none("420.00") == Decimal("420.00")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_invalid_string(self):
    assert _decimal_or_none("abc") is None

  def test_integer(self):
    assert _decimal_or_none(42) == Decimal("42")


# ==================================================================
# Price classification
# ==================================================================


class TestClassifyPriceText:
  def test_fixed(self):
    assert _classify_price_text("420 €") == "FIXED"

  def test_negotiable_vb(self):
    assert _classify_price_text("6.500 € VB") == "NEGOTIABLE"

  def test_negotiable_full(self):
    assert _classify_price_text("100 € Verhandlungsbasis") == "NEGOTIABLE"

  def test_free(self):
    assert _classify_price_text("Zu verschenken") == "FREE"

  def test_on_request(self):
    assert _classify_price_text("Auf Anfrage") == "ON_REQUEST"

  def test_empty(self):
    assert _classify_price_text("") is None


# ==================================================================
# BelenConf extraction
# ==================================================================


class TestExtractBelenConf:
  def test_full_extraction(self):
    html = _read_fixture("listing_fixed.html")
    belen = _extract_belen_conf(html)
    assert belen["ad_id"] == "3376469369"
    assert belen["ad_price"] == "420.00"
    assert belen["ad_price_type"] == "FIXED"

  def test_negotiable_type(self):
    html = _read_fixture("listing_negotiable.html")
    belen = _extract_belen_conf(html)
    assert belen["ad_price_type"] == "NEGOTIABLE"
    assert belen["ad_price"] == "6500.00"

  def test_free_type(self):
    html = _read_fixture("listing_free.html")
    belen = _extract_belen_conf(html)
    assert belen["ad_price_type"] == "FREE"

  def test_no_belen_conf(self):
    belen = _extract_belen_conf("<html><body>Nothing here</body></html>")
    assert belen == {}


# ==================================================================
# Listing detail helpers
# ==================================================================


class TestExtractPrice:
  def test_from_belen_conf(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    belen = _extract_belen_conf(html)
    price, price_type = _extract_price(tree, belen)
    assert price == Decimal("420.00")
    assert price_type == "FIXED"

  def test_negotiable_from_belen(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_negotiable.html")
    tree = HTMLParser(html)
    belen = _extract_belen_conf(html)
    price, price_type = _extract_price(tree, belen)
    assert price == Decimal("6500.00")
    assert price_type == "NEGOTIABLE"

  def test_fallback_to_html(self):
    from selectolax.parser import HTMLParser
    # No BelenConf, so falls back to the #viewad-price element.
    html = """<html><body>
      <h2 id="viewad-price">250 € VB</h2>
    </body></html>"""
    tree = HTMLParser(html)
    price, price_type = _extract_price(tree, {})
    assert price == Decimal("250")
    assert price_type == "NEGOTIABLE"

  def test_free_fallback(self):
    from selectolax.parser import HTMLParser
    html = """<html><body>
      <h2 id="viewad-price">Zu verschenken</h2>
    </body></html>"""
    tree = HTMLParser(html)
    price, price_type = _extract_price(tree, {})
    assert price == Decimal("0")
    assert price_type == "FREE"


class TestExtractDescription:
  def test_description_present(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    description = _extract_description(tree)
    assert description is not None
    assert "Pilot Carbonesque Capless" in description

  def test_description_absent(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body></body></html>")
    assert _extract_description(tree) is None


class TestExtractLocation:
  def test_location_present(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    location = _extract_location(tree)
    assert location == "53225 Bonn - Beuel"

  def test_location_absent(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body></body></html>")
    assert _extract_location(tree) is None


class TestExtractPostingDate:
  def test_date_present(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    posting_date = _extract_posting_date(tree)
    assert posting_date == "09.04.2026"

  def test_date_absent(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body></body></html>")
    assert _extract_posting_date(tree) is None


class TestExtractSeller:
  def test_private_seller(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    seller = _extract_seller(tree)
    assert seller is not None
    assert seller.username == "Carsten"
    assert seller.external_id == "26118228"
    assert seller.country == "DE"
    assert seller.member_since == date(2014, 11, 20)
    assert seller.profile_url == "https://www.kleinanzeigen.de/s-bestandsliste.html?userId=26118228"

  def test_commercial_seller(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_commercial.html")
    tree = HTMLParser(html)
    seller = _extract_seller(tree)
    assert seller is not None
    assert seller.username == "TechShop Hamburg"
    assert seller.external_id == "99887766"

  def test_no_seller(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body></body></html>")
    assert _extract_seller(tree) is None


class TestExtractSellerRating:
  def test_top_zufriedenheit_and_freundlichkeit(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    contact = tree.css_first("#viewad-contact")
    rating = _extract_seller_rating(contact)
    assert rating == 100.0

  def test_top_zufriedenheit_only(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_commercial.html")
    tree = HTMLParser(html)
    contact = tree.css_first("#viewad-contact")
    rating = _extract_seller_rating(contact)
    assert rating == 90.0

  def test_no_badges(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_negotiable.html")
    tree = HTMLParser(html)
    contact = tree.css_first("#viewad-contact")
    rating = _extract_seller_rating(contact)
    assert rating is None


class TestExtractShippingCost:
  def test_shipping_present(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    cost = _extract_shipping_cost(tree)
    assert cost == Decimal("7.69")

  def test_shipping_absent(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_negotiable.html")
    tree = HTMLParser(html)
    cost = _extract_shipping_cost(tree)
    assert cost is None

  def test_commercial_shipping(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_commercial.html")
    tree = HTMLParser(html)
    cost = _extract_shipping_cost(tree)
    assert cost == Decimal("5.99")


class TestShippingAvailable:
  def test_with_versand(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    assert _shipping_available(tree) is True

  def test_without_versand(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>Nur Abholung</body></html>")
    assert _shipping_available(tree) is False


class TestExtractImageUrls:
  def test_multiple_images(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    urls = _extract_image_urls(tree)
    assert len(urls) == 4
    assert all("img.kleinanzeigen.de" in u for u in urls)
    # No query parameters.
    assert all("?" not in u for u in urls)

  def test_deduplicated(self):
    from selectolax.parser import HTMLParser
    html = """<html><body>
      <img src="https://img.kleinanzeigen.de/api/v1/prod-ads/images/ab/abc?rule=x" />
      <img src="https://img.kleinanzeigen.de/api/v1/prod-ads/images/ab/abc?rule=y" />
    </body></html>"""
    tree = HTMLParser(html)
    urls = _extract_image_urls(tree)
    assert len(urls) == 1


class TestExtractAttributes:
  def test_attributes_present(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    attributes = _extract_attributes(tree)
    assert attributes.get("Farbe") == "Rot"
    assert attributes.get("Marke") == "Pilot"

  def test_car_attributes(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_negotiable.html")
    tree = HTMLParser(html)
    attributes = _extract_attributes(tree)
    assert attributes.get("Marke") == "BMW"
    assert attributes.get("Kilometerstand") == "74.000 km"


class TestExtractAdIdFromSidebar:
  def test_sidebar_id(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_fixed.html")
    tree = HTMLParser(html)
    assert _extract_ad_id_from_sidebar(tree) == "3376469369"

  def test_no_sidebar(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body></body></html>")
    assert _extract_ad_id_from_sidebar(tree) == ""


# ==================================================================
# Integration: parse_search_results
# ==================================================================


class TestParseSearchResults:
  def test_result_count(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert len(results) == 3

  def test_first_result(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "3376469369"
    assert first.title == "Pilot Capless Füller Fountain Pen"
    assert first.current_price == Decimal("420")
    assert first.currency == "EUR"
    assert first.listing_type == "buy_now"
    assert "kleinanzeigen.de" in first.url

  def test_second_result_negotiable(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    second = results[1]
    assert second.external_id == "3373761346"
    assert second.title == "Ellington Pens Fountain Pen F"
    assert second.current_price == Decimal("35")

  def test_third_result_thousands(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    third = results[2]
    assert third.external_id == "3369635222"
    assert third.current_price == Decimal("1990")

  def test_image_url_from_srcset(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    # Should prefer srcset (higher resolution).
    assert first.image_url is not None
    assert "$_35.AUTO" in first.image_url

  def test_image_url_fallback_to_src(self, parser: KleinanzeigenParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    second = results[1]
    # No srcset on this card, so falls back to src.
    assert second.image_url is not None
    assert "$_2.AUTO" in second.image_url

  def test_empty_page(self, parser: KleinanzeigenParser):
    html = "<html><body></body></html>"
    results = parser.parse_search_results(html)
    assert results == []


# ==================================================================
# Integration: parse_listing
# ==================================================================


class TestParseListingFixed:
  def test_basic_fields(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html, url="https://www.kleinanzeigen.de/s-anzeige/test/3376469369-93-1055")
    assert listing.external_id == "3376469369"
    assert listing.title == "Pilot Capless Füller Fountain Pen"
    assert listing.listing_type == "buy_now"
    assert listing.currency == "EUR"
    assert listing.status == "active"

  def test_price(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("420.00")
    assert listing.buy_now_price == Decimal("420.00")

  def test_description(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert listing.description is not None
    assert "Pilot Carbonesque Capless" in listing.description

  def test_images(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 4

  def test_seller(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "Carsten"
    assert listing.seller.external_id == "26118228"
    assert listing.seller.country == "DE"
    assert listing.seller.member_since == date(2014, 11, 20)
    # TOP Zufriedenheit + Freundlichkeit = 100
    assert listing.seller.rating == 100.0

  def test_shipping(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_cost == Decimal("7.69")
    assert listing.shipping_from_country == "DE"

  def test_attributes(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_fixed.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["Farbe"] == "Rot"
    assert listing.attributes["Marke"] == "Pilot"
    assert listing.attributes["price_type"] == "FIXED"
    assert listing.attributes["location"] == "53225 Bonn - Beuel"
    assert listing.attributes["posting_date"] == "09.04.2026"


class TestParseListingNegotiable:
  def test_price_type_negotiable(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_negotiable.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("6500.00")
    assert listing.attributes["price_type"] == "NEGOTIABLE"

  def test_seller_no_badges(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_negotiable.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "Kevin"
    assert listing.seller.rating is None

  def test_no_shipping(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_negotiable.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_cost is None


class TestParseListingFree:
  def test_free_price(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_free.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("0")
    assert listing.buy_now_price == Decimal("0")
    assert listing.attributes["price_type"] == "FREE"

  def test_free_seller(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_free.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "Maria"


class TestParseListingCommercial:
  def test_commercial_seller(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_commercial.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "TechShop Hamburg"
    assert listing.seller.rating == 90.0

  def test_commercial_shipping(self, parser: KleinanzeigenParser):
    html = _read_fixture("listing_commercial.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_cost == Decimal("5.99")

  def test_no_title_raises(self, parser: KleinanzeigenParser):
    with pytest.raises(ValueError, match="No listing title"):
      parser.parse_listing("<html><body></body></html>")
