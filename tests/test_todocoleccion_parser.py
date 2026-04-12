"""Tests for the Todocoleccion parser."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.todocoleccion import (
  TodocoleccionParser,
  _decimal_or_none,
  _derive_status,
  _extract_auction_info,
  _extract_condition,
  _extract_end_time,
  _extract_image_urls,
  _extract_jsonld_product,
  _extract_seller,
  _extract_shipping_cost,
  _parse_euro_price,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "todocoleccion"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> TodocoleccionParser:
  return TodocoleccionParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered_in_registry(self):
    assert ParserRegistry.has("todocoleccion")

  def test_get_returns_instance(self):
    instance = ParserRegistry.get("todocoleccion")
    assert isinstance(instance, TodocoleccionParser)

  def test_website_name(self, parser: TodocoleccionParser):
    assert parser.website_name == "todocoleccion"


class TestCapabilities:
  def test_can_search(self, parser: TodocoleccionParser):
    assert parser.capabilities.can_search is True

  def test_can_parse_listing(self, parser: TodocoleccionParser):
    assert parser.capabilities.can_parse_listing is True

  def test_has_seller_info(self, parser: TodocoleccionParser):
    assert parser.capabilities.has_seller_info is True

  def test_has_buy_now(self, parser: TodocoleccionParser):
    assert parser.capabilities.has_buy_now is True

  def test_no_bid_history(self, parser: TodocoleccionParser):
    assert parser.capabilities.has_bid_history is False

  def test_no_estimates(self, parser: TodocoleccionParser):
    assert parser.capabilities.has_estimates is False


# ==================================================================
# URL helpers
# ==================================================================


class TestBuildSearchUrl:
  def test_basic_query(self, parser: TodocoleccionParser):
    url = parser.build_search_url("pluma montblanc")
    assert "todocoleccion.net/buscador" in url
    assert "bu=pluma+montblanc" in url

  def test_page_one_omits_page_param(self, parser: TodocoleccionParser):
    url = parser.build_search_url("pen", page=1)
    assert "P=" not in url

  def test_page_two(self, parser: TodocoleccionParser):
    url = parser.build_search_url("pen", page=2)
    assert "P=2" in url

  def test_page_three(self, parser: TodocoleccionParser):
    url = parser.build_search_url("pen", page=3)
    assert "P=3" in url


class TestExtractExternalId:
  def test_standard_url(self, parser: TodocoleccionParser):
    url = "https://www.todocoleccion.net/estilograficas-antiguas/montblanc-pluma~x430695627"
    assert parser.extract_external_id(url) == "430695627"

  def test_no_id_returns_none(self, parser: TodocoleccionParser):
    assert parser.extract_external_id("https://www.todocoleccion.net/buscador") is None

  def test_relative_url(self, parser: TodocoleccionParser):
    url = "/estilograficas-antiguas/montblanc-pluma~x12345"
    assert parser.extract_external_id(url) == "12345"


# ==================================================================
# Price parsing
# ==================================================================


class TestParseEuroPrice:
  def test_simple_price(self):
    assert _parse_euro_price("40,00 €") == Decimal("40.00")

  def test_thousands_separator(self):
    assert _parse_euro_price("1.200,00 €") == Decimal("1200.00")

  def test_no_cents(self):
    assert _parse_euro_price("100 €") == Decimal("100")

  def test_with_comma_cents(self):
    assert _parse_euro_price("99,00") == Decimal("99.00")

  def test_empty_string(self):
    assert _parse_euro_price("") is None

  def test_none(self):
    assert _parse_euro_price(None) is None

  def test_large_price(self):
    assert _parse_euro_price("12.500,50 €") == Decimal("12500.50")


class TestDecimalOrNone:
  def test_string_value(self):
    assert _decimal_or_none("99.00") == Decimal("99.00")

  def test_int_value(self):
    assert _decimal_or_none(42) == Decimal("42")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_invalid_string(self):
    assert _decimal_or_none("not-a-number") is None


# ==================================================================
# JSON-LD extraction
# ==================================================================


class TestExtractJsonldProduct:
  def test_finds_product_block(self):
    html = _read_fixture("listing_buy_now.html")
    product = _extract_jsonld_product(html)
    assert product is not None
    assert product["@type"] == "Product"
    assert product["sku"] == 430695627

  def test_ignores_breadcrumb(self):
    html = _read_fixture("listing_buy_now.html")
    product = _extract_jsonld_product(html)
    assert product["@type"] == "Product"

  def test_no_product_returns_none(self):
    html = "<html><body>No JSON-LD here</body></html>"
    assert _extract_jsonld_product(html) is None

  def test_auction_product(self):
    html = _read_fixture("listing_auction.html")
    product = _extract_jsonld_product(html)
    assert product["sku"] == 401566449
    assert product["offers"]["price"] == "100.00"


# ==================================================================
# Listing detail extraction
# ==================================================================


class TestExtractEndTime:
  def test_auction_end_time(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    end_time = _extract_end_time(tree)
    assert end_time is not None
    assert end_time.year == 2026
    assert end_time.month == 4
    assert end_time.day == 15
    assert end_time.hour == 18
    assert end_time.minute == 0
    # CEST = UTC+2
    assert end_time.tzinfo == timezone(timedelta(hours=2))

  def test_buy_now_no_end_time(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    assert _extract_end_time(tree) is None


class TestExtractAuctionInfo:
  def test_auction_listing(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    listing_type, bid_count, starting_price = _extract_auction_info(tree)
    assert listing_type == "auction"
    assert bid_count == 7
    assert starting_price == Decimal("100.00")

  def test_buy_now_listing(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    listing_type, bid_count, starting_price = _extract_auction_info(tree)
    assert listing_type == "buy_now"
    assert bid_count is None
    assert starting_price is None


class TestDeriveStatus:
  def test_in_stock(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>Active listing</body></html>")
    offers = {"availability": "https://schema.org/InStock"}
    assert _derive_status(tree, offers) == "active"

  def test_sold_out(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_sold.html")
    tree = HTMLParser(html)
    offers = {"availability": "https://schema.org/SoldOut"}
    assert _derive_status(tree, offers) == "sold"

  def test_vendido_in_text(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>Vendido el 05/07/2025</body></html>")
    offers = {}
    assert _derive_status(tree, offers) == "sold"


class TestExtractCondition:
  def test_normal_condition(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    condition = _extract_condition(tree)
    assert condition is not None
    assert "Normal" in condition

  def test_muy_bueno_condition(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    condition = _extract_condition(tree)
    assert condition is not None
    assert "Muy Bueno" in condition

  def test_no_condition(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>No condition info</body></html>")
    assert _extract_condition(tree) is None


class TestExtractShippingCost:
  def test_shipping_found(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    cost = _extract_shipping_cost(tree)
    assert cost == Decimal("6.00")

  def test_auction_shipping(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    cost = _extract_shipping_cost(tree)
    assert cost == Decimal("4.50")

  def test_no_shipping(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>No shipping info</body></html>")
    assert _extract_shipping_cost(tree) is None


class TestExtractSeller:
  def test_buy_now_seller(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    seller = _extract_seller(tree)
    assert seller is not None
    assert seller.username == "antiguedades-maritxu"
    assert seller.rating == 100.0  # 5 stars * 20
    assert seller.feedback_count == 894
    assert seller.country == "ES"
    assert seller.member_since is not None
    assert seller.member_since.year == 2003
    assert seller.member_since.month == 9
    assert seller.member_since.day == 4
    assert seller.profile_url is not None
    assert "antiguedades-maritxu" in seller.profile_url

  def test_auction_seller(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    seller = _extract_seller(tree)
    assert seller is not None
    assert seller.username == "laprimitiva"
    # title says "4 estrellas" -> 4 * 20 = 80
    assert seller.rating == 80.0
    assert seller.feedback_count == 11854
    assert seller.country == "ES"
    assert seller.member_since is not None
    assert seller.member_since.year == 2014

  def test_sold_listing_seller(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_sold.html")
    tree = HTMLParser(html)
    seller = _extract_seller(tree)
    assert seller is not None
    assert seller.username == "collector99"
    assert seller.rating == 60.0  # 3 stars * 20
    assert seller.feedback_count == 256

  def test_no_seller(self):
    from selectolax.parser import HTMLParser
    tree = HTMLParser("<html><body>No seller info</body></html>")
    assert _extract_seller(tree) is None


class TestExtractImageUrls:
  def test_buy_now_images(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_buy_now.html")
    tree = HTMLParser(html)
    urls = _extract_image_urls(tree, "430695627")
    assert len(urls) >= 2
    assert all("430695627" in url for url in urls)
    # No query parameters (size/crop) in the URLs.
    assert all("?" not in url for url in urls)

  def test_auction_images(self):
    from selectolax.parser import HTMLParser
    html = _read_fixture("listing_auction.html")
    tree = HTMLParser(html)
    urls = _extract_image_urls(tree, "401566449")
    assert len(urls) >= 1
    assert "401566449" in urls[0]


# ==================================================================
# Integration: parse_search_results
# ==================================================================


class TestParseSearchResults:
  def test_parses_three_results(self, parser: TodocoleccionParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert len(results) == 3

  def test_first_result_buy_now(self, parser: TodocoleccionParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "403780244"
    assert first.title == "CAJA ESTUCHE PARA PLUMA ESTILOGRAFICA MONTBLANC"
    assert first.current_price == Decimal("40.00")
    assert first.currency == "EUR"
    assert first.listing_type == "buy_now"
    assert first.bid_count is None
    assert "~x403780244" in first.url
    assert first.image_url is not None

  def test_second_result_auction(self, parser: TodocoleccionParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    second = results[1]
    assert second.external_id == "401566449"
    assert second.title == "Antigua pluma MontBlanc"
    assert second.current_price == Decimal("100.00")
    assert second.listing_type == "auction"
    assert second.bid_count == 0

  def test_third_result_expensive(self, parser: TodocoleccionParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    third = results[2]
    assert third.external_id == "625730674"
    assert third.current_price == Decimal("1200.00")
    assert third.listing_type == "buy_now"

  def test_empty_page(self, parser: TodocoleccionParser):
    html = "<html><body><div>No results</div></body></html>"
    results = parser.parse_search_results(html)
    assert results == []


# ==================================================================
# Integration: parse_listing (buy now)
# ==================================================================


class TestParseBuyNowListing:
  def test_basic_fields(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.external_id == "430695627"
    assert listing.title == "MONTBLANC PLUMA ESTILOGRAFICA ANTIGUA, TRAZO FINO"
    assert listing.description is not None
    assert listing.currency == "EUR"

  def test_price(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("99.00")
    assert listing.buy_now_price == Decimal("99.00")

  def test_listing_type(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.listing_type == "buy_now"

  def test_status(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.status == "active"

  def test_condition(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.condition is not None
    assert "Normal" in listing.condition

  def test_shipping(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_cost == Decimal("6.00")

  def test_seller(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "antiguedades-maritxu"

  def test_images(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) >= 2

  def test_no_end_time(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert listing.end_time is None

  def test_url(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)
    assert "todocoleccion.net" in listing.url
    assert "~x430695627" in listing.url


# ==================================================================
# Integration: parse_listing (auction)
# ==================================================================


class TestParseAuctionListing:
  def test_listing_type(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.listing_type == "auction"

  def test_bid_count(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.bid_count == 7

  def test_starting_price(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.starting_price == Decimal("100.00")

  def test_end_time(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.end_time is not None
    assert listing.end_time.year == 2026
    assert listing.end_time.month == 4
    assert listing.end_time.day == 15

  def test_no_buy_now_price(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.buy_now_price is None

  def test_shipping(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_cost == Decimal("4.50")

  def test_seller(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "laprimitiva"
    assert listing.seller.rating == 80.0


# ==================================================================
# Integration: parse_listing (sold)
# ==================================================================


class TestParseSoldListing:
  def test_status_sold(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_final_price(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("350.00")

  def test_bid_count(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.bid_count == 23

  def test_seller_three_stars(self, parser: TodocoleccionParser):
    html = _read_fixture("listing_sold.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.rating == 60.0
    assert listing.seller.feedback_count == 256


# ==================================================================
# Error handling
# ==================================================================


class TestErrorHandling:
  def test_no_jsonld_raises(self, parser: TodocoleccionParser):
    html = "<html><body>No JSON-LD</body></html>"
    with pytest.raises(ValueError, match="No JSON-LD Product"):
      parser.parse_listing(html)

  def test_blocking_page_raises(self, parser: TodocoleccionParser):
    from auction_tracker.parsing.base import ParserBlocked
    html = "<html><head><title>Just a moment...</title></head><body>Checking browser</body></html>"
    with pytest.raises(ParserBlocked):
      parser.parse_search_results(html)
    with pytest.raises(ParserBlocked):
      parser.parse_listing(html)
