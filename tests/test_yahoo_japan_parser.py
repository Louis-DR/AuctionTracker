"""Tests for the Yahoo Japan (Buyee) parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.yahoo_japan import (
  YahooJapanParser,
  _decimal_or_none,
  _derive_status,
  _extract_buy_now_price,
  _extract_card_title,
  _extract_current_price,
  _extract_description,
  _extract_image_urls,
  _extract_seller_section,
  _extract_starting_price,
  _extract_title,
  _extract_watcher_count,
  _int_or_none,
  _parse_buyee_datetime,
  _parse_seller,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "yahoo_japan"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> YahooJapanParser:
  return YahooJapanParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered(self):
    assert ParserRegistry.has("yahoo_japan")

  def test_website_name(self, parser: YahooJapanParser):
    assert parser.website_name == "yahoo_japan"

  def test_capabilities(self, parser: YahooJapanParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.has_watcher_count is True
    assert capabilities.has_buy_now is True
    assert capabilities.has_bid_history is False
    assert capabilities.has_estimates is False


# ==================================================================
# URL helpers
# ==================================================================


class TestUrlHelpers:
  def test_build_search_url(self, parser: YahooJapanParser):
    url = parser.build_search_url("fountain pen")
    assert "buyee.jp" in url
    assert "fountain%20pen" in url
    assert "translationType=1" in url

  def test_build_search_url_page_2(self, parser: YahooJapanParser):
    url = parser.build_search_url("test", page=2)
    assert "page=2" in url

  def test_extract_external_id(self, parser: YahooJapanParser):
    url = "https://buyee.jp/item/yahoo/auction/m1175690842"
    assert parser.extract_external_id(url) == "m1175690842"

  def test_extract_external_id_no_match(self, parser: YahooJapanParser):
    assert parser.extract_external_id("https://example.com") is None


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: YahooJapanParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert len(results) == 2

  def test_first_result(self, parser: YahooJapanParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "m1175690842"
    assert "Montblanc" in first.title
    assert first.current_price == Decimal("15000")
    assert first.currency == "JPY"
    assert first.listing_type == "auction"

  def test_second_result_image(self, parser: YahooJapanParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    second = results[1]
    assert second.image_url is not None
    assert "yimg.jp" in second.image_url

  def test_end_time_is_none(self, parser: YahooJapanParser):
    html = _read_fixture("search_results.html")
    results = parser.parse_search_results(html)
    assert results[0].end_time is None


# ==================================================================
# Active listing
# ==================================================================


class TestActiveListing:
  def test_basic_fields(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.external_id == "m1175690842"
    assert "Montblanc" in listing.title
    assert listing.status == "active"
    assert listing.currency == "JPY"

  def test_pricing(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.current_price == Decimal("15000")
    assert listing.starting_price == Decimal("10000")
    assert listing.final_price is None

  def test_seller(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.seller is not None
    assert listing.seller.display_name == "pen_collector_tokyo"
    assert listing.seller.country == "JP"
    assert listing.seller.rating == 98.5
    assert listing.seller.feedback_count == 1265

  def test_condition(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.condition == "like_new"

  def test_bid_count(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.bid_count == 8

  def test_watcher_count(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.watcher_count == 12

  def test_images(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert len(listing.image_urls) == 3
    assert all("cdnyauction.buyee.jp" in url for url in listing.image_urls)

  def test_times(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.start_time is not None
    assert listing.end_time is not None
    assert listing.start_time.year == 2026

  def test_attributes(self, parser: YahooJapanParser):
    html = _read_fixture("listing_active.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/m1175690842",
    )
    assert listing.attributes["condition_label"] == "Close to unused"
    assert listing.attributes["early_finish"] == "Allowed"
    assert listing.attributes["item_quantity"] == "1"


# ==================================================================
# Ended listing
# ==================================================================


class TestEndedListing:
  def test_status_sold(self, parser: YahooJapanParser):
    html = _read_fixture("listing_ended.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/p2298765432",
    )
    assert listing.status == "sold"

  def test_final_price(self, parser: YahooJapanParser):
    html = _read_fixture("listing_ended.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/p2298765432",
    )
    assert listing.final_price == Decimal("25000")

  def test_condition_very_good(self, parser: YahooJapanParser):
    html = _read_fixture("listing_ended.html")
    listing = parser.parse_listing(
      html, url="https://buyee.jp/item/yahoo/auction/p2298765432",
    )
    assert listing.condition == "very_good"


# ==================================================================
# Helper function tests
# ==================================================================


class TestDeriveStatus:
  def test_ended_with_bids(self):
    html = '<span class="itemInformation__timeRemaining">Ended</span>'
    assert _derive_status(html, 5, None) == "sold"

  def test_ended_no_bids(self):
    html = '<span class="itemInformation__timeRemaining">Ended</span>'
    assert _derive_status(html, 0, None) == "unsold"

  def test_active(self):
    html = '<span class="itemInformation__timeRemaining">2 Days</span>'
    assert _derive_status(html, 3, None) == "active"


class TestParseBuyeeDatetime:
  def test_common_format(self):
    result = _parse_buyee_datetime("7 Feb 2026 21:41:41")
    assert result is not None
    assert result.year == 2026
    assert result.month == 2

  def test_iso_format(self):
    result = _parse_buyee_datetime("2026-02-07 21:41:41")
    assert result is not None

  def test_empty(self):
    assert _parse_buyee_datetime("") is None

  def test_invalid(self):
    assert _parse_buyee_datetime("not a date") is None


class TestDecimalOrNone:
  def test_with_commas(self):
    assert _decimal_or_none("15,000") == Decimal("15000")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_empty(self):
    assert _decimal_or_none("") is None


class TestIntOrNone:
  def test_with_commas(self):
    assert _int_or_none("1,250") == 1250

  def test_none(self):
    assert _int_or_none(None) is None


class TestExtractSellerSection:
  def test_basic(self):
    html = '''<div class="itemSeller">
      <span>Seller</span><span>test_user</span>
      <span>Item Condition</span><span>New</span>
    </div><div class="itemDescription">'''
    info = _extract_seller_section(html)
    assert info["seller"] == "test_user"
    assert info["item_condition"] == "New"


class TestParseSeller:
  def test_valid(self):
    info = {"seller": "test", "percentage_of_good_ratings": "99.0%", "good": "100", "bad": "5"}
    seller = _parse_seller(info)
    assert seller is not None
    assert seller.rating == 99.0
    assert seller.feedback_count == 105

  def test_no_seller(self):
    assert _parse_seller({}) is None
