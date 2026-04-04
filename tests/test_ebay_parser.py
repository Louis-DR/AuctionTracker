"""Tests for the eBay parser using golden file HTML fixtures.

Each test feeds saved HTML into the parser and verifies that the
structured output matches expectations. This catches regressions
when the parser code changes and immediately reveals when eBay
changes their HTML structure (the fixture will need updating).
"""

from __future__ import annotations

from datetime import UTC
from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.ebay import (
  EbayParser,
  _derive_status,
  _detect_currency,
  _extract_price_from_text,
  _is_blocked_page,
  _parse_bid_amount,
  _parse_ebay_bid_datetime,
  _parse_ebay_datetime,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ebay"


@pytest.fixture
def parser() -> EbayParser:
  return EbayParser()


def _read_fixture(name: str) -> str:
  return (FIXTURES / name).read_text(encoding="utf-8")


# ------------------------------------------------------------------
# Registration and capabilities
# ------------------------------------------------------------------


class TestRegistration:

  def test_ebay_parser_is_registered(self):
    assert ParserRegistry.has("ebay")

  def test_capabilities(self, parser: EbayParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.can_parse_listing is True
    assert capabilities.has_bid_history is True
    assert capabilities.has_seller_info is True
    assert capabilities.has_watcher_count is True
    assert capabilities.has_buy_now is True


# ------------------------------------------------------------------
# URL building and ID extraction
# ------------------------------------------------------------------


class TestUrlBuilding:

  def test_build_search_url_default(self, parser: EbayParser):
    url = parser.build_search_url("fountain pen")
    assert "ebay.com" in url
    assert "_nkw=fountain+pen" in url
    assert "_sacat=0" in url

  def test_build_search_url_with_page(self, parser: EbayParser):
    url = parser.build_search_url("montblanc", page=3)
    assert "_pgn=3" in url

  def test_build_search_url_custom_domain(self, parser: EbayParser):
    url = parser.build_search_url("stylo plume", domain="ebay.fr")
    assert "ebay.fr" in url

  def test_extract_external_id(self, parser: EbayParser):
    assert parser.extract_external_id("https://www.ebay.com/itm/1234567890") == "1234567890"
    assert parser.extract_external_id("1234567890") == "1234567890"
    assert parser.extract_external_id("not-a-url") is None


# ------------------------------------------------------------------
# Search results parsing
# ------------------------------------------------------------------


class TestSearchParsing:

  def test_parse_search_results(self, parser: EbayParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)

    assert len(results) == 3

  def test_first_result_fields(self, parser: EbayParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    first = results[0]

    assert first.external_id == "1234567890"
    assert "ebay.com/itm/1234567890" in first.url
    assert "Montblanc 149" in first.title
    assert first.current_price == Decimal("450.00")
    assert first.currency == "USD"
    assert first.bid_count == 12
    assert first.image_url is not None
    assert "ebayimg.com" in first.image_url

  def test_gbp_result(self, parser: EbayParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    second = results[1]

    assert second.external_id == "9876543210"
    assert second.current_price == Decimal("325.00")
    assert second.currency == "GBP"

  def test_hybrid_listing_detection(self, parser: EbayParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    third = results[2]

    assert third.external_id == "5555555555"
    assert third.listing_type == "hybrid"

  def test_empty_html_returns_empty(self, parser: EbayParser):
    results = parser.parse_search_results("<html><body>Nothing here</body></html>")
    assert results == []


# ------------------------------------------------------------------
# Listing detail parsing
# ------------------------------------------------------------------


class TestListingParsing:

  def test_auction_listing(self, parser: EbayParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)

    assert listing.external_id == "1234567890"
    assert "Montblanc 149" in listing.title
    assert listing.current_price == Decimal("450.00")
    assert listing.currency == "USD"
    assert listing.bid_count == 12
    assert listing.listing_type == "auction"
    assert listing.condition == "good"
    assert listing.watcher_count == 24
    assert listing.shipping_cost == Decimal("15.00")

  def test_auction_seller(self, parser: EbayParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)

    assert listing.seller is not None
    assert listing.seller.username == "pen_collector_99"

  def test_auction_dates(self, parser: EbayParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)

    assert listing.start_time is not None
    assert listing.start_time.year == 2026
    assert listing.end_time is not None
    assert listing.end_time.year == 2026

  def test_auction_images(self, parser: EbayParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)

    assert len(listing.image_urls) >= 1
    assert all("ebayimg.com" in url for url in listing.image_urls)
    # Images should be upgraded to full size.
    assert all("s-l1600" in url for url in listing.image_urls)

  def test_auction_description(self, parser: EbayParser):
    html = _read_fixture("listing_auction.html")
    listing = parser.parse_listing(html)

    assert listing.description is not None
    assert "scratches" in listing.description.lower()

  def test_buy_now_listing(self, parser: EbayParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)

    assert listing.external_id == "2222222222"
    assert listing.listing_type == "buy_now"
    assert listing.current_price == Decimal("280.00")
    assert listing.status == "active"
    assert listing.bid_count == 0

  def test_buy_now_attributes(self, parser: EbayParser):
    html = _read_fixture("listing_buy_now.html")
    listing = parser.parse_listing(html)

    assert listing.attributes.get("best_offer_enabled") == "true"
    assert listing.attributes.get("quantity_available") == "3"
    assert listing.attributes.get("quantity_sold") == "7"

  def test_ended_sold_listing(self, parser: EbayParser):
    html = _read_fixture("listing_ended_sold.html")
    listing = parser.parse_listing(html)

    assert listing.external_id == "3333333333"
    assert listing.status == "sold"
    assert listing.final_price == Decimal("185.00")
    assert listing.bid_count == 8

  def test_blocked_page_raises(self, parser: EbayParser):
    blocked_html = """<html><head><title>Security Measure</title></head>
    <body>Please confirm your identity. challenge page.</body></html>"""
    with pytest.raises(ValueError, match="blocked"):
      parser.parse_listing(blocked_html)


# ------------------------------------------------------------------
# Bid history parsing
# ------------------------------------------------------------------


class TestBidHistoryParsing:

  def test_parse_bid_history(self, parser: EbayParser):
    html = _read_fixture("bid_history.html")
    bids = parser.parse_bid_history(html, currency="USD")

    assert len(bids) == 5

  def test_bids_sorted_by_amount(self, parser: EbayParser):
    html = _read_fixture("bid_history.html")
    bids = parser.parse_bid_history(html, currency="USD")

    amounts = [bid.amount for bid in bids]
    assert amounts == sorted(amounts)

  def test_bid_fields(self, parser: EbayParser):
    html = _read_fixture("bid_history.html")
    bids = parser.parse_bid_history(html, currency="USD")

    highest_bid = bids[-1]
    assert highest_bid.amount == Decimal("450.00")
    assert highest_bid.currency == "USD"
    assert highest_bid.bid_time is not None
    assert highest_bid.bidder_username is not None

  def test_blocked_bid_page_returns_empty(self, parser: EbayParser):
    blocked = "<html><body>sign in or register to view bids</body></html>"
    bids = parser.parse_bid_history(blocked, currency="USD")
    assert bids == []


# ------------------------------------------------------------------
# Helper function tests
# ------------------------------------------------------------------


class TestHelperFunctions:

  def test_parse_ebay_datetime_iso(self):
    result = _parse_ebay_datetime("2026-03-20T10:00:00.000Z")
    assert result is not None
    assert result.year == 2026
    assert result.month == 3
    assert result.day == 20

  def test_parse_ebay_datetime_none(self):
    assert _parse_ebay_datetime(None) is None
    assert _parse_ebay_datetime("") is None

  def test_parse_bid_datetime_us_format(self):
    result = _parse_ebay_bid_datetime("Apr 02, 2026 14:23:45 PST")
    assert result is not None
    assert result.month == 4
    assert result.day == 2

  def test_parse_bid_datetime_european_format(self):
    result = _parse_ebay_bid_datetime("02 Apr 2026 14:23:45 GMT")
    assert result is not None
    assert result.month == 4

  def test_parse_bid_datetime_german_format(self):
    result = _parse_ebay_bid_datetime("02.04.2026 14:23:45 MEZ")
    assert result is not None
    assert result.month == 4

  def test_detect_currency_usd(self):
    assert _detect_currency("US $450.00") == "USD"

  def test_detect_currency_gbp(self):
    assert _detect_currency("£325.00") == "GBP"

  def test_detect_currency_eur(self):
    assert _detect_currency("€100.00") == "EUR"

  def test_extract_price_usd(self):
    assert _extract_price_from_text("US $450.00") == Decimal("450.00")

  def test_extract_price_gbp(self):
    assert _extract_price_from_text("£1,250.00") == Decimal("1250.00")

  def test_parse_bid_amount_usd(self):
    assert _parse_bid_amount("US $450.00") == Decimal("450.00")

  def test_parse_bid_amount_eur(self):
    assert _parse_bid_amount("€1.234,56") == Decimal("1234.56")

  def test_parse_bid_amount_plain(self):
    assert _parse_bid_amount("450.00") == Decimal("450.00")

  def test_parse_bid_amount_non_price(self):
    assert _parse_bid_amount("abc123") is None

  def test_is_blocked_page_challenge(self):
    html = "<html><body>challenge page confirm your identity</body></html>"
    assert _is_blocked_page(html) is True

  def test_is_blocked_page_normal(self):
    html = "<html><body>" + "x" * 60000 + "</body></html>"
    assert _is_blocked_page(html) is False

  def test_derive_status_active_auction(self):
    from datetime import datetime
    future = datetime(2099, 1, 1, tzinfo=UTC)
    assert _derive_status("", 3, future, "auction", None) == "active"

  def test_derive_status_ended_sold(self):
    assert _derive_status('"ENDED"', 5, None, "auction", None) == "sold"

  def test_derive_status_ended_unsold(self):
    assert _derive_status('"ENDED"', 0, None, "auction", None) == "unsold"

  def test_derive_status_buy_now_active(self):
    assert _derive_status("", 0, None, "buy_now", 3) == "active"

  def test_derive_status_buy_now_sold_out(self):
    assert _derive_status("", 0, None, "buy_now", 0) == "sold"
