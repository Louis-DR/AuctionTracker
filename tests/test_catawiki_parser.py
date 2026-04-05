"""Tests for the Catawiki parser.

Uses golden file fixtures to test search result parsing, lot page
parsing (active, sold, unsold), bid history parsing, and various
helper functions.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserCapabilities, ParserRegistry
from auction_tracker.parsing.sites.catawiki import (
  CatawikiParser,
  _derive_lot_status,
  _extract_lot_id,
  _extract_page_props,
  _parse_api_bids,
  _parse_condition,
  _parse_embedded_bids,
  _parse_images,
  _parse_seller,
  _parse_specifications,
  _timestamp_ms_to_datetime,
)

FIXTURES = Path(__file__).parent / "fixtures" / "catawiki"


def _read_fixture(name: str) -> str:
  return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture()
def parser() -> CatawikiParser:
  return CatawikiParser()


# ------------------------------------------------------------------
# Registration and capabilities
# ------------------------------------------------------------------


class TestRegistration:

  def test_registered_in_parser_registry(self):
    assert ParserRegistry.has("catawiki")

  def test_website_name(self, parser: CatawikiParser):
    assert parser.website_name == "catawiki"

  def test_capabilities(self, parser: CatawikiParser):
    capabilities = parser.capabilities
    assert isinstance(capabilities, ParserCapabilities)
    assert capabilities.can_search is True
    assert capabilities.can_parse_listing is True
    assert capabilities.has_bid_history is True
    assert capabilities.has_seller_info is True
    assert capabilities.has_estimates is True
    assert capabilities.has_reserve_price is True
    assert capabilities.has_lot_numbers is True
    assert capabilities.has_buy_now is False
    assert capabilities.has_watcher_count is False


# ------------------------------------------------------------------
# URL building and extraction
# ------------------------------------------------------------------


class TestUrlBuilding:

  def test_build_search_url_default(self, parser: CatawikiParser):
    url = parser.build_search_url("fountain pen")
    assert "catawiki.com" in url
    assert "buyer/api/v1/search" in url
    assert "q=fountain+pen" in url
    assert "page=1" in url

  def test_build_search_url_with_page(self, parser: CatawikiParser):
    url = parser.build_search_url("montblanc", page=3)
    assert "page=3" in url

  def test_extract_external_id(self, parser: CatawikiParser):
    assert parser.extract_external_id(
      "https://www.catawiki.com/en/l/101149019-montblanc-149"
    ) == "101149019"
    assert parser.extract_external_id(
      "https://www.catawiki.com/en/l/12345"
    ) == "12345"
    assert parser.extract_external_id("not-a-catawiki-url") is None

  def test_build_bids_url(self, parser: CatawikiParser):
    url = parser.build_bids_url("101149019")
    assert "buyer/api/v3/lots/101149019/bids" in url
    assert "currency_code=EUR" in url
    assert "per_page=100" in url


# ------------------------------------------------------------------
# Search results parsing
# ------------------------------------------------------------------


class TestSearchParsing:

  def test_parse_search_results(self, parser: CatawikiParser):
    json_text = _read_fixture("search_fountain_pen.json")
    results = parser.parse_search_results(json_text)
    assert len(results) == 3

  def test_first_result_fields(self, parser: CatawikiParser):
    json_text = _read_fixture("search_fountain_pen.json")
    results = parser.parse_search_results(json_text)
    first = results[0]

    assert first.external_id == "101149019"
    assert "catawiki.com" in first.url
    assert "Montblanc" in first.title
    assert first.currency == "EUR"
    assert first.listing_type == "auction"
    assert first.image_url is not None
    assert "catawiki" in first.image_url

  def test_empty_search(self, parser: CatawikiParser):
    results = parser.parse_search_results('{"lots": [], "total": 0}')
    assert results == []

  def test_invalid_json_raises(self, parser: CatawikiParser):
    with pytest.raises(ValueError, match="not valid JSON"):
      parser.parse_search_results("<html>not json</html>")


# ------------------------------------------------------------------
# Lot page parsing (active)
# ------------------------------------------------------------------


class TestActiveLotParsing:

  def test_parse_active_lot(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html, url="https://www.catawiki.com/en/l/101149019")
    assert listing.external_id == "101149019"
    assert listing.status == "active"

  def test_active_lot_title(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert "Montblanc" in listing.title
    assert "149 Meisterstuck" in listing.title

  def test_active_lot_prices(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("285")
    assert listing.starting_price == Decimal("1")
    assert listing.currency == "EUR"
    assert listing.final_price is None
    assert listing.buyer_premium_percent == Decimal("9.0000")

  def test_active_lot_estimates(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.estimate_low == Decimal("400")
    assert listing.estimate_high == Decimal("600")

  def test_active_lot_timing(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.start_time is not None
    assert listing.end_time is not None
    assert listing.start_time < listing.end_time

  def test_active_lot_seller(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "pendealer_nl"
    assert listing.seller.display_name == "Pen Dealer Netherlands"
    assert listing.seller.country == "NL"
    assert listing.seller.rating == 4.8
    assert listing.seller.feedback_count == 312
    assert listing.seller.member_since is not None
    assert listing.seller.profile_url is not None

  def test_active_lot_images(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    # Gallery images (xl) come first, then non-duplicate lot images.
    assert len(listing.image_urls) == 5
    assert all("xl_" in url for url in listing.image_urls[:3])

  def test_active_lot_embedded_bids(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert len(listing.bids) == 5
    assert listing.bid_count == 5
    # Bids should be sorted by amount ascending.
    amounts = [bid.amount for bid in listing.bids]
    assert amounts == sorted(amounts)

  def test_active_lot_condition(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "like_new"

  def test_active_lot_reserve(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.reserve_price == Decimal("350")
    assert listing.attributes["has_reserve_price"] == "True"
    assert listing.attributes["reserve_price_met"] == "False"
    assert listing.attributes["close_to_reserve_price"] == "True"

  def test_active_lot_attributes(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["brand"] == "Montblanc"
    assert listing.attributes["model"] == "149 Meisterstuck"
    assert listing.attributes["filling_system"] == "Piston"
    assert listing.attributes["era"] == "1990s"

  def test_active_lot_auction_metadata(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.sale_name == "Fine Writing Instruments Auction"
    assert listing.attributes["auction_name"] == "Fine Writing Instruments Auction"
    assert "categories" in listing.attributes
    assert "Fountain Pens" in listing.attributes["categories"]

  def test_active_lot_description(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.description is not None
    assert "Montblanc" in listing.description

  def test_active_lot_watcher_count(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.watcher_count == 42

  def test_active_lot_shipping(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_from_country == "NL"
    assert listing.ships_internationally is True

  def test_active_lot_ai_summary(self, parser: CatawikiParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert "ai_summary" in listing.attributes
    assert "Montblanc" in listing.attributes["ai_summary"]


# ------------------------------------------------------------------
# Lot page parsing (sold)
# ------------------------------------------------------------------


class TestSoldLotParsing:

  def test_parse_sold_lot_status(self, parser: CatawikiParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_sold_lot_final_price(self, parser: CatawikiParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price == Decimal("320")
    assert listing.current_price == Decimal("320")

  def test_sold_lot_no_reserve(self, parser: CatawikiParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.reserve_price is None
    assert listing.attributes["has_reserve_price"] == "False"

  def test_sold_lot_seller(self, parser: CatawikiParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "vintage_pens_de"
    assert listing.seller.country == "DE"

  def test_sold_lot_condition(self, parser: CatawikiParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.condition == "very_good"

  def test_sold_lot_images_fallback_to_large(self, parser: CatawikiParser):
    """When rawGallery is null, images come from the images list."""
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 1
    assert "large_1" in listing.image_urls[0]


# ------------------------------------------------------------------
# Lot page parsing (unsold)
# ------------------------------------------------------------------


class TestUnsoldLotParsing:

  def test_parse_unsold_lot_status(self, parser: CatawikiParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "unsold"

  def test_unsold_lot_no_final_price(self, parser: CatawikiParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price is None
    assert listing.current_price == Decimal("95")

  def test_unsold_lot_reserve_not_met(self, parser: CatawikiParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.reserve_price == Decimal("200")
    assert listing.attributes["reserve_price_met"] == "False"


# ------------------------------------------------------------------
# Bid history API parsing
# ------------------------------------------------------------------


class TestBidHistoryParsing:

  def test_parse_api_bids(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    assert len(bids) == 11

  def test_bids_sorted_by_amount(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    amounts = [bid.amount for bid in bids]
    assert amounts == sorted(amounts)

  def test_bidder_name_prefix_stripped(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    # "Bidder 12345" should become "12345".
    bidder_names = {bid.bidder_username for bid in bids if bid.bidder_username}
    assert "12345" in bidder_names
    assert "67890" in bidder_names
    assert all(not name.startswith("Bidder ") for name in bidder_names)

  def test_bidder_country_parsed(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    countries = {bid.bidder_country for bid in bids if bid.bidder_country}
    assert "FR" in countries
    assert "GB" in countries
    assert "DE" in countries

  def test_automatic_bids_flagged(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    automatic_bids = [bid for bid in bids if bid.is_automatic]
    assert len(automatic_bids) == 1
    assert automatic_bids[0].amount == Decimal("310")

  def test_bid_currencies(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    assert all(bid.currency == "EUR" for bid in bids)

  def test_bid_timestamps_present(self, parser: CatawikiParser):
    json_text = _read_fixture("bids_api.json")
    bids = parser.parse_bid_history(json_text)
    assert all(bid.bid_time is not None for bid in bids)

  def test_empty_bids_response(self, parser: CatawikiParser):
    assert parser.parse_bid_history('{"bids": []}') == []
    assert parser.parse_bid_history("{}") == []
    assert parser.parse_bid_history("invalid json") == []

  def test_country_as_string(self):
    """The country field can be a plain string instead of a dict."""
    bids = _parse_api_bids({
      "bids": [{
        "amount": 100,
        "currency_code": "EUR",
        "created_at": "2025-12-01T10:00:00Z",
        "bidder": {"name": "Bidder 999", "country": "NL"},
        "from_order": False,
      }],
    })
    assert len(bids) == 1
    assert bids[0].bidder_country == "NL"


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------


class TestHelpers:

  def test_extract_lot_id_from_full_url(self):
    assert _extract_lot_id(
      "https://www.catawiki.com/en/l/101149019-montblanc-149"
    ) == "101149019"

  def test_extract_lot_id_from_short_url(self):
    assert _extract_lot_id("https://www.catawiki.com/en/l/12345") == "12345"

  def test_extract_lot_id_bare_number(self):
    # A bare numeric "URL" falls through to the last-segment approach.
    assert _extract_lot_id("99999999") == "99999999"

  def test_extract_lot_id_non_numeric(self):
    assert _extract_lot_id("not-a-catawiki-url") is None

  def test_derive_lot_status_sold(self):
    assert _derive_lot_status({}, {"sold": True}) == "sold"

  def test_derive_lot_status_closed_unsold(self):
    assert _derive_lot_status({}, {"sold": False, "closed": True}) == "unsold"
    assert _derive_lot_status({"isClosed": True}, {}) == "unsold"

  def test_derive_lot_status_not_open(self):
    assert _derive_lot_status({"open": False}, {}) == "unsold"

  def test_derive_lot_status_live_closed(self):
    bidding = {"live": {"lot": {"closeStatus": "Closed"}}}
    assert _derive_lot_status({}, bidding) == "unsold"

  def test_derive_lot_status_active(self):
    assert _derive_lot_status({"open": True}, {}) == "active"
    assert _derive_lot_status({}, {}) == "active"

  def test_parse_seller_complete(self):
    seller_info = {
      "id": 12345,
      "userName": "pen_collector",
      "sellerName": "Pen Collector Shop",
      "address": {"country": {"shortCode": "fr"}},
      "score": {"score": 4.7, "lifetimeCount": 200},
      "createdAt": "2020-01-15T08:00:00Z",
      "url": "https://www.catawiki.com/en/seller/12345",
    }
    seller = _parse_seller(seller_info)
    assert seller is not None
    assert seller.external_id == "12345"
    assert seller.username == "pen_collector"
    assert seller.display_name == "Pen Collector Shop"
    assert seller.country == "FR"
    assert seller.rating == 4.7
    assert seller.feedback_count == 200
    assert str(seller.member_since) == "2020-01-15"
    assert seller.profile_url == "https://www.catawiki.com/en/seller/12345"

  def test_parse_seller_missing(self):
    assert _parse_seller(None) is None
    assert _parse_seller({}) is None
    assert _parse_seller({"id": "", "userName": ""}) is None

  def test_parse_condition_mapping(self):
    assert _parse_condition([{"name": "Condition", "value": "Excellent - Well-maintained"}]) == "like_new"
    assert _parse_condition([{"name": "Condition", "value": "Very good - Some wear"}]) == "very_good"
    assert _parse_condition([{"name": "Condition", "value": "Good - Normal wear"}]) == "good"
    assert _parse_condition([{"name": "Condition", "value": "Fair"}]) == "fair"
    assert _parse_condition([{"name": "Condition", "value": "As new"}]) == "like_new"
    assert _parse_condition([{"name": "Condition", "value": "Mint condition"}]) == "like_new"

  def test_parse_condition_missing(self):
    assert _parse_condition([]) is None
    assert _parse_condition([{"name": "Brand", "value": "Montblanc"}]) is None

  def test_parse_images_gallery_preferred(self):
    images_list = [
      {"large": "https://example.com/large_1.jpg"},
    ]
    gallery = {
      "gallery": [{
        "images": [{"xl": {"url": "https://example.com/xl_1.jpg"}}],
      }],
    }
    urls = _parse_images(images_list, gallery)
    assert urls[0] == "https://example.com/xl_1.jpg"
    # Large should also be included since it has a different URL.
    assert len(urls) == 2

  def test_parse_images_deduplicates(self):
    images_list = [
      {"large": "https://example.com/xl_1.jpg"},
    ]
    gallery = {
      "gallery": [{
        "images": [{"xl": {"url": "https://example.com/xl_1.jpg"}}],
      }],
    }
    urls = _parse_images(images_list, gallery)
    assert urls.count("https://example.com/xl_1.jpg") == 1

  def test_parse_images_no_gallery(self):
    images_list = [
      {"large": "https://example.com/large_1.jpg"},
      {"medium": "https://example.com/medium_2.jpg"},
    ]
    urls = _parse_images(images_list, None)
    assert len(urls) == 2

  def test_parse_specifications(self):
    specs = [
      {"name": "Brand", "value": "Montblanc"},
      {"name": "Nib material", "value": "14K Gold"},
      {"name": "Filling system", "value": "Piston"},
    ]
    attrs = _parse_specifications(specs)
    assert attrs["brand"] == "Montblanc"
    assert attrs["nib_material"] == "14K Gold"
    assert attrs["filling_system"] == "Piston"

  def test_parse_specifications_skips_empty(self):
    specs = [
      {"name": "Brand", "value": "Montblanc"},
      {"name": None, "value": "Missing"},
      {"name": "Empty", "value": None},
    ]
    attrs = _parse_specifications(specs)
    assert len(attrs) == 1

  def test_timestamp_ms_to_datetime(self):
    # 1733050800000 ms = 2024-12-01T11:00:00 UTC
    result = _timestamp_ms_to_datetime(1733050800000)
    assert result is not None
    assert result.year == 2024
    assert result.month == 12

  def test_timestamp_ms_to_datetime_none(self):
    assert _timestamp_ms_to_datetime(None) is None

  def test_extract_page_props_standard(self):
    html = _read_fixture("lot_active.html")
    props = _extract_page_props(html)
    assert "lotDetailsData" in props
    assert "biddingBlockResponse" in props

  def test_extract_page_props_missing_raises(self):
    with pytest.raises(ValueError, match="__NEXT_DATA__"):
      _extract_page_props("<html><body>No data here</body></html>")


# ------------------------------------------------------------------
# Embedded bids parsing
# ------------------------------------------------------------------


class TestEmbeddedBidsParsing:

  def test_parse_embedded_bids(self):
    bidding = {
      "biddingHistory": {
        "bids": [
          {
            "id": 1,
            "localizedBidAmount": 100,
            "createdAt": "2025-12-01T10:00:00Z",
            "bidderName": "UserA",
            "bidType": "bid",
          },
          {
            "id": 2,
            "localizedBidAmount": 90,
            "createdAt": "2025-12-01T09:00:00Z",
            "bidderName": "UserB",
            "bidType": "autobid",
          },
        ],
      },
    }
    bids = _parse_embedded_bids(bidding)
    assert len(bids) == 2
    assert bids[0].amount == Decimal("90")
    assert bids[1].amount == Decimal("100")
    assert bids[1].is_automatic is False
    assert bids[0].is_automatic is True

  def test_embedded_bids_deduplication(self):
    bidding = {
      "biddingHistory": {
        "bids": [{"id": 1, "localizedBidAmount": 50, "createdAt": "2025-12-01T10:00:00Z"}],
      },
      "bidHistory": {
        "bids": [{"id": 1, "localizedBidAmount": 50, "createdAt": "2025-12-01T10:00:00Z"}],
      },
    }
    bids = _parse_embedded_bids(bidding)
    assert len(bids) == 1

  def test_embedded_bids_empty(self):
    assert _parse_embedded_bids({}) == []
    assert _parse_embedded_bids({"biddingHistory": {}}) == []
