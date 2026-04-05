"""Tests for the Drouot parser.

Uses golden file fixtures to test search result parsing, lot page
parsing (active, sold, unsold, JSON-LD fallback), and the various
helper functions including JavaScript-to-JSON conversion.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserCapabilities, ParserRegistry
from auction_tracker.parsing.sites.drouot import (
  DrouotParser,
  _build_image_url,
  _build_title,
  _country_id_to_code,
  _derive_status,
  _drouot_image_url_to_high_res,
  _extract_balanced_bracket,
  _extract_lot_detail,
  _extract_lot_id,
  _extract_search_lots,
  _find_result_in_dict,
  _get_result_value,
  _html_says_unsold,
  _parse_images,
  _parse_js_object,
  _timestamp_to_datetime,
)

FIXTURES = Path(__file__).parent / "fixtures" / "drouot"


def _read_fixture(name: str) -> str:
  return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture()
def parser() -> DrouotParser:
  return DrouotParser()


# ------------------------------------------------------------------
# Registration and capabilities
# ------------------------------------------------------------------


class TestRegistration:

  def test_registered_in_parser_registry(self):
    assert ParserRegistry.has("drouot")

  def test_website_name(self, parser: DrouotParser):
    assert parser.website_name == "drouot"

  def test_capabilities(self, parser: DrouotParser):
    capabilities = parser.capabilities
    assert isinstance(capabilities, ParserCapabilities)
    assert capabilities.can_search is True
    assert capabilities.can_parse_listing is True
    assert capabilities.has_bid_history is False
    assert capabilities.has_seller_info is True
    assert capabilities.has_estimates is True
    assert capabilities.has_reserve_price is True
    assert capabilities.has_lot_numbers is True
    assert capabilities.has_auction_house_info is True
    assert capabilities.has_buy_now is False


# ------------------------------------------------------------------
# URL building and extraction
# ------------------------------------------------------------------


class TestUrlBuilding:

  def test_build_search_url_default(self, parser: DrouotParser):
    url = parser.build_search_url("fountain pen")
    assert "drouot.com" in url
    assert "/en/s" in url
    assert "query=fountain+pen" in url

  def test_build_search_url_with_page(self, parser: DrouotParser):
    url = parser.build_search_url("montblanc", page=3)
    assert "page=3" in url

  def test_extract_external_id(self, parser: DrouotParser):
    assert parser.extract_external_id(
      "https://drouot.com/en/l/12345678-montblanc-149"
    ) == "12345678"
    assert parser.extract_external_id(
      "https://drouot.com/en/l/99999"
    ) == "99999"
    assert parser.extract_external_id("not-a-drouot-url") is None


# ------------------------------------------------------------------
# Search results parsing
# ------------------------------------------------------------------


class TestSearchParsing:

  def test_parse_search_results(self, parser: DrouotParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert len(results) == 3

  def test_first_result_fields(self, parser: DrouotParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    first = results[0]

    assert first.external_id == "12345678"
    assert "drouot.com" in first.url
    assert "montblanc-149" in first.url
    assert "42" in first.title
    assert "MONTBLANC" in first.title
    assert first.current_price == Decimal("450")
    assert first.currency == "EUR"
    assert first.listing_type == "auction"
    assert first.end_time is not None
    assert first.image_url is not None
    assert "cdn.drouot.com" in first.image_url
    assert "ftall" in first.image_url

  def test_upcoming_lot_uses_next_bid(self, parser: DrouotParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    upcoming = results[1]
    assert upcoming.current_price == Decimal("100")

  def test_empty_search(self, parser: DrouotParser):
    html = "<html><body><script>var data = {};</script></body></html>"
    results = parser.parse_search_results(html)
    assert results == []


# ------------------------------------------------------------------
# Lot page parsing (active)
# ------------------------------------------------------------------


class TestActiveLotParsing:

  def test_parse_active_lot(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html, url="https://drouot.com/en/l/12345678")
    assert listing.external_id == "12345678"

  def test_active_lot_title(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert "42" in listing.title
    assert "MONTBLANC" in listing.title

  def test_active_lot_prices(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.current_price == Decimal("450")
    assert listing.currency == "EUR"
    assert listing.final_price is None

  def test_active_lot_estimates(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.estimate_low == Decimal("300")
    assert listing.estimate_high == Decimal("500")

  def test_active_lot_buyer_premium(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_percent == Decimal("25.5")

  def test_active_lot_timing(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.start_time is not None
    assert listing.end_time is not None

  def test_active_lot_seller(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "Artcurial"
    assert listing.seller.display_name == "Artcurial"
    assert listing.seller.country == "FR"
    assert listing.seller.profile_url is not None
    assert "artcurial" in listing.seller.profile_url

  def test_active_lot_images(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 3
    assert all("cdn.drouot.com" in url for url in listing.image_urls)
    assert all("ftall" in url for url in listing.image_urls)

  def test_active_lot_status(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.status == "active"

  def test_active_lot_attributes(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["sale_name"] == "Fine Writing Instruments"
    assert listing.attributes["sale_city"] == "Paris"
    assert listing.attributes["sale_country"] == "FR"
    assert listing.attributes["sale_venue"] == "Hotel Drouot"
    assert listing.attributes["hotel_drouot"] == "True"
    assert listing.attributes["sale_type"] == "ONLINE"
    assert listing.attributes["transport_size"] == "SMALL"

  def test_active_lot_original_description(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert "original_description" in listing.attributes
    assert "Stylo plume" in listing.attributes["original_description"]

  def test_active_lot_auction_house(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.auction_house_name == "Artcurial"
    assert listing.sale_name == "Fine Writing Instruments"

  def test_active_lot_number(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.lot_number == "42"

  def test_active_lot_shipping_country(self, parser: DrouotParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.shipping_from_country == "FR"


# ------------------------------------------------------------------
# Lot page parsing (sold)
# ------------------------------------------------------------------


class TestSoldLotParsing:

  def test_sold_lot_status(self, parser: DrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_sold_lot_final_price(self, parser: DrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price == Decimal("380")

  def test_sold_lot_seller(self, parser: DrouotParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.username == "Drouot Estimations"


# ------------------------------------------------------------------
# Lot page parsing (unsold)
# ------------------------------------------------------------------


class TestUnsoldLotParsing:

  def test_unsold_lot_status(self, parser: DrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "unsold"

  def test_unsold_lot_no_final_price(self, parser: DrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price is None

  def test_unsold_lot_reserve_not_reached(self, parser: DrouotParser):
    html = _read_fixture("lot_unsold.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["reserve_not_reached"] == "True"


# ------------------------------------------------------------------
# JSON-LD fallback
# ------------------------------------------------------------------


class TestJsonLdFallback:

  def test_json_ld_only_page(self, parser: DrouotParser):
    html = _read_fixture("lot_json_ld_only.html")
    listing = parser.parse_listing(
      html, url="https://drouot.com/en/l/12345681",
    )
    assert listing.external_id == "12345681"
    assert "PARKER" in listing.title
    assert listing.currency == "EUR"
    assert listing.attributes.get("source") == "json_ld_fallback"

  def test_json_ld_image_upgraded_to_high_res(self, parser: DrouotParser):
    html = _read_fixture("lot_json_ld_only.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 1
    assert "ftall" in listing.image_urls[0]

  def test_no_data_raises(self, parser: DrouotParser):
    with pytest.raises(ValueError, match="Could not extract"):
      parser.parse_listing("<html><body>Nothing here</body></html>")


# ------------------------------------------------------------------
# JavaScript-to-JSON conversion
# ------------------------------------------------------------------


class TestJsToJson:

  def test_void_0_to_null(self):
    assert _parse_js_object("{a:void 0}") == {"a": None}

  def test_new_date_to_timestamp(self):
    result = _parse_js_object("{created:new Date(1733050800000)}")
    assert result == {"created": 1733050800000}

  def test_new_map_to_null(self):
    result = _parse_js_object("{m:new Map([[1,2]])}")
    assert result == {"m": None}

  def test_unquoted_keys(self):
    result = _parse_js_object('{name:"test",count:42}')
    assert result == {"name": "test", "count": 42}

  def test_trailing_commas(self):
    result = _parse_js_object('{a:1,b:2,}')
    assert result == {"a": 1, "b": 2}

  def test_string_contents_preserved(self):
    result = _parse_js_object('{desc:"Hello, world: test"}')
    assert result == {"desc": "Hello, world: test"}

  def test_nested_objects(self):
    result = _parse_js_object('{outer:{inner:{val:42}}}')
    assert result["outer"]["inner"]["val"] == 42

  def test_real_world_lot(self):
    raw = '{id:12345,slug:"montblanc",currentBid:450,nextBid:500,date:new Date(1733050800000),saleStatus:"IN_PROGRESS",photo:{path:"/test.jpg"},extra:void 0}'
    result = _parse_js_object(raw)
    assert result["id"] == 12345
    assert result["currentBid"] == 450
    assert result["date"] == 1733050800000
    assert result["extra"] is None
    assert result["photo"]["path"] == "/test.jpg"


# ------------------------------------------------------------------
# Bracket matching
# ------------------------------------------------------------------


class TestBracketMatching:

  def test_simple_object(self):
    assert _extract_balanced_bracket("{a:1}", 0, "{", "}") == "{a:1}"

  def test_nested_object(self):
    text = '{outer:{inner:1}}'
    assert _extract_balanced_bracket(text, 0, "{", "}") == text

  def test_array(self):
    text = 'prefix[1,2,3]suffix'
    assert _extract_balanced_bracket(text, 6, "[", "]") == "[1,2,3]"

  def test_quoted_braces_ignored(self):
    text = '{desc:"hello {world}"}'
    assert _extract_balanced_bracket(text, 0, "{", "}") == text

  def test_unbalanced_returns_none(self):
    assert _extract_balanced_bracket("{a:1", 0, "{", "}") is None

  def test_wrong_start_returns_none(self):
    assert _extract_balanced_bracket("abc", 0, "{", "}") is None


# ------------------------------------------------------------------
# Search lot extraction
# ------------------------------------------------------------------


class TestSearchLotExtraction:

  def test_extract_from_fixture(self):
    html = _read_fixture("search_fountain_pen.html")
    lots = _extract_search_lots(html)
    assert len(lots) == 3
    assert lots[0]["id"] == 12345678
    assert lots[0]["slug"] == "montblanc-149-fountain-pen"

  def test_empty_page(self):
    assert _extract_search_lots("<html></html>") == []


# ------------------------------------------------------------------
# Lot detail extraction
# ------------------------------------------------------------------


class TestLotDetailExtraction:

  def test_extract_from_fixture(self):
    html = _read_fixture("lot_active.html")
    lot = _extract_lot_detail(html)
    assert lot is not None
    assert lot["id"] == 12345678
    assert lot["currentBid"] == 450

  def test_no_lot_data(self):
    assert _extract_lot_detail("<html></html>") is None

  def test_none_input(self):
    assert _extract_lot_detail(None) is None


# ------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------


class TestHelpers:

  def test_extract_lot_id_from_full_url(self):
    assert _extract_lot_id("https://drouot.com/en/l/12345678-montblanc") == "12345678"

  def test_extract_lot_id_short(self):
    assert _extract_lot_id("https://drouot.com/en/l/99999") == "99999"

  def test_extract_lot_id_non_url(self):
    assert _extract_lot_id("not-a-url") is None

  def test_derive_status_sold(self):
    assert _derive_status({"result": 500}, {}) == "sold"

  def test_derive_status_ended(self):
    assert _derive_status({"saleStatus": "ENDED"}, {}) == "unsold"

  def test_derive_status_closed(self):
    assert _derive_status({"saleStatus": "CLOSED"}, {}) == "unsold"

  def test_derive_status_in_progress(self):
    assert _derive_status({"saleStatus": "IN_PROGRESS"}, {}) == "active"

  def test_derive_status_created(self):
    assert _derive_status({"saleStatus": "CREATED"}, {}) == "upcoming"

  def test_derive_status_cancelled(self):
    assert _derive_status({"saleStatus": "CANCELLED"}, {}) == "cancelled"

  def test_derive_status_reserve_not_reached(self):
    assert _derive_status({"saleStatus": "ENDED", "reserveNotReached": True}, {}) == "unsold"

  def test_derive_status_from_sale_info(self):
    assert _derive_status({}, {"saleStatus": "IN_PROGRESS"}) == "active"

  def test_get_result_value(self):
    assert _get_result_value({"result": 500}, {}) == 500
    assert _get_result_value({"hammerPrice": 300}, {}) == 300
    assert _get_result_value({}, {"soldPrice": 200}) == 200
    assert _get_result_value({}, {}) == 0

  def test_find_result_in_dict_nested(self):
    data = {"sale": {"detail": {"result": 750}}}
    assert _find_result_in_dict(data) == 750

  def test_find_result_in_dict_list(self):
    data = {"lots": [{"result": 0}, {"result": 400}]}
    assert _find_result_in_dict(data) == 400

  def test_build_title_with_lot_number(self):
    title = _build_title("MONTBLANC\\nFountain pen 149", 42)
    assert title.startswith("42 - ")
    assert "MONTBLANC" in title

  def test_build_title_without_lot_number(self):
    title = _build_title("PELIKAN M800 Souveran")
    assert "PELIKAN" in title

  def test_build_title_empty(self):
    assert _build_title("") == "(no description)"
    assert _build_title("", 10) == "(no description)"

  def test_build_title_long_truncated(self):
    long_desc = "A" * 200
    title = _build_title(long_desc)
    assert len(title) <= 125

  def test_parse_images(self):
    lot = {"photos": [
      {"path": "/img1.jpg"},
      {"path": "/img2.jpg"},
      {"path": "/img1.jpg"},
    ]}
    urls = _parse_images(lot)
    assert len(urls) == 2
    assert all("cdn.drouot.com" in url for url in urls)

  def test_parse_images_single_photo(self):
    lot = {"photo": {"path": "/img1.jpg"}}
    urls = _parse_images(lot)
    assert len(urls) == 1

  def test_parse_images_empty(self):
    assert _parse_images({}) == []

  def test_build_image_url(self):
    url = _build_image_url("/lots/2025/01/pen.jpg")
    assert "cdn.drouot.com" in url
    assert "size=ftall" in url
    assert "path=" in url

  def test_drouot_image_url_to_high_res(self):
    url = "https://cdn.drouot.com/d/image/lot?size=small&path=test"
    high_res = _drouot_image_url_to_high_res(url)
    assert "size=ftall" in high_res
    assert "size=small" not in high_res

  def test_drouot_image_url_non_cdn(self):
    url = "https://example.com/image.jpg"
    assert _drouot_image_url_to_high_res(url) == url

  def test_country_id_to_code(self):
    assert _country_id_to_code(75) == "FR"
    assert _country_id_to_code(44) == "GB"
    assert _country_id_to_code(None) is None
    assert _country_id_to_code(99999) is None

  def test_timestamp_to_datetime(self):
    result = _timestamp_to_datetime(1733050800)
    assert result is not None
    assert result.year == 2024

  def test_timestamp_to_datetime_none(self):
    assert _timestamp_to_datetime(None) is None
    assert _timestamp_to_datetime(0) is None

  def test_html_says_unsold(self):
    assert _html_says_unsold("Some text Lot not sold more text") is True
    assert _html_says_unsold("Lot non vendu") is True
    assert _html_says_unsold("Normal page content") is False
    assert _html_says_unsold("") is False
