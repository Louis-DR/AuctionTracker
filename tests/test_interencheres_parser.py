"""Tests for the Interencheres parser."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from auction_tracker.parsing.base import ParserRegistry
from auction_tracker.parsing.sites.interencheres import (
  InterencheresParser,
  _build_lot_url,
  _decimal_or_none,
  _derive_status,
  _ensure_absolute_url,
  _extract_auction_house_name,
  _extract_balanced_bracket,
  _extract_buyer_premium,
  _extract_description,
  _extract_first_image_url,
  _extract_lot_detail,
  _extract_search_items,
  _extract_title,
  _nuxt_js_to_json,
  _parse_function_args,
  _parse_iso_datetime,
  _parse_nuxt_object,
  _parse_nuxt_payload,
  _substitute_vars,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "interencheres"


def _read_fixture(name: str) -> str:
  return (_FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def parser() -> InterencheresParser:
  return InterencheresParser()


# ==================================================================
# Registration and capabilities
# ==================================================================


class TestRegistration:
  def test_registered(self):
    assert ParserRegistry.has("interencheres")

  def test_website_name(self, parser: InterencheresParser):
    assert parser.website_name == "interencheres"

  def test_capabilities(self, parser: InterencheresParser):
    capabilities = parser.capabilities
    assert capabilities.can_search is True
    assert capabilities.can_parse_listing is True
    assert capabilities.has_bid_history is False
    assert capabilities.has_seller_info is True
    assert capabilities.has_estimates is True
    assert capabilities.has_reserve_price is True
    assert capabilities.has_lot_numbers is True
    assert capabilities.has_auction_house_info is True


# ==================================================================
# URL helpers
# ==================================================================


class TestUrlHelpers:
  def test_build_search_url(self, parser: InterencheresParser):
    url = parser.build_search_url("stylo plume")
    assert "recherche/lots" in url
    assert "search=stylo plume" in url

  def test_build_search_url_page_2(self, parser: InterencheresParser):
    url = parser.build_search_url("test", page=2)
    assert "offset=30" in url

  def test_extract_external_id(self, parser: InterencheresParser):
    url = "https://www.interencheres.com/art-decoration/s-5001/lot-98765.html"
    assert parser.extract_external_id(url) == "98765"

  def test_extract_external_id_no_match(self, parser: InterencheresParser):
    assert parser.extract_external_id("https://example.com") is None


# ==================================================================
# Nuxt payload parsing
# ==================================================================


class TestNuxtPayload:
  def test_parse_nuxt_payload(self):
    html = _read_fixture("search_fountain_pen.html")
    result = _parse_nuxt_payload(html)
    assert result is not None
    body, sub_map, param_names = result
    assert len(param_names) > 0
    assert len(sub_map) > 0
    assert len(body) > 0

  def test_parse_nuxt_payload_no_nuxt(self):
    assert _parse_nuxt_payload("<html><body>no data</body></html>") is None


class TestParseArgs:
  def test_basic_args(self):
    args = _parse_function_args('"hello",42,true')
    assert args == ['"hello"', "42", "true"]

  def test_quoted_commas(self):
    args = _parse_function_args('"a,b",3')
    assert args == ['"a,b"', "3"]

  def test_void_split_as_separate_tokens(self):
    """In practice void 0 appears in the body, not in arguments.

    When it does appear in args, the space-based parser splits it into
    two tokens. This is fine because the JS-to-JSON converter handles
    ``void 0`` at the body level.
    """
    args = _parse_function_args('"x",void 0,5')
    assert args == ['"x"', "void", "0", "5"]


class TestSubstituteVars:
  def test_basic_substitution(self):
    result = _substitute_vars(
      "{title:a,price:b}", {"a": '"Hello"', "b": "42"}, ["a", "b"],
    )
    assert '"Hello"' in result
    assert "42" in result

  def test_does_not_substitute_in_strings(self):
    result = _substitute_vars(
      '{title:"has a inside",val:a}',
      {"a": '"replaced"'},
      ["a"],
    )
    assert "has a inside" in result
    assert '"replaced"' in result


class TestJsToJson:
  def test_void_0(self):
    assert _parse_nuxt_object("{a:void 0}") == {"a": None}

  def test_new_date(self):
    result = _parse_nuxt_object("{t:new Date(1700000000000)}")
    assert result == {"t": 1700000000000}

  def test_new_map(self):
    result = _parse_nuxt_object("{m:new Map([])}")
    assert result == {"m": None}

  def test_unquoted_keys(self):
    result = _parse_nuxt_object("{name:'test',value:42}")
    assert result == {"name": "test", "value": 42}

  def test_trailing_commas(self):
    result = _parse_nuxt_object("{a:1,b:2,}")
    assert result == {"a": 1, "b": 2}

  def test_single_quotes_to_double(self):
    result = _parse_nuxt_object("{'key':'value'}")
    assert result == {"key": "value"}


class TestBracketMatching:
  def test_simple(self):
    assert _extract_balanced_bracket("{a:1}", 0, "{", "}") == "{a:1}"

  def test_nested(self):
    result = _extract_balanced_bracket("{a:{b:1}}", 0, "{", "}")
    assert result == "{a:{b:1}}"

  def test_with_strings(self):
    result = _extract_balanced_bracket('{a:"}"}', 0, "{", "}")
    assert result == '{a:"}"}'

  def test_no_match(self):
    assert _extract_balanced_bracket("hello", 0, "{", "}") is None


# ==================================================================
# Search results
# ==================================================================


class TestSearchResults:
  def test_parse_count(self, parser: InterencheresParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert len(results) == 2

  def test_first_result_active(self, parser: InterencheresParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    first = results[0]
    assert first.external_id == "98765"
    assert "Montblanc" in first.title
    assert first.current_price == Decimal("100")
    assert first.currency == "EUR"
    assert first.listing_type == "auction"

  def test_second_result_sold(self, parser: InterencheresParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    second = results[1]
    assert second.external_id == "98766"
    assert second.current_price == Decimal("250")

  def test_search_image_urls(self, parser: InterencheresParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert results[0].image_url is not None
    assert results[0].image_url.startswith("https://")

  def test_lot_url_constructed(self, parser: InterencheresParser):
    html = _read_fixture("search_fountain_pen.html")
    results = parser.parse_search_results(html)
    assert "lot-98765.html" in results[0].url

  def test_no_nuxt_raises(self, parser: InterencheresParser):
    with pytest.raises(ValueError, match="__NUXT__"):
      parser.parse_search_results("<html><body>empty</body></html>")


# ==================================================================
# Active lot
# ==================================================================


class TestActiveLot:
  def test_basic_fields(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.external_id == "98765"
    assert "Montblanc" in listing.title
    assert listing.listing_type == "auction"
    assert listing.status == "active"

  def test_pricing(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.starting_price == Decimal("100")
    assert listing.reserve_price == Decimal("80")
    assert listing.estimate_low == Decimal("150")
    assert listing.estimate_high == Decimal("250")
    assert listing.current_price == Decimal("100")
    assert listing.final_price is None

  def test_description(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.description is not None
    assert "Magnifique" in listing.description

  def test_seller(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.seller is not None
    assert listing.seller.external_id == "777"
    assert listing.seller.display_name == "Maison des Enchères de Paris"
    assert listing.seller.country == "FR"
    assert "commissaire-priseur" in listing.seller.profile_url

  def test_buyer_premium(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_percent == Decimal("14.28")

  def test_images(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert len(listing.image_urls) == 2
    assert all(url.startswith("https://") for url in listing.image_urls)

  def test_lot_number(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.lot_number == "42"

  def test_sale_info(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.auction_house_name == "Maison des Enchères de Paris"
    assert listing.sale_name is not None
    assert "prestige" in listing.sale_name.lower()
    assert listing.start_time is not None

  def test_attributes(self, parser: InterencheresParser):
    html = _read_fixture("lot_active.html")
    listing = parser.parse_listing(html)
    assert listing.attributes["sale_type"] == "voluntary"
    assert listing.attributes["sale_city"] == "Paris"
    assert listing.attributes["sale_country"] == "FR"
    assert listing.attributes["category"] == "Objets d'art"


# ==================================================================
# Sold lot
# ==================================================================


class TestSoldLot:
  def test_status_sold(self, parser: InterencheresParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.status == "sold"

  def test_final_price(self, parser: InterencheresParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.final_price == Decimal("480")
    assert listing.current_price == Decimal("480")

  def test_buyer_premium_scalar(self, parser: InterencheresParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.buyer_premium_percent == Decimal("16.5")

  def test_confirmed_attribute(self, parser: InterencheresParser):
    html = _read_fixture("lot_sold.html")
    listing = parser.parse_listing(html)
    assert listing.attributes.get("result_confirmed") == "true"


# ==================================================================
# Helper functions
# ==================================================================


class TestExtractTitle:
  def test_french(self):
    item = {"title_translations": {"fr-FR": "Titre français"}}
    assert _extract_title(item) == "Titre français"

  def test_english_fallback(self):
    item = {"title_translations": {"en-US": "English title"}}
    assert _extract_title(item) == "English title"

  def test_description_fallback(self):
    item = {"description_translations": {"fr-FR": "Description"}}
    assert _extract_title(item) == "Description"

  def test_no_title(self):
    assert _extract_title({}) == "(sans titre)"

  def test_truncation(self):
    long_title = "A" * 250
    result = _extract_title({"title_translations": {"fr-FR": long_title}})
    assert len(result) == 200
    assert result.endswith("...")


class TestDeriveStatus:
  def test_auctioned(self):
    item = {"pricing": {"auctioned": 500}}
    assert _derive_status(item) == "sold"

  def test_confirmed_active(self):
    item = {"status": "confirmed", "sale": {"live": {"has_started": True, "has_ended": False}}}
    assert _derive_status(item) == "active"

  def test_cancelled(self):
    item = {"status": "cancelled"}
    assert _derive_status(item) == "cancelled"

  def test_unknown_status(self):
    assert _derive_status({}) == "active"


class TestBuildLotUrl:
  def test_with_sale_id(self):
    item = {"id": 123, "sale": {"id": 456}}
    url = _build_lot_url(item)
    assert "s-456" in url
    assert "lot-123" in url

  def test_without_sale_id(self):
    item = {"id": 123}
    url = _build_lot_url(item)
    assert "lot-123" in url


class TestEnsureAbsoluteUrl:
  def test_protocol_relative(self):
    assert _ensure_absolute_url("//cdn.example.com/img.jpg") == "https://cdn.example.com/img.jpg"

  def test_no_protocol(self):
    assert _ensure_absolute_url("cdn.example.com/img.jpg") == "https://cdn.example.com/img.jpg"

  def test_already_absolute(self):
    assert _ensure_absolute_url("https://cdn.example.com/img.jpg") == "https://cdn.example.com/img.jpg"


class TestExtractAuctionHouseName:
  def test_voluntary(self):
    org = {"names": {"voluntary": "Maison A", "judicial": "00"}}
    assert _extract_auction_house_name(org, "voluntary") == "Maison A"

  def test_fallback_to_address(self):
    org = {"names": {"voluntary": "AB"}, "address": {"name": "Real Name"}}
    assert _extract_auction_house_name(org, "voluntary") == "Real Name"

  def test_no_names(self):
    assert _extract_auction_house_name({}, "voluntary") is None


class TestExtractBuyerPremium:
  def test_dict_rate(self):
    org = {"options": {"commission_rate": {"voluntary": 14.5}}}
    assert _extract_buyer_premium(org, "voluntary") == Decimal("14.5")

  def test_scalar_rate(self):
    org = {"options": {"commission_rate": 16.0}}
    assert _extract_buyer_premium(org, "voluntary") == Decimal("16.0")

  def test_no_rate(self):
    assert _extract_buyer_premium({}, "voluntary") is None


class TestDecimalOrNone:
  def test_number(self):
    assert _decimal_or_none(42) == Decimal("42")

  def test_none(self):
    assert _decimal_or_none(None) is None

  def test_false(self):
    assert _decimal_or_none(False) is None


class TestParseIsoDatetime:
  def test_with_offset(self):
    result = _parse_iso_datetime("2026-06-15T14:00:00+02:00")
    assert result is not None
    assert result.year == 2026
    assert result.month == 6

  def test_with_z(self):
    result = _parse_iso_datetime("2026-06-15T12:00:00Z")
    assert result is not None

  def test_none(self):
    assert _parse_iso_datetime(None) is None

  def test_invalid(self):
    assert _parse_iso_datetime("not a date") is None
