"""Catawiki parser.

Catawiki is a curated online auction platform (Netherlands). Lot pages
are rendered with Next.js and embed structured JSON in a
``<script id="__NEXT_DATA__">`` tag. Search results come from an
internal JSON API at ``/buyer/api/v1/search``. Full bid history is
available via ``/buyer/api/v3/lots/{id}/bids``; the embedded page
data only includes the last 10 bids.

Key Catawiki facts:
- Buyer premium is a flat 9% on the hammer price.
- All prices default to EUR.
- Every auction starts at 1 EUR.
- Bids near the end extend the auction close time.
- Items can go unsold (reserve not met, no bids, etc.).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

from auction_tracker.parsing.base import Parser, ParserCapabilities, ParserRegistry
from auction_tracker.parsing.models import (
  ScrapedBid,
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_BASE_URL = "https://www.catawiki.com"
_SEARCH_API_URL = f"{_BASE_URL}/buyer/api/v1/search"
_BIDS_API_URL = f"{_BASE_URL}/buyer/api/v3/lots/{{lot_id}}/bids"
_BIDS_PER_PAGE = 100

_BUYER_PREMIUM_PERCENT = Decimal("9.0000")
_DEFAULT_CURRENCY = "EUR"
_CATAWIKI_STARTING_PRICE = Decimal("1")

# Mapping from Catawiki's condition strings (in the "Condition"
# specification) to our normalised condition keys.
_CONDITION_MAP: dict[str, str] = {
  "as new": "like_new",
  "mint": "like_new",
  "excellent": "like_new",
  "very good": "very_good",
  "good": "good",
  "fair": "fair",
  "poor": "poor",
  "for parts": "for_parts",
  "not working": "for_parts",
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class CatawikiParser(Parser):
  """Pure parser for Catawiki search results, lot pages, and bid history."""

  @property
  def website_name(self) -> str:
    return "catawiki"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=True,
      has_seller_info=True,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    """Build a Catawiki search API URL.

    The search endpoint returns JSON, not HTML. The transport layer
    fetches it identically; the parser treats the body as JSON.
    """
    page = kwargs.get("page", 1)
    params = {"q": query, "page": page}
    return f"{_SEARCH_API_URL}?{urlencode(params)}"

  def extract_external_id(self, url: str) -> str | None:
    return _extract_lot_id(url)

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, text: str, url: str = "") -> list[ScrapedSearchResult]:
    """Parse Catawiki search API JSON response.

    The transport fetches the search API URL and returns the body as
    text. Since this is a JSON API, we parse it as JSON.
    """
    try:
      data = json.loads(text)
    except (json.JSONDecodeError, TypeError) as error:
      raise ValueError(f"Catawiki search response is not valid JSON: {error}") from error

    results: list[ScrapedSearchResult] = []
    for lot in data.get("lots", []):
      external_id = str(lot.get("id", ""))
      if not external_id:
        continue

      lot_url = lot.get("url") or f"{_BASE_URL}/en/l/{external_id}"
      image_url = lot.get("originalImageUrl") or lot.get("thumbImageUrl")

      results.append(ScrapedSearchResult(
        external_id=external_id,
        url=lot_url,
        title=lot.get("title", ""),
        current_price=None,
        currency=_DEFAULT_CURRENCY,
        listing_type="auction",
        end_time=None,
        image_url=image_url,
        bid_count=None,
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    """Parse a Catawiki lot page.

    The HTML contains a ``<script id="__NEXT_DATA__">`` tag with all
    the structured data as JSON.
    """
    page_props = _extract_page_props(html)

    lot_data = page_props.get("lotDetailsData", {})
    bidding = page_props.get("biddingBlockResponse", {})
    auction = page_props.get("auction", {})
    gallery = page_props.get("rawGallery") or {}

    external_id = str(
      lot_data.get("lotId")
      or page_props.get("lotId")
      or (url and _extract_lot_id(url))
      or ""
    )
    if not external_id:
      raise ValueError("Could not determine Catawiki lot ID from page data")

    lot_url = url or f"{_BASE_URL}/en/l/{external_id}"

    # -- Seller --
    seller = _parse_seller(lot_data.get("sellerInfo"))

    # -- Condition --
    condition = _parse_condition(lot_data.get("specifications", []))

    # -- Prices --
    current_price = _decimal_or_none(bidding.get("localizedCurrentBidAmount"))
    starting_price = _CATAWIKI_STARTING_PRICE

    estimate = lot_data.get("expertsEstimate") or {}
    estimate_low = _decimal_or_none(
      (estimate.get("min") or {}).get(_DEFAULT_CURRENCY)
    )
    estimate_high = _decimal_or_none(
      (estimate.get("max") or {}).get(_DEFAULT_CURRENCY)
    )

    # -- Status --
    status = _derive_lot_status(lot_data, bidding)
    final_price = current_price if status == "sold" else None

    # -- Timing --
    start_time = _timestamp_ms_to_datetime(bidding.get("biddingStartTime"))
    end_time = _timestamp_ms_to_datetime(bidding.get("biddingEndTime"))

    # -- Reserve --
    reserve_price_met_raw = bidding.get("reservePriceMet")
    has_reserve = reserve_price_met_raw is not None
    quick_bids = bidding.get("quickBids") or []
    reserve_price: Decimal | None = None
    if has_reserve and len(quick_bids) >= 3:
      reserve_price = _decimal_or_none(quick_bids[2])

    # -- Images --
    image_urls = _parse_images(lot_data.get("images", []), gallery)

    # -- Embedded bids (up to 10; full history comes via API) --
    bids = _parse_embedded_bids(bidding)

    # -- Specifications --
    attributes = _parse_specifications(lot_data.get("specifications", []))

    # -- Reserve attributes --
    attributes["has_reserve_price"] = str(has_reserve)
    if has_reserve:
      attributes["reserve_price_met"] = str(bool(reserve_price_met_raw))
      if reserve_price is not None:
        attributes["reserve_price_value"] = str(reserve_price)
    close_to_reserve = bidding.get("closeToReservePrice")
    if close_to_reserve is not None:
      attributes["close_to_reserve_price"] = str(close_to_reserve)

    # -- Catawiki-specific metadata --
    if lot_data.get("summary"):
      attributes["ai_summary"] = lot_data["summary"]
    if auction.get("title"):
      attributes["auction_name"] = auction["title"]

    category_names = [
      cat.get("title", "")
      for cat in auction.get("categories", [])
      if cat.get("title")
    ]
    if category_names:
      attributes["categories"] = " > ".join(category_names)

    return ScrapedListing(
      external_id=external_id,
      url=lot_url,
      title=lot_data.get("lotTitle", ""),
      description=lot_data.get("description"),
      listing_type="auction",
      condition=condition,
      currency=_DEFAULT_CURRENCY,
      starting_price=starting_price,
      reserve_price=reserve_price,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=None,
      current_price=current_price,
      final_price=final_price,
      buyer_premium_percent=_BUYER_PREMIUM_PERCENT,
      buyer_premium_fixed=None,
      shipping_cost=None,
      shipping_from_country=(
        seller.country.upper() if seller and seller.country else None
      ),
      ships_internationally=True,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=len(bids),
      watcher_count=lot_data.get("favoriteCount"),
      view_count=None,
      lot_number=external_id,
      auction_house_name=None,
      sale_name=auction.get("title"),
      sale_date=end_time.date() if end_time else None,
      seller=seller,
      image_urls=image_urls,
      bids=bids,
      attributes=attributes,
    )

  # ----------------------------------------------------------------
  # Bid history (from dedicated API)
  # ----------------------------------------------------------------

  def build_bids_url(self, external_id: str) -> str:
    """Build the URL for the v3 bids API."""
    params = urlencode({
      "currency_code": _DEFAULT_CURRENCY,
      "per_page": str(_BIDS_PER_PAGE),
    })
    return f"{_BIDS_API_URL.format(lot_id=external_id)}?{params}"

  def parse_bid_history(self, text: str) -> list[ScrapedBid]:
    """Parse the v3 bids API JSON response.

    This returns the complete bid history, unlike the embedded page
    data which caps at 10 bids.
    """
    try:
      data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
      return []

    return _parse_api_bids(data)


# ------------------------------------------------------------------
# Pure parsing helpers
# ------------------------------------------------------------------


def _extract_page_props(html: str) -> dict:
  """Extract ``pageProps`` from the ``__NEXT_DATA__`` script tag."""
  match = re.search(
    r'<script\s+id="__NEXT_DATA__"\s+type="application/json">\s*(\{.*?\})\s*</script>',
    html,
    re.DOTALL,
  )
  if match is None:
    marker = "__NEXT_DATA__"
    if marker not in html:
      raise ValueError("Could not find __NEXT_DATA__ in the Catawiki page")
    start = html.index(marker)
    json_start = html.index("{", start)
    json_end = html.index("</script>", json_start)
    raw_json = html[json_start:json_end]
  else:
    raw_json = match.group(1)

  data = json.loads(raw_json)
  return data.get("props", {}).get("pageProps", {})


def _extract_lot_id(url: str) -> str | None:
  """Extract the numeric lot ID from a Catawiki URL.

  URLs look like ``/en/l/101149019-montblanc-149-fountain-pen``.
  """
  match = re.search(r"/l/(\d+)", url)
  if match:
    return match.group(1)
  # Fallback: try the last path segment before the first dash.
  last_segment = url.rsplit("/", 1)[-1]
  id_part = last_segment.split("-", 1)[0]
  if id_part.isdigit():
    return id_part
  return None


def _derive_lot_status(lot_data: dict, bidding: dict) -> str:
  """Determine the listing status string from lot-page data."""
  if bidding.get("sold"):
    return "sold"
  if bidding.get("closed") or lot_data.get("isClosed"):
    return "unsold"
  if not lot_data.get("open", True):
    return "unsold"
  live = bidding.get("live", {}).get("lot", {})
  if live.get("closeStatus", "") == "Closed":
    return "unsold"
  return "active"


def _parse_seller(seller_info: dict | None) -> ScrapedSeller | None:
  """Build a ScrapedSeller from the ``sellerInfo`` block."""
  if not seller_info:
    return None

  external_id = str(seller_info.get("id", ""))
  username = seller_info.get("userName", "")
  if not external_id or not username:
    return None

  country_block = (seller_info.get("address") or {}).get("country") or {}
  country_code = (country_block.get("shortCode") or "").upper() or None

  score_block = seller_info.get("score") or {}
  rating = score_block.get("score")
  feedback_count = score_block.get("lifetimeCount")

  member_since = None
  created_at_raw = seller_info.get("createdAt")
  if created_at_raw:
    try:
      member_since_str = created_at_raw[:10]
      from datetime import date as date_type
      member_since = date_type.fromisoformat(member_since_str)
    except (TypeError, IndexError, ValueError):
      pass

  profile_url = seller_info.get("url")

  return ScrapedSeller(
    external_id=external_id,
    username=username,
    display_name=seller_info.get("sellerName"),
    country=country_code,
    rating=float(rating) if rating is not None else None,
    feedback_count=feedback_count,
    member_since=member_since,
    profile_url=profile_url,
  )


def _parse_condition(specifications: list[dict]) -> str | None:
  """Extract the normalised condition string from specifications."""
  for spec in specifications:
    if (spec.get("name") or "").lower() == "condition":
      value = (spec.get("value") or "").lower().strip()
      for prefix, condition in _CONDITION_MAP.items():
        if value.startswith(prefix):
          return condition
  return None


def _parse_images(images_list: list[dict], raw_gallery: dict | None) -> list[str]:
  """Collect image URLs, preferring high-res from ``rawGallery``."""
  urls: list[str] = []
  seen: set[str] = set()

  # High-res gallery images first.
  if raw_gallery:
    for group in raw_gallery.get("gallery", []):
      for image_entry in group.get("images", []):
        url = (image_entry.get("xl") or image_entry.get("l") or {}).get("url")
        if url and url not in seen:
          urls.append(url)
          seen.add(url)

  # Fallback to lot images list.
  for image in images_list:
    url = image.get("large") or image.get("medium")
    if url and url not in seen:
      urls.append(url)
      seen.add(url)

  return urls


def _parse_specifications(specifications: list[dict]) -> dict[str, str]:
  """Convert the Catawiki specifications array into a flat dict."""
  attributes: dict[str, str] = {}
  for spec in specifications:
    name = spec.get("name")
    value = spec.get("value")
    if name and value:
      key = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
      attributes[key] = value
  return attributes


# ------------------------------------------------------------------
# Bid parsing
# ------------------------------------------------------------------


def _parse_api_bids(data: dict | None) -> list[ScrapedBid]:
  """Parse bids from the v3 bids API response (full history)."""
  if not data:
    return []
  bids: list[ScrapedBid] = []
  for raw_bid in data.get("bids") or []:
    amount = _decimal_or_none(raw_bid.get("amount"))
    if amount is None:
      continue

    bid_time_str = raw_bid.get("created_at")
    if not bid_time_str:
      continue
    bid_time = _parse_iso_datetime(bid_time_str)
    if bid_time is None:
      continue

    bidder = raw_bid.get("bidder") or {}
    bidder_name = bidder.get("name")
    if bidder_name and bidder_name.startswith("Bidder "):
      bidder_name = bidder_name[len("Bidder "):]

    country_block = bidder.get("country") or {}
    if isinstance(country_block, dict):
      bidder_country = country_block.get("code")
    elif isinstance(country_block, str):
      bidder_country = country_block
    else:
      bidder_country = None
    if isinstance(bidder_country, str) and bidder_country:
      bidder_country = bidder_country.upper()[:2]
    else:
      bidder_country = None

    bids.append(ScrapedBid(
      amount=amount,
      currency=raw_bid.get("currency_code", _DEFAULT_CURRENCY),
      bid_time=bid_time,
      bidder_username=bidder_name,
      bidder_country=bidder_country,
      is_automatic=raw_bid.get("from_order", False),
    ))

  # Sort by amount ascending. Automatic (proxy) bids share the same
  # timestamp as the manual bid that triggered them but have a higher
  # amount, so sorting by amount is more reliable than by time.
  bids.sort(key=lambda bid: bid.amount)
  return bids


def _parse_embedded_bids(bidding: dict) -> list[ScrapedBid]:
  """Extract bids from the biddingBlockResponse (up to 10).

  Used as a fallback when the v3 bids API is not available.
  """
  bids: list[ScrapedBid] = []
  seen_ids: set[int] = set()

  history = bidding.get("biddingHistory") or {}
  raw_bids = history.get("bids", [])

  alt_history = bidding.get("bidHistory") or {}
  alt_bids = alt_history.get("bids", [])

  for raw_bid in raw_bids + alt_bids:
    bid_id = raw_bid.get("id")
    if bid_id in seen_ids:
      continue
    if bid_id is not None:
      seen_ids.add(bid_id)

    amount = _decimal_or_none(raw_bid.get("localizedBidAmount"))
    if amount is None:
      continue

    bid_time_str = raw_bid.get("createdAt")
    if not bid_time_str:
      continue
    bid_time = _parse_iso_datetime(bid_time_str)
    if bid_time is None:
      continue

    bids.append(ScrapedBid(
      amount=amount,
      currency=_DEFAULT_CURRENCY,
      bid_time=bid_time,
      bidder_username=raw_bid.get("bidderName"),
      is_automatic=raw_bid.get("bidType") == "autobid",
    ))

  bids.sort(key=lambda bid: bid.amount)
  return bids


# ------------------------------------------------------------------
# Value helpers
# ------------------------------------------------------------------


def _decimal_or_none(value) -> Decimal | None:
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None


def _timestamp_ms_to_datetime(timestamp_ms) -> datetime | None:
  """Convert a millisecond Unix timestamp to a UTC datetime."""
  if timestamp_ms is None:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=UTC)
  except (ValueError, TypeError, OSError):
    return None


def _parse_iso_datetime(value: str) -> datetime | None:
  if not value:
    return None
  try:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
  except (ValueError, TypeError):
    return None
