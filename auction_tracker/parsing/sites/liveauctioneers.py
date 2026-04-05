"""LiveAuctioneers parser.

LiveAuctioneers is a US-based online auction marketplace. The site is a
React/Redux SPA that embeds a full application state snapshot in
``window.__data`` inside an inline ``<script>`` tag.

Key technical facts:

* All item data lives under ``window.__data.itemSummary.byId[itemId]``.
* Buyer's premium (tiered) is in ``catalog.byId[catalogId].buyersPremium``.
  Typical structure: ``{"low": 28, "lowCutoff": 20000, "middle": 25, ...}``.
* Images follow a CDN pattern:
  ``https://p1.liveauctioneers.com/{sellerId}/{catalogId}/{itemId}_{n}_x.jpg``
* Search pages embed ``search.itemIds`` + ``itemSummary.byId``.
* Prices vary by auction house currency (USD, EUR, GBP, etc).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from auction_tracker.parsing.base import (
  Parser,
  ParserCapabilities,
  ParserRegistry,
)
from auction_tracker.parsing.models import (
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.liveauctioneers.com"
_CDN_BASE = "https://p1.liveauctioneers.com"
_IMAGE_QUALITY = 95


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class LiveAuctioneersParser(Parser):
  """Parser for liveauctioneers.com."""

  @property
  def website_name(self) -> str:
    return "liveauctioneers"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=True,
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=True,
      has_estimates=True,
      has_reserve_price=False,
      has_lot_numbers=True,
      has_auction_house_info=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    page = int(kwargs.get("page", 1))
    params = f"keyword={query}"
    if page > 1:
      params += f"&page={page}"
    return f"{_BASE_URL}/search/?{params}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/item/(\d+)", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str) -> list[ScrapedSearchResult]:
    state = _extract_window_data(html)
    if state is None:
      raise ValueError(
        "Could not extract window.__data from LiveAuctioneers page"
      )

    search_data = state.get("search", {})
    item_ids = search_data.get("itemIds", [])
    items_by_id = state.get("itemSummary", {}).get("byId", {})

    results: list[ScrapedSearchResult] = []
    for item_id in item_ids:
      item = items_by_id.get(str(item_id))
      if item is None:
        continue

      slug = item.get("slug", "")
      item_url = (
        f"{_BASE_URL}/item/{item_id}_{slug}"
        if slug
        else f"{_BASE_URL}/item/{item_id}"
      )

      current_price = _get_current_price(item)
      end_time = _timestamp_to_datetime(item.get("lotEndTimeEstimatedTs"))
      image_url = _build_single_image_url(item, photo_index=1)

      listing_type = "auction"
      if item.get("buyNowPrice", 0) > 0 and item.get("buyNowStatus", 0) > 0:
        listing_type = "buy_now"

      results.append(ScrapedSearchResult(
        external_id=str(item_id),
        url=item_url,
        title=item.get("title", ""),
        current_price=current_price,
        currency=item.get("currency", "USD"),
        image_url=image_url,
        end_time=end_time,
        listing_type=listing_type,
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    state = _extract_window_data(html)
    if state is None:
      raise ValueError(
        "Could not extract window.__data from LiveAuctioneers page"
      )

    item_id = _extract_item_id(url) if url else None
    items_by_id = state.get("itemSummary", {}).get("byId", {})

    item = _find_item(items_by_id, item_id)
    if item is None:
      raise ValueError(f"Item not found in LiveAuctioneers page data")

    actual_item_id = str(item.get("itemId", item_id or ""))

    status = _derive_status(item)
    current_price = _get_current_price(item)
    starting_price = _decimal_or_none(item.get("startPrice"))
    sale_price = _decimal_or_none(item.get("salePrice"))
    final_price = sale_price if sale_price else None
    if final_price and status not in ("sold", "unsold"):
      status = "sold"

    estimate_low = _decimal_or_none(item.get("lowBidEstimate"))
    estimate_high = _decimal_or_none(item.get("highBidEstimate"))

    # Buyer premium from catalog data.
    catalog_id = str(item.get("catalogId", ""))
    catalogs_by_id = state.get("catalog", {}).get("byId", {})
    catalog_data = catalogs_by_id.get(catalog_id, {})
    premium_data = catalog_data.get("buyersPremium", {})
    buyer_premium = _decimal_or_none(premium_data.get("low"))

    start_time = _timestamp_to_datetime(item.get("saleStartTs"))
    lot_end = item.get("lotEndTimeEstimatedTs") or 0
    end_time = _timestamp_to_datetime(lot_end) if lot_end > 0 else None

    seller = _parse_seller(item)
    image_urls = _parse_image_urls(item)

    buy_now_price = _decimal_or_none(item.get("buyNowPrice"))
    if buy_now_price and buy_now_price <= 0:
      buy_now_price = None

    description = item.get("shortDescription", "")
    item_detail = state.get("itemDetail", {}).get("byId", {}).get(actual_item_id, {})
    if item_detail and item_detail.get("description"):
      description = item_detail["description"]

    listing_type = "auction"
    if buy_now_price and item.get("buyNowStatus", 0) > 0:
      listing_type = "buy_now"

    lot_location = item.get("lotLocation", {})
    seller_country = item.get("sellerCountryCode", "")
    location_country = lot_location.get("countryCode", seller_country)

    catalog_title = item.get("catalogTitle", "")
    sale_date = start_time.date() if start_time else None

    attributes = _build_attributes(item, premium_data, lot_location, state, actual_item_id)

    return ScrapedListing(
      external_id=actual_item_id,
      url=url or f"{_BASE_URL}/item/{actual_item_id}",
      title=item.get("title", ""),
      description=description or None,
      listing_type=listing_type,
      currency=item.get("currency", "USD"),
      starting_price=starting_price,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=buy_now_price,
      current_price=current_price,
      final_price=final_price,
      buyer_premium_percent=buyer_premium,
      shipping_from_country=location_country or None,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=item.get("bidCount", 0) or 0,
      lot_number=item.get("lotNumber"),
      auction_house_name=item.get("sellerName") or None,
      sale_name=catalog_title or None,
      sale_date=sale_date,
      seller=seller,
      image_urls=image_urls,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# window.__data extraction
# ------------------------------------------------------------------


def _extract_window_data(html: str) -> dict | None:
  """Extract ``window.__data`` JSON from an LA page.

  The assignment is followed by ``};window.__amplitude`` or
  ``};window.__feature``. JavaScript ``undefined`` is replaced
  with ``null`` for valid JSON.
  """
  match = re.search(r"window\.__data\s*=\s*", html)
  if match is None:
    return None

  start = match.end()
  end_match = re.search(r"\};\s*window\.__(?:amplitude|feature)", html[start:])
  if end_match is None:
    return None

  raw = html[start:start + end_match.start() + 1]
  raw = raw.replace("undefined", "null")

  try:
    return json.loads(raw)
  except json.JSONDecodeError:
    return None


# ------------------------------------------------------------------
# Item lookup
# ------------------------------------------------------------------


def _extract_item_id(url: str) -> str | None:
  """Extract numeric item ID from a LiveAuctioneers URL."""
  match = re.search(r"/item/(\d+)", url)
  if match:
    return match.group(1)
  digits = re.match(r"(\d+)", url)
  if digits:
    return digits.group(1)
  return None


def _find_item(items_by_id: dict, item_id: str | None) -> dict | None:
  """Find an item in the Redux store by ID or scan for it."""
  if item_id and str(item_id) in items_by_id:
    return items_by_id[str(item_id)]
  if item_id:
    for value in items_by_id.values():
      if str(value.get("itemId")) == str(item_id):
        return value
  # Return the first item if only one exists.
  if len(items_by_id) == 1:
    return next(iter(items_by_id.values()))
  return None


# ------------------------------------------------------------------
# Status and type
# ------------------------------------------------------------------


def _derive_status(item: dict) -> str:
  """Map LA item flags to listing status."""
  if item.get("isDeleted"):
    return "cancelled"
  if item.get("isSold"):
    return "sold"
  if item.get("isPassed"):
    return "unsold"
  if item.get("isAvailable"):
    return "active"
  if item.get("isLocked"):
    return "active"
  catalog_status = (item.get("catalogStatus") or "").lower()
  if catalog_status == "online":
    return "active"
  if catalog_status in ("preview", "upcoming"):
    return "active"
  return "active"


# ------------------------------------------------------------------
# Price helpers
# ------------------------------------------------------------------


def _get_current_price(item: dict) -> Decimal | None:
  """Determine the current effective price."""
  sale_price = item.get("salePrice", 0) or 0
  if sale_price > 0:
    return _decimal_or_none(sale_price)
  leading = item.get("leadingBid", 0) or 0
  if leading > 0:
    return _decimal_or_none(leading)
  start = item.get("startPrice", 0) or 0
  if start > 0:
    return _decimal_or_none(start)
  return None


# ------------------------------------------------------------------
# Images
# ------------------------------------------------------------------


def _build_single_image_url(item: dict, photo_index: int = 1) -> str | None:
  """Build a CDN image URL for a single photo."""
  seller_id = item.get("sellerId", 0)
  catalog_id = item.get("catalogId", 0)
  item_id = item.get("itemId", 0)
  image_version = item.get("imageVersion", 0)
  if not all([seller_id, catalog_id, item_id]):
    return None
  return (
    f"{_CDN_BASE}/{seller_id}/{catalog_id}/{item_id}_{photo_index}_x.jpg"
    f"?quality={_IMAGE_QUALITY}&version={image_version}"
  )


def _parse_image_urls(item: dict) -> list[str]:
  """Build image URLs from the item's photos array."""
  photos = item.get("photos", [])
  if not photos:
    return []

  seller_id = item.get("sellerId", 0)
  catalog_id = item.get("catalogId", 0)
  item_id = item.get("itemId", 0)
  image_version = item.get("imageVersion", 0)

  urls: list[str] = []
  for photo_index in photos:
    url = (
      f"{_CDN_BASE}/{seller_id}/{catalog_id}/{item_id}_{photo_index}_x.jpg"
      f"?quality={_IMAGE_QUALITY}&version={image_version}"
    )
    urls.append(url)
  return urls


# ------------------------------------------------------------------
# Seller
# ------------------------------------------------------------------


def _parse_seller(item: dict) -> ScrapedSeller | None:
  """Build a ScrapedSeller from the item's seller fields."""
  seller_id = str(item.get("sellerId", ""))
  seller_name = item.get("sellerName", "")
  if not seller_id or not seller_name:
    return None

  seller_logo_id = item.get("sellerLogoId", "")
  profile_url = (
    f"{_BASE_URL}/auctioneer/{seller_id}/{seller_logo_id}/"
    if seller_logo_id
    else None
  )

  return ScrapedSeller(
    external_id=seller_id,
    username=seller_name,
    display_name=seller_name,
    country=item.get("sellerCountryCode") or None,
    rating=item.get("houseRating"),
    feedback_count=item.get("houseReviewCount"),
    profile_url=profile_url,
  )


# ------------------------------------------------------------------
# Attributes
# ------------------------------------------------------------------


def _build_attributes(
  item: dict,
  premium_data: dict,
  lot_location: dict,
  state: dict,
  item_id: str,
) -> dict[str, str]:
  """Build the free-form attributes dict."""
  attributes: dict[str, str] = {}

  catalog_title = item.get("catalogTitle", "")
  if catalog_title:
    attributes["sale_name"] = catalog_title

  seller_city = item.get("sellerCity", "")
  seller_country = item.get("sellerCountryCode", "")
  seller_state = item.get("sellerStateCode", "")
  location_city = lot_location.get("city", seller_city)
  location_region = lot_location.get("region", seller_state)
  location_country = lot_location.get("countryCode", seller_country)

  if location_city:
    attributes["sale_city"] = location_city
  if location_region:
    attributes["sale_region"] = location_region
  if location_country:
    attributes["sale_country"] = location_country

  if item.get("isLiveAuction"):
    attributes["auction_type"] = "live"
  elif item.get("isTimedAuction"):
    attributes["auction_type"] = "timed"
  elif item.get("isTimedPlusAuction"):
    attributes["auction_type"] = "timed_plus"

  catalog_status = item.get("catalogStatus", "")
  if catalog_status:
    attributes["catalog_status"] = catalog_status

  if item.get("isReserveMet") is not None:
    attributes["reserve_met"] = str(item["isReserveMet"])

  if premium_data:
    for field, key in [
      ("low", "buyers_premium_low_pct"),
      ("lowCutoff", "buyers_premium_low_cutoff"),
      ("middle", "buyers_premium_mid_pct"),
      ("middleCutoff", "buyers_premium_mid_cutoff"),
      ("high", "buyers_premium_high_pct"),
    ]:
      value = premium_data.get(field)
      if value is not None:
        attributes[key] = str(value)

  if item.get("houseRating"):
    attributes["house_rating"] = str(round(item["houseRating"], 2))
  if item.get("houseReviewCount"):
    attributes["house_review_count"] = str(item["houseReviewCount"])

  return attributes


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _timestamp_to_datetime(timestamp: int | None) -> datetime | None:
  """Convert Unix timestamp (seconds) to timezone-aware datetime."""
  if timestamp is None or timestamp == 0:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None


def _decimal_or_none(value) -> Decimal | None:
  """Safely convert to Decimal; treats 0 as None."""
  if value is None:
    return None
  try:
    result = Decimal(str(value))
    if result == 0:
      return None
    return result
  except (InvalidOperation, ValueError, TypeError):
    return None
