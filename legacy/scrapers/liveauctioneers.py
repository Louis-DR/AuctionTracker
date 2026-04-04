"""LiveAuctioneers scraper.

LiveAuctioneers is a major US-based online auction marketplace that
aggregates sales from auction houses worldwide.  The website is a
React (Redux) SPA that embeds a full application state snapshot in
``window.__data`` inside an inline ``<script>`` tag.

Key LiveAuctioneers facts used in the scraper:

* All item data lives under ``window.__data.itemSummary.byId[itemId]``.
* Buyer's premium (tiered) is in ``window.__data.catalog.byId[catalogId].buyersPremium``.
  Typical structure: ``{"low": 28, "lowCutoff": 20000, "middle": 25,
  "middleCutoff": 50000, "high": 22}`` — meaning 28 % up to $20 k,
  25 % up to $50 k, 22 % above $50 k.  For most low-value items only
  the ``low`` rate applies, so we store that as the primary rate and
  keep the full tier structure in attributes.
* Images follow the CDN pattern
  ``https://p1.liveauctioneers.com/{sellerId}/{catalogId}/{itemId}_{n}_x.jpg``
  where *n* is the 1-based photo index from ``item.photos``.
* Search pages at ``/search/?keyword=…`` embed the same ``window.__data``
  with ``search.itemIds`` + ``itemSummary.byId``.
* Prices can be in USD, EUR, GBP, CAD, etc. depending on the auction house.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

from auction_tracker.config import ScrapingConfig
from auction_tracker.database.models import (
  ItemCondition,
  ListingStatus,
  ListingType,
)
from auction_tracker.scrapers.base import (
  BaseScraper,
  ScrapedBid,
  ScrapedImage,
  ScrapedListing,
  ScrapedSeller,
  ScraperCapabilities,
  SearchResult,
)
from auction_tracker.scrapers.registry import ScraperRegistry

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_BASE_URL = "https://www.liveauctioneers.com"
_SEARCH_URL = f"{_BASE_URL}/search/"
_ITEM_URL_TEMPLATE = f"{_BASE_URL}/item/{{item_id}}_{{slug}}"
_CDN_BASE = "https://p1.liveauctioneers.com"
_DEFAULT_CURRENCY = "USD"

# Image quality parameters.
_IMAGE_QUALITY = 95
_IMAGE_WIDTH = 800


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("liveauctioneers")
class LiveAuctioneersScraper(BaseScraper):
  """Scraper for liveauctioneers.com."""

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "LiveAuctioneers"

  @property
  def website_base_url(self) -> str:
    return _BASE_URL

  @property
  def capabilities(self) -> ScraperCapabilities:
    return ScraperCapabilities(
      can_search=True,
      can_fetch_listing=True,
      can_fetch_bids=False,
      can_fetch_seller=False,
      has_bid_history=False,
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=True,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
      has_auction_house_info=True,
      monitoring_strategy="post_auction",  # Only check after end.
    )

  # ------------------------------------------------------------------
  # Search
  # ------------------------------------------------------------------

  def search(
    self,
    query: str,
    *,
    category: Optional[str] = None,
    page: int = 1,
  ) -> Sequence[SearchResult]:
    """Search LiveAuctioneers via the SSR search page."""
    from urllib.parse import urlencode

    params: dict[str, str] = {"keyword": query}
    if page > 1:
      params["page"] = str(page)
    if category:
      params["categoryId"] = category

    url = f"{_SEARCH_URL}?{urlencode(params)}"
    html = self._get_html(url)
    state = _extract_window_data(html)
    if state is None:
      logger.warning("Could not extract window.__data from LA search page.")
      return []

    search_data = state.get("search", {})
    item_ids = search_data.get("itemIds", [])
    items_by_id = state.get("itemSummary", {}).get("byId", {})

    results: list[SearchResult] = []
    for item_id in item_ids:
      item = items_by_id.get(str(item_id))
      if item is None:
        continue

      slug = item.get("slug", "")
      item_url = f"{_BASE_URL}/item/{item_id}_{slug}" if slug else f"{_BASE_URL}/item/{item_id}"

      status = _derive_status(item)
      current_price = _get_current_price(item)
      end_time = _timestamp_to_datetime(item.get("lotEndTimeEstimatedTs"))

      image_url = _build_image_url(item, photo_index=1)

      results.append(SearchResult(
        external_id=str(item_id),
        url=item_url,
        title=item.get("title", ""),
        current_price=current_price,
        currency=item.get("currency", _DEFAULT_CURRENCY),
        image_url=image_url,
        end_time=end_time,
        listing_type=_derive_listing_type(item),
        status=status,
      ))

    logger.info(
      "LiveAuctioneers search '%s' page %d: %d results.",
      query, page, len(results),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch an item page and extract data from ``window.__data``."""
    url = self._normalise_item_url(url_or_external_id)
    html = self._get_html(url)
    state = _extract_window_data(html)
    if state is None:
      raise ValueError(f"Could not extract window.__data from {url}")

    # Find the item ID from the URL.
    item_id = _extract_item_id(url)

    items_by_id = state.get("itemSummary", {}).get("byId", {})
    item = items_by_id.get(str(item_id))
    if item is None:
      # Try to find any item that matches.
      for k, v in items_by_id.items():
        if str(v.get("itemId")) == str(item_id):
          item = v
          break
    if item is None:
      raise ValueError(f"Item {item_id} not found in page data.")

    # ----- Prices -----
    status = _derive_status(item)
    current_price = _get_current_price(item)
    starting_price = _decimal_or_none(item.get("startPrice"))
    sale_price = _decimal_or_none(item.get("salePrice"))

    final_price = sale_price if (sale_price and sale_price > 0) else None
    if final_price and status not in (ListingStatus.SOLD, ListingStatus.UNSOLD):
      status = ListingStatus.SOLD

    estimate_low = _decimal_or_none(item.get("lowBidEstimate"))
    estimate_high = _decimal_or_none(item.get("highBidEstimate"))

    # ----- Buyer's premium (tiered) -----
    catalog_id = str(item.get("catalogId", ""))
    catalogs_by_id = state.get("catalog", {}).get("byId", {})
    catalog_data = catalogs_by_id.get(catalog_id, {})
    premium_data = catalog_data.get("buyersPremium", {})

    # For low-value items (like pens), the ``low`` rate applies.
    buyer_premium_pct = _decimal_or_none(premium_data.get("low"))

    # ----- Timing -----
    start_time = _timestamp_to_datetime(item.get("saleStartTs"))
    lot_end = item.get("lotEndTimeEstimatedTs") or 0
    end_time = _timestamp_to_datetime(lot_end) if lot_end > 0 else None

    # ----- Seller (auction house) -----
    seller_id = str(item.get("sellerId", ""))
    seller_name = item.get("sellerName", "")
    seller_city = item.get("sellerCity", "")
    seller_country = item.get("sellerCountryCode", "")
    seller_state = item.get("sellerStateCode", "")

    seller = None
    if seller_id and seller_name:
      seller_logo_id = item.get("sellerLogoId", "")
      seller = ScrapedSeller(
        external_id=seller_id,
        username=seller_name,
        display_name=seller_name,
        country=seller_country or None,
        rating=item.get("houseRating"),
        feedback_count=item.get("houseReviewCount"),
        profile_url=f"{_BASE_URL}/auctioneer/{seller_id}/{seller_logo_id}/" if seller_logo_id else None,
      )

    # ----- Images -----
    images = _parse_images(item)

    # ----- Buy Now -----
    buy_now_price = _decimal_or_none(item.get("buyNowPrice"))

    # ----- Description -----
    # The SSR page includes ``shortDescription``.  Full description
    # may require an extra API call, but we'll use what we have.
    description = item.get("shortDescription", "")
    # Also look in itemDetail for full description
    item_detail = state.get("itemDetail", {}).get("byId", {}).get(str(item_id), {})
    if item_detail and item_detail.get("description"):
      description = item_detail["description"]

    # ----- Location -----
    lot_location = item.get("lotLocation", {})
    location_city = lot_location.get("city", seller_city)
    location_country = lot_location.get("countryCode", seller_country)
    location_region = lot_location.get("region", seller_state)

    # ----- Attributes -----
    attributes: dict[str, str] = {}
    catalog_title = item.get("catalogTitle", "")
    if catalog_title:
      attributes["sale_name"] = catalog_title
    if location_city:
      attributes["sale_city"] = location_city
    if location_region:
      attributes["sale_region"] = location_region
    if location_country:
      attributes["sale_country"] = location_country

    postal_code = lot_location.get("postalCode", "")
    if postal_code:
      attributes["postal_code"] = postal_code

    # Auction type.
    if item.get("isLiveAuction"):
      attributes["auction_type"] = "live"
    elif item.get("isTimedAuction"):
      attributes["auction_type"] = "timed"
    elif item.get("isTimedPlusAuction"):
      attributes["auction_type"] = "timed_plus"

    # Catalog status.
    catalog_status = item.get("catalogStatus", "")
    if catalog_status:
      attributes["catalog_status"] = catalog_status

    # Reserve info.
    if item.get("isReserveMet") is not None:
      attributes["reserve_met"] = str(item.get("isReserveMet"))

    # Buyer's premium tiers as attributes.
    if premium_data:
      attributes["buyers_premium_low_pct"] = str(premium_data.get("low", ""))
      attributes["buyers_premium_low_cutoff"] = str(premium_data.get("lowCutoff", ""))
      attributes["buyers_premium_mid_pct"] = str(premium_data.get("middle", ""))
      attributes["buyers_premium_mid_cutoff"] = str(premium_data.get("middleCutoff", ""))
      attributes["buyers_premium_high_pct"] = str(premium_data.get("high", ""))

    # Shipping.
    if item.get("hasFreeShipping"):
      attributes["free_shipping"] = "True"
    if item.get("hasFreeLocalPickup"):
      attributes["free_local_pickup"] = "True"
    if item.get("hasFlatRateShipping"):
      attributes["flat_rate_shipping"] = str(item.get("flatRateShippingAmount", 0))

    # House ratings.
    if item.get("houseRating"):
      attributes["house_rating"] = str(round(item["houseRating"], 2))
    if item.get("houseReviewCount"):
      attributes["house_review_count"] = str(item["houseReviewCount"])
    if item.get("houseIsTopRated"):
      attributes["house_top_rated"] = "True"

    # Categories from facets.
    facets = state.get("itemFacets", {}).get("byId", {}).get(str(item_id), {})
    categories = facets.get("categories", [])
    if categories:
      cat_names = [
        c.get("l2CategoryName") or ""
        for c in categories
        if c.get("l2CategoryName")
      ]
      # Filter out any empty strings
      cat_names = [name for name in cat_names if name]
      if cat_names:
        attributes["categories"] = ", ".join(cat_names)
    creators = facets.get("creators", [])
    if creators:
      creator_names = [
        c.get("l1CategoryName") or ""
        for c in creators
        if c.get("l1CategoryName")
      ]
      # Filter out any empty strings
      creator_names = [name for name in creator_names if name]
      if creator_names:
        attributes["creators"] = ", ".join(creator_names)
    origins = facets.get("origins", [])
    if origins:
      origin_names = [
        o.get("l2CategoryName") or o.get("l1CategoryName") or ""
        for o in origins
        if o.get("l1CategoryName")
      ]
      # Filter out any empty strings or None values that slipped through
      origin_names = [name for name in origin_names if name]
      if origin_names:
        attributes["origins"] = ", ".join(origin_names)

    # ----- Bid count -----
    bid_count = item.get("bidCount", 0) or 0

    return ScrapedListing(
      external_id=str(item_id),
      url=url,
      title=item.get("title", ""),
      description=description,
      listing_type=_derive_listing_type(item),
      condition=ItemCondition.UNKNOWN,
      currency=item.get("currency", _DEFAULT_CURRENCY),
      starting_price=starting_price,
      reserve_price=None,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=buy_now_price if buy_now_price and buy_now_price > 0 else None,
      current_price=current_price,
      final_price=final_price,
      buyer_premium_percent=buyer_premium_pct,
      buyer_premium_fixed=None,
      shipping_cost=None,
      shipping_from_country=location_country or seller_country or None,
      ships_internationally=None,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=bid_count,
      watcher_count=None,
      view_count=None,
      lot_number=item.get("lotNumber"),
      auction_house_name=seller_name or None,
      sale_name=catalog_title or None,
      sale_date=start_time.strftime("%Y-%m-%d") if start_time else None,
      seller=seller,
      images=images,
      bids=[],
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # HTTP helpers
  # ------------------------------------------------------------------

  def _get_html(self, url: str) -> str:
    """Perform a rate-limited GET and return the response body.

    LiveAuctioneers uses Incapsula (Imperva) anti-bot protection.
    Plain ``requests`` always returns a challenge page, so when
    browser mode is enabled we MUST use the browser — there is
    no useful fallback to requests.
    """
    # --- Browser path (required for Incapsula bypass) ---
    if self._browser_enabled:
      return self._get_html_via_browser(url)

    response = self._get(url)
    response.encoding = "utf-8"
    return response.text

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_item_url(url_or_id: str) -> str:
    """Accept a full URL or a numeric item ID and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID — build a minimal URL.
    return f"{_BASE_URL}/item/{url_or_id}"


# ------------------------------------------------------------------
# State extraction
# ------------------------------------------------------------------

def _extract_window_data(html: str) -> Optional[dict]:
  """Extract the ``window.__data`` JSON object from an LA page.

  The state is a standard JavaScript object literal assigned to
  ``window.__data = {…};``.  The only non-JSON value we need to
  handle is ``undefined`` which we map to ``null``.
  """
  match = re.search(r'window\.__data\s*=\s*', html)
  if match is None:
    return None

  start = match.end()

  # Find the end marker: ``};window.__amplitude`` or ``};window.__feature``
  end_match = re.search(r'\};\s*window\.__(?:amplitude|feature)', html[start:])
  if end_match is None:
    # Fallback: try to find a balanced closing brace.
    logger.warning("Could not find end marker for window.__data.")
    return None

  raw = html[start : start + end_match.start() + 1]

  # Replace JavaScript ``undefined`` with JSON ``null``.
  raw = raw.replace("undefined", "null")

  try:
    return json.loads(raw)
  except json.JSONDecodeError as error:
    logger.warning(
      "Failed to parse window.__data: %s (near position %d)",
      error.msg, error.pos,
    )
    return None


# ------------------------------------------------------------------
# Pure parsing helpers
# ------------------------------------------------------------------

def _extract_item_id(url: str) -> str:
  """Extract the numeric item ID from a LiveAuctioneers URL.

  URL format: ``/item/{itemId}_{slug}`` or ``/item/{itemId}``.
  """
  match = re.search(r'/item/(\d+)', url)
  if match:
    return match.group(1)
  # Maybe it's already just a number.
  digits = re.match(r'(\d+)', url)
  if digits:
    return digits.group(1)
  return url


def _derive_status(item: dict) -> ListingStatus:
  """Map LiveAuctioneers item flags to our listing status enum."""
  if item.get("isDeleted"):
    return ListingStatus.CANCELLED
  if item.get("isSold"):
    return ListingStatus.SOLD
  if item.get("isPassed"):
    return ListingStatus.UNSOLD
  if item.get("isAvailable"):
    return ListingStatus.ACTIVE
  if item.get("isLocked"):
    # Locked but not sold/passed → usually ended without result yet.
    return ListingStatus.UNKNOWN
  catalog_status = (item.get("catalogStatus") or "").lower()
  if catalog_status == "online":
    return ListingStatus.ACTIVE
  if catalog_status in ("preview", "upcoming"):
    return ListingStatus.UPCOMING
  if catalog_status == "done":
    return ListingStatus.UNKNOWN
  return ListingStatus.UNKNOWN


def _derive_listing_type(item: dict) -> ListingType:
  """Determine listing type from auction flags."""
  if item.get("buyNowPrice", 0) > 0 and item.get("buyNowStatus", 0) > 0:
    return ListingType.BUY_NOW
  return ListingType.AUCTION


def _get_current_price(item: dict) -> Optional[Decimal]:
  """Determine the current effective price of an item."""
  # If sold, the sale price is definitive.
  sale_price = item.get("salePrice", 0) or 0
  if sale_price > 0:
    return _decimal_or_none(sale_price)
  # Otherwise, the leading bid.
  leading = item.get("leadingBid", 0) or 0
  if leading > 0:
    return _decimal_or_none(leading)
  # Fall back to start price.
  start = item.get("startPrice", 0) or 0
  if start > 0:
    return _decimal_or_none(start)
  return None


def _build_image_url(item: dict, photo_index: int = 1) -> Optional[str]:
  """Build a single image URL for a given photo index."""
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


def _parse_images(item: dict) -> list[ScrapedImage]:
  """Build image URLs from the item's ``photos`` array.

  The CDN pattern is:
  ``https://p1.liveauctioneers.com/{sellerId}/{catalogId}/{itemId}_{n}_x.jpg``

  We add quality/size parameters for a good resolution.
  """
  photos = item.get("photos", [])
  if not photos:
    return []

  seller_id = item.get("sellerId", 0)
  catalog_id = item.get("catalogId", 0)
  item_id = item.get("itemId", 0)
  image_version = item.get("imageVersion", 0)

  images: list[ScrapedImage] = []
  for position, photo_index in enumerate(photos):
    url = (
      f"{_CDN_BASE}/{seller_id}/{catalog_id}/{item_id}_{photo_index}_x.jpg"
      f"?quality={_IMAGE_QUALITY}&version={image_version}"
    )
    images.append(ScrapedImage(source_url=url, position=position))

  return images


def _timestamp_to_datetime(timestamp: Optional[int]) -> Optional[datetime]:
  """Convert a Unix timestamp (seconds) to a timezone-aware datetime."""
  if timestamp is None or timestamp == 0:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None


def _decimal_or_none(value) -> Optional[Decimal]:
  """Safely convert a numeric value to Decimal, treating 0 as None."""
  if value is None:
    return None
  try:
    d = Decimal(str(value))
    if d == 0:
      return None
    return d
  except (InvalidOperation, ValueError, TypeError):
    return None
