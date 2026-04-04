"""Catawiki scraper.

Catawiki is a curated online auction platform based in the Netherlands.
Their lot pages are rendered with Next.js and embed structured JSON in
a ``<script id="__NEXT_DATA__">`` tag.  Search results are available
via an internal JSON API at ``/buyer/api/v1/search``.

Because Catawiki uses Akamai Bot Manager, standard HTTP libraries are
blocked.  We use ``curl_cffi`` with Chrome TLS-fingerprint
impersonation to bypass this.

Key Catawiki facts used in the scraper:

* Buyer premium is a flat **9 %** on the hammer price.
* All prices on the platform default to **EUR**.
* Each lot belongs to a single *auction* (i.e. a themed sale) that
  opens and closes on a fixed schedule.
* The **full** bid history is available via a dedicated JSON API at
  ``/buyer/api/v3/lots/{id}/bids``.  The ``__NEXT_DATA__`` embedded
  in the lot page only includes the last **10** bids.  The "See all
  bids" button on the frontend triggers a fetch to this API.
* Item specifications (brand, era, condition, …) are exposed as a
  ``specifications`` array in the lot data.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

from curl_cffi import requests as cffi_requests

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

_BASE_URL = "https://www.catawiki.com"
_SEARCH_API_URL = f"{_BASE_URL}/buyer/api/v1/search"
_LOT_PAGE_URL_PATTERN = f"{_BASE_URL}/en/l/{{slug}}"
_BUYER_PREMIUM_PERCENT = Decimal("9.0000")
_DEFAULT_CURRENCY = "EUR"
_RESULTS_PER_PAGE = 25
_BIDS_API_URL = f"{_BASE_URL}/buyer/api/v3/lots/{{lot_id}}/bids"
_BIDS_PER_PAGE = 100

# All Catawiki auctions start at 1 EUR.
_CATAWIKI_STARTING_PRICE = Decimal("1")

# Mapping from Catawiki's condition strings (in the "Condition"
# specification) to our enum.  The keys are lowered substrings that
# appear at the start of the condition value.
_CONDITION_MAP: dict[str, ItemCondition] = {
  "as new": ItemCondition.NEW,
  "mint": ItemCondition.NEW,
  "excellent": ItemCondition.LIKE_NEW,
  "very good": ItemCondition.VERY_GOOD,
  "good": ItemCondition.GOOD,
  "fair": ItemCondition.FAIR,
  "poor": ItemCondition.POOR,
  "for parts": ItemCondition.FOR_PARTS,
  "not working": ItemCondition.FOR_PARTS,
}


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("catawiki")
class CatawikiScraper(BaseScraper):
  """Scraper for www.catawiki.com."""

  def __init__(self, config: ScrapingConfig) -> None:
    super().__init__(config)
    # Replace the default requests session with a curl_cffi session
    # that impersonates Chrome to get past Akamai bot detection.
    self._cffi_session = cffi_requests.Session(impersonate="chrome")
    self._cffi_session.headers.update({
      "Accept-Language": "en-US,en;q=0.9",
    })

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "Catawiki"

  @property
  def website_base_url(self) -> str:
    return _BASE_URL

  @property
  def capabilities(self) -> ScraperCapabilities:
    return ScraperCapabilities(
      can_search=True,
      can_fetch_listing=True,
      can_fetch_bids=True,
      can_fetch_seller=False,
      has_bid_history=True,
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=False,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
      has_auction_house_info=False,
      monitoring_strategy="full",  # Bids extend auction time.
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
    """Search Catawiki via their internal buyer API."""
    params: dict[str, str | int] = {"q": query, "page": page}
    data = self._api_get(_SEARCH_API_URL, params=params)

    results: list[SearchResult] = []
    for lot in data.get("lots", []):
      external_id = str(lot["id"])
      url = lot.get("url", f"{_BASE_URL}/en/l/{external_id}")

      # The search API does not expose the current price, only
      # whether bidding has started and whether a reserve is set.
      status = _derive_search_status(lot)

      end_time = None
      # Bidding start time is available; the close time is not in the
      # search API – it must be fetched from the lot page.

      results.append(SearchResult(
        external_id=external_id,
        url=url,
        title=lot.get("title", ""),
        current_price=None,
        currency=_DEFAULT_CURRENCY,
        image_url=lot.get("originalImageUrl") or lot.get("thumbImageUrl"),
        end_time=end_time,
        listing_type=ListingType.AUCTION,
        status=status,
      ))

    logger.info(
      "Catawiki search '%s' page %d: %d results (total %d).",
      query, page, len(results), data.get("total", "?"),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch a lot page and extract all available data from ``__NEXT_DATA__``."""
    url = self._normalise_lot_url(url_or_external_id)
    page_props = self._fetch_page_props(url)

    lot_data = page_props.get("lotDetailsData", {})
    bidding = page_props.get("biddingBlockResponse", {})
    auction = page_props.get("auction", {})
    # Ensure gallery is never None - use empty dict if missing or None.
    gallery = page_props.get("rawGallery") or {}

    external_id = str(
      lot_data.get("lotId")
      or page_props.get("lotId")
      or _extract_lot_id_from_url(url)
    )

    # ----- Seller -----
    seller = _parse_seller(lot_data.get("sellerInfo"))

    # ----- Condition -----
    condition = _parse_condition(lot_data.get("specifications", []))

    # ----- Prices -----
    current_price = _decimal_or_none(bidding.get("localizedCurrentBidAmount"))

    # On Catawiki the starting price is always 1 EUR.
    # ``localizedStartBidAmount`` is the minimum *next* bid, not the
    # original starting price.
    starting_price = _CATAWIKI_STARTING_PRICE

    # Experts estimate
    estimate = lot_data.get("expertsEstimate") or {}
    estimate_low = _decimal_or_none(
      (estimate.get("min") or {}).get(_DEFAULT_CURRENCY)
    )
    estimate_high = _decimal_or_none(
      (estimate.get("max") or {}).get(_DEFAULT_CURRENCY)
    )

    # ----- Status -----
    status = _derive_lot_status(lot_data, bidding)

    # If the lot is sold, the current bid is the final (hammer) price.
    final_price = current_price if status == ListingStatus.SOLD else None

    # ----- Timing -----
    start_time = _timestamp_ms_to_datetime(bidding.get("biddingStartTime"))
    end_time = _timestamp_ms_to_datetime(bidding.get("biddingEndTime"))

    # ----- Reserve price -----
    # ``reservePriceMet`` is ``None`` when no reserve is set,
    # ``False`` when set but not yet met, and ``True`` when met.
    reserve_price_met_raw = bidding.get("reservePriceMet")
    has_reserve_price = reserve_price_met_raw is not None

    # When a reserve price exists and is not yet met, the third entry
    # in ``quickBids`` equals the seller's reserve price.
    quick_bids = bidding.get("quickBids") or []
    reserve_price: Optional[Decimal] = None
    if has_reserve_price and len(quick_bids) >= 3:
      reserve_price = _decimal_or_none(quick_bids[2])

    # ----- Images -----
    images = _parse_images(lot_data.get("images", []), gallery)

    # ----- Bids -----
    # Prefer the dedicated v3 bids API which returns the full history
    # (the ``__NEXT_DATA__`` caps at 10 bids).
    bids = self._fetch_bid_history(external_id)
    if not bids:
      # Fallback to ``__NEXT_DATA__`` bids if the API fails.
      bids = _parse_bids(bidding)

    # ----- Specifications → attributes -----
    attributes = _parse_specifications(lot_data.get("specifications", []))

    # ----- Reserve price attributes -----
    attributes["has_reserve_price"] = str(has_reserve_price)
    if has_reserve_price:
      attributes["reserve_price_met"] = str(bool(reserve_price_met_raw))
      if reserve_price is not None:
        attributes["reserve_price_value"] = str(reserve_price)
    close_to_reserve = bidding.get("closeToReservePrice")
    if close_to_reserve is not None:
      attributes["close_to_reserve_price"] = str(close_to_reserve)

    # Add Catawiki-specific metadata as extra attributes.
    if lot_data.get("summary"):
      attributes["ai_summary"] = lot_data["summary"]
    if auction.get("title"):
      attributes["auction_name"] = auction["title"]

    # ----- Categories -----
    category_names = [
      cat.get("title", "")
      for cat in auction.get("categories", [])
      if cat.get("title")
    ]
    if category_names:
      attributes["categories"] = " > ".join(category_names)

    # ----- Lot number -----
    # Catawiki lots don't have a traditional lot number, but we can
    # store the auction + lot id as one.
    lot_number = external_id

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=lot_data.get("lotTitle", ""),
      description=lot_data.get("description"),
      listing_type=ListingType.AUCTION,
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
      lot_number=lot_number,
      auction_house_name=None,
      sale_name=auction.get("title"),
      sale_date=(
        end_time.strftime("%Y-%m-%d") if end_time else None
      ),
      seller=seller,
      images=images,
      bids=bids,
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # Fetch bids (convenience – data comes from the lot page)
  # ------------------------------------------------------------------

  def fetch_bids(self, url_or_external_id: str) -> Sequence[ScrapedBid]:
    """Fetch bid history for a lot via the dedicated v3 bids API.

    Falls back to :meth:`fetch_listing` if the API call fails.
    """
    lot_id = _extract_lot_id_from_url(url_or_external_id)
    bids = self._fetch_bid_history(lot_id)
    if bids:
      return bids
    # Fallback: parse bids from the lot page's __NEXT_DATA__.
    listing = self.fetch_listing(url_or_external_id)
    return listing.bids

  # ------------------------------------------------------------------
  # Bid history via dedicated API
  # ------------------------------------------------------------------

  def _fetch_bid_history(self, lot_id: str) -> list[ScrapedBid]:
    """Fetch the full bid history via the v3 bids API.

    Returns all bids (not capped at 10 like ``__NEXT_DATA__``).
    Returns an empty list on failure so callers can fall back to
    the ``__NEXT_DATA__`` embedded bids.
    """
    url = _BIDS_API_URL.format(lot_id=lot_id)
    params = {
      "currency_code": _DEFAULT_CURRENCY,
      "per_page": str(_BIDS_PER_PAGE),
    }
    try:
      data = self._api_get(url, params=params)
    except Exception:
      logger.warning(
        "Failed to fetch bid history via API for lot %s, "
        "will fall back to __NEXT_DATA__.",
        lot_id,
      )
      return []

    return _parse_api_bids(data)

  # ------------------------------------------------------------------
  # HTTP helpers (override base to use curl_cffi)
  # ------------------------------------------------------------------

  def _api_get(self, url: str, *, params: Optional[dict] = None) -> dict:
    """GET a JSON API endpoint via curl_cffi."""
    self._rate_limit()
    logger.debug("API GET %s params=%s", url, params)
    response = self._cffi_session.get(
      url, params=params, timeout=self.config.timeout,
    )
    response.raise_for_status()
    return response.json()

  def _cffi_get_text(self, url: str) -> str:
    """GET an HTML page via curl_cffi (or browser if enabled) and return the body text."""
    self._rate_limit()
    if self._browser_enabled:
      try:
        return self._get_html_via_browser(url)
      except Exception as exc:
        logger.warning(
          "Catawiki browser fetch failed for %s, falling back to curl_cffi: %s",
          url, exc,
        )
    logger.debug("GET %s", url)
    response = self._cffi_session.get(url, timeout=self.config.timeout)
    response.raise_for_status()
    return response.text

  def _fetch_page_props(self, url: str) -> dict:
    """Fetch a Next.js page and extract ``pageProps`` from
    ``__NEXT_DATA__``.
    """
    html = self._cffi_get_text(url)
    match = re.search(
      r'<script\s+id="__NEXT_DATA__"\s+type="application/json">\s*(\{.*?\})\s*</script>',
      html,
      re.DOTALL,
    )
    if match is None:
      # Fallback: look for the pattern without the id attribute.
      marker = "__NEXT_DATA__"
      if marker not in html:
        raise ValueError(
          f"Could not find __NEXT_DATA__ in the Catawiki page: {url}"
        )
      start = html.index(marker)
      json_start = html.index("{", start)
      json_end = html.index("</script>", json_start)
      raw_json = html[json_start:json_end]
    else:
      raw_json = match.group(1)

    data = json.loads(raw_json)
    return data.get("props", {}).get("pageProps", {})

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_lot_url(url_or_id: str) -> str:
    """Accept a full URL or a numeric lot ID and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID – build a minimal URL; the server will redirect.
    return f"{_BASE_URL}/en/l/{url_or_id}"


# ------------------------------------------------------------------
# Pure parsing helpers (module-level, easily testable)
# ------------------------------------------------------------------

def _extract_lot_id_from_url(url: str) -> str:
  """Extract the numeric lot ID from a Catawiki URL like
  ``/en/l/101149019-montblanc-…``.
  """
  match = re.search(r"/l/(\d+)", url)
  if match:
    return match.group(1)
  return url.rsplit("/", 1)[-1].split("-", 1)[0]


def _derive_search_status(lot: dict) -> ListingStatus:
  """Derive listing status from the search API lot object."""
  bidding_start_raw = lot.get("biddingStartTime") or lot.get("bidding_start_time")
  if bidding_start_raw:
    try:
      start = datetime.fromisoformat(bidding_start_raw.replace("Z", "+00:00"))
      if start > datetime.now(timezone.utc):
        return ListingStatus.UPCOMING
    except (ValueError, TypeError):
      pass
  return ListingStatus.ACTIVE


def _derive_lot_status(lot_data: dict, bidding: dict) -> ListingStatus:
  """Determine the listing status from lot-page data."""
  if bidding.get("sold"):
    return ListingStatus.SOLD
  if bidding.get("closed") or lot_data.get("isClosed"):
    # Closed but not sold → unsold (reserve not met, etc.).
    return ListingStatus.UNSOLD
  if not lot_data.get("open", True):
    return ListingStatus.UNSOLD
  # Check the live block for close status.
  live = bidding.get("live", {}).get("lot", {})
  close_status = live.get("closeStatus", "")
  if close_status == "Closed":
    return ListingStatus.UNSOLD
  return ListingStatus.ACTIVE


def _parse_seller(seller_info: Optional[dict]) -> Optional[ScrapedSeller]:
  """Build a ``ScrapedSeller`` from the ``sellerInfo`` block."""
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

  created_at_raw = seller_info.get("createdAt")
  member_since = None
  if created_at_raw:
    try:
      member_since = created_at_raw[:10]
    except (TypeError, IndexError):
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


def _parse_condition(specifications: list[dict]) -> ItemCondition:
  """Extract the item condition from the specifications list."""
  for spec in specifications:
    if (spec.get("name") or "").lower() == "condition":
      value = (spec.get("value") or "").lower().strip()
      for prefix, mapped_condition in _CONDITION_MAP.items():
        if value.startswith(prefix):
          return mapped_condition
  return ItemCondition.UNKNOWN


def _parse_images(
  images_list: list[dict],
  raw_gallery: Optional[dict],
) -> list[ScrapedImage]:
  """Collect image URLs, preferring high-res from ``rawGallery``."""
  result: list[ScrapedImage] = []
  seen_urls: set[str] = set()

  # First pass: raw gallery (has "xl" high-res URLs).
  # Handle None case defensively.
  position = 0
  if raw_gallery:
    for group in raw_gallery.get("gallery", []):
      for image_entry in group.get("images", []):
        url = (image_entry.get("xl") or image_entry.get("l") or {}).get("url")
        if url and url not in seen_urls:
          result.append(ScrapedImage(source_url=url, position=position))
          seen_urls.add(url)
          position += 1

  # Second pass: lot images list (fallback).
  for image in images_list:
    url = image.get("large") or image.get("medium")
    if url and url not in seen_urls:
      result.append(ScrapedImage(source_url=url, position=position))
      seen_urls.add(url)
      position += 1

  return result


def _parse_api_bids(data: dict | None) -> list[ScrapedBid]:
  """Parse bids from the v3 ``/buyer/api/v3/lots/{id}/bids`` response.

  This API returns the **complete** bid history (unlike the
  ``__NEXT_DATA__`` which caps at 10 bids).
  """
  if not data:
    return []
  bids: list[ScrapedBid] = []
  for raw_bid in (data.get("bids") or []):
    amount = _decimal_or_none(raw_bid.get("amount"))
    if amount is None:
      continue

    bid_time_str = raw_bid.get("created_at")
    if not bid_time_str:
      continue
    try:
      bid_time = datetime.fromisoformat(bid_time_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
      continue

    bidder = raw_bid.get("bidder") or {}
    bidder_name = bidder.get("name")
    # Strip the "Bidder " prefix that the API includes.
    if bidder_name and bidder_name.startswith("Bidder "):
      bidder_name = bidder_name[len("Bidder "):]

    # Country code – nested under ``bidder.country.code``.
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

    is_from_order = raw_bid.get("from_order", False)

    bids.append(ScrapedBid(
      amount=amount,
      currency=raw_bid.get("currency_code", _DEFAULT_CURRENCY),
      bid_time=bid_time,
      bidder_username=bidder_name,
      bidder_country=bidder_country,
      is_automatic=is_from_order,
    ))

  # Sort by amount ascending – this is more reliable than time because
  # automatic (proxy) bids share the exact same timestamp as the manual
  # bid that triggered them but have a higher amount.
  bids.sort(key=lambda bid: bid.amount)
  return bids


def _parse_bids(bidding: dict) -> list[ScrapedBid]:
  """Extract bids from the ``biddingBlockResponse`` (fallback).

  Catawiki includes at most **10** bids (the most recent ones) in
  the ``biddingHistory`` block.  Use :func:`_parse_api_bids` for the
  complete history from the dedicated v3 API.
  """
  bids: list[ScrapedBid] = []

  # The main history is in ``biddingHistory.bids``.
  history = bidding.get("biddingHistory") or {}
  raw_bids = history.get("bids", [])

  # There is also a ``bidHistory.bids`` which may contain data for
  # logged-in users.  Merge them.
  alt_history = bidding.get("bidHistory") or {}
  alt_bids = alt_history.get("bids", [])

  all_raw_bids = raw_bids + alt_bids

  seen_ids: set[int] = set()
  for raw_bid in all_raw_bids:
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
    try:
      bid_time = datetime.fromisoformat(bid_time_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
      continue

    bidder_name = raw_bid.get("bidderName")
    is_automatic = raw_bid.get("bidType") == "autobid"

    bids.append(ScrapedBid(
      amount=amount,
      currency=_DEFAULT_CURRENCY,
      bid_time=bid_time,
      bidder_username=bidder_name,
      is_automatic=is_automatic,
    ))

  # Sort by amount ascending (see _parse_api_bids for rationale).
  bids.sort(key=lambda bid: bid.amount)
  return bids


def _parse_specifications(specifications: list[dict]) -> dict[str, str]:
  """Convert the Catawiki ``specifications`` array into a flat
  attribute dictionary.
  """
  attributes: dict[str, str] = {}
  for spec in specifications:
    name = spec.get("name")
    value = spec.get("value")
    if name and value:
      # Normalise key to snake_case for consistency.
      key = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
      attributes[key] = value
  return attributes


def _decimal_or_none(value) -> Optional[Decimal]:
  """Safely convert a numeric value to ``Decimal``."""
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None


def _timestamp_ms_to_datetime(timestamp_ms) -> Optional[datetime]:
  """Convert a millisecond Unix timestamp to a timezone-aware
  ``datetime``, or return ``None``.
  """
  if timestamp_ms is None:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None
