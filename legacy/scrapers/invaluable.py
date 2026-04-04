"""Invaluable scraper.

Invaluable is a major online auction aggregator based in the United
States.  It connects hundreds of auction houses worldwide and offers
both live (simulcast) and timed online bidding.

Data is extracted from two sources:

1. **Search API** (``/api/search``) — a JSON endpoint that accepts
   a ``keyword`` parameter and returns paginated results.  Each result
   includes title, estimate, photos, current bid price, event date,
   auction house info, and lot reference.

2. **Lot detail page** — an HTML page that embeds a
   ``window.__PRELOADED_STATE__`` JavaScript object, parsed as JSON.
   Inside, the ``pdp`` key holds:

   * ``lotData`` — title, description, prices, photos, sale status.
   * ``catalogData`` — sale title, date, timezone, live/timed flags.
   * ``auctionHouseData`` — auction house name, address, country.
   * ``catalogTermsData`` — buyer premium (``payableBP``).
   * ``paymentAndTermsDetail`` — detailed buyer premium tiers.

Because Invaluable uses anti-bot protections, we use ``curl_cffi``
with Chrome TLS-fingerprint impersonation.

Key Invaluable facts:

* Buyer premium varies by auction house, typically 20 %–35 %.
  It is available from ``catalogTermsData.payableBP`` on lot pages.
* Estimates (``estimateLow`` / ``estimateHigh``) are almost always
  provided.
* Prices default to the auction house's currency (often **USD** for
  US houses, **EUR** or **GBP** for European houses).
* Each lot belongs to a *catalog* (sale) managed by an *auction
  house*.  The catalog has its own date and timezone.
* The lot reference (``lotRef``) is a stable identifier used in URLs.
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

_BASE_URL = "https://www.invaluable.com"
_SEARCH_API_URL = f"{_BASE_URL}/api/search"
_DEFAULT_CURRENCY = "USD"
_SEARCH_PAGE_SIZE = 96

# Image URL patterns.  Invaluable serves images via their CDN.
# The search API provides ``_links`` in each photo, while lot pages
# use simple ``medium`` / ``large`` URLs.
_IMAGE_BASE = "https://image.invaluable.com"


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("invaluable")
class InvaluableScraper(BaseScraper):
  """Scraper for invaluable.com."""

  def __init__(self, config: ScrapingConfig) -> None:
    super().__init__(config)
    # Use curl_cffi for anti-bot bypass.
    self._cffi_session = cffi_requests.Session(impersonate="chrome")
    self._last_cffi_request_time: Optional[float] = None

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "Invaluable"

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
      has_watcher_count=True,
      has_view_count=False,
      has_buy_now=True,
      has_estimates=True,
      has_reserve_price=False,
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
    """Search Invaluable via their JSON search API.

    The Invaluable API uses **0-based** page numbering, whereas the
    ``BaseScraper`` interface uses **1-based** pages.  We convert here.
    """
    api_page = max(0, page - 1)
    params: dict[str, str] = {
      "keyword": query,
      "page": str(api_page),
      "size": str(_SEARCH_PAGE_SIZE),
    }

    data = self._cffi_get_json(_SEARCH_API_URL, params=params)
    if data is None:
      return []

    page_info = data.get("page", {})
    total_elements = page_info.get("totalElements", 0)

    items = data.get("itemViewList") or []
    results: list[SearchResult] = []

    for item in items:
      item_view = item.get("itemView", {})
      ref = item_view.get("ref", "")
      if not ref:
        continue

      title = item_view.get("title", "(no title)")
      currency = item_view.get("currency", _DEFAULT_CURRENCY)

      # Current price: use ``price`` (current bid or starting price).
      current_price = _decimal_or_none(item_view.get("price"))

      # URL: prefer the ``url`` field if present; otherwise build one.
      url = item_view.get("url", "")
      if not url or not url.startswith("http"):
        url = _build_lot_url(ref, title)

      # Normalise Invaluable URLs.
      if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)
      if "invaluable.com" not in url:
        # External link (e.g. auctionzip.com) - build Invaluable URL.
        url = _build_lot_url(ref, title)

      # Status.
      status = _derive_search_status(item_view)

      # End time from eventDate (milliseconds since epoch).
      end_time = _millis_to_datetime(item_view.get("eventDate"))

      # Image.
      photos = item_view.get("photos") or []
      image_url = None
      if photos:
        photo = photos[0]
        links = photo.get("_links", {})
        if links:
          medium_link = links.get("medium", {})
          image_url = medium_link.get("href")
        if not image_url:
          medium_filename = photo.get("mediumFileName")
          if medium_filename:
            image_url = f"{_IMAGE_BASE}/housePhotos/{medium_filename}"

      # Listing type.
      listing_type = ListingType.AUCTION
      if item_view.get("buyItNow"):
        listing_type = ListingType.BUY_NOW

      results.append(SearchResult(
        external_id=ref,
        url=url,
        title=title,
        current_price=current_price,
        currency=currency,
        image_url=image_url,
        end_time=end_time,
        listing_type=listing_type,
        status=status,
      ))

    logger.info(
      "Invaluable search '%s' page %d: %d results (total %d).",
      query, page, len(results), total_elements,
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch a lot page and extract data from __PRELOADED_STATE__."""
    url = self._normalise_lot_url(url_or_external_id)
    html = self._cffi_get_html(url)

    preloaded = _extract_preloaded_state(html)
    if preloaded is None:
      raise ValueError(
        f"Could not extract __PRELOADED_STATE__ from {url}"
      )

    pdp = preloaded.get("pdp", {})
    lot_data = pdp.get("lotData", {})
    catalog_data = pdp.get("catalogData", {})
    auction_house_data = pdp.get("auctionHouseData", {})
    catalog_terms = pdp.get("catalogTermsData", {})
    payment_terms = pdp.get("paymentAndTermsDetail", {})

    if not lot_data:
      raise ValueError(f"No lotData found in page {url}")

    external_id = lot_data.get("lotRef", "")
    lot_id = lot_data.get("lotId", 0)
    title = lot_data.get("lotTitle", "(no title)")
    description = lot_data.get("lotDescription", "")
    currency = lot_data.get("currency", _DEFAULT_CURRENCY)

    # ----- Prices -----
    current_bid = _decimal_or_none(lot_data.get("currentBid"))
    sold_amount = _decimal_or_none(lot_data.get("soldAmount"))
    estimate_low = _decimal_or_none(lot_data.get("estimateLow"))
    estimate_high = _decimal_or_none(lot_data.get("estimateHigh"))

    # ----- Status -----
    status = _derive_lot_status(lot_data, catalog_data)
    final_price = sold_amount if status == ListingStatus.SOLD else None

    # ----- Buyer premium -----
    buyer_premium_percent = _parse_buyer_premium(
      catalog_terms, payment_terms,
    )

    # ----- Listing type -----
    listing_type = ListingType.AUCTION
    is_live = catalog_data.get("isLive", False)
    is_timed = catalog_data.get("isTimed", False)
    if is_timed and not is_live:
      listing_type = ListingType.AUCTION

    # ----- Timing -----
    # ``postedDate`` is when the lot was listed (millis).
    # ``catalogData.date`` is the sale date as ISO string.
    # ``catalogData.eventDate`` is the sale date as millis.
    sale_date_str = catalog_data.get("date")
    event_date_millis = catalog_data.get("eventDate")
    start_time = _millis_to_datetime(event_date_millis)
    if not start_time and sale_date_str:
      start_time = _parse_iso_datetime(sale_date_str)

    # Use the sale date as both start and end for live auctions
    # since we don't know the exact end time.
    end_time = start_time

    # ----- Seller (auction house) -----
    seller = _parse_auction_house(auction_house_data)

    # ----- Images -----
    images = _parse_lot_images(lot_data)

    # ----- Condition -----
    condition = _parse_condition(lot_data.get("conditionReport", ""))

    # ----- Lot metadata -----
    lot_number = lot_data.get("lotNumber")
    if lot_number is not None:
      lot_number = str(lot_number)
    auction_house_name = auction_house_data.get("name")
    sale_name = catalog_data.get("title")
    sale_date = (
      start_time.strftime("%Y-%m-%d") if start_time else None
    )

    # ----- Attributes -----
    attributes = _build_attributes(
      lot_data, catalog_data, auction_house_data,
      catalog_terms, payment_terms,
    )

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency=currency,
      starting_price=None,
      reserve_price=None,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=None,
      current_price=current_bid,
      final_price=final_price,
      buyer_premium_percent=buyer_premium_percent,
      buyer_premium_fixed=None,
      shipping_cost=None,
      shipping_from_country=(
        auction_house_data.get("countryCode")
      ),
      ships_internationally=None,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=lot_data.get("bidCount", 0),
      watcher_count=lot_data.get("lotWatchedCount"),
      view_count=None,
      lot_number=lot_number,
      auction_house_name=auction_house_name,
      sale_name=sale_name,
      sale_date=sale_date,
      seller=seller,
      images=images,
      bids=[],
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # HTTP helpers using curl_cffi
  # ------------------------------------------------------------------

  def _cffi_rate_limit(self) -> None:
    """Sleep if the minimum delay between requests has not elapsed."""
    if self._last_cffi_request_time is not None:
      elapsed = time.time() - self._last_cffi_request_time
      remaining = self.config.request_delay - elapsed
      if remaining > 0:
        time.sleep(remaining)
    self._last_cffi_request_time = time.time()

  def _cffi_get_html(self, url: str) -> str:
    """Fetch an HTML page using curl_cffi with Chrome impersonation."""
    # --- Browser path ---
    if self._browser_enabled:
      try:
        return self._get_html_via_browser(url)
      except Exception as exc:
        logger.debug("Browser fetch failed for %s (%s), falling back to curl_cffi.", url, exc)

    self._cffi_rate_limit()
    logger.debug("GET (cffi) %s", url)
    response = self._cffi_session.get(
      url, timeout=self.config.timeout,
    )
    response.raise_for_status()
    return response.text

  def _cffi_get_json(
    self,
    url: str,
    params: Optional[dict] = None,
  ) -> Optional[dict]:
    """Fetch a JSON endpoint using curl_cffi."""
    self._cffi_rate_limit()
    logger.debug("GET (cffi/json) %s params=%s", url, params)
    response = self._cffi_session.get(
      url,
      params=params,
      headers={"Accept": "application/json"},
      timeout=self.config.timeout,
    )
    if response.status_code != 200:
      logger.warning(
        "Invaluable API returned %d for %s", response.status_code, url,
      )
      return None
    try:
      return response.json()
    except (ValueError, json.JSONDecodeError) as error:
      logger.warning("Could not decode JSON from %s: %s", url, error)
      return None

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_lot_url(url_or_id: str) -> str:
    """Accept a full URL or a lot reference and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Lot reference (e.g. "16749F011E").
    return f"{_BASE_URL}/auction-lot/-{url_or_id.lower()}"


# ------------------------------------------------------------------
# Preloaded state extraction
# ------------------------------------------------------------------

def _extract_preloaded_state(html: str) -> Optional[dict]:
  """Extract the ``window.__PRELOADED_STATE__`` object from the HTML.

  Invaluable embeds a large JSON object that is assigned to this
  global variable.  The assignment is followed by another
  ``window.__`` assignment on the next logical line.
  """
  match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*', html)
  if match is None:
    logger.warning("Could not find __PRELOADED_STATE__ in page.")
    return None

  start = match.end()

  # Find the boundary: ``}\n … window.__``
  end_match = re.search(r'\}\s*\n\s*window\.__', html[start:])
  if end_match:
    raw = html[start : start + end_match.start() + 1]
  else:
    # Fallback: take up to ``</script>``.
    end_idx = html.find("</script>", start)
    if end_idx == -1:
      logger.warning("Could not find end of __PRELOADED_STATE__.")
      return None
    raw = html[start:end_idx].rstrip().rstrip(";")

  try:
    return json.loads(raw)
  except json.JSONDecodeError as error:
    logger.warning(
      "Failed to parse __PRELOADED_STATE__: %s (pos %d)",
      error.msg, error.pos,
    )
    return None


# ------------------------------------------------------------------
# Status helpers
# ------------------------------------------------------------------

def _derive_search_status(item_view: dict) -> ListingStatus:
  """Derive listing status from search API item fields."""
  price_result = item_view.get("priceResult", 0.0) or 0.0
  is_passed = item_view.get("isPassed", False)
  results_posted = item_view.get("resultsPosted", False)

  if is_passed:
    return ListingStatus.UNSOLD

  if results_posted and price_result > 0:
    return ListingStatus.SOLD
  if results_posted:
    # Results posted but no sale price — the lot went unsold.
    return ListingStatus.UNSOLD

  # Check event date to determine upcoming vs active.
  event_date = item_view.get("eventDate")
  if event_date:
    event_dt = _millis_to_datetime(event_date)
    if event_dt and event_dt > datetime.now(timezone.utc):
      return ListingStatus.UPCOMING

  return ListingStatus.ACTIVE


def _derive_lot_status(
  lot_data: dict,
  catalog_data: dict,
) -> ListingStatus:
  """Derive listing status from lot page data."""
  is_sold = lot_data.get("isLotSold", False)
  is_closed = lot_data.get("isLotClosed", False)
  is_passed = lot_data.get("isLotPassed", False)
  is_in_progress = lot_data.get("isLotInProgress", False)
  is_upcoming = catalog_data.get("isUpcoming", False)

  if is_sold:
    return ListingStatus.SOLD
  if is_passed:
    return ListingStatus.UNSOLD
  if is_closed:
    # Auction is closed but not explicitly sold or passed — the
    # results may not be published yet.  Return UNKNOWN so the
    # monitor keeps polling for the final outcome.
    return ListingStatus.UNKNOWN
  if is_in_progress:
    return ListingStatus.ACTIVE
  if is_upcoming:
    return ListingStatus.UPCOMING

  return ListingStatus.UNKNOWN


# ------------------------------------------------------------------
# Buyer premium parsing
# ------------------------------------------------------------------

def _parse_buyer_premium(
  catalog_terms: dict,
  payment_terms: dict,
) -> Optional[Decimal]:
  """Extract the buyer premium percentage.

  The simplest source is ``catalogTermsData.payableBP`` (e.g.
  ``"28.0%"``).  For a more detailed breakdown, we also check
  ``paymentAndTermsDetail.buyersPremiums``.
  """
  # Try the simple string first.
  payable_bp = catalog_terms.get("payableBP", "")
  if payable_bp:
    # Strip trailing '%' and convert.
    bp_str = payable_bp.rstrip("%").strip()
    premium = _decimal_or_none(bp_str)
    if premium is not None:
      return premium

  # Try the detailed array (take the first tier which is typically
  # the most common bracket).
  premiums = payment_terms.get("buyersPremiums") or []
  if premiums:
    first_tier = premiums[0]
    premium_value = first_tier.get("premium")
    if premium_value is not None:
      return _decimal_or_none(premium_value)

  return None


# ------------------------------------------------------------------
# Image parsing
# ------------------------------------------------------------------

def _parse_lot_images(lot_data: dict) -> list[ScrapedImage]:
  """Parse the ``photos`` array from lot data."""
  photos = lot_data.get("photos") or []
  images: list[ScrapedImage] = []

  for position, photo in enumerate(photos):
    # Prefer the ``large`` URL, fall back to ``medium``.
    image_url = photo.get("large") or photo.get("medium")
    if not image_url:
      continue
    images.append(ScrapedImage(
      source_url=image_url,
      position=position,
    ))

  return images


# ------------------------------------------------------------------
# Auction house / seller parsing
# ------------------------------------------------------------------

def _parse_auction_house(
  auction_house_data: dict,
) -> Optional[ScrapedSeller]:
  """Parse auction house data into a ScrapedSeller."""
  if not auction_house_data:
    return None

  name = auction_house_data.get("name")
  ref = auction_house_data.get("ref", "")
  if not name:
    return None

  country = auction_house_data.get("countryCode")
  address = auction_house_data.get("address", "")

  profile_url = None
  if ref:
    profile_url = f"{_BASE_URL}/auction-house/{ref}"

  return ScrapedSeller(
    external_id=ref,
    username=name,
    display_name=name,
    country=country,
    profile_url=profile_url,
  )


# ------------------------------------------------------------------
# Condition parsing
# ------------------------------------------------------------------

def _parse_condition(condition_report: str) -> ItemCondition:
  """Attempt to derive a condition enum from the condition report."""
  if not condition_report:
    return ItemCondition.UNKNOWN

  text = condition_report.lower()

  # Very brief condition strings often indicate condition directly.
  if any(keyword in text for keyword in ["mint", "as new", "unused"]):
    return ItemCondition.NEW
  if any(keyword in text for keyword in ["excellent", "near mint"]):
    return ItemCondition.LIKE_NEW
  if "very good" in text:
    return ItemCondition.VERY_GOOD
  if "good condition" in text or "good overall" in text:
    return ItemCondition.GOOD
  if "fair" in text:
    return ItemCondition.FAIR

  return ItemCondition.UNKNOWN


# ------------------------------------------------------------------
# Attributes
# ------------------------------------------------------------------

def _build_attributes(
  lot_data: dict,
  catalog_data: dict,
  auction_house_data: dict,
  catalog_terms: dict,
  payment_terms: dict,
) -> dict[str, str]:
  """Build the free-form attributes dictionary."""
  attributes: dict[str, str] = {}

  # Condition report (full text, stored as attribute).
  condition_report = lot_data.get("conditionReport", "")
  if condition_report:
    attributes["condition_report"] = condition_report

  # Lot metadata.
  lot_circa = lot_data.get("lotCirca", "")
  if lot_circa:
    attributes["circa"] = lot_circa

  lot_medium = lot_data.get("lotMedium", "")
  if lot_medium:
    attributes["medium"] = lot_medium

  lot_dimensions = lot_data.get("lotDimensions", "")
  if lot_dimensions:
    attributes["dimensions"] = lot_dimensions

  lot_provenance = lot_data.get("lotProvenance", "")
  if lot_provenance:
    attributes["provenance"] = lot_provenance

  lot_exhibited = lot_data.get("lotExhibited", "")
  if lot_exhibited:
    attributes["exhibited"] = lot_exhibited

  lot_literature = lot_data.get("lotLiterature", "")
  if lot_literature:
    attributes["literature"] = lot_literature

  notes = lot_data.get("notes", "")
  if notes:
    attributes["notes"] = notes

  # Watcher count.
  watcher_count = lot_data.get("lotWatchedCount")
  if watcher_count:
    attributes["watcher_count"] = str(watcher_count)

  # Catalog / sale info.
  sale_title = catalog_data.get("title", "")
  if sale_title:
    attributes["sale_name"] = sale_title

  sale_timezone = catalog_data.get("timeZone", "")
  if sale_timezone:
    attributes["sale_timezone"] = sale_timezone

  if catalog_data.get("isLive"):
    attributes["sale_type"] = "live"
  elif catalog_data.get("isTimed"):
    attributes["sale_type"] = "timed"

  # Categories.
  supercategory = catalog_data.get("supercategory", {})
  category = catalog_data.get("category", {})
  subcategory = catalog_data.get("subcategory", {})
  if supercategory.get("categoryName"):
    attributes["supercategory"] = supercategory["categoryName"]
  if category.get("categoryName"):
    attributes["category"] = category["categoryName"]
  if subcategory.get("categoryName"):
    attributes["subcategory"] = subcategory["categoryName"]

  # Auction house location.
  ah_address = auction_house_data.get("address", "")
  if ah_address:
    attributes["auction_house_location"] = ah_address

  # Buyer premium tiers (for audit/reference).
  premiums = payment_terms.get("buyersPremiums") or []
  if len(premiums) > 1:
    # Multiple tiers - store them for reference.
    tiers = []
    for tier in premiums:
      from_to = tier.get("fromToAmounts")
      bp_amount = tier.get("buyersPremiumAmount")
      if from_to and bp_amount:
        tiers.append(f"{from_to}: {bp_amount}")
    if tiers:
      attributes["buyer_premium_tiers"] = "; ".join(tiers)

  # Extension time (for timed auctions).
  extension_time = catalog_terms.get("houseExtensionTime")
  if extension_time:
    attributes["extension_time_minutes"] = str(extension_time)

  # Local currency conversion.
  local_currency = lot_data.get("localCurrencyCode", "")
  local_rate = lot_data.get("localCurrencyConversionRate")
  if local_currency and local_rate:
    attributes["local_currency"] = local_currency
    attributes["local_currency_rate"] = str(local_rate)

  return attributes


# ------------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------------

def _build_lot_url(ref: str, title: str = "") -> str:
  """Build an Invaluable lot URL from a reference and title."""
  # Invaluable lot URLs have the form:
  # /auction-lot/<slugified-title>-<lowercased-ref>
  slug = _slugify(title) if title else ""
  ref_lower = ref.lower()
  if slug:
    return f"{_BASE_URL}/auction-lot/{slug}-{ref_lower}"
  return f"{_BASE_URL}/auction-lot/-{ref_lower}"


def _slugify(text: str) -> str:
  """Create a URL-friendly slug from text."""
  # Lowercase, replace non-alphanumeric with hyphens, collapse.
  slug = re.sub(r'[^a-z0-9]+', '-', text.lower())
  return slug.strip('-')


# ------------------------------------------------------------------
# Generic helpers
# ------------------------------------------------------------------

def _decimal_or_none(value) -> Optional[Decimal]:
  """Safely convert a numeric value to Decimal."""
  if value is None:
    return None
  try:
    decimal_value = Decimal(str(value))
    if decimal_value == 0:
      return None
    return decimal_value
  except (InvalidOperation, ValueError, TypeError):
    return None


def _millis_to_datetime(
  millis: Optional[int],
) -> Optional[datetime]:
  """Convert milliseconds since epoch to a timezone-aware datetime."""
  if millis is None or millis == 0:
    return None
  try:
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None


def _parse_iso_datetime(date_str: str) -> Optional[datetime]:
  """Parse an ISO 8601 date string to a timezone-aware datetime."""
  if not date_str:
    return None
  try:
    # Handle timezone offsets like ``-05:00``.
    parsed = datetime.fromisoformat(date_str)
    if parsed.tzinfo is None:
      parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
  except (ValueError, TypeError):
    return None
