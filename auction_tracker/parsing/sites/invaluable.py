"""Invaluable parser.

Invaluable is a major online auction aggregator. Data is extracted from:

1. **Search API** (``/api/search``) -- a JSON endpoint returning paginated
   results with title, estimate, photos, current bid, event date, etc.
2. **Lot detail page** -- HTML embedding ``window.__PRELOADED_STATE__``
   JSON. Under ``pdp`` it holds ``lotData``, ``catalogData``,
   ``auctionHouseData``, ``catalogTermsData``, and
   ``paymentAndTermsDetail``.

All prices default to the auction house's currency (often USD for US
houses, EUR/GBP for European houses).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from auction_tracker.parsing.base import (
  Parser,
  ParserBlocked,
  ParserCapabilities,
  ParserRegistry,
  check_html_for_blocking,
  check_json_response_for_blocking,
)
from auction_tracker.parsing.models import (
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.invaluable.com"
_IMAGE_BASE = "https://image.invaluable.com"


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class InvaluableParser(Parser):
  """Parser for invaluable.com auction lots."""

  @property
  def website_name(self) -> str:
    return "invaluable"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=True,
      has_watcher_count=True,
      has_view_count=False,
      has_buy_now=True,
      has_estimates=True,
      has_reserve_price=False,
      has_lot_numbers=True,
      has_auction_house_info=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    """Build the JSON search API URL.

    Invaluable uses 0-based pages; we accept 1-based and convert.
    """
    from urllib.parse import urlencode
    page = int(kwargs.get("page", 1))
    api_page = max(0, page - 1)
    params = urlencode({"keyword": query, "page": api_page, "size": 96})
    return f"{_BASE_URL}/api/search?{params}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/auction-lot/.*?-?([a-zA-Z0-9]+)$", url)
    if not match:
      return None
    candidate = match.group(1)
    if len(candidate) >= 6:
      return candidate
    return None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, raw: str, url: str = "") -> list[ScrapedSearchResult]:
    """Parse search results from JSON API response."""
    try:
      data = json.loads(raw)
    except json.JSONDecodeError as error:
      check_json_response_for_blocking(raw, url=url)
      raise ValueError("Invaluable search response is not valid JSON") from error

    items = data.get("itemViewList") or []
    results: list[ScrapedSearchResult] = []

    for item in items:
      item_view = item.get("itemView", {})
      ref = item_view.get("ref", "")
      if not ref:
        continue

      title = item_view.get("title", "(no title)")
      currency = item_view.get("currency", "USD")
      current_price = _decimal_or_none(item_view.get("price"))

      url = item_view.get("url", "")
      if not url or not url.startswith("http") or "invaluable.com" not in url:
        url = _build_lot_url(ref, title)
      url = url.replace("http://", "https://", 1)

      end_time = _millis_to_datetime(item_view.get("eventDate"))

      image_url = _extract_search_image(item_view)

      listing_type = "auction"
      if item_view.get("buyItNow"):
        listing_type = "buy_now"

      results.append(ScrapedSearchResult(
        external_id=ref,
        url=url,
        title=title,
        current_price=current_price,
        currency=currency,
        image_url=image_url,
        end_time=end_time,
        listing_type=listing_type,
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    """Parse a lot detail page from embedded __PRELOADED_STATE__."""
    check_html_for_blocking(html, url=url)
    preloaded = _extract_preloaded_state(html)
    # When Invaluable soft-blocks us (rate-limit / behavioural challenge
    # returned with HTTP 200), the response is either an empty shell
    # page or a cached marketing page without the SSR-injected lot
    # payload.  Raise ParserBlocked so the caller knows to retry on the
    # fallback transport (camoufox) rather than counting it as a hard
    # parser error and parking the listing.
    if preloaded is None:
      raise ParserBlocked(
        "Invaluable page has no __PRELOADED_STATE__ "
        "(likely a rate-limit / soft-block response)",
        url=url,
      )

    pdp = preloaded.get("pdp", {})
    lot_data = pdp.get("lotData", {})
    catalog_data = pdp.get("catalogData", {})
    auction_house_data = pdp.get("auctionHouseData", {})
    catalog_terms = pdp.get("catalogTermsData", {})
    payment_terms = pdp.get("paymentAndTermsDetail", {})

    if not lot_data:
      raise ParserBlocked(
        "Invaluable page has __PRELOADED_STATE__ but no lotData "
        "(partial / stripped response, likely a soft-block)",
        url=url,
      )

    external_id = lot_data.get("lotRef", "")
    title = lot_data.get("lotTitle", "(no title)")
    description = lot_data.get("lotDescription", "") or None
    currency = lot_data.get("currency", "USD")

    current_bid = _decimal_or_none(lot_data.get("currentBid"))
    sold_amount = _decimal_or_none(lot_data.get("soldAmount"))
    estimate_low = _decimal_or_none(lot_data.get("estimateLow"))
    estimate_high = _decimal_or_none(lot_data.get("estimateHigh"))

    status = _derive_lot_status(lot_data, catalog_data)
    final_price = sold_amount if status == "sold" else None

    # If sold_amount exists but status isn't sold/unsold, force sold.
    if sold_amount and status not in ("sold", "unsold"):
      status = "sold"
      final_price = sold_amount

    buyer_premium = _parse_buyer_premium(catalog_terms, payment_terms)
    condition = _parse_condition(lot_data.get("conditionReport", ""))

    event_date_millis = catalog_data.get("eventDate")
    start_time = _millis_to_datetime(event_date_millis)
    if not start_time:
      sale_date_str = catalog_data.get("date")
      if sale_date_str:
        start_time = _parse_iso_datetime(sale_date_str)
    end_time = start_time

    seller = _parse_auction_house(auction_house_data)
    image_urls = _parse_lot_image_urls(lot_data)

    lot_number = lot_data.get("lotNumber")
    if lot_number is not None:
      lot_number = str(lot_number)

    auction_house_name = auction_house_data.get("name")
    sale_name = catalog_data.get("title")
    sale_date = start_time.date() if start_time else None

    attributes = _build_attributes(
      lot_data, catalog_data, auction_house_data, payment_terms,
    )

    return ScrapedListing(
      external_id=external_id,
      url=url or _build_lot_url(external_id),
      title=title,
      description=description,
      listing_type="auction",
      condition=condition,
      currency=currency,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      current_price=current_bid,
      final_price=final_price,
      buyer_premium_percent=buyer_premium,
      shipping_from_country=auction_house_data.get("countryCode"),
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=lot_data.get("bidCount", 0),
      watcher_count=lot_data.get("lotWatchedCount"),
      lot_number=lot_number,
      auction_house_name=auction_house_name,
      sale_name=sale_name,
      sale_date=sale_date,
      seller=seller,
      image_urls=image_urls,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# __PRELOADED_STATE__ extraction
# ------------------------------------------------------------------


def _extract_preloaded_state(html: str) -> dict | None:
  """Extract ``window.__PRELOADED_STATE__`` JSON from the HTML."""
  match = re.search(r"window\.__PRELOADED_STATE__\s*=\s*", html)
  if match is None:
    return None

  start = match.end()

  end_match = re.search(r"\}\s*\n\s*window\.__", html[start:])
  if end_match:
    raw = html[start:start + end_match.start() + 1]
  else:
    end_idx = html.find("</script>", start)
    if end_idx == -1:
      return None
    raw = html[start:end_idx].rstrip().rstrip(";")

  try:
    return json.loads(raw)
  except json.JSONDecodeError:
    return None


# ------------------------------------------------------------------
# Status derivation
# ------------------------------------------------------------------


def _derive_search_status(item_view: dict) -> str:
  """Derive status from search API item fields."""
  price_result = item_view.get("priceResult", 0.0) or 0.0
  is_passed = item_view.get("isPassed", False)
  results_posted = item_view.get("resultsPosted", False)

  if is_passed:
    return "unsold"
  if results_posted and price_result > 0:
    return "sold"
  if results_posted:
    return "unsold"

  event_date = item_view.get("eventDate")
  if event_date:
    event_dt = _millis_to_datetime(event_date)
    if event_dt and event_dt > datetime.now(timezone.utc):
      return "active"

  return "active"


def _derive_lot_status(lot_data: dict, catalog_data: dict) -> str:
  """Derive status from lot page data flags."""
  if lot_data.get("isLotSold"):
    return "sold"
  if lot_data.get("isLotPassed"):
    return "unsold"
  if lot_data.get("isLotClosed"):
    return "active"
  if lot_data.get("isLotInProgress"):
    return "active"
  if catalog_data.get("isUpcoming"):
    return "active"
  return "active"


# ------------------------------------------------------------------
# Buyer premium
# ------------------------------------------------------------------


def _parse_buyer_premium(
  catalog_terms: dict, payment_terms: dict,
) -> Decimal | None:
  """Extract buyer premium percentage from catalog terms."""
  payable_bp = catalog_terms.get("payableBP", "")
  if payable_bp:
    bp_str = str(payable_bp).rstrip("%").strip()
    premium = _decimal_or_none(bp_str)
    if premium is not None:
      return premium

  premiums = payment_terms.get("buyersPremiums") or []
  if premiums:
    first_tier = premiums[0]
    premium_value = first_tier.get("premium")
    if premium_value is not None:
      return _decimal_or_none(premium_value)

  return None


# ------------------------------------------------------------------
# Condition
# ------------------------------------------------------------------


def _parse_condition(condition_report: str) -> str | None:
  """Derive condition from the condition report text."""
  if not condition_report:
    return None
  text = condition_report.lower()
  if any(keyword in text for keyword in ["mint", "as new", "unused"]):
    return "new"
  if any(keyword in text for keyword in ["excellent", "near mint"]):
    return "like_new"
  if "very good" in text:
    return "very_good"
  if "good condition" in text or "good overall" in text:
    return "good"
  if "fair" in text:
    return "fair"
  return None


# ------------------------------------------------------------------
# Images
# ------------------------------------------------------------------


def _extract_search_image(item_view: dict) -> str | None:
  """Extract the first image URL from search results."""
  photos = item_view.get("photos") or []
  if not photos:
    return None
  photo = photos[0]
  links = photo.get("_links", {})
  if links:
    medium_link = links.get("medium", {})
    href = medium_link.get("href")
    if href:
      return href
  medium_filename = photo.get("mediumFileName")
  if medium_filename:
    return f"{_IMAGE_BASE}/housePhotos/{medium_filename}"
  return None


def _parse_lot_image_urls(lot_data: dict) -> list[str]:
  """Parse image URLs from the lot data photos array."""
  photos = lot_data.get("photos") or []
  urls: list[str] = []
  for photo in photos:
    image_url = photo.get("large") or photo.get("medium")
    if image_url:
      urls.append(image_url)
  return urls


# ------------------------------------------------------------------
# Seller (auction house)
# ------------------------------------------------------------------


def _parse_auction_house(auction_house_data: dict) -> ScrapedSeller | None:
  """Parse auction house data into a ScrapedSeller."""
  if not auction_house_data:
    return None
  name = auction_house_data.get("name")
  ref = auction_house_data.get("ref", "")
  if not name:
    return None
  profile_url = f"{_BASE_URL}/auction-house/{ref}" if ref else None
  return ScrapedSeller(
    external_id=ref,
    username=name,
    display_name=name,
    country=auction_house_data.get("countryCode"),
    profile_url=profile_url,
  )


# ------------------------------------------------------------------
# Attributes
# ------------------------------------------------------------------


def _build_attributes(
  lot_data: dict,
  catalog_data: dict,
  auction_house_data: dict,
  payment_terms: dict,
) -> dict[str, str]:
  """Build free-form attributes from various data sources."""
  attributes: dict[str, str] = {}

  condition_report = lot_data.get("conditionReport", "")
  if condition_report:
    attributes["condition_report"] = condition_report

  for field, key in [
    ("lotCirca", "circa"),
    ("lotMedium", "medium"),
    ("lotDimensions", "dimensions"),
    ("lotProvenance", "provenance"),
    ("lotExhibited", "exhibited"),
    ("lotLiterature", "literature"),
    ("notes", "notes"),
  ]:
    value = lot_data.get(field, "")
    if value:
      attributes[key] = value

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

  for cat_key, attr_key in [
    ("supercategory", "supercategory"),
    ("category", "category"),
    ("subcategory", "subcategory"),
  ]:
    cat_obj = catalog_data.get(cat_key, {})
    if isinstance(cat_obj, dict) and cat_obj.get("categoryName"):
      attributes[attr_key] = cat_obj["categoryName"]

  ah_address = auction_house_data.get("address", "")
  if ah_address:
    attributes["auction_house_location"] = ah_address

  premiums = payment_terms.get("buyersPremiums") or []
  if len(premiums) > 1:
    tiers = []
    for tier in premiums:
      from_to = tier.get("fromToAmounts")
      bp_amount = tier.get("buyersPremiumAmount")
      if from_to and bp_amount:
        tiers.append(f"{from_to}: {bp_amount}")
    if tiers:
      attributes["buyer_premium_tiers"] = "; ".join(tiers)

  return attributes


# ------------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------------


def _build_lot_url(ref: str, title: str = "") -> str:
  """Build an Invaluable lot URL from reference and title."""
  slug = _slugify(title) if title else ""
  ref_lower = ref.lower()
  if slug:
    return f"{_BASE_URL}/auction-lot/{slug}-{ref_lower}"
  return f"{_BASE_URL}/auction-lot/-{ref_lower}"


def _slugify(text: str) -> str:
  """Create a URL-friendly slug from text."""
  slug = re.sub(r"[^a-z0-9]+", "-", text.lower())
  return slug.strip("-")


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


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


def _millis_to_datetime(millis: int | None) -> datetime | None:
  """Convert milliseconds since epoch to timezone-aware datetime."""
  if millis is None or millis == 0:
    return None
  try:
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None


def _parse_iso_datetime(date_str: str) -> datetime | None:
  """Parse an ISO 8601 date string to a timezone-aware datetime."""
  if not date_str:
    return None
  try:
    parsed = datetime.fromisoformat(date_str)
    if parsed.tzinfo is None:
      parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
  except (ValueError, TypeError):
    return None
