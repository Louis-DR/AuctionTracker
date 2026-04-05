"""LeBonCoin parser.

LeBonCoin is France's largest classified-ads marketplace. Unlike
auction sites, listings have a fixed asking price (sometimes negotiable)
and no end date. Items disappear when the seller removes them, whether
because the item was sold, the ad expired, or for any other reason.

Key technical facts used by this parser:

* The site is a **Next.js** application. Both search and listing pages
  embed a ``__NEXT_DATA__`` JSON blob inside a ``<script>`` tag.
* **DataDome** anti-bot protection is active. The transport layer
  handles bypassing it (browser with Playwright). This parser detects
  DataDome challenge pages and raises ``ParserBlocked``.
* Search URL: ``https://www.leboncoin.fr/recherche?text=QUERY&page=N``
  Results live in ``props.pageProps.searchData.ads``.
* Listing URL: ``https://www.leboncoin.fr/ad/{category_slug}/{list_id}``
  Data lives in ``props.pageProps.ad``.
* When a listing is removed the server returns HTTP 410 and
  ``pageProps.ad`` is ``None``.
* Prices are in EUR. ``price_cents`` is the precise amount in cents;
  ``price`` is a list of whole-euro amounts.
* Condition slugs: ``etatneuf`` (new), ``tresbonetat`` (very good),
  ``bonetat`` (good), ``etatsatisfaisant`` (fair).
* Datetimes (``first_publication_date``, ``index_date``) are in Paris
  local time with the format ``"YYYY-MM-DD HH:MM:SS"``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from auction_tracker.parsing.base import (
  Parser,
  ParserBlocked,
  ParserCapabilities,
  ParserRegistry,
)
from auction_tracker.parsing.models import (
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_BASE_URL = "https://www.leboncoin.fr"
_PARIS_TZ = ZoneInfo("Europe/Paris")

# Map LeBonCoin condition slugs to our normalised strings.
_CONDITION_MAP: dict[str, str] = {
  "etatneuf": "new",
  "tresbonetat": "very_good",
  "bonetat": "good",
  "etatsatisfaisant": "fair",
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class LeBonCoinParser(Parser):
  """Parser for LeBonCoin classified ads.

  LeBonCoin is a fixed-price marketplace. There are no auctions, no
  bids, and no end times. Listings are considered active until they
  disappear (HTTP 410). The monitoring strategy is ``snapshot``:
  periodic checks to detect price changes and removal.
  """

  @property
  def website_name(self) -> str:
    return "leboncoin"

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
      has_estimates=False,
      has_reserve_price=False,
      has_lot_numbers=False,
      has_auction_house_info=False,
    )

  # ----------------------------------------------------------------
  # URL helpers
  # ----------------------------------------------------------------

  def build_search_url(self, query: str, **kwargs) -> str:
    """Build a LeBonCoin search URL.

    Optional kwargs:
      category: Category slug to filter results.
      page: Page number (1-indexed, omitted when 1).
    """
    params: dict[str, str] = {"text": query}
    category = kwargs.get("category")
    if category:
      params["category"] = category
    page = kwargs.get("page")
    if page and int(page) > 1:
      params["page"] = str(page)
    return f"{_BASE_URL}/recherche?{urlencode(params)}"

  def extract_external_id(self, url: str) -> str | None:
    return _extract_list_id(url)

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    _check_for_datadome(html)

    next_data = _extract_next_data(html)
    if next_data is None:
      raise ValueError(
        "Could not extract __NEXT_DATA__ from LeBonCoin search page"
      )

    search_data = (
      next_data
      .get("props", {})
      .get("pageProps", {})
      .get("searchData", {})
    )
    ads = search_data.get("ads") or []

    results: list[ScrapedSearchResult] = []
    for ad in ads:
      result = _ad_to_search_result(ad)
      if result is not None:
        results.append(result)

    logger.info(
      "LeBonCoin search: parsed %d results from %d ads",
      len(results), len(ads),
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    _check_for_datadome(html, url=url)

    next_data = _extract_next_data(html)
    if next_data is None:
      raise ValueError(
        "Could not extract __NEXT_DATA__ from LeBonCoin listing page"
      )

    page_props = next_data.get("props", {}).get("pageProps", {})
    ad = page_props.get("ad")

    if ad is None:
      # The listing has been removed (HTTP 410 or equivalent).
      list_id = _extract_list_id(url) or "unknown"
      logger.info("LeBonCoin listing %s has been removed.", list_id)
      return ScrapedListing(
        external_id=list_id,
        url=url or f"{_BASE_URL}/ad/offres/{list_id}",
        title=f"[Removed] LeBonCoin #{list_id}",
        listing_type="buy_now",
        status="sold",
        currency="EUR",
      )

    return _parse_ad(ad)


# ------------------------------------------------------------------
# DataDome detection
# ------------------------------------------------------------------


def _check_for_datadome(html: str, url: str = "") -> None:
  """Raise ParserBlocked if the HTML is a DataDome challenge page.

  DataDome challenge pages are small (< 10 KB) and contain markers
  like ``geo.captcha-delivery.com``, ``datadome``, or ``dd.js``.
  """
  if len(html) > 10_000:
    return
  lower_prefix = html[:5000].lower()
  is_challenge = (
    "geo.captcha-delivery.com" in html[:5000]
    or "datadome" in lower_prefix[:3000]
    or "dd.js" in html[:5000]
  )
  if is_challenge:
    raise ParserBlocked(
      "DataDome challenge page detected",
      url=url,
    )


# ------------------------------------------------------------------
# __NEXT_DATA__ extraction
# ------------------------------------------------------------------


def _extract_next_data(html: str) -> dict | None:
  """Extract the ``__NEXT_DATA__`` JSON blob from a LeBonCoin page.

  Tries several regex patterns to handle minor variations in the
  script tag format across Next.js versions.
  """
  # Primary pattern: <script id="__NEXT_DATA__" type="application/json">
  match = re.search(
    r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    html,
    re.DOTALL,
  )
  if match:
    try:
      return json.loads(match.group(1).strip())
    except json.JSONDecodeError as error:
      logger.error("Failed to parse __NEXT_DATA__ JSON: %s", error)

  # Fallback: string search for the marker.
  for marker in (
    'id="__NEXT_DATA__" type="application/json">',
    "id='__NEXT_DATA__' type='application/json'>",
    '__NEXT_DATA__" type="application/json">',
  ):
    start = html.find(marker)
    if start < 0:
      continue
    start += len(marker)
    end = html.find("</script>", start)
    if end < 0:
      continue
    try:
      return json.loads(html[start:end].strip())
    except json.JSONDecodeError:
      continue

  logger.warning(
    "No __NEXT_DATA__ found in HTML (length=%d, contains marker=%s)",
    len(html), "__NEXT_DATA__" in html,
  )
  return None


# ------------------------------------------------------------------
# Search result parsing
# ------------------------------------------------------------------


def _ad_to_search_result(ad: dict) -> ScrapedSearchResult | None:
  """Convert a search-result ad dict to a ScrapedSearchResult."""
  list_id = ad.get("list_id")
  if list_id is None:
    return None

  title = ad.get("subject", "")
  url = ad.get("url") or f"{_BASE_URL}/ad/{list_id}"
  # Ensure URL is absolute.
  if url.startswith("/"):
    url = f"{_BASE_URL}{url}"

  price = _extract_price(ad)

  # First image thumbnail.
  image_url = None
  images_block = ad.get("images") or {}
  urls = images_block.get("urls") or images_block.get("urls_large") or []
  if urls:
    image_url = urls[0]

  return ScrapedSearchResult(
    external_id=str(list_id),
    url=url,
    title=title,
    current_price=price,
    currency="EUR",
    listing_type="buy_now",
    image_url=image_url,
  )


# ------------------------------------------------------------------
# Full listing parsing
# ------------------------------------------------------------------


def _parse_ad(ad: dict) -> ScrapedListing:
  """Parse a full ad JSON object into a ScrapedListing."""
  list_id = str(ad.get("list_id", ""))
  title = ad.get("subject", "")
  url = ad.get("url") or f"{_BASE_URL}/ad/{list_id}"
  if url.startswith("/"):
    url = f"{_BASE_URL}{url}"
  body = ad.get("body") or None
  price = _extract_price(ad)

  # Status.
  status = _derive_status(ad)

  # Images: prefer large URLs.
  image_urls = _extract_image_urls(ad)

  # Seller.
  seller = _extract_seller(ad)

  # Condition.
  condition = _extract_condition(ad)

  # Location.
  location = ad.get("location") or {}
  city = location.get("city_label") or location.get("city", "")
  region = location.get("region_name", "")
  department = location.get("department_name", "")
  country = location.get("country_id", "FR")

  # Shipping.
  shippable = _get_attribute(ad, "shippable") == "true"

  # Buyer fee (in cents).
  buyer_fee = _extract_buyer_fee(ad)

  # Dates: LeBonCoin datetimes are in Paris local time.
  publication_date = _parse_leboncoin_datetime(ad.get("first_publication_date"))

  # Counters.
  watcher_count = _extract_watcher_count(ad)

  # Attributes.
  attributes = _build_attributes(ad, city, region, department)

  return ScrapedListing(
    external_id=list_id,
    url=url,
    title=title,
    description=body,
    listing_type="buy_now",
    condition=condition,
    currency="EUR",
    buy_now_price=price,
    current_price=price,
    buyer_premium_fixed=buyer_fee,
    shipping_from_country=country,
    ships_internationally=shippable if shippable else None,
    start_time=publication_date,
    status=status,
    watcher_count=watcher_count,
    image_urls=image_urls,
    seller=seller,
    attributes=attributes,
  )


# ------------------------------------------------------------------
# Field extraction helpers
# ------------------------------------------------------------------


def _extract_price(ad: dict) -> Decimal | None:
  """Extract the price from an ad dict.

  LeBonCoin provides ``price_cents`` (precise, in cents) and ``price``
  (a list of whole-euro amounts). We prefer ``price_cents``.
  """
  price_cents = ad.get("price_cents")
  if price_cents is not None:
    return _decimal_or_none(price_cents, divisor=100)

  price_list = ad.get("price")
  if isinstance(price_list, list) and price_list:
    return _decimal_or_none(price_list[0])

  return None


def _derive_status(ad: dict) -> str:
  """Derive listing status from the ad dict."""
  raw_status = (ad.get("status") or "").lower()
  if raw_status == "active":
    return "active"
  if raw_status in ("expired", "deleted"):
    return "sold"
  return "active"


def _extract_image_urls(ad: dict) -> list[str]:
  """Extract image URLs, preferring large versions."""
  images_block = ad.get("images") or {}
  return images_block.get("urls_large") or images_block.get("urls") or []


def _extract_seller(ad: dict) -> ScrapedSeller | None:
  """Extract seller information from the ad's ``owner`` field."""
  owner = ad.get("owner")
  if not owner:
    return None

  store_id = owner.get("store_id") or owner.get("user_id") or ""
  name = owner.get("name", "")
  if not store_id and not name:
    return None

  # Rating and feedback from the ad's attributes list.
  rating = None
  feedback_count = None
  for attr_dict in ad.get("attributes", []):
    key = attr_dict.get("key", "")
    if key == "rating_score":
      with contextlib.suppress(ValueError, TypeError):
        # LeBonCoin rating is 0-1; scale to 0-5 for consistency.
        raw_rating = float(attr_dict.get("value", "0"))
        rating = round(raw_rating * 5, 2)
    elif key == "rating_count":
      with contextlib.suppress(ValueError, TypeError):
        feedback_count = int(attr_dict.get("value", "0"))

  # Country from attributes or location.
  country = _get_attribute(ad, "country_isocode3166")
  if not country:
    country = (ad.get("location") or {}).get("country_id")

  seller_type = owner.get("type", "")
  display_name = f"{name} ({seller_type})" if seller_type else name

  profile_url = None
  user_id = owner.get("user_id")
  if user_id:
    profile_url = f"{_BASE_URL}/profile/{user_id}/offers"

  return ScrapedSeller(
    external_id=str(store_id),
    username=name,
    display_name=display_name,
    country=country,
    rating=rating,
    feedback_count=feedback_count,
    profile_url=profile_url,
  )


def _extract_condition(ad: dict) -> str | None:
  """Extract item condition from the ad's attributes."""
  condition_value = _get_attribute(ad, "condition")
  if condition_value:
    return _CONDITION_MAP.get(condition_value)
  return None


def _extract_buyer_fee(ad: dict) -> Decimal | None:
  """Extract buyer fee from the ad (in cents, converted to euros)."""
  buyer_fee_data = ad.get("buyer_fee")
  if isinstance(buyer_fee_data, dict) and buyer_fee_data.get("amount"):
    return _decimal_or_none(buyer_fee_data["amount"], divisor=100)
  return None


def _extract_watcher_count(ad: dict) -> int | None:
  """Extract the favorites count as watcher count."""
  counters = ad.get("counters") or {}
  favorites = counters.get("favorites")
  if favorites is None:
    return None
  if isinstance(favorites, str):
    return int(favorites) if favorites.isdigit() else None
  if isinstance(favorites, int):
    return favorites
  return None


def _build_attributes(
  ad: dict,
  city: str,
  region: str,
  department: str,
) -> dict[str, str]:
  """Build the attributes dict from various ad fields."""
  attributes: dict[str, str] = {}

  category_name = ad.get("category_name", "")
  if category_name:
    attributes["category"] = category_name
  category_id = ad.get("category_id")
  if category_id:
    attributes["category_id"] = str(category_id)

  if city:
    attributes["city"] = city
  if region:
    attributes["region"] = region
  if department:
    attributes["department"] = department

  shipping_type = _get_attribute(ad, "shipping_type")
  if shipping_type:
    attributes["shipping_type"] = shipping_type

  # Seller type (private vs professional).
  owner = ad.get("owner") or {}
  seller_type = owner.get("type", "")
  if seller_type:
    attributes["seller_type"] = seller_type

  # Ad type (offer, demand, etc.).
  ad_type = ad.get("ad_type", "")
  if ad_type:
    attributes["ad_type"] = ad_type

  # Negotiation possible?
  negotiable = _get_attribute(ad, "negotiation_cta_visible")
  if negotiable == "true":
    attributes["negotiable"] = "true"

  # Collect selected extra attributes.
  for attr_dict in ad.get("attributes", []):
    key = attr_dict.get("key", "")
    if key in ("ean", "isbn", "brand", "model", "energy_rate"):
      label = attr_dict.get("value_label") or attr_dict.get("value", "")
      if label:
        attributes[key] = label

  return attributes


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _get_attribute(ad: dict, key: str) -> str | None:
  """Get a single attribute value from the ad's attributes list."""
  for attr_dict in ad.get("attributes", []):
    if attr_dict.get("key") == key:
      return attr_dict.get("value")
  return None


def _extract_list_id(url_or_id: str) -> str | None:
  """Extract the numeric listing ID from a URL or bare ID.

  Returns None if the format is not recognised.
  """
  if not url_or_id:
    return None
  if url_or_id.isdigit():
    return url_or_id
  # Match /ad/{slug}/{digits} followed by .htm, query, hash, slash, or end.
  match = re.search(r"/(\d{8,12})(?:\.\w+)?(?:\?|$|#|/)", url_or_id)
  if match:
    return match.group(1)
  # Fallback: last numeric segment before optional extension.
  path = url_or_id.split("?")[0]
  match = re.search(r"/(\d+)(?:\.\w+)?$", path)
  if match:
    return match.group(1)
  return None


def _decimal_or_none(
  value: str | int | float | None,
  divisor: int = 1,
) -> Decimal | None:
  """Convert a value to Decimal, optionally dividing. Returns None on failure."""
  if value is None:
    return None
  try:
    result = Decimal(str(value))
    if divisor != 1:
      result = result / Decimal(divisor)
    return result
  except (InvalidOperation, ValueError, TypeError):
    return None


def _parse_leboncoin_datetime(value: str | None) -> datetime | None:
  """Parse a LeBonCoin datetime string.

  Format is ``"YYYY-MM-DD HH:MM:SS"`` in Paris local time (CET/CEST).
  We convert to UTC for consistent storage.
  """
  if not value:
    return None
  try:
    naive = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    # Interpret as Paris time, then convert to UTC.
    paris_dt = naive.replace(tzinfo=_PARIS_TZ)
    return paris_dt.astimezone(UTC)
  except (ValueError, TypeError):
    return None
