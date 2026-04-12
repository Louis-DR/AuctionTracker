"""Vinted parser.

Vinted is a peer-to-peer fashion marketplace. Listings have a fixed
price (sometimes negotiable) and no end date. Items disappear when
the seller marks them as sold, the buyer completes a transaction, or
the item is deleted.

Key technical facts used by this parser:

* The site is a client-side React SPA.  Static HTML scraping yields
  nothing useful; data is fetched via internal JSON APIs.
* **Search API**: ``GET /api/v2/catalog/items?search_text=QUERY``
  returns a JSON payload with an ``"items"`` array.
* **Item details API**: ``GET /api/v2/items/{id}/details``
  returns a JSON payload with an ``"item"`` object.
* Anti-bot protection (Cloudflare / Datadome-class) requires a
  session cookie obtained by visiting the homepage first
  (``http_warm_up`` in transport config).  The HTTP transport's
  ``curl_cffi`` with Chrome TLS impersonation handles this.
* Prices are in the seller's local currency (usually EUR).
* Condition is indicated by ``status_id``:
  6 = new with tags, 1 = new without tags, 2 = very good,
  3 = good, 4 = satisfactory.
* ``is_closed`` on the item detail indicates sold / removed.
* Vinted uses many regional domains (vinted.fr, vinted.de, etc.)
  configured via ``preferred_domain`` in the website config.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode, urlparse

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

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_DEFAULT_DOMAIN = "www.vinted.fr"
_ITEMS_PER_PAGE = 96

# Vinted condition status_id → normalised condition key.
_CONDITION_MAP: dict[int, str] = {
  6: "new",
  1: "like_new",
  2: "very_good",
  3: "good",
  4: "fair",
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class VintedParser(Parser):
  """Parser for Vinted marketplace listings.

  Vinted is a fixed-price peer-to-peer marketplace.  There are no
  auctions, no bids, and no end times.  Listings are considered
  active until the item is sold or removed.  The monitoring strategy
  is ``snapshot``: periodic checks to detect status changes.
  """

  @property
  def website_name(self) -> str:
    return "vinted"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=True,
      has_watcher_count=True,
      has_view_count=True,
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
    """Build a Vinted catalog search API URL.

    The search endpoint returns JSON.  Optional kwargs:
      domain: Regional domain (e.g. "vinted.fr").
      page: Page number (1-indexed, default 1).
    """
    domain = kwargs.get("domain") or _DEFAULT_DOMAIN
    if not domain.startswith("www."):
      domain = f"www.{domain}"
    params: dict[str, str | int] = {
      "search_text": query,
      "per_page": _ITEMS_PER_PAGE,
      "order": "newest_first",
    }
    page = kwargs.get("page")
    if page and int(page) > 1:
      params["page"] = int(page)
    return f"https://{domain}/api/v2/catalog/items?{urlencode(params)}"

  def build_fetch_url(self, url: str) -> str:
    """Rewrite a public item URL to the API details endpoint.

    Public URLs look like ``/items/123456-some-title``.
    The API endpoint is ``/api/v2/items/123456/details``.
    If the URL is already an API URL it is returned unchanged.
    """
    if "/api/v2/" in url:
      return url
    match = re.search(r"/items/(\d+)", url)
    if match:
      item_id = match.group(1)
      parsed = urlparse(url)
      return f"https://{parsed.netloc}/api/v2/items/{item_id}/details"
    return url

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/items/(\d+)", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    data = _parse_json(html)
    items = data.get("items")
    if items is None:
      raise ValueError(
        "Vinted search response does not contain 'items' key"
      )

    domain = _domain_from_url(url)
    results: list[ScrapedSearchResult] = []
    for item in items:
      result = _item_to_search_result(item, domain)
      if result is not None:
        results.append(result)

    logger.info(
      "Vinted search: parsed %d results from %d items",
      len(results), len(items),
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    data = _parse_json(html)
    item = data.get("item")
    if item is None:
      raise ValueError(
        "Vinted item details response does not contain 'item' key"
      )
    return _parse_item_detail(item, url)


# ------------------------------------------------------------------
# JSON parsing
# ------------------------------------------------------------------


def _parse_json(text: str) -> dict:
  """Parse a JSON response body.

  Raises ValueError with a helpful message if parsing fails.
  """
  text = text.strip()
  if not text:
    raise ValueError("Empty response body")
  try:
    return json.loads(text)
  except json.JSONDecodeError as error:
    # Detect common anti-bot HTML responses.
    if "<html" in text[:500].lower():
      raise ValueError(
        "Received HTML instead of JSON — likely blocked by anti-bot protection"
      ) from error
    raise ValueError(f"Failed to parse JSON: {error}") from error


# ------------------------------------------------------------------
# Search result parsing
# ------------------------------------------------------------------


def _item_to_search_result(
  item: dict, domain: str,
) -> ScrapedSearchResult | None:
  """Convert a search-result item dict to a ScrapedSearchResult."""
  item_id = item.get("id")
  if item_id is None:
    return None

  title = item.get("title", "")

  item_url = item.get("url", "")
  if item_url and not item_url.startswith("http"):
    item_url = f"https://{domain}{item_url}"
  if not item_url:
    item_url = f"https://{domain}/items/{item_id}"

  price = _extract_price(item.get("price"))
  currency = _extract_currency(item.get("price"))

  photo = item.get("photo") or {}
  image_url = photo.get("url")

  return ScrapedSearchResult(
    external_id=str(item_id),
    url=item_url,
    title=title,
    current_price=price,
    currency=currency,
    listing_type="buy_now",
    image_url=image_url,
  )


# ------------------------------------------------------------------
# Full listing parsing
# ------------------------------------------------------------------


def _parse_item_detail(item: dict, url: str) -> ScrapedListing:
  """Parse a full item details JSON object into a ScrapedListing."""
  item_id = str(item.get("id", ""))
  title = item.get("title", "")
  description = item.get("description") or None

  item_url = item.get("url", "") or url
  if item_url and not item_url.startswith("http"):
    domain = _domain_from_url(url)
    item_url = f"https://{domain}{item_url}"

  price = _extract_price(item.get("price"))
  currency = _extract_currency(item.get("price"))
  total_price = _extract_price(item.get("total_item_price"))

  # Shipping cost derived from total minus item price.
  shipping_cost = None
  if total_price is not None and price is not None and total_price > price:
    shipping_cost = total_price - price

  # Status.
  status = _derive_status(item)

  # Condition.
  condition = _extract_condition(item)

  # Seller.
  seller = _extract_seller(item)

  # Images.
  image_urls = _extract_image_urls(item)

  # Counters.
  favourite_count = _safe_int(item.get("favourite_count"))
  view_count = _safe_int(item.get("view_count"))

  # Attributes.
  attributes = _build_attributes(item)

  return ScrapedListing(
    external_id=item_id,
    url=item_url,
    title=title,
    description=description,
    listing_type="buy_now",
    condition=condition,
    currency=currency,
    buy_now_price=price,
    current_price=price,
    shipping_cost=shipping_cost,
    status=status,
    watcher_count=favourite_count,
    view_count=view_count,
    image_urls=image_urls,
    seller=seller,
    attributes=attributes,
  )


# ------------------------------------------------------------------
# Field extraction helpers
# ------------------------------------------------------------------


def _extract_price(price_data: dict | str | None) -> Decimal | None:
  """Extract price from the Vinted price object or string."""
  if price_data is None:
    return None
  if isinstance(price_data, str):
    return _decimal_or_none(price_data)
  if isinstance(price_data, dict):
    amount = price_data.get("amount")
    return _decimal_or_none(amount)
  return None


def _extract_currency(price_data: dict | str | None) -> str:
  """Extract currency code from the Vinted price object."""
  if isinstance(price_data, dict):
    return price_data.get("currency_code", "EUR")
  return "EUR"


def _derive_status(item: dict) -> str:
  """Derive listing status from the item dict."""
  if item.get("is_closed"):
    return "sold"
  # Vinted uses "can_buy" to indicate if the item is still purchasable.
  can_buy = item.get("can_buy")
  if can_buy is False:
    return "sold"
  return "active"


def _extract_condition(item: dict) -> str | None:
  """Extract condition from the Vinted status_id field."""
  status_id = item.get("status_id")
  if isinstance(status_id, int):
    return _CONDITION_MAP.get(status_id)
  return None


def _extract_seller(item: dict) -> ScrapedSeller | None:
  """Extract seller information from the user field."""
  user = item.get("user")
  if not user:
    return None

  user_id = user.get("id")
  login = user.get("login", "")
  if not user_id and not login:
    return None

  # Rating: Vinted gives a star rating (0-5).  Normalise to 0-100.
  rating = None
  feedback_reputation = user.get("feedback_reputation")
  if feedback_reputation is not None:
    with contextlib.suppress(ValueError, TypeError):
      rating = round(float(feedback_reputation) * 20, 1)

  feedback_count = _safe_int(user.get("feedback_count"))

  country_code = (user.get("country_iso_code") or "").upper() or None

  profile_url = user.get("profile_url")
  if not profile_url and user_id:
    domain = _DEFAULT_DOMAIN
    profile_url = f"https://{domain}/member/{user_id}"

  return ScrapedSeller(
    external_id=str(user_id or login),
    username=login,
    display_name=login,
    country=country_code,
    rating=rating,
    feedback_count=feedback_count,
    profile_url=profile_url,
  )


def _extract_image_urls(item: dict) -> list[str]:
  """Extract image URLs from the photos array."""
  photos = item.get("photos") or []
  urls: list[str] = []
  for photo in photos:
    if not isinstance(photo, dict):
      continue
    # Prefer full_size_url, fall back to url.
    photo_url = photo.get("full_size_url") or photo.get("url")
    if photo_url:
      urls.append(photo_url)
  return urls


def _build_attributes(item: dict) -> dict[str, str]:
  """Build the attributes dict from various item fields."""
  attributes: dict[str, str] = {}

  brand_title = item.get("brand_title") or ""
  if not brand_title:
    brand_dto = item.get("brand_dto") or {}
    brand_title = brand_dto.get("title", "")
  if brand_title:
    attributes["brand"] = brand_title

  size_title = item.get("size_title", "")
  if not size_title:
    # DetailedItem stores size in plugins.
    for plugin in item.get("plugins", []):
      if plugin.get("name") == "attributes":
        for attr in plugin.get("data", {}).get("attributes", []):
          if attr.get("code") == "size":
            size_title = str(attr.get("data", {}).get("value", ""))
            break
  if size_title:
    attributes["size"] = size_title

  color = item.get("color1", "")
  if color:
    attributes["color"] = color
  color2 = item.get("color2", "")
  if color2:
    attributes["color2"] = color2

  catalog_id = item.get("catalog_id")
  if catalog_id:
    attributes["catalog_id"] = str(catalog_id)

  material = item.get("material")
  if material:
    attributes["material"] = str(material)

  return attributes


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _domain_from_url(url: str) -> str:
  """Extract the domain from a URL, falling back to the default."""
  if url:
    parsed = urlparse(url)
    if parsed.netloc:
      return parsed.netloc
  return _DEFAULT_DOMAIN


def _decimal_or_none(value: str | int | float | None) -> Decimal | None:
  """Convert a value to Decimal.  Returns None on failure."""
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None


def _safe_int(value: int | str | None) -> int | None:
  """Convert a value to int.  Returns None on failure."""
  if value is None:
    return None
  if isinstance(value, int):
    return value
  try:
    return int(value)
  except (ValueError, TypeError):
    return None
