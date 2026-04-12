"""Todocoleccion parser.

Todocoleccion (todocoleccion.net) is Spain's largest online marketplace
for antiques, art, books, and collectibles. It supports both auctions
(``subasta``) and fixed-price listings (``venta directa``), some of
which accept offers (``admite ofertas``).

Key technical facts used by this parser:

* The site is server-rendered HTML — no SPA framework, no internal
  JSON API. All data is extracted from the HTML response.
* Listing detail pages embed Schema.org JSON-LD ``Product`` data with
  ``sku``, ``name``, ``description``, and ``offers`` (price, currency,
  availability).
* Additional listing data (condition, seller, bids, end time, images)
  is extracted from the HTML using ``selectolax``.
* **Search URL**: ``GET /buscador?bu=QUERY`` returns paginated results
  (30 per page). Pagination parameter is ``P`` (1-indexed).
* **Listing URL**: ``/{category_slug}/{title_slug}~x{numeric_id}``
* The external ID is the numeric suffix after ``~x``.
* All prices are in EUR.
* Seller ratings use 1-5 stars, normalised to 0-100.
* Seller profile URL is at ``/usuario/valoraciones/{username}/vendedor``.
* Auction end times are displayed in Spanish:
  ``"15 de abril de 2026 18:00:00 CEST"``.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, urlencode

from selectolax.parser import HTMLParser

from auction_tracker.parsing.base import (
  Parser,
  ParserCapabilities,
  ParserRegistry,
  check_html_for_blocking,
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

_BASE_URL = "https://www.todocoleccion.net"
_SEARCH_PATH = "/buscador"
_ITEMS_PER_PAGE = 30
_DEFAULT_CURRENCY = "EUR"

_SPANISH_MONTHS: dict[str, int] = {
  "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
  "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
  "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# CEST / CET offset from UTC (Spain uses CET in winter, CEST in summer).
_SPAIN_OFFSETS: dict[str, timezone] = {
  "CEST": timezone(timedelta(hours=2)),
  "CET": timezone(timedelta(hours=1)),
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class TodocoleccionParser(Parser):
  """Parser for todocoleccion.net listings.

  Todocoleccion has both timed auctions and fixed-price items.
  Auctions expire with a 3-minute extension rule (any bid in the
  last 3 minutes extends the auction).  Fixed-price items stay
  indefinitely until sold or removed.  The monitoring strategy is
  ``snapshot``: periodic status checks.
  """

  @property
  def website_name(self) -> str:
    return "todocoleccion"

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
      has_estimates=False,
      has_reserve_price=False,
      has_lot_numbers=False,
      has_auction_house_info=False,
    )

  # ----------------------------------------------------------------
  # URL helpers
  # ----------------------------------------------------------------

  def build_search_url(self, query: str, **kwargs) -> str:
    page = int(kwargs.get("page", 1))
    params: dict[str, str | int] = {"bu": query}
    if page > 1:
      params["P"] = page
    return f"{_BASE_URL}{_SEARCH_PATH}?{urlencode(params, quote_via=quote_plus)}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"~x(\d+)", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)
    results: list[ScrapedSearchResult] = []

    for card in tree.css("div.card-lote"):
      result = _parse_search_card(card)
      if result is not None:
        results.append(result)

    logger.info(
      "Todocoleccion search: parsed %d results", len(results),
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)

    # Primary data from JSON-LD Product schema.
    product = _extract_jsonld_product(html)
    if product is None:
      raise ValueError("No JSON-LD Product found on page")

    external_id = str(product.get("sku", ""))
    title = product.get("name", "")
    description = product.get("description") or None

    listing_url = product.get("url", "") or url
    offers = product.get("offers") or {}
    price = _decimal_or_none(offers.get("price"))
    currency = offers.get("priceCurrency", _DEFAULT_CURRENCY)

    # Listing type and status from the HTML.
    listing_type, bid_count, starting_price = _extract_auction_info(tree)
    end_time = _extract_end_time(tree)
    status = _derive_status(tree, offers)
    condition = _extract_condition(tree)
    shipping_cost = _extract_shipping_cost(tree)
    seller = _extract_seller(tree)
    image_urls = _extract_image_urls(tree, external_id)

    # For auctions, the JSON-LD price is the current bid or starting
    # price.  For buy-now items, it is the fixed price.
    current_price = price
    buy_now_price = price if listing_type == "buy_now" else None

    return ScrapedListing(
      external_id=external_id,
      url=listing_url,
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency=currency,
      starting_price=starting_price,
      buy_now_price=buy_now_price,
      current_price=current_price,
      shipping_cost=shipping_cost,
      end_time=end_time,
      status=status,
      bid_count=bid_count,
      image_urls=image_urls,
      seller=seller,
    )


# ------------------------------------------------------------------
# Search result card parsing
# ------------------------------------------------------------------


def _parse_search_card(card) -> ScrapedSearchResult | None:
  """Parse a single ``div.card-lote`` element from the search page."""
  # Title and URL from the lot title link.
  title_link = card.css_first("a[id^='lot-title-']")
  if title_link is None:
    return None

  href = title_link.attributes.get("href", "")
  title = title_link.text(strip=True)
  external_id = title_link.attributes.get("data-id-lote", "")
  if not external_id:
    id_match = re.search(r"~x(\d+)", href)
    external_id = id_match.group(1) if id_match else ""
  if not external_id:
    return None

  item_url = href if href.startswith("http") else f"{_BASE_URL}{href}"

  # Image URL from the data attribute on the title link.
  image_url = title_link.attributes.get("data-image-url")
  if not image_url:
    img_tag = card.css_first("img.card-lote-main-image")
    if img_tag:
      image_url = img_tag.attributes.get("src")

  # Price from `span.card-price`.
  price = None
  price_node = card.css_first("span.card-price")
  if price_node:
    price = _parse_euro_price(price_node.text(strip=True))

  # Listing type: auction if "pujas" is present, otherwise buy_now.
  listing_type = "buy_now"
  bid_count = None
  full_text = card.text()
  bid_match = re.search(r"(\d+)\s*pujas?", full_text)
  if bid_match:
    listing_type = "auction"
    bid_count = int(bid_match.group(1))

  return ScrapedSearchResult(
    external_id=external_id,
    url=item_url,
    title=title,
    current_price=price,
    currency=_DEFAULT_CURRENCY,
    listing_type=listing_type,
    image_url=image_url,
    bid_count=bid_count,
  )


# ------------------------------------------------------------------
# JSON-LD extraction
# ------------------------------------------------------------------


def _extract_jsonld_product(html: str) -> dict | None:
  """Find the JSON-LD Product block in the HTML."""
  blocks = re.findall(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    html, re.DOTALL,
  )
  for block in blocks:
    try:
      data = json.loads(block)
    except json.JSONDecodeError:
      continue
    if isinstance(data, dict) and data.get("@type") == "Product":
      return data
  return None


# ------------------------------------------------------------------
# Listing detail helpers
# ------------------------------------------------------------------


def _extract_auction_info(tree: HTMLParser) -> tuple[str, int | None, Decimal | None]:
  """Determine listing type, bid count, and starting price."""
  body_text = tree.body.text() if tree.body else ""

  # Bid count.
  bid_match = re.search(r"(\d+)\s*pujas?", body_text)
  bid_count = int(bid_match.group(1)) if bid_match else None

  # Starting price (auctions show "Precio de salida: X,XX €").
  starting_price = None
  starting_match = re.search(
    r"Precio de salida[^€]*?([\d.,]+)\s*€", body_text,
  )
  if starting_match:
    starting_price = _parse_euro_price(starting_match.group(1))

  # If we found bids or a starting price, it is an auction.
  if bid_count is not None or starting_price is not None:
    return "auction", bid_count, starting_price

  # "Admite ofertas" suggests buy-now with negotiation.
  if "Admite ofertas" in body_text:
    return "buy_now", None, None

  return "buy_now", None, None


def _extract_end_time(tree: HTMLParser) -> datetime | None:
  """Extract the auction end datetime from the listing HTML.

  Looks for the pattern ``Finaliza: 15 de abril de 2026 18:00:00 CEST``.
  """
  body_text = tree.body.text() if tree.body else ""
  match = re.search(
    r"Finaliza:\s*(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})\s+"
    r"(\d{1,2}:\d{2}:\d{2})\s*(\w+)?",
    body_text,
  )
  if not match:
    return None

  day = int(match.group(1))
  month_name = match.group(2).lower()
  year = int(match.group(3))
  time_str = match.group(4)
  tz_abbr = (match.group(5) or "").upper()

  month = _SPANISH_MONTHS.get(month_name)
  if month is None:
    return None

  hour, minute, second = (int(part) for part in time_str.split(":"))
  tz_info = _SPAIN_OFFSETS.get(tz_abbr)
  return datetime(year, month, day, hour, minute, second, tzinfo=tz_info)


def _derive_status(tree: HTMLParser, offers: dict) -> str:
  """Derive the listing status from availability and HTML cues."""
  availability = offers.get("availability", "")
  if "InStock" in availability:
    return "active"

  body_text = tree.body.text() if tree.body else ""
  if "Vendido" in body_text or "vendido" in body_text:
    return "sold"

  return "active"


def _extract_condition(tree: HTMLParser) -> str | None:
  """Extract condition from ``Estado del lote`` text."""
  body_text = tree.body.text() if tree.body else ""
  match = re.search(r"Estado del lote:\s*(.+?)(?:\n|$)", body_text)
  if match:
    return match.group(1).strip()

  # Selectolax: look for the span after the condition label.
  for node in tree.css("span"):
    text = node.text(strip=True)
    if text.startswith("Estado del lote"):
      # The condition text is usually in the next sibling or further.
      remainder = text.replace("Estado del lote", "").strip(": ")
      if remainder:
        return remainder
  return None


def _extract_shipping_cost(tree: HTMLParser) -> Decimal | None:
  """Extract shipping cost from ``Envío desde X,XX€``."""
  body_text = tree.body.text() if tree.body else ""
  match = re.search(r"[Ee]nv[ií]o\s+desde\s+([\d.,]+)", body_text)
  if match:
    return _parse_euro_price(match.group(1))
  return None


def _extract_seller(tree: HTMLParser) -> ScrapedSeller | None:
  """Extract seller information from the listing page."""
  # Seller profile URL and username from the obfuscated link.
  username = None
  profile_url = None
  for button in tree.css("button[data-href]"):
    href = button.attributes.get("data-href", "")
    if "/usuario/valoraciones/" in href:
      profile_url = f"{_BASE_URL}{href}"
      # Extract username from ``/usuario/valoraciones/{username}/vendedor``.
      parts = href.strip("/").split("/")
      if len(parts) >= 3:
        username = parts[2]
      break

  if not username:
    return None

  # Star rating: count ``bi-star-fill`` icons inside the button.
  star_count = 0
  rating = None
  for button in tree.css("button[data-href]"):
    href = button.attributes.get("data-href", "")
    if "/usuario/valoraciones/" in href:
      stars = button.css("i.bi-star-fill")
      star_count = len(stars)
      if star_count > 0:
        rating = round(star_count * 20.0, 1)

      # Also try to get the exact rating from the title attribute.
      title_attr = button.attributes.get("title", "")
      star_match = re.search(r"(\d+(?:[.,]\d+)?)\s*estrellas?", title_attr)
      if star_match:
        raw_stars = float(star_match.group(1).replace(",", "."))
        rating = round(raw_stars * 20.0, 1)
      break

  # Feedback count from ``(N)`` pattern in the rating button.
  feedback_count = None
  for small_tag in tree.css("button[data-href] small"):
    text = small_tag.text(strip=True)
    count_match = re.search(r"\(([\d.]+)\)", text)
    if count_match:
      feedback_count = int(count_match.group(1).replace(".", ""))
      break

  # Member since date (``Desde DD/MM/YYYY``).
  member_since = None
  body_text = tree.body.text() if tree.body else ""
  desde_match = re.search(r"Desde\s+(\d{2}/\d{2}/\d{4})", body_text)
  if desde_match:
    try:
      member_since = datetime.strptime(
        desde_match.group(1), "%d/%m/%Y",
      ).date()
    except ValueError:
      pass

  # Country and city (``España (Madrid)``).
  country = None
  location_match = re.search(r"(Espa[ñn]a)\s*\(([^)]+)\)", body_text)
  if location_match:
    country = "ES"
  else:
    # Some sellers may be from other countries.
    country_match = re.search(
      r"Desde\s+\d{2}/\d{2}/\d{4}\s*\n?\s*(\w[\w\s]*?)(?:\s*\(|$)",
      body_text,
    )
    if country_match:
      country_name = country_match.group(1).strip()
      if country_name in ("España", "Espana"):
        country = "ES"

  return ScrapedSeller(
    external_id=username,
    username=username,
    display_name=username,
    country=country,
    rating=rating,
    feedback_count=feedback_count,
    member_since=member_since,
    profile_url=profile_url,
  )


def _extract_image_urls(tree: HTMLParser, external_id: str) -> list[str]:
  """Extract listing image URLs from the HTML.

  Prefers full-size images (no ``size=`` query parameter) and
  filters out avatar, thumbnail, and unrelated CDN images.
  """
  seen: set[str] = set()
  urls: list[str] = []

  # Pattern: full-size images on cloud*.todocoleccion.online whose
  # path contains the lot ID.
  for node in tree.css("img[src], source[srcset]"):
    src = node.attributes.get("src") or node.attributes.get("srcset") or ""
    if not src or "todocoleccion.online" not in src:
      continue
    # Skip avatars and thumbnails from other listings.
    if "avatar" in src:
      continue
    # Only keep images belonging to this listing (contain the ID).
    if external_id and external_id not in src:
      continue
    # Prefer the base URL without size/crop parameters.
    clean = re.sub(r"\?.*$", "", src)
    if clean not in seen:
      seen.add(clean)
      urls.append(clean)

  return urls


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _parse_euro_price(text: str) -> Decimal | None:
  """Parse a European-format price string like ``1.200,50`` into Decimal."""
  if not text:
    return None
  # Remove currency symbol and whitespace.
  cleaned = re.sub(r"[€\s]", "", text.strip())
  if not cleaned:
    return None
  # European format: dots as thousands separators, comma as decimal.
  cleaned = cleaned.replace(".", "").replace(",", ".")
  return _decimal_or_none(cleaned)


def _decimal_or_none(value: str | int | float | None) -> Decimal | None:
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None
