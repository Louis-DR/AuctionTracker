"""Kleinanzeigen parser.

Kleinanzeigen (kleinanzeigen.de, formerly eBay Kleinanzeigen) is
Germany's largest classifieds marketplace.  It is a fixed-price-only
platform — there are no auctions.  Listings may be free
(``Zu verschenken``), negotiable (``VB``), on request, or at a fixed
price.

Key technical facts used by this parser:

* The site is server-rendered HTML — no SPA framework.
* No JSON-LD ``Product`` schema is present on listing pages. The
  structured ad metadata is embedded in a ``window.BelenConf`` JS
  object (ad_id, ad_price, ad_price_type).
* **Search URL**: ``GET /s-QUERY/k0`` returns 25 results per page.
  Pagination via ``/s-seite:N/QUERY/k0``.
* **Listing URL**: ``/s-anzeige/SLUG/NUMERIC_ID-CAT-LOC``
* The external ID is the leading numeric portion of the last URL
  segment (before the first ``-``).
* All prices are in EUR, formatted with dots as thousands separators
  (German locale).
* Seller info is available: username, private/commercial type, user
  ID, profile URL, "Aktiv seit" date, and reputation badges.
* Images are hosted on ``img.kleinanzeigen.de``.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

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

_BASE_URL = "https://www.kleinanzeigen.de"
_DEFAULT_CURRENCY = "EUR"
_RESULTS_PER_PAGE = 25


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class KleinanzeigenParser(Parser):
  """Parser for kleinanzeigen.de classified listings.

  Kleinanzeigen is a classifieds-only platform.  All listings are
  fixed-price (with optional negotiation flag ``VB``).  There are
  no auctions, bids, or end times.  The monitoring strategy is
  ``snapshot``: periodic status checks.
  """

  @property
  def website_name(self) -> str:
    return "kleinanzeigen"

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
    encoded_query = quote(query, safe="")
    if page > 1:
      return f"{_BASE_URL}/s-seite:{page}/{encoded_query}/k0"
    return f"{_BASE_URL}/s-{encoded_query}/k0"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/(\d+)-\d+-\d+(?:\?|$|#)", url)
    if match:
      return match.group(1)
    # Fallback: trailing numeric ID segment.
    match = re.search(r"/(\d{6,})", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)
    results: list[ScrapedSearchResult] = []

    for card in tree.css("article.aditem"):
      result = _parse_search_card(card)
      if result is not None:
        results.append(result)

    logger.info(
      "Kleinanzeigen search: parsed %d results", len(results),
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)

    # Title from <h1>.
    title_node = tree.css_first("h1#viewad-title")
    if title_node is None:
      raise ValueError("No listing title found on page")
    title = title_node.text(strip=True)

    # External ID and structured metadata from BelenConf.
    belen = _extract_belen_conf(html)
    external_id = belen.get("ad_id", "")
    if not external_id:
      # Fallback to the ad ID box in the sidebar.
      external_id = _extract_ad_id_from_sidebar(tree)

    # Price.
    price, price_type = _extract_price(tree, belen)

    # Description.
    description = _extract_description(tree)

    # Location.
    location = _extract_location(tree)

    # Posting date.
    posting_date = _extract_posting_date(tree)

    # Seller.
    seller = _extract_seller(tree)

    # Images.
    image_urls = _extract_image_urls(tree)

    # Shipping.
    shipping_cost = _extract_shipping_cost(tree)
    shipping_available = _shipping_available(tree)

    # Category-specific attributes.
    attributes = _extract_attributes(tree)
    if price_type:
      attributes["price_type"] = price_type
    if location:
      attributes["location"] = location
    if posting_date:
      attributes["posting_date"] = posting_date

    listing_url = url or ""

    return ScrapedListing(
      external_id=external_id,
      url=listing_url,
      title=title,
      description=description,
      listing_type="buy_now",
      currency=_DEFAULT_CURRENCY,
      current_price=price,
      buy_now_price=price,
      shipping_cost=shipping_cost,
      ships_internationally=None,
      shipping_from_country="DE" if shipping_available else None,
      status="active",
      image_urls=image_urls,
      seller=seller,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# Search result card parsing
# ------------------------------------------------------------------


def _parse_search_card(card) -> ScrapedSearchResult | None:
  """Parse a single ``article.aditem`` element from the search page."""
  # Ad ID from data attribute.
  external_id = card.attributes.get("data-adid", "")
  if not external_id:
    return None

  # URL from data-href attribute.
  href = card.attributes.get("data-href", "")
  if not href:
    # Fallback: find the title link.
    title_link = card.css_first("a.ellipsis")
    if title_link:
      href = title_link.attributes.get("href", "")
  if not href:
    return None
  item_url = href if href.startswith("http") else f"{_BASE_URL}{href}"

  # Title from the link text.
  title = ""
  title_link = card.css_first("a.ellipsis")
  if title_link:
    title = title_link.text(strip=True)
  if not title:
    return None

  # Image URL from the thumbnail <img>.
  image_url = None
  img_tag = card.css_first("div.imagebox img")
  if img_tag:
    image_url = img_tag.attributes.get("src")
    # Prefer the srcset (higher resolution).
    srcset = img_tag.attributes.get("srcset")
    if srcset:
      image_url = srcset.strip()

  # Price from the price element.
  price = None
  price_node = card.css_first("p.aditem-main--middle--price-shipping--price")
  if price_node:
    price = _parse_german_price(price_node.text(strip=True))

  # Location from the top-left area.
  location = None
  loc_node = card.css_first("div.aditem-main--top--left")
  if loc_node:
    location = loc_node.text(strip=True)

  # Posting date from top-right area.
  date_text = None
  date_node = card.css_first("div.aditem-main--top--right")
  if date_node:
    date_text = date_node.text(strip=True)

  return ScrapedSearchResult(
    external_id=external_id,
    url=item_url,
    title=title,
    current_price=price,
    currency=_DEFAULT_CURRENCY,
    listing_type="buy_now",
    image_url=image_url,
  )


# ------------------------------------------------------------------
# BelenConf extraction (JS-embedded metadata)
# ------------------------------------------------------------------


def _extract_belen_conf(html: str) -> dict:
  """Extract ad metadata from the ``window.BelenConf`` JS object.

  Returns a flat dict with keys like ``ad_id``, ``ad_price``,
  ``ad_price_type``.  Returns an empty dict on failure.
  """
  result: dict = {}
  ad_id = re.search(r'"ad_id"\s*:\s*"(\d+)"', html)
  if ad_id:
    result["ad_id"] = ad_id.group(1)
  ad_price = re.search(r'"ad_price"\s*:\s*"([\d.]+)"', html)
  if ad_price:
    result["ad_price"] = ad_price.group(1)
  ad_price_type = re.search(r'"ad_price_type"\s*:\s*"(\w+)"', html)
  if ad_price_type:
    result["ad_price_type"] = ad_price_type.group(1)
  return result


# ------------------------------------------------------------------
# Listing detail helpers
# ------------------------------------------------------------------


def _extract_ad_id_from_sidebar(tree: HTMLParser) -> str:
  """Extract the ad ID from the ``#viewad-ad-id-box`` sidebar."""
  box = tree.css_first("#viewad-ad-id-box")
  if box:
    items = box.css("li")
    if len(items) >= 2:
      return items[1].text(strip=True)
  return ""


def _extract_price(tree: HTMLParser, belen: dict) -> tuple[Decimal | None, str | None]:
  """Extract price and price type from the listing page.

  Tries ``window.BelenConf`` first (precise numeric value), then
  falls back to the visible ``#viewad-price`` element.
  """
  # BelenConf gives us a clean numeric value and type.
  belen_price = _decimal_or_none(belen.get("ad_price"))
  belen_type = belen.get("ad_price_type")
  if belen_price is not None:
    return belen_price, belen_type

  # Fallback: parse from the rendered HTML.
  price_node = tree.css_first("#viewad-price")
  if price_node is None:
    return None, None

  text = price_node.text(strip=True)
  price_type = _classify_price_text(text)
  if price_type in ("FREE", "ON_REQUEST"):
    return Decimal("0") if price_type == "FREE" else None, price_type
  price = _parse_german_price(text)
  return price, price_type


def _classify_price_text(text: str) -> str | None:
  """Classify the price type from the visible price text."""
  lower = text.lower()
  if "zu verschenken" in lower:
    return "FREE"
  if "vb" in lower or "verhandlungsbasis" in lower:
    return "NEGOTIABLE"
  if "auf anfrage" in lower:
    return "ON_REQUEST"
  if re.search(r"\d", text):
    return "FIXED"
  return None


def _extract_description(tree: HTMLParser) -> str | None:
  """Extract the full description text."""
  desc_node = tree.css_first("#viewad-description-text")
  if desc_node is None:
    return None
  # The description uses <br/> for newlines.
  text = desc_node.text(strip=True)
  return text if text else None


def _extract_location(tree: HTMLParser) -> str | None:
  """Extract the location string (postal code + city)."""
  loc_node = tree.css_first("#viewad-locality")
  if loc_node is None:
    return None
  text = loc_node.text(strip=True)
  return text if text else None


def _extract_posting_date(tree: HTMLParser) -> str | None:
  """Extract the posting date as a ``DD.MM.YYYY`` string."""
  extra = tree.css_first("#viewad-extra-info")
  if extra is None:
    return None
  match = re.search(r"(\d{2}\.\d{2}\.\d{4})", extra.text())
  return match.group(1) if match else None


def _extract_seller(tree: HTMLParser) -> ScrapedSeller | None:
  """Extract seller information from the listing page."""
  contact = tree.css_first("#viewad-contact")
  if contact is None:
    return None

  # Seller name and profile URL from the profile link.
  # The badge link also contains userId= but only has the initial letter.
  # The actual name link is inside span.userprofile-vip.
  username = None
  user_id = None
  profile_url = None
  name_link = contact.css_first("span.userprofile-vip a[href*='userId=']")
  if name_link:
    username = name_link.text(strip=True)
    href = name_link.attributes.get("href", "")
    profile_url = href if href.startswith("http") else f"{_BASE_URL}{href}"
    user_id_match = re.search(r"userId=(\d+)", href)
    if user_id_match:
      user_id = user_id_match.group(1)

  if not user_id and not username:
    return None

  # Seller type (private or commercial).
  seller_type = None
  for detail in contact.css("span.userprofile-vip-details-text"):
    text = detail.text(strip=True)
    if "Privater" in text:
      seller_type = "PRIVATE"
    elif "Gewerblicher" in text:
      seller_type = "COMMERCIAL"

  # Active since date.
  member_since = None
  for detail in contact.css("span.userprofile-vip-details-text"):
    text = detail.text(strip=True)
    since_match = re.search(r"Aktiv seit\s+(\d{2}\.\d{2}\.\d{4})", text)
    if since_match:
      try:
        member_since = datetime.strptime(
          since_match.group(1), "%d.%m.%Y",
        ).date()
      except ValueError:
        pass

  # Reputation badges (rough rating).
  rating = _extract_seller_rating(contact)

  external_id = user_id or username or ""

  return ScrapedSeller(
    external_id=external_id,
    username=username or external_id,
    display_name=username,
    country="DE",
    rating=rating,
    member_since=member_since,
    profile_url=profile_url,
  )


def _extract_seller_rating(contact_node) -> float | None:
  """Derive a rating from Kleinanzeigen reputation badges.

  Kleinanzeigen shows text badges rather than numeric ratings. We
  map them to a coarse 0-100 scale:
    TOP Zufriedenheit -> 90
    Zufriedenheit     -> 70
    Freundlichkeit    -> +10 bonus
  """
  text = contact_node.text()
  score = None
  if "TOP\xa0Zufriedenheit" in text or "TOP Zufriedenheit" in text:
    score = 90.0
  elif "Zufriedenheit" in text:
    score = 70.0
  if score is not None and "Freundlichkeit" in text:
    score += 10.0
  return score


def _extract_shipping_cost(tree: HTMLParser) -> Decimal | None:
  """Extract the shipping cost from ``Versand ab X,XX €``."""
  price_area = tree.css_first("#viewad-price")
  if price_area is None:
    return None
  # Look at the parent container for shipping info.
  parent = price_area.parent
  if parent is None:
    return None
  text = parent.text()
  match = re.search(r"Versand\s+ab\s+([\d.,]+)\s*€", text)
  if match:
    return _parse_german_price(match.group(1) + " €")
  return None


def _shipping_available(tree: HTMLParser) -> bool:
  """Check whether shipping is offered."""
  body_text = tree.body.text() if tree.body else ""
  return "Versand" in body_text


def _extract_image_urls(tree: HTMLParser) -> list[str]:
  """Extract listing image URLs from the page.

  Images are hosted on ``img.kleinanzeigen.de/api/v1/prod-ads/images/``.
  We de-duplicate and strip query parameters to get the base URL.
  """
  seen: set[str] = set()
  urls: list[str] = []

  for node in tree.css("img[src*='img.kleinanzeigen.de']"):
    src = node.attributes.get("src", "")
    if "/prod-ads/images/" not in src:
      continue
    clean = re.sub(r"\?.*$", "", src)
    if clean not in seen:
      seen.add(clean)
      urls.append(clean)

  return urls


def _extract_attributes(tree: HTMLParser) -> dict[str, str]:
  """Extract category-specific attributes from the details list."""
  attributes: dict[str, str] = {}
  details = tree.css_first("#viewad-details")
  if details is None:
    return attributes

  for item in details.css("li.addetailslist--detail"):
    text = item.text(strip=True)
    value_node = item.css_first("span.addetailslist--detail--value")
    if value_node:
      key = text.replace(value_node.text(strip=True), "").strip()
      value = value_node.text(strip=True)
      if key and value:
        attributes[key] = value

  return attributes


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _parse_german_price(text: str) -> Decimal | None:
  """Parse a German-format price string like ``6.500 € VB`` into Decimal.

  Handles thousands separators (dots), comma decimals, and strips
  currency symbols and negotiation markers.
  """
  if not text:
    return None
  # Remove currency symbol, VB, whitespace, and other non-numeric noise.
  cleaned = re.sub(r"[€VB\s]", "", text.strip(), flags=re.IGNORECASE)
  # Remove other alphabetic text (e.g., "Zu verschenken").
  cleaned = re.sub(r"[a-zA-ZäöüÄÖÜß]+", "", cleaned).strip()
  if not cleaned:
    return None
  # German format: dots as thousands separators, comma as decimal.
  cleaned = cleaned.replace(".", "").replace(",", ".")
  return _decimal_or_none(cleaned)


def _decimal_or_none(value: str | int | float | None) -> Decimal | None:
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None
