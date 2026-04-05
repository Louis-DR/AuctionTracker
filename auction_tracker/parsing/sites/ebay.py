"""eBay parser — extracts structured data from eBay HTML pages.

This parser is a pure function: it takes raw HTML and returns Pydantic
models. It never performs HTTP requests.

eBay page structure:
- Search results: ``<ul class="srp-results">`` containing
  ``<li data-listingid=XXXX>`` cards.
- Listing pages: Marko.js SSR with structured data in inline
  ``<script>`` tags containing keys like ``sellerUserName``,
  ``listingId``, ``bidCount``, ``currency``, ``endTime``, etc.
- Bid history pages: ``/bfl/viewbids/{item_id}`` with table rows
  of (bidder, amount, time).
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

from auction_tracker.parsing.base import Parser, ParserBlocked, ParserCapabilities, ParserRegistry
from auction_tracker.parsing.models import (
  ScrapedBid,
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

# eBay regional domains, ordered by anti-bot leniency.
EBAY_DOMAINS = [
  "ebay.fr",
  "ebay.com",
  "ebay.co.uk",
  "ebay.it",
  "ebay.es",
  "ebay.de",
  "ebay.com.au",
  "ebay.ca",
]

# France is the reference market for this tool; ebay.fr is the default
# search and fallback domain. This means listings are fetched in French,
# which prevents German labels (e.g. "Artikelstandort") from appearing
# when the fallback resolver settles on ebay.de.
DEFAULT_DOMAIN = "ebay.fr"

# eBay condition ID to our condition string mapping.
_CONDITION_MAP: dict[str, str] = {
  "1000": "new",
  "1500": "new",
  "2000": "like_new",
  "2500": "like_new",
  "2750": "like_new",
  "3000": "good",
  "4000": "very_good",
  "5000": "good",
  "6000": "fair",
  "7000": "for_parts",
}

_DOMAIN_CURRENCY: dict[str, str] = {
  "ebay.com": "USD",
  "ebay.co.uk": "GBP",
  "ebay.de": "EUR",
  "ebay.fr": "EUR",
  "ebay.it": "EUR",
  "ebay.es": "EUR",
  "ebay.com.au": "AUD",
  "ebay.ca": "CAD",
}

_MONTH_NAMES: dict[str, int] = {
  "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
  "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
  "janv": 1, "févr": 2, "mars": 3, "avr": 4, "mai": 5, "juin": 6,
  "juil": 7, "août": 8, "sept": 9, "déc": 12,
  "mär": 3, "okt": 10, "dez": 12,
  "gen": 1, "mag": 5, "giu": 6, "lug": 7, "ago": 8, "set": 9,
  "ott": 10, "dic": 12,
  "ene": 1, "abr": 4,
  "january": 1, "february": 2, "march": 3, "april": 4,
  "june": 6, "july": 7, "august": 8, "september": 9,
  "october": 10, "november": 11, "december": 12,
}


@ParserRegistry.register
class EbayParser(Parser):
  """Pure parser for eBay search and listing pages."""

  @property
  def website_name(self) -> str:
    return "ebay"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=True,
      has_seller_info=True,
      has_watcher_count=True,
      has_buy_now=True,
      has_reserve_price=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    """Build an eBay search URL.

    Supports kwargs: domain, category, page, sort_order.
    """
    domain = kwargs.get("domain", DEFAULT_DOMAIN)
    category = kwargs.get("category", "0")
    page = kwargs.get("page", 1)

    params = {"_nkw": query, "_sacat": category}
    if page > 1:
      params["_pgn"] = str(page)
    return f"https://www.{domain}/sch/i.html?{urlencode(params)}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/itm/(\d+)", url)
    if match:
      return match.group(1)
    match = re.search(r"^(\d{10,15})$", url)
    if match:
      return match.group(1)
    return None

  @staticmethod
  def fallback_urls_for(url: str) -> list[str]:
    """Return the same URL rewritten for each eBay domain, skipping the current one.

    eBay item IDs are global — any item accessible on ebay.fr is also
    accessible on ebay.com with the same numeric ID. US and UK domains
    tend to be less restrictive about cookie consent / sign-in walls.
    """
    current_domain_match = re.search(r"https?://www\.(ebay\.[^/]+)", url)
    current_domain = current_domain_match.group(1) if current_domain_match else None
    results = []
    for domain in EBAY_DOMAINS:
      if domain == current_domain:
        continue
      rewritten = re.sub(r"https?://www\.ebay\.[^/]+", f"https://www.{domain}", url)
      results.append(rewritten)
    return results

  # ------------------------------------------------------------------
  # Search results parsing
  # ------------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    results: list[ScrapedSearchResult] = []

    if _is_blocked_page(html):
      raise ParserBlocked(
        "eBay returned a blocked/challenge page for search",
        url=url,
        fallback_urls=self.fallback_urls_for(url) if url else [],
      )

    srp_start = html.find("srp-results")
    if srp_start < 0:
      logger.warning("No srp-results container found in eBay search HTML")
      return []

    srp_html = html[srp_start:]

    # Each item is a <li data-listingid=XXXX ...>.
    items = re.split(r"<li[^>]*\bdata-listingid=(\d{10,15})\b", srp_html)

    for index in range(1, len(items) - 1, 2):
      listing_id = items[index]
      card_html = items[index + 1]

      url_match = re.search(
        r'href=(https://www\.ebay\.[^/\s]+/itm/\d+)',
        card_html,
      )
      if not url_match:
        continue
      item_url = url_match.group(1)

      title = _extract_card_title(card_html)
      price = _extract_price_from_text(card_html[:3000])
      currency = _detect_currency(card_html[:3000])
      listing_type = _detect_card_listing_type(card_html)

      image_url = None
      image_match = re.search(
        r'<img[^>]*\bsrc="?(https://i\.ebayimg\.com/[^"\s>]+)',
        card_html,
      )
      if image_match:
        image_url = image_match.group(1)

      bid_count = None
      bid_match = re.search(r"(\d+)\s*(?:bid|enchère|Gebot|offre)", card_html, re.I)
      if bid_match:
        bid_count = int(bid_match.group(1))

      results.append(ScrapedSearchResult(
        external_id=listing_id,
        url=item_url,
        title=title,
        current_price=price,
        currency=currency,
        listing_type=listing_type,
        image_url=image_url,
        bid_count=bid_count,
      ))

    logger.info("Parsed %d eBay search results", len(results))
    return results

  # ------------------------------------------------------------------
  # Listing detail parsing
  # ------------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    if _is_blocked_page(html):
      raise ParserBlocked(
        "eBay returned a blocked/challenge page",
        url=url,
        fallback_urls=self.fallback_urls_for(url) if url else [],
      )

    # Find the Marko.js data script containing listing data.
    data_script = _find_data_script(html)

    item_id = _script_str(data_script, "listingId") or ""
    title = _extract_title(html)
    currency = _script_str(data_script, "currency") or _detect_currency(html)
    current_price = _decimal_or_none(_script_str(data_script, "price"))
    bid_count = _int_or_none(_script_str(data_script, "bidCount")) or 0

    # Dates.
    start_time = _parse_ebay_datetime(
      _script_nested_str(data_script, "startDate", "value")
    )
    end_time = _extract_end_time(html, data_script)

    # Listing type.
    auction_possible = _script_bool(data_script, "auctionPossible")
    immediate_pay = _script_bool(data_script, "immediatePay")
    has_bin = '"binPrice"' in data_script or '"buyItNowPrice"' in data_script
    best_offer = _script_bool(data_script, "bestOfferEnabled")

    if immediate_pay:
      listing_type = "buy_now"
    elif auction_possible and has_bin:
      listing_type = "hybrid"
    elif auction_possible:
      listing_type = "auction"
    else:
      listing_type = "buy_now"

    # Buy It Now price.
    buy_now_price = None
    bin_match = re.search(
      r'"binPrice"\s*:\{[^}]*"value"\s*:\s*"?([0-9.]+)',
      data_script,
    )
    if bin_match:
      buy_now_price = _decimal_or_none(bin_match.group(1))

    # Condition.
    condition_id = _script_str(data_script, "conditionId")
    condition = _CONDITION_MAP.get(condition_id or "", "unknown")

    # Seller.
    seller_username = _script_str(data_script, "sellerUserName") or ""
    seller = None
    if seller_username:
      seller = ScrapedSeller(
        external_id=seller_username,
        username=seller_username,
        display_name=seller_username,
      )

    # Shipping.
    shipping_cost = None
    ship_match = re.search(r'"shippingCost"\s*:\s*"?([0-9.]+)', data_script)
    if ship_match:
      shipping_cost = _decimal_or_none(ship_match.group(1))

    # Location.
    location = _extract_item_location(html, data_script)

    # Description (condition notes + subtitle, not the iframe body).
    description = _extract_description(html)

    # Images.
    image_urls = _extract_image_urls(data_script, html)

    # Watchers.
    watcher_count = None
    watcher_match = re.search(r'"watchCount"\s*:\s*(\d+)', data_script)
    if watcher_match:
      watcher_count = int(watcher_match.group(1))

    # Quantity (for fixed-price listings).
    qty_available = _int_or_none(_script_str(data_script, "quantityAvailable"))
    qty_sold = _int_or_none(_script_str(data_script, "quantitySold"))

    # Status.
    status = _derive_status(
      data_script, bid_count, end_time, listing_type, qty_available,
    )

    # Final price (for ended auctions).
    final_price = None
    if status in ("sold", "unsold"):
      final_price = current_price

    # URL reconstruction.
    url = f"https://www.ebay.com/itm/{item_id}" if item_id else ""

    # Attributes.
    attributes: dict[str, str] = {}
    if condition_id:
      attributes["condition_id"] = condition_id
    if best_offer:
      attributes["best_offer_enabled"] = "true"
    has_reserve = _script_bool(data_script, "hasReservePrice")
    reserve_met = _script_bool(data_script, "reservePriceMet")
    if has_reserve:
      attributes["has_reserve_price"] = "true"
      if reserve_met is not None:
        attributes["reserve_price_met"] = str(reserve_met).lower()
    if qty_available is not None:
      attributes["quantity_available"] = str(qty_available)
    if qty_sold is not None:
      attributes["quantity_sold"] = str(qty_sold)
    if location:
      attributes["item_location"] = location
    _extract_item_specifics(html, attributes)

    return ScrapedListing(
      external_id=item_id,
      url=url,
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency=currency,
      buy_now_price=buy_now_price,
      current_price=current_price,
      final_price=final_price,
      shipping_cost=shipping_cost,
      shipping_from_country=location,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=bid_count,
      watcher_count=watcher_count,
      seller=seller,
      image_urls=image_urls,
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # Bid history parsing
  # ------------------------------------------------------------------

  def parse_bid_history(self, html: str, currency: str = "USD") -> list[ScrapedBid]:
    """Parse an eBay bid history page (``/bfl/viewbids/``)."""
    if _is_blocked_page(html):
      return []
    bids = _parse_bids_from_table(html, currency)
    if not bids:
      bids = _parse_bids_from_regex(html, currency)
    bids.sort(key=lambda bid: bid.amount)
    return bids


# ====================================================================
# Module-level helper functions (pure, stateless)
# ====================================================================


def _find_data_script(html: str) -> str:
  """Find the Marko.js data script containing listing data."""
  scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
  for script in scripts:
    if '"sellerUserName"' in script and '"listingId"' in script:
      return script
  # Fallback: find the largest script with pricing data.
  for script in scripts:
    if '"price"' in script and '"currency"' in script:
      return script
  return ""


def _is_blocked_page(html: str) -> bool:
  """Detect captcha, sign-in, or security challenge pages.

  Real listing pages are 200-800 KB. Genuine blocked/redirect pages
  are tiny (< 50 KB). Indicators like 'signin.ebay' or 'sign in or
  register' appear in the navigation of every valid eBay page, so
  they are only meaningful on small pages. The page title is the
  most reliable signal at any size.
  """
  if len(html) < 50000:
    lower_html = html.lower()
    challenge_markers = [
      "challenge", "captcha",
      "confirmer votre identité", "confirm your identity",
      "bestätigen sie ihre identität", "confirma tu identidad",
      # Sign-in redirect URL — meaningful only on small pages because
      # every real eBay page has a signin.ebay link in the header.
      "signin.ebay",
      "sign in or register",
    ]
    if any(marker in lower_html for marker in challenge_markers):
      return True

  title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.DOTALL)
  if title_match:
    title_lower = title_match.group(1).lower().strip()
    sign_in_markers = [
      "sign in", "se connecter", "einloggen", "accedi",
      "inicia sesión", "security measure",
      "pardon our interruption",
      # eBay's bot-detection "sorry" page (200 OK but no real content).
      "nous sommes désolés", "we are sorry",
    ]
    if any(marker in title_lower for marker in sign_in_markers):
      return True

  return False


# ------------------------------------------------------------------
# Script data extraction
# ------------------------------------------------------------------


def _script_str(script: str, key: str) -> str | None:
  match = re.search(rf'"{key}"\s*:\s*"([^"]*)"', script)
  return match.group(1) if match else None


def _script_bool(script: str, key: str) -> bool | None:
  match = re.search(rf'"{key}"\s*:\s*(true|false)', script)
  if match:
    return match.group(1) == "true"
  return None


def _script_nested_str(script: str, parent_key: str, child_key: str) -> str | None:
  match = re.search(
    rf'"{parent_key}"\s*:\s*\{{[^}}]*"{child_key}"\s*:\s*"([^"]*)"',
    script,
  )
  return match.group(1) if match else None


# ------------------------------------------------------------------
# Content extraction
# ------------------------------------------------------------------


def _extract_card_title(card_html: str) -> str:
  title_match = re.search(
    r"role=heading[^>]*>(.*?)</(?:span|div|h\d)",
    card_html, re.DOTALL,
  )
  if title_match:
    title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
    if title:
      return title
  clean = re.sub(r"<[^>]+>", " ", card_html[:2000])
  clean = re.sub(r"\s+", " ", clean).strip()
  return clean[:120] or "Untitled"


def _extract_title(html: str) -> str:
  title_match = re.search(r"<title[^>]*>(.*?)\s*\|", html, re.DOTALL)
  if title_match:
    title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
    if title and len(title) > 5:
      return title
  heading_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
  if heading_match:
    title = re.sub(r"<[^>]+>", "", heading_match.group(1)).strip()
    if title:
      return title
  return "Untitled eBay listing"


def _extract_description(html: str) -> str | None:
  parts: list[str] = []
  condition_match = re.search(r'"conditionNote"\s*:\s*"([^"]+)"', html)
  if condition_match:
    parts.append(condition_match.group(1))
  subtitle_match = re.search(r'"subtitle"\s*:\s*"([^"]+)"', html)
  if subtitle_match:
    parts.append(subtitle_match.group(1))
  return " | ".join(parts) if parts else None


def _extract_image_urls(script: str, html: str) -> list[str]:
  urls = re.findall(
    r'"originalImg"\s*:\{[^}]*"URL"\s*:\s*"([^"]+)"',
    script,
  )
  if not urls:
    urls = re.findall(
      r"(https://i\.ebayimg\.com/images/g/[^\"\s]+)",
      html,
    )
    urls = list(dict.fromkeys(urls))
  # Upgrade to full-size.
  return [re.sub(r"s-l\d+\.", "s-l1600.", url) for url in urls]


def _extract_item_location(html: str, data_script: str) -> str | None:
  for key in ("itemLocation", "location", "itemLocationText", "shipFromLocation"):
    value = _script_str(data_script, key)
    if value and value.strip():
      return value.strip()

  nested_match = re.search(
    r'"itemLocation"\s*:\s*\{[^}]*"(?:location|country|countryCode|text)"\s*:\s*"([^"]+)"',
    data_script,
  )
  if nested_match:
    return nested_match.group(1).strip()

  patterns = [
    r'Ships?\s+from\s+([^<"\'|]+?)(?:\s*</|"|\||$)',
    r'Item\s+location[:\s]+([^<"\'|]+?)(?:\s*</|"|\||$)',
    r'Located\s+in\s+([^<"\'|]+?)(?:\s*</|"|\||$)',
  ]
  for pattern in patterns:
    match = re.search(pattern, html, re.IGNORECASE)
    if match:
      location = match.group(1).strip()
      location = re.sub(r"\s*[|,.\s]+\s*$", "", location)
      location = re.sub(r"\s+", " ", location)
      if 2 <= len(location) <= 120:
        return location

  return None


def _extract_item_specifics(html: str, attributes: dict[str, str]) -> None:
  """Extract item specifics (Brand, Model, etc.) from page data."""
  pairs = re.findall(
    r'"name"\s*:\s*"([^"]+)"[^}]*"value"\s*:\s*"([^"]+)"',
    html,
  )
  for name, value in pairs:
    key = name.lower().replace(" ", "_")
    if key in ("item_number", "ebay_item_number"):
      continue
    if len(key) > 50 or len(value) > 500:
      continue
    attributes[key] = value


def _extract_end_time(html: str, data_script: str) -> datetime | None:
  """Extract listing end time from script or HTML.

  Tries multiple data sources: nested endTime.value, flat endTime
  string, alternative keys, ISO timestamps in HTML, and relative
  "Sale ends in" patterns.
  """
  # 1. Nested: "endTime": { "value": "..." }
  value = _script_nested_str(data_script, "endTime", "value")
  if value:
    parsed = _parse_ebay_datetime(value)
    if parsed:
      return parsed

  # 2. Flat string.
  flat_match = re.search(r'"endTime"\s*:\s*"([^"]+)"', data_script)
  if flat_match:
    parsed = _parse_ebay_datetime(flat_match.group(1))
    if parsed:
      return parsed

  # 3. Alternative keys.
  for key in ("listingEndTime", "endDate", "closeTime"):
    value = _script_nested_str(data_script, key, "value")
    if value:
      parsed = _parse_ebay_datetime(value)
      if parsed:
        return parsed
    flat_match = re.search(rf'"{key}"\s*:\s*"([^"]+)"', data_script)
    if flat_match:
      parsed = _parse_ebay_datetime(flat_match.group(1))
      if parsed:
        return parsed

  # 4. "Sale ends in Xd Xh" — compute from now.
  sale_ends_match = re.search(
    r"(?:Sale\s+ends\s+in|Ends?\s+in)\s*:?\s*"
    r"(?:(?P<days>\d+)\s*d(?:ays?)?\s*)?"
    r"(?:(?P<hours>\d+)\s*h(?:ours?)?\s*)?"
    r"(?:(?P<mins>\d+)\s*m(?:in(?:utes?)?)?\s*)?",
    html,
    re.IGNORECASE,
  )
  if sale_ends_match:
    days = int(sale_ends_match.group("days") or 0)
    hours = int(sale_ends_match.group("hours") or 0)
    minutes = int(sale_ends_match.group("mins") or 0)
    if days > 0 or hours > 0 or minutes > 0:
      delta = timedelta(days=days, hours=hours, minutes=minutes)
      return datetime.now(UTC) + delta

  return None


# ------------------------------------------------------------------
# Price and currency detection
# ------------------------------------------------------------------


def _extract_price_from_text(text: str) -> Decimal | None:
  """Extract a price from HTML text containing currency symbols."""
  price_patterns = [
    r"(?:£|€|\$|US\s*\$)\s*([0-9,]+(?:\.\d{2})?)",
    r"([0-9,]+(?:\.\d{2})?)\s*(?:EUR|GBP|USD)",
  ]
  for pattern in price_patterns:
    match = re.search(pattern, text)
    if match:
      return _decimal_or_none(match.group(1).replace(",", ""))
  return None


def _detect_currency(html: str) -> str:
  sample = html[:50000]
  if "£" in sample:
    return "GBP"
  if "€" in sample:
    return "EUR"
  if "US $" in sample or "US$" in sample:
    return "USD"
  if "AU $" in sample:
    return "AUD"
  if "C $" in sample or "CA$" in sample:
    return "CAD"
  currency_match = re.search(r'"currency"\s*:\s*"([A-Z]{3})"', sample)
  if currency_match:
    return currency_match.group(1)
  return "USD"


def _detect_card_listing_type(card_html: str) -> str | None:
  has_bids = bool(re.search(
    r"\d+\s*(?:bid|enchère|Gebot|offre)", card_html, re.I,
  ))
  has_buy_now = bool(re.search(
    r"(?:Buy\s*[Ii]t\s*[Nn]ow|Achat\s*immédiat|Sofort-Kaufen|Acheter)",
    card_html, re.I,
  ))
  has_auction_signal = bool(re.search(
    r"(?:\d+\s*(?:bid|enchère|Gebot|offre)|time\s*left|[Tt]emps\s*restant)",
    card_html, re.I,
  ))
  if has_auction_signal and has_buy_now:
    return "hybrid"
  if has_auction_signal or has_bids:
    return "auction"
  return "buy_now"


# ------------------------------------------------------------------
# Status derivation
# ------------------------------------------------------------------


def _derive_status(
  script: str,
  bid_count: int,
  end_time: datetime | None,
  listing_type: str,
  qty_available: int | None,
) -> str:
  """Derive listing status from embedded data.

  Buy Now listings use stock status. Auction listings use bid count
  and end time.
  """
  if listing_type == "buy_now":
    out_of_stock_match = re.search(r'"outOfStock"\s*:\s*(true|false)', script)
    if out_of_stock_match and out_of_stock_match.group(1) == "true":
      return "sold"
    if qty_available is not None and qty_available == 0:
      return "sold"
    # Multi-unit store listings (qty > 1) are bulk inventory pages, not
    # individual collectible sales. Cancel them to avoid endless monitoring.
    if qty_available is not None and qty_available > 1:
      return "cancelled"
    return "active"

  if '"ENDED"' in script or '"ended"' in script:
    return "sold" if bid_count > 0 else "unsold"

  if end_time:
    now = datetime.now(UTC)
    if end_time < now:
      return "sold" if bid_count > 0 else "unsold"
    return "active"

  return "active"


# ------------------------------------------------------------------
# Bid history parsing
# ------------------------------------------------------------------


def _parse_bid_amount(text: str) -> Decimal | None:
  text = text.replace("\xa0", " ").strip()
  if not text:
    return None

  has_currency = bool(re.search(
    r"(?:US\s*\$|AU\s*\$|C\s*\$|CA\s*\$|£|€|\$|EUR|GBP|USD|AUD|CAD|JPY)",
    text,
  ))
  is_numeric = bool(re.match(r"^[\d,.\s]+$", text))
  if not has_currency and not is_numeric:
    return None

  cleaned = re.sub(
    r"(?:US\s*\$|AU\s*\$|C\s*\$|CA\s*\$|£|€|\$|EUR|GBP|USD|AUD|CAD|JPY)",
    "", text,
  ).strip()
  if not cleaned:
    return None

  # Handle European format (1.234,56).
  if re.match(r"^\d{1,3}(?:\.\d{3})*,\d{2}$", cleaned):
    cleaned = cleaned.replace(".", "").replace(",", ".")

  cleaned = cleaned.replace(",", "")
  cleaned = re.sub(r"[^\d.]", "", cleaned)
  return _decimal_or_none(cleaned)


def _parse_bids_from_table(html: str, currency: str) -> list[ScrapedBid]:
  """Extract bids from HTML table rows on the bid history page."""
  bids: list[ScrapedBid] = []

  # Find table rows.
  rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.I)
  for row_html in rows:
    cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.DOTALL | re.I)
    if len(cells) < 3:
      continue
    cell_texts = [re.sub(r"<[^>]+>", "", cell).strip() for cell in cells]

    # Skip header rows.
    if any(
      keyword in cell_texts[0].lower()
      for keyword in ["bidder", "enchérisseur", "bieter", "offerente"]
    ):
      continue

    bid = _try_parse_bid_row(cell_texts, currency)
    if bid is not None:
      bids.append(bid)

  return bids


def _parse_bids_from_regex(html: str, currency: str) -> list[ScrapedBid]:
  """Regex fallback for extracting bids from the HTML."""
  bids: list[ScrapedBid] = []
  bid_pattern = re.compile(
    r"(?P<bidder>[a-zA-Z0-9*._-]{2,30})"
    r"\s+"
    r"(?P<amount>(?:US\s*\$|AU\s*\$|C\s*\$|£|€|\$)?\s*[\d,.]+(?:\s*(?:EUR|GBP|USD|AUD|CAD))?)"
    r"\s+"
    r"(?P<datetime>\w{3}\s+\d{1,2},?\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*\w*)",
    re.MULTILINE,
  )
  for match in bid_pattern.finditer(html):
    amount = _parse_bid_amount(match.group("amount"))
    bid_time = _parse_ebay_bid_datetime(match.group("datetime"))
    if amount is not None and bid_time is not None:
      bids.append(ScrapedBid(
        amount=amount,
        currency=currency,
        bid_time=bid_time,
        bidder_username=match.group("bidder"),
      ))
  return bids


def _try_parse_bid_row(cell_texts: list[str], currency: str) -> ScrapedBid | None:
  bidder_username = None
  amount = None
  bid_time = None
  is_automatic = False

  for text in cell_texts:
    if any(marker in text.lower() for marker in ["automatic", "automatique", "automatisch"]):
      is_automatic = True

    if amount is None:
      parsed = _parse_bid_amount(text)
      if parsed is not None:
        amount = parsed
        continue

    if bid_time is None:
      parsed_time = _parse_ebay_bid_datetime(text)
      if parsed_time is not None:
        bid_time = parsed_time
        continue

    if (
      bidder_username is None
      and text
      and len(text) <= 50
      and ("*" in text or re.match(r"^[a-zA-Z0-9._-]+", text))
    ):
        cleaned = re.sub(r"\s*\(\d+\)\s*$", "", text).strip()
        if cleaned:
          bidder_username = cleaned

  if amount is not None and bid_time is not None:
    return ScrapedBid(
      amount=amount,
      currency=currency,
      bid_time=bid_time,
      bidder_username=bidder_username,
      is_automatic=is_automatic,
    )
  return None


# ------------------------------------------------------------------
# Datetime parsing
# ------------------------------------------------------------------


def _decimal_or_none(value: str | None) -> Decimal | None:
  if not value:
    return None
  try:
    return Decimal(value.replace(",", ""))
  except (InvalidOperation, ValueError):
    return None


def _int_or_none(value: str | None) -> int | None:
  if not value:
    return None
  try:
    return int(value)
  except (ValueError, TypeError):
    return None


def _parse_ebay_datetime(value: str | None) -> datetime | None:
  """Parse an ISO datetime string from eBay (e.g. 2026-02-21T14:52:47.000Z)."""
  if not value:
    return None
  try:
    clean = value.replace("Z", "+00:00")
    return datetime.fromisoformat(clean)
  except (ValueError, TypeError):
    return None


def _resolve_month(name: str) -> int | None:
  return _MONTH_NAMES.get(name.lower().rstrip("."))


def _parse_ebay_bid_datetime(text: str) -> datetime | None:
  """Parse a datetime from eBay bid history (various localized formats)."""
  if not text:
    return None
  text = text.strip()

  # Remove timezone abbreviations (treat as UTC).
  text = re.sub(
    r"\s*(?:PST|PDT|EST|EDT|CST|CDT|MST|MDT|GMT|UTC|CET|CEST|MEZ|MESZ|HNP|HAP)\s*$",
    "", text,
  )
  text = text.replace(" à ", " ").replace(" à", " ")
  text = re.sub(r"\.(?=\s)", "", text)

  # ISO-like: 2026-02-08 12:34:56
  iso_match = re.match(
    r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})", text,
  )
  if iso_match:
    try:
      return datetime(
        int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)),
        int(iso_match.group(4)), int(iso_match.group(5)), int(iso_match.group(6)),
        tzinfo=UTC,
      )
    except ValueError:
      pass

  # German numeric: 08.02.2026 12:34:56
  de_match = re.match(
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})", text,
  )
  if de_match:
    try:
      return datetime(
        int(de_match.group(3)), int(de_match.group(2)), int(de_match.group(1)),
        int(de_match.group(4)), int(de_match.group(5)), int(de_match.group(6)),
        tzinfo=UTC,
      )
    except ValueError:
      pass

  # US format: Feb 08, 2026 12:34:56
  us_match = re.match(
    r"([A-Za-zéèêëàâäùûüîïôöç]+)\s+(\d{1,2}),?\s+(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if us_match:
    month = _resolve_month(us_match.group(1))
    if month is not None:
      try:
        return datetime(
          int(us_match.group(3)), month, int(us_match.group(2)),
          int(us_match.group(4)), int(us_match.group(5)), int(us_match.group(6)),
          tzinfo=UTC,
        )
      except ValueError:
        pass

  # European format: 08 Feb 2026 12:34:56
  eu_match = re.match(
    r"(\d{1,2})\s+([A-Za-zéèêëàâäùûüîïôöç]+)\s+(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if eu_match:
    month = _resolve_month(eu_match.group(2))
    if month is not None:
      try:
        return datetime(
          int(eu_match.group(3)), month, int(eu_match.group(1)),
          int(eu_match.group(4)), int(eu_match.group(5)), int(eu_match.group(6)),
          tzinfo=UTC,
        )
      except ValueError:
        pass

  return None
