"""eBay scraper.

eBay is the world's largest online auction and fixed-price marketplace.
The website is built with Marko.js and embeds structured data in inline
``<script>`` tags using Marko's server-side rendering serialization format.

Key eBay facts used in the scraper:

* eBay has **multiple regional domains** (ebay.com, ebay.co.uk, ebay.fr,
  ebay.de, etc.).  Listings are shared across domains, but search results
  and prices may differ.  eBay.com has aggressive anti-bot detection;
  regional domains are more lenient.
* Listings can be **Auction**, **Buy It Now** (fixed price), or **Hybrid**
  (auction with a Buy It Now option).  The ``auctionPossible`` and
  ``immediatePay`` / ``binPrice`` fields determine the type.
* eBay has **no buyer premium** (unlike auction houses).  Shipping costs
  are listed separately.
* **Bid history** is available on the ``/bfl/viewbids/{item_id}`` page.
  This page requires session cookies from a prior item-page visit and is
  sometimes blocked by CAPTCHA or a sign-in wall.  When accessible, it
  exposes the full bid table with (anonymised) bidder usernames, bid
  amounts, and timestamps.  The scraper attempts to fetch this page
  after loading the item page; if it is blocked, it falls back
  gracefully to price-snapshot tracking.
* eBay auctions have a **fixed end time** with **no time extensions**
  (unlike Catawiki).  The smart monitor should poll shortly before and
  right after the end time to capture the final price.
* Item pages embed a JSON-like model in a ``<script>`` tag containing
  ``bidCount``, ``sellerUserName``, ``listingId``, ``startDate``,
  ``endTime``, ``currency``, ``hasReservePrice``, ``conditionId``, etc.
* Search results use ``<li data-listingid=…>`` elements inside a
  ``<ul class="srp-results">`` container.
* Images are on ``i.ebayimg.com`` with ``s-l500.webp`` sizing.
* eBay condition IDs: 1000=New, 1500=New (Other), 2000=Certified
  Refurbished, 2500=Seller Refurbished, 3000=Used, 7000=For parts.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

from bs4 import BeautifulSoup
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

# Default domain — configurable via the ``ebay_domain`` attribute.
_DEFAULT_DOMAIN = "ebay.com"

# Regional domains to try as fallbacks when the primary returns a
# challenge page.  Ordered by likelihood of success.
_FALLBACK_DOMAINS = [
  "ebay.co.uk",
  "ebay.de",
  "ebay.fr",
  "ebay.it",
  "ebay.es",
  "ebay.com.au",
  "ebay.ca",
]

_DEFAULT_CURRENCY = "USD"

# eBay condition ID → our ItemCondition mapping.
_CONDITION_MAP: dict[str, ItemCondition] = {
  "1000": ItemCondition.NEW,
  "1500": ItemCondition.NEW,       # "New other"
  "2000": ItemCondition.LIKE_NEW,  # "Certified Refurbished"
  "2500": ItemCondition.LIKE_NEW,  # "Seller Refurbished"
  "2750": ItemCondition.LIKE_NEW,  # "Refurbished"
  "3000": ItemCondition.GOOD,      # "Used"
  "4000": ItemCondition.VERY_GOOD, # "Very Good"
  "5000": ItemCondition.GOOD,      # "Good"
  "6000": ItemCondition.FAIR,      # "Acceptable"
  "7000": ItemCondition.FOR_PARTS, # "For parts / not working"
}

# Currency codes typically seen on regional domains.
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


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("ebay")
class EbayScraper(BaseScraper):
  """Scraper for eBay auction and fixed-price listings.

  Because eBay.com often serves challenge pages for bot detection,
  the scraper supports multiple regional domains.  The ``domain``
  parameter (default ``ebay.com``) can be overridden at init time,
  and the scraper will automatically fall back to other domains if
  the primary one returns a challenge.
  """

  def __init__(
    self,
    config: ScrapingConfig,
    *,
    domain: str = _DEFAULT_DOMAIN,
  ) -> None:
    super().__init__(config)
    self.domain = domain
    self._base_url = f"https://www.{domain}"
    self._cffi_session = cffi_requests.Session(impersonate="chrome")
    self._working_domain: Optional[str] = None

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "eBay"

  @property
  def website_base_url(self) -> str:
    return self._base_url

  @property
  def capabilities(self) -> ScraperCapabilities:
    return ScraperCapabilities(
      can_search=True,
      can_fetch_listing=True,
      can_fetch_bids=True,
      can_fetch_seller=False,
      has_bid_history=True,
      has_watcher_count=True,
      has_view_count=False,
      has_buy_now=True,
      has_estimates=False,
      has_reserve_price=True,
      has_lot_numbers=False,
      has_auction_house_info=False,
      monitoring_strategy="snapshot",  # Fixed end time, no extensions.
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
    """Search eBay and return summarised results.

    Uses the classic eBay search page (``/sch/i.html``) which embeds
    item data in Marko.js SSR HTML.
    """
    params: dict[str, str] = {
      "_nkw": query,
      "_sacat": category or "0",
    }
    if page > 1:
      params["_pgn"] = str(page)

    html = self._get_html(f"/sch/i.html", params=params)
    if html is None:
      return []

    return self._parse_search_results(html)

  def _parse_search_results(self, html: str) -> list[SearchResult]:
    """Parse search results from eBay's Marko.js SSR HTML."""
    results: list[SearchResult] = []

    # Find the results container.
    srp_start = html.find("srp-results")
    if srp_start < 0:
      logger.warning("No srp-results container found in search HTML.")
      return []

    srp_html = html[srp_start:]

    # Each item is a <li data-listingid=XXXX ...> element.
    # We split by data-listingid to isolate each card.
    items = re.split(r'<li[^>]*\bdata-listingid=(\d{10,15})\b', srp_html)
    # items[0] is before the first item, then alternating: ID, content.

    for i in range(1, len(items) - 1, 2):
      listing_id = items[i]
      card_html = items[i + 1]

      # --- URL ---
      url_m = re.search(
        r'href=(https://www\.ebay\.[^/\s]+/itm/\d+)',
        card_html,
      )
      if not url_m:
        continue
      item_url = url_m.group(1)

      # --- Title ---
      # eBay puts the title in role=heading or in a textSpan.
      title_m = re.search(
        r'role=heading[^>]*>(.*?)</(?:span|div|h\d)',
        card_html, re.DOTALL,
      )
      title = ""
      if title_m:
        title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip()
      if not title:
        # Fallback: extract from clean text.
        clean = re.sub(r'<[^>]+>', ' ', card_html[:2000])
        clean = re.sub(r'\s+', ' ', clean).strip()
        first_text = clean[:120]
        if first_text:
          title = first_text

      # --- Price ---
      price = self._extract_search_price(card_html)

      # --- Currency ---
      currency = self._detect_currency(card_html)

      # --- Image ---
      img_m = re.search(
        r'<img[^>]*\bsrc="?(https://i\.ebayimg\.com/[^"\s>]+)',
        card_html,
      )
      image_url = img_m.group(1) if img_m else None

      # --- Listing type ---
      listing_type, buy_now_price = self._detect_listing_type_from_card(
        card_html
      )

      # --- End time / bids ---
      bid_m = re.search(r'(\d+)\s*(?:bid|enchère|Gebot|offre)', card_html, re.I)
      end_time = None  # Not reliably available in search cards.

      # --- Status ---
      status = ListingStatus.ACTIVE  # Search only returns active items.

      results.append(SearchResult(
        external_id=listing_id,
        url=item_url,
        title=title,
        current_price=price,
        currency=currency,
        image_url=image_url,
        end_time=end_time,
        listing_type=listing_type,
        status=status,
      ))

    logger.info(
      "eBay search '%s' on %s: %d results.",
      "query", self._get_working_domain(), len(results),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch the full details of an eBay listing.

    For auction-type listings with at least one bid, the scraper also
    attempts to fetch the bid history from the ``/bfl/viewbids/`` page.
    If that page is blocked (CAPTCHA, sign-in wall), bids are simply
    left empty and the monitor will still track prices via snapshots.
    """
    url = self._normalise_item_url(url_or_external_id)
    item_id = self._extract_item_id(url)

    html = self._get_html(url)
    if html is None:
      raise ValueError(f"Could not fetch eBay listing: {url}")

    listing = self._parse_item_page(html, item_id, url)

    # Attempt to fetch bid history for auctions with bids.
    if listing.listing_type in (ListingType.AUCTION, ListingType.HYBRID) and listing.bid_count > 0:
      bids = self._fetch_bid_history(item_id, listing.currency)
      if bids:
        listing.bids = bids
        logger.info(
          "  Fetched %d bid(s) from bid history for item %s.",
          len(bids), item_id,
        )

    return listing

  # ------------------------------------------------------------------
  # Bid history
  # ------------------------------------------------------------------

  def fetch_bids(self, url_or_external_id: str) -> Sequence[ScrapedBid]:
    """Fetch the bid history for an eBay listing.

    Accesses the ``/bfl/viewbids/{item_id}`` page.  If the page is
    blocked by CAPTCHA or a sign-in wall, returns an empty list.
    """
    url = self._normalise_item_url(url_or_external_id)
    item_id = self._extract_item_id(url)

    # We need to know the currency.  Try to detect from the URL domain.
    domain = self._get_working_domain()
    currency = _DOMAIN_CURRENCY.get(domain, _DEFAULT_CURRENCY)

    return self._fetch_bid_history(item_id, currency)

  def _fetch_bid_history(
    self,
    item_id: str,
    currency: str,
  ) -> list[ScrapedBid]:
    """Fetch and parse the bid history page for *item_id*.

    Returns a list of :class:`ScrapedBid` sorted by amount ascending
    (earliest/lowest bid first).  Returns an empty list when the page
    is inaccessible (CAPTCHA, sign-in, 404, etc.).
    """
    bid_page_path = f"/bfl/viewbids/{item_id}?item={item_id}&rt=nc"
    html = self._get_html(bid_page_path)

    if html is None:
      logger.debug("Bid history page returned None for item %s.", item_id)
      return []

    # Check if we got a sign-in or CAPTCHA page instead of the actual
    # bid history.
    if self._is_blocked_page(html):
      logger.debug(
        "Bid history page for item %s is blocked (CAPTCHA or sign-in).",
        item_id,
      )
      return []

    return self._parse_bid_history_page(html, currency)

  def _parse_bid_history_page(
    self,
    html: str,
    currency: str,
  ) -> list[ScrapedBid]:
    """Parse the ``/bfl/viewbids/`` page and extract individual bids.

    eBay's bid history page shows a table with columns for the bidder
    (anonymised username), bid amount, and bid time.  The exact HTML
    structure varies by region and eBay updates, so we try multiple
    parsing strategies.
    """
    soup = BeautifulSoup(html, "lxml")
    bids: list[ScrapedBid] = []

    # ----------------------------------------------------------
    # Strategy 1: Find a well-structured table by looking for
    # table rows that contain price and date patterns.
    # ----------------------------------------------------------
    tables = soup.find_all("table")
    for table in tables:
      rows = table.find_all("tr")
      for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
          continue

        cell_texts = [cell.get_text(strip=True) for cell in cells]

        # Skip header rows.
        if any(
          keyword in cell_texts[0].lower()
          for keyword in ["bidder", "enchérisseur", "bieter", "offerente"]
        ):
          continue

        bid = self._try_parse_bid_row(cell_texts, currency)
        if bid is not None:
          bids.append(bid)

    if bids:
      # Sort by amount ascending (lowest/earliest first).
      bids.sort(key=lambda bid: bid.amount)
      logger.debug(
        "Parsed %d bid(s) from bid history table.", len(bids),
      )
      return bids

    # ----------------------------------------------------------
    # Strategy 2: Fall back to regex-based extraction from the
    # raw HTML when table parsing fails.
    # ----------------------------------------------------------
    bids = self._parse_bids_from_html_regex(html, currency)
    if bids:
      bids.sort(key=lambda bid: bid.amount)
      logger.debug(
        "Parsed %d bid(s) from bid history via regex fallback.",
        len(bids),
      )
    else:
      logger.debug(
        "Could not parse any bids from bid history page "
        "(page length: %d chars).",
        len(html),
      )
    return bids

  def _try_parse_bid_row(
    self,
    cell_texts: list[str],
    currency: str,
  ) -> Optional[ScrapedBid]:
    """Try to parse a single table row as a bid.

    Returns a ``ScrapedBid`` if the row looks like a valid bid, or
    ``None`` otherwise.  The ``cell_texts`` list contains the
    text content of each ``<td>`` or ``<th>`` element in the row.
    """
    bidder_username = None
    amount = None
    bid_time = None
    is_automatic = False

    for text in cell_texts:
      # Detect automatic/proxy bid markers.
      text_lower = text.lower()
      if any(
        marker in text_lower
        for marker in ["automatic", "automatique", "automatisch", "auto"]
      ):
        is_automatic = True

      # Try to parse as a price.
      if amount is None:
        parsed_amount = self._parse_bid_amount(text)
        if parsed_amount is not None:
          amount = parsed_amount
          continue

      # Try to parse as a date/time.
      if bid_time is None:
        parsed_time = _parse_ebay_bid_datetime(text)
        if parsed_time is not None:
          bid_time = parsed_time
          continue

      # If it looks like a username (contains asterisks for anonymisation,
      # or is a short alphanumeric string).
      if bidder_username is None and text and len(text) <= 50:
        if "***" in text or "*" in text or re.match(r"^[a-zA-Z0-9._-]+", text):
          # Strip trailing feedback score like "(123)".
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

  @staticmethod
  def _parse_bid_amount(text: str) -> Optional[Decimal]:
    """Try to parse a price string from a bid history cell.

    Handles various formats: ``US $12.50``, ``12,50 EUR``, ``€12.50``,
    ``£12.50``, ``GBP 12.50``, ``12.50``, etc.

    Returns ``None`` for text that does not convincingly look like a
    monetary amount (e.g. usernames that happen to contain digits).
    """
    # Strip whitespace and non-breaking spaces.
    text = text.replace("\xa0", " ").strip()
    if not text:
      return None

    # The text must look like a price: it should either contain a
    # currency symbol/code or be predominantly numeric.
    has_currency_marker = bool(re.search(
      r"(?:US\s*\$|AU\s*\$|C\s*\$|CA\s*\$|£|€|\$|EUR|GBP|USD|AUD|CAD|JPY)",
      text,
    ))
    is_mostly_numeric = bool(re.match(
      r"^[\d,.\s]+$", text,
    ))
    if not has_currency_marker and not is_mostly_numeric:
      return None

    # Remove currency symbols and codes.
    cleaned = re.sub(
      r"(?:US\s*\$|AU\s*\$|C\s*\$|CA\s*\$|£|€|\$|EUR|GBP|USD|AUD|CAD|JPY)",
      "",
      text,
    ).strip()

    if not cleaned:
      return None

    # Handle European number format (1.234,56 → 1234.56).
    if re.match(r"^\d{1,3}(?:\.\d{3})*,\d{2}$", cleaned):
      cleaned = cleaned.replace(".", "").replace(",", ".")

    # Handle standard format (1,234.56 → 1234.56).
    cleaned = cleaned.replace(",", "")

    # Remove any remaining non-numeric characters except dot.
    cleaned = re.sub(r"[^\d.]", "", cleaned)

    return _decimal_or_none(cleaned)

  def _parse_bids_from_html_regex(
    self,
    html: str,
    currency: str,
  ) -> list[ScrapedBid]:
    """Regex-based fallback for extracting bids from the HTML.

    This handles cases where the table structure is not standard or
    uses div-based layouts instead of tables.
    """
    bids: list[ScrapedBid] = []

    # Look for repeating patterns of (username, amount, date) in the
    # page text.  eBay bid history pages typically list bids in a
    # structured format even if not in a standard HTML table.

    # Pattern: Find bid entries with an amount and a date.
    # This catches patterns like:
    #   a***b  US $12.50  Feb 08, 2026 12:34:56 PST
    #   or similar locale variations.
    bid_pattern = re.compile(
      r"(?P<bidder>[a-zA-Z0-9*._-]{2,30})"  # Bidder name
      r"\s+"
      r"(?P<amount>(?:US\s*\$|AU\s*\$|C\s*\$|£|€|\$)?\s*[\d,.]+(?:\s*(?:EUR|GBP|USD|AUD|CAD))?)"  # Amount
      r"\s+"
      r"(?P<datetime>\w{3}\s+\d{1,2},?\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*\w*)",  # Date/time
      re.MULTILINE,
    )

    for match in bid_pattern.finditer(html):
      bidder = match.group("bidder")
      amount = self._parse_bid_amount(match.group("amount"))
      bid_time = _parse_ebay_bid_datetime(match.group("datetime"))

      if amount is not None and bid_time is not None:
        bids.append(ScrapedBid(
          amount=amount,
          currency=currency,
          bid_time=bid_time,
          bidder_username=bidder,
        ))

    return bids

  @staticmethod
  def _is_blocked_page(html: str) -> bool:
    """Detect if the page is a CAPTCHA, sign-in, or security wall."""
    # Check for short pages that are likely challenge pages.
    if len(html) < 50000:
      lower = html.lower()
      if any(marker in lower for marker in [
        "challenge",
        "captcha",
        "confirmer votre identité",
        "confirm your identity",
        "bestätigen sie ihre identität",
        "confirma tu identidad",
      ]):
        return True

    # Check for sign-in redirect.
    if "sign in or register" in html.lower() or "einloggen oder" in html.lower():
      return True
    if 'signin.ebay' in html.lower():
      return True
    if '<title' in html.lower():
      title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.DOTALL)
      if title_match:
        title = title_match.group(1).lower().strip()
        if any(keyword in title for keyword in [
          "sign in", "se connecter", "einloggen", "accedi",
          "inicia sesión", "security measure", "mesure de sécurité",
          "sicherheitsmaßnahme",
        ]):
          return True

    return False

  def _parse_item_page(
    self,
    html: str,
    item_id: str,
    url: str,
  ) -> ScrapedListing:
    """Extract listing data from the eBay item page."""

    # Find the main Marko.js data script containing listing data.
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    data_script = ""
    for s in scripts:
      if '"sellerUserName"' in s and '"listingId"' in s:
        data_script = s
        break

    # --- Core fields (from Marko embedded data) ---
    title = self._extract_title(html)
    currency = self._script_str(data_script, "currency") or self._detect_currency(html)
    current_price = _decimal_or_none(self._script_str(data_script, "price"))
    bid_count = _int_or_none(self._script_str(data_script, "bidCount")) or 0

    # --- Dates ---
    start_time = _parse_ebay_datetime(
      self._script_nested_str(data_script, "startDate", "value")
    )
    # End time can be nested (endTime.value), flat string, or under another key.
    # For Buy It Now, eBay still exposes a listing end (e.g. "Sale ends in 1d 22h").
    end_time = self._extract_end_time(html, data_script, start_time)

    # --- Listing type ---
    auction_possible = self._script_bool(data_script, "auctionPossible")
    immediate_pay = self._script_bool(data_script, "immediatePay")
    has_bin = '"binPrice"' in data_script or '"buyItNowPrice"' in data_script
    best_offer = self._script_bool(data_script, "bestOfferEnabled")

    # immediatePay is the definitive marker for fixed-price Buy Now
    # listings. Some Buy Now listings have auctionPossible=true but are
    # not actually auctions.
    if immediate_pay:
      listing_type = ListingType.BUY_NOW
    elif auction_possible and has_bin:
      listing_type = ListingType.HYBRID
    elif auction_possible:
      listing_type = ListingType.AUCTION
    else:
      listing_type = ListingType.BUY_NOW

    # --- Buy It Now price ---
    buy_now_price = None
    bin_m = re.search(
      r'"binPrice"\s*:\{[^}]*"value"\s*:\s*"?([0-9.]+)',
      data_script,
    )
    if bin_m:
      buy_now_price = _decimal_or_none(bin_m.group(1))

    # --- Reserve price ---
    has_reserve = self._script_bool(data_script, "hasReservePrice")
    reserve_met = self._script_bool(data_script, "reservePriceMet")

    # --- Condition ---
    condition_id = self._script_str(data_script, "conditionId")
    condition = _CONDITION_MAP.get(condition_id or "", ItemCondition.UNKNOWN)

    # --- Seller ---
    seller_username = self._script_str(data_script, "sellerUserName") or ""
    seller = None
    if seller_username:
      seller = ScrapedSeller(
        external_id=seller_username,
        username=seller_username,
        display_name=seller_username,
      )

    # --- Item location (where the item is sent from) ---
    location = self._extract_item_location(html, data_script)

    # --- Shipping ---
    shipping_cost = None
    ship_m = re.search(r'"shippingCost"\s*:\s*"?([0-9.]+)', data_script)
    if ship_m:
      shipping_cost = _decimal_or_none(ship_m.group(1))

    # --- Description ---
    description = self._extract_description(html)

    # --- Images ---
    images = self._extract_images(data_script, html)

    # --- Watchers ---
    watcher_count = None
    watcher_m = re.search(r'"watchCount"\s*:\s*(\d+)', data_script)
    if watcher_m:
      watcher_count = int(watcher_m.group(1))

    # --- Quantity (for fixed-price listings) ---
    qty_available = _int_or_none(
      self._script_str(data_script, "quantityAvailable")
    )
    qty_sold = _int_or_none(
      self._script_str(data_script, "quantitySold")
    )

    # --- Status ---
    # Pass listing_type and quantity to make better decisions for Buy
    # Now vs Auction listings.
    status = self._derive_status(
      data_script, bid_count, end_time, listing_type, qty_available,
    )

    # --- Attributes ---
    attributes: dict[str, str] = {}
    if condition_id:
      attributes["condition_id"] = condition_id
    if best_offer:
      attributes["best_offer_enabled"] = "true"
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

    # Extract item specifics from the HTML (Brand, Model, etc.)
    self._extract_item_specifics(html, attributes)

    # --- Final price (for ended auctions) ---
    final_price = None
    if status in (ListingStatus.SOLD, ListingStatus.UNSOLD):
      final_price = current_price

    return ScrapedListing(
      external_id=item_id,
      url=url,
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency=currency,
      starting_price=None,  # Not available on eBay.
      reserve_price=None,   # Hidden on eBay.
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
      images=images,
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # HTML / data helpers
  # ------------------------------------------------------------------

  def _get_html(
    self,
    path_or_url: str,
    *,
    params: Optional[dict[str, str]] = None,
  ) -> Optional[str]:
    """Fetch a page, trying the primary domain first, then fallbacks.

    Returns ``None`` if all domains fail or return challenge pages.
    """
    # --- Browser path (bypasses challenge pages natively) ---
    if self._browser_enabled:
      if path_or_url.startswith("http"):
        full_url = path_or_url
      else:
        full_url = f"https://www.{self._get_working_domain()}{path_or_url}"
      if params:
        from urllib.parse import urlencode
        full_url = f"{full_url}?{urlencode(params)}"
      try:
        html = self._get_html_via_browser(full_url)
        if html and not self._is_challenge_page(html):
          return html
        logger.debug("Browser returned challenge page, falling back to curl_cffi.")
      except Exception as exc:
        logger.debug("Browser fetch failed (%s), falling back to curl_cffi.", exc)

    # --- curl_cffi path with domain fallback ---
    domains_to_try = [self._get_working_domain()] + [
      d for d in _FALLBACK_DOMAINS
      if d != self._get_working_domain()
    ]

    for domain in domains_to_try:
      if path_or_url.startswith("http"):
        # Full URL — replace the domain.
        url = re.sub(
          r'https://www\.ebay\.[^/]+',
          f'https://www.{domain}',
          path_or_url,
        )
      else:
        url = f"https://www.{domain}{path_or_url}"

      try:
        self._rate_limit()
        logger.debug("GET %s (params=%s)", url, params)
        response = self._cffi_session.get(
          url,
          params=params,
          timeout=self.config.timeout,
        )
        response.encoding = "utf-8"
        html = response.text

        # Check for challenge page.
        if self._is_challenge_page(html):
          logger.debug(
            "Challenge page on %s — trying fallback.", domain
          )
          continue

        if response.status_code == 404:
          logger.warning("404 on %s for %s", domain, path_or_url)
          return None

        self._working_domain = domain
        return html

      except Exception as exc:
        logger.debug("Request to %s failed: %s", domain, exc)
        continue

    logger.error(
      "All eBay domains returned challenge pages or errors for: %s",
      path_or_url,
    )
    return None

  def _get_working_domain(self) -> str:
    """Return the domain that most recently worked."""
    return self._working_domain or self.domain

  @staticmethod
  def _is_challenge_page(html: str) -> bool:
    """Detect eBay's anti-bot challenge pages."""
    return len(html) < 50000 and (
      "challenge" in html.lower()
      or "confirmer votre identité" in html
      or "confirm your identity" in html
      or "Bestätigen Sie Ihre Identität" in html
    )

  @staticmethod
  def _normalise_item_url(url_or_id: str) -> str:
    """Accept a full URL or a bare item ID and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID.
    if url_or_id.isdigit():
      return f"https://www.{_DEFAULT_DOMAIN}/itm/{url_or_id}"
    raise ValueError(f"Cannot normalise eBay URL: {url_or_id}")

  @staticmethod
  def _extract_item_id(url: str) -> str:
    """Extract the numeric item ID from an eBay URL."""
    m = re.search(r'/itm/(\d+)', url)
    if m:
      return m.group(1)
    # Maybe it's already just the ID.
    m2 = re.search(r'^(\d{10,15})$', url)
    if m2:
      return m2.group(1)
    raise ValueError(f"Cannot extract eBay item ID from: {url}")

  # ------------------------------------------------------------------
  # Script data extraction helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _script_str(script: str, key: str) -> Optional[str]:
    """Extract a string value for *key* from the script data."""
    m = re.search(rf'"{key}"\s*:\s*"([^"]*)"', script)
    return m.group(1) if m else None

  @staticmethod
  def _script_bool(script: str, key: str) -> Optional[bool]:
    """Extract a boolean value for *key*."""
    m = re.search(rf'"{key}"\s*:\s*(true|false)', script)
    if m:
      return m.group(1) == "true"
    return None

  @staticmethod
  def _script_nested_str(script: str, parent_key: str, child_key: str) -> Optional[str]:
    """Extract ``parent_key: { child_key: "value" }``."""
    m = re.search(
      rf'"{parent_key}"\s*:\s*\{{[^}}]*"{child_key}"\s*:\s*"([^"]*)"',
      script,
    )
    return m.group(1) if m else None

  # ------------------------------------------------------------------
  # Content extraction
  # ------------------------------------------------------------------

  def _extract_end_time(
    self,
    html: str,
    data_script: str,
    start_time: Optional[datetime],
  ) -> Optional[datetime]:
    """Extract listing end time from script or HTML.

    eBay uses different structures: nested endTime.value, flat endTime
    string, or listingEndTime. For Buy It Now, a listing end is still
    shown (e.g. "Sale ends in 1d 22h"). Tries multiple sources so the
    stored end_time stays in sync with the live page.
    """
    # 1. Nested: "endTime": { "value": "2026-02-21T14:52:47.000Z" }
    value = self._script_nested_str(data_script, "endTime", "value")
    if value:
      parsed = _parse_ebay_datetime(value)
      if parsed:
        return parsed

    # 2. Flat string: "endTime":"2026-02-21T14:52:47.000Z"
    flat_m = re.search(
      r'"endTime"\s*:\s*"([^"]+)"',
      data_script,
    )
    if flat_m:
      parsed = _parse_ebay_datetime(flat_m.group(1))
      if parsed:
        return parsed

    # 3. Alternative keys (nested or flat).
    for key in ("listingEndTime", "endDate", "closeTime"):
      value = self._script_nested_str(data_script, key, "value")
      if value:
        parsed = _parse_ebay_datetime(value)
        if parsed:
          return parsed
      flat_m = re.search(rf'"{key}"\s*:\s*"([^"]+)"', data_script)
      if flat_m:
        parsed = _parse_ebay_datetime(flat_m.group(1))
        if parsed:
          return parsed

    # 4. HTML fallback: look for ISO timestamp anywhere in page.
    iso_m = re.search(
      r'(?:endTime|listingEndTime|endDate)["\s:]+'
      r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)"',
      html,
    )
    if iso_m:
      parsed = _parse_ebay_datetime(iso_m.group(1))
      if parsed:
        return parsed

    # 5. "Sale ends in Xd Xh" / "Ends in X days" — compute from now.
    sale_ends_m = re.search(
      r'(?:Sale\s+ends\s+in|Ends?\s+in)\s*:?\s*'
      r'(?:(?P<days>\d+)\s*d(?:ays?)?\s*)?'
      r'(?:(?P<hours>\d+)\s*h(?:ours?)?\s*)?'
      r'(?:(?P<mins>\d+)\s*m(?:in(?:utes?)?)?\s*)?',
      html,
      re.IGNORECASE,
    )
    if sale_ends_m:
      now = datetime.now(timezone.utc)
      days = int(sale_ends_m.group("days") or 0)
      hours = int(sale_ends_m.group("hours") or 0)
      minutes = int(sale_ends_m.group("mins") or 0)
      if days > 0 or hours > 0 or minutes > 0:
        delta = timedelta(days=days, hours=hours, minutes=minutes)
        return now + delta

    return None

  def _extract_item_location(self, html: str, data_script: str) -> Optional[str]:
    """Extract item location (where the item is sent from).

    Tries embedded script data first, then falls back to HTML patterns
    (e.g. "Ships from Taiwan", "Item location: Sydney, Australia").
    """
    # Try script keys (eBay uses different keys across domains/layouts).
    for key in ("itemLocation", "location", "itemLocationText", "shipFromLocation"):
      value = self._script_str(data_script, key)
      if value and value.strip():
        return value.strip()

    # Try nested object: "itemLocation":{"location":"Taiwan"} or similar.
    nested_m = re.search(
      r'"itemLocation"\s*:\s*\{[^}]*"(?:location|country|countryCode|text)"\s*:\s*"([^"]+)"',
      data_script,
    )
    if nested_m:
      return nested_m.group(1).strip()

    # Fallback: scan full HTML for location patterns.
    # "Ships from Taiwan", "Item location: Taiwan", "From United Kingdom", etc.
    patterns = [
      r'Ships?\s+from\s+([^<"\'|]+?)(?:\s*</|"|\||$)',
      r'Item\s+location[:\s]+([^<"\'|]+?)(?:\s*</|"|\||$)',
      r'Located\s+in\s+([^<"\'|]+?)(?:\s*</|"|\||$)',
      r'"itemLocation"\s*:\s*"([^"]+)"',
      r'data-location="([^"]+)"',
      r'"location"\s*:\s*"([^"]{2,80})"',
    ]
    for pattern in patterns:
      match = re.search(pattern, html, re.IGNORECASE)
      if match:
        location = match.group(1).strip()
        # Trim trailing punctuation and normalize whitespace.
        location = re.sub(r'\s*[|,.\s]+\s*$', '', location)
        location = re.sub(r'\s+', ' ', location)
        if len(location) >= 2 and len(location) <= 120:
          return location

    return None

  @staticmethod
  def _extract_title(html: str) -> str:
    """Extract the listing title from the page."""
    # First try the <title> tag.
    title_m = re.search(r'<title[^>]*>(.*?)\s*\|', html, re.DOTALL)
    if title_m:
      title = title_m.group(1).strip()
      title = re.sub(r'<[^>]+>', '', title)
      if title and len(title) > 5:
        return title

    # Fallback: look for the main heading.
    h1_m = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    if h1_m:
      title = re.sub(r'<[^>]+>', '', h1_m.group(1)).strip()
      if title:
        return title

    return "Untitled eBay listing"

  @staticmethod
  def _extract_description(html: str) -> Optional[str]:
    """Extract the item description.

    eBay descriptions are often in an iframe, so we can only get the
    seller-provided condition notes from the main page.
    """
    # Look for condition description / seller notes.
    desc_parts: list[str] = []

    # Condition notes.
    cond_m = re.search(
      r'"conditionNote"\s*:\s*"([^"]+)"',
      html,
    )
    if cond_m:
      desc_parts.append(cond_m.group(1))

    # Subtitle.
    sub_m = re.search(r'"subtitle"\s*:\s*"([^"]+)"', html)
    if sub_m:
      desc_parts.append(sub_m.group(1))

    return " | ".join(desc_parts) if desc_parts else None

  @staticmethod
  def _extract_images(script: str, html: str) -> list[ScrapedImage]:
    """Extract image URLs."""
    images: list[ScrapedImage] = []

    # From Marko data: originalImg.URL pattern.
    img_urls = re.findall(
      r'"originalImg"\s*:\{[^}]*"URL"\s*:\s*"([^"]+)"',
      script,
    )
    if not img_urls:
      # Fallback: look for ebayimg URLs in the HTML.
      img_urls = re.findall(
        r'(https://i\.ebayimg\.com/images/g/[^"\s]+)',
        html,
      )
      # De-duplicate.
      img_urls = list(dict.fromkeys(img_urls))

    for i, url in enumerate(img_urls):
      # Upgrade to full-size image.
      url = re.sub(r's-l\d+\.', 's-l1600.', url)
      images.append(ScrapedImage(source_url=url, position=i))

    return images

  @staticmethod
  def _extract_item_specifics(html: str, attributes: dict[str, str]) -> None:
    """Extract item specifics (Brand, Model, etc.) from the HTML.

    eBay renders these as name/value pairs in the page.
    """
    # Look for pairs in the Marko data.
    pairs = re.findall(
      r'"name"\s*:\s*"([^"]+)"[^}]*"value"\s*:\s*"([^"]+)"',
      html,
    )
    for name, value in pairs:
      # Skip internal/technical keys.
      key = name.lower().replace(" ", "_")
      if key in ("item_number", "ebay_item_number"):
        continue
      if len(key) > 50 or len(value) > 500:
        continue
      attributes[key] = value

  def _extract_search_price(self, card_html: str) -> Optional[Decimal]:
    """Extract price from a search result card."""
    # Look for currency symbol + number patterns.
    price_patterns = [
      r'(?:£|€|\$|US\s*\$)\s*([0-9,]+(?:\.\d{2})?)',
      r'([0-9,]+(?:\.\d{2})?)\s*(?:EUR|GBP|USD)',
      r'([0-9]+(?:[.,]\d{2})?)\s*(?:€|£|\$)',
    ]
    for pat in price_patterns:
      m = re.search(pat, card_html)
      if m:
        price_str = m.group(1).replace(",", "")
        return _decimal_or_none(price_str)
    return None

  @staticmethod
  def _detect_currency(html: str) -> str:
    """Detect the currency from page content."""
    # Check for currency symbols in the HTML.
    if "£" in html[:50000]:
      return "GBP"
    if "€" in html[:50000]:
      return "EUR"
    if "US $" in html[:50000] or "US$" in html[:50000]:
      return "USD"
    if "AU $" in html[:50000]:
      return "AUD"
    if "C $" in html[:50000] or "CA$" in html[:50000]:
      return "CAD"
    # Check from Marko data.
    currency_m = re.search(r'"currency"\s*:\s*"([A-Z]{3})"', html[:100000])
    if currency_m:
      return currency_m.group(1)
    return _DEFAULT_CURRENCY

  def _detect_listing_type_from_card(
    self, card_html: str
  ) -> tuple[ListingType, Optional[Decimal]]:
    """Detect listing type from a search result card.

    Returns ``(listing_type, buy_now_price)``.
    """
    has_bids = bool(re.search(
      r'\d+\s*(?:bid|enchère|Gebot|offre)',
      card_html, re.I,
    ))
    has_buy = bool(re.search(
      r'(?:Buy\s*[Ii]t\s*[Nn]ow|Achat\s*immédiat|Sofort-Kaufen|Acheter)',
      card_html, re.I,
    ))
    has_auction_keyword = bool(re.search(
      r'(?:\d+\s*(?:bid|enchère|Gebot|offre)|time\s*left|[Tt]emps\s*restant)',
      card_html, re.I,
    ))

    if has_auction_keyword and has_buy:
      return ListingType.HYBRID, None
    elif has_auction_keyword or has_bids:
      return ListingType.AUCTION, None
    else:
      return ListingType.BUY_NOW, None

  @staticmethod
  def _derive_status(
    script: str,
    bid_count: int,
    end_time: Optional[datetime],
    listing_type: ListingType,
    qty_available: Optional[int],
  ) -> ListingStatus:
    """Derive listing status from embedded data.

    For **auction** listings: uses bid count, end time, and "ENDED"
    markers.  For **Buy Now** listings: uses quantity available and
    out-of-stock status (not bid count or end time).
    """
    # ---- Buy Now listings -------------------------------------
    # Buy Now listings don't have bids or fixed end times; they run
    # until sold out or manually ended by the seller.  When a Buy
    # Now item goes out of stock, it was purchased — that is SOLD,
    # not UNSOLD (UNSOLD is for auctions that ended with no buyer).
    if listing_type == ListingType.BUY_NOW:
      # Check stock status.
      out_of_stock_m = re.search(
        r'"outOfStock"\s*:\s*(true|false)', script,
      )
      if out_of_stock_m and out_of_stock_m.group(1) == "true":
        return ListingStatus.SOLD

      # Check quantity.
      if qty_available is not None and qty_available == 0:
        return ListingStatus.SOLD

      # In stock and available.
      return ListingStatus.ACTIVE

    # ---- Auction or Hybrid listings ---------------------------
    # Use bid count and end time to determine status.

    # Check for explicit ended marker.
    if '"ENDED"' in script or '"ended"' in script:
      if bid_count > 0:
        return ListingStatus.SOLD
      return ListingStatus.UNSOLD

    # Check end time.
    if end_time:
      now = datetime.now(timezone.utc)
      if end_time < now:
        return ListingStatus.SOLD if bid_count > 0 else ListingStatus.UNSOLD
      return ListingStatus.ACTIVE

    # Active auction (no end time yet).
    return ListingStatus.ACTIVE


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _decimal_or_none(value: Optional[str]) -> Optional[Decimal]:
  """Convert a string to Decimal, returning None on failure."""
  if not value:
    return None
  try:
    return Decimal(value.replace(",", ""))
  except (InvalidOperation, ValueError):
    return None


def _int_or_none(value: Optional[str]) -> Optional[int]:
  """Convert a string to int, returning None on failure."""
  if not value:
    return None
  try:
    return int(value)
  except (ValueError, TypeError):
    return None


def _parse_ebay_datetime(value: Optional[str]) -> Optional[datetime]:
  """Parse an ISO datetime string from eBay.

  eBay uses formats like ``2026-02-21T14:52:47.000Z``.
  """
  if not value:
    return None
  try:
    # Remove trailing Z and parse as UTC.
    clean = value.replace("Z", "+00:00")
    return datetime.fromisoformat(clean)
  except (ValueError, TypeError):
    return None


# Month name mappings for multiple eBay locales.
_MONTH_NAMES: dict[str, int] = {
  # English
  "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
  "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
  # French
  "janv": 1, "févr": 2, "mars": 3, "avr": 4, "mai": 5, "juin": 6,
  "juil": 7, "août": 8, "sept": 9, "déc": 12,
  # German
  "mär": 3, "mai": 5, "okt": 10, "dez": 12,
  # Italian
  "gen": 1, "mag": 5, "giu": 6, "lug": 7, "ago": 8, "set": 9,
  "ott": 10, "dic": 12,
  # Spanish
  "ene": 1, "abr": 4, "ago": 8,
  # Full English month names
  "january": 1, "february": 2, "march": 3, "april": 4,
  "june": 6, "july": 7, "august": 8, "september": 9,
  "october": 10, "november": 11, "december": 12,
}


def _parse_ebay_bid_datetime(text: str) -> Optional[datetime]:
  """Parse a date/time string from the eBay bid history page.

  Handles various localized formats:

  * ``Feb 08, 2026 12:34:56 PST``   (US)
  * ``08 Feb 2026 12:34:56 GMT``     (UK)
  * ``08 févr. 2026 à 12:34:56``     (FR)
  * ``08.02.2026 12:34:56 MEZ``      (DE)
  * ``2026-02-08 12:34:56``          (ISO-ish)
  """
  if not text:
    return None

  text = text.strip()

  # Remove common timezone abbreviations (we treat everything as UTC
  # since eBay does not consistently expose the timezone offset).
  text = re.sub(
    r"\s*(?:PST|PDT|EST|EDT|CST|CDT|MST|MDT|GMT|UTC|CET|CEST|MEZ|MESZ|HNP|HAP)\s*$",
    "",
    text,
  )
  # Remove French "à" separator.
  text = text.replace(" à ", " ").replace(" à", " ")
  # Remove trailing dots from abbreviated months.
  text = re.sub(r"\.(?=\s)", "", text)

  # --- ISO-like: 2026-02-08 12:34:56 ---
  iso_match = re.match(
    r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if iso_match:
    try:
      return datetime(
        int(iso_match.group(1)),
        int(iso_match.group(2)),
        int(iso_match.group(3)),
        int(iso_match.group(4)),
        int(iso_match.group(5)),
        int(iso_match.group(6)),
        tzinfo=timezone.utc,
      )
    except ValueError:
      pass

  # --- German numeric: 08.02.2026 12:34:56 ---
  de_match = re.match(
    r"(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if de_match:
    try:
      return datetime(
        int(de_match.group(3)),
        int(de_match.group(2)),
        int(de_match.group(1)),
        int(de_match.group(4)),
        int(de_match.group(5)),
        int(de_match.group(6)),
        tzinfo=timezone.utc,
      )
    except ValueError:
      pass

  # --- Month-name formats: "Feb 08, 2026 12:34:56" or "08 Feb 2026 12:34:56" ---
  # Pattern A: Month Day, Year Time (US format)
  us_match = re.match(
    r"([A-Za-zéèêëàâäùûüîïôöç]+)\s+(\d{1,2}),?\s+(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if us_match:
    month = _resolve_month(us_match.group(1))
    if month is not None:
      try:
        return datetime(
          int(us_match.group(3)),
          month,
          int(us_match.group(2)),
          int(us_match.group(4)),
          int(us_match.group(5)),
          int(us_match.group(6)),
          tzinfo=timezone.utc,
        )
      except ValueError:
        pass

  # Pattern B: Day Month Year Time (European format)
  eu_match = re.match(
    r"(\d{1,2})\s+([A-Za-zéèêëàâäùûüîïôöç]+)\s+(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})",
    text,
  )
  if eu_match:
    month = _resolve_month(eu_match.group(2))
    if month is not None:
      try:
        return datetime(
          int(eu_match.group(3)),
          month,
          int(eu_match.group(1)),
          int(eu_match.group(4)),
          int(eu_match.group(5)),
          int(eu_match.group(6)),
          tzinfo=timezone.utc,
        )
      except ValueError:
        pass

  # --- Relaxed: just date and time without seconds ---
  relaxed_match = re.match(
    r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})\s+(\d{1,2}):(\d{2})",
    text,
  )
  if relaxed_match:
    day_or_month = int(relaxed_match.group(1))
    month_or_day = int(relaxed_match.group(2))
    year = int(relaxed_match.group(3))
    # Heuristic: if the first number > 12, it must be the day.
    if day_or_month > 12:
      day, month = day_or_month, month_or_day
    else:
      # Assume day/month/year (European order) by default.
      day, month = day_or_month, month_or_day
    try:
      return datetime(
        year, month, day,
        int(relaxed_match.group(4)),
        int(relaxed_match.group(5)),
        0,
        tzinfo=timezone.utc,
      )
    except ValueError:
      pass

  return None


def _resolve_month(name: str) -> Optional[int]:
  """Resolve a month name or abbreviation to a 1-based month number."""
  key = name.lower().rstrip(".")
  return _MONTH_NAMES.get(key)
