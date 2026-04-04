"""Yahoo Japan Auctions scraper (via Buyee proxy).

Yahoo! オークション (formerly Yahoo! Auctions Japan) is the largest online
auction platform in Japan.  Since April 2022, Yahoo Japan blocks all
access from the EEA and the United Kingdom for GDPR compliance reasons.
International users must use a proxy service such as **Buyee**
(``buyee.jp``), which provides English-language access to Yahoo Japan
Auction listings.

Key facts used in the scraper:

* **Buyee** is used as the gateway.  All URLs point to ``buyee.jp``.
* Search: ``/item/search/query/{query}?translationType=1&page={n}``
  returns up to ~100 items per page.
* Item detail pages: ``/item/yahoo/auction/{auction_id}`` (automatically
  redirected to ``/item/jdirectitems/auction/{auction_id}``).
* The item page embeds all data directly in HTML — no JSON APIs or
  ``__NEXT_DATA__``-style blobs.  We parse it with regex.
* Data available per listing: title, current price (JPY), starting
  price, seller name and rating, item condition, bid count, watcher
  count, opening and closing times (JST), images, auction ID.
* **No bid history** is available through Buyee.
* Yahoo Japan auctions have **fixed end times** — no extensions like
  Catawiki.  The "Early Finish" flag means the seller can terminate
  the auction *early*, not that time extends on bids.
* The monitoring strategy is ``"snapshot"`` (periodic price snapshots
  plus aggressive polling near the end), identical to eBay.
* Currency is always **JPY** (Japanese Yen).
* Buyee charges a service fee (typically 500 JPY per order or a
  percentage), but this is not shown on the listing page itself.
"""

from __future__ import annotations

import html as html_lib
import logging
import re
import time
from datetime import datetime, timezone, timedelta
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

_BASE_URL = "https://buyee.jp"
_SEARCH_PATH = "/item/search/query"
_ITEM_PATH = "/item/yahoo/auction"
_DEFAULT_CURRENCY = "JPY"

# Japan Standard Time offset (UTC+9).
_JST = timezone(timedelta(hours=9))

# Buyee condition labels → our ItemCondition mapping.
_CONDITION_MAP: dict[str, ItemCondition] = {
  "new": ItemCondition.NEW,
  "unused": ItemCondition.NEW,
  "close to unused": ItemCondition.LIKE_NEW,
  "no noticeable scratches or stains": ItemCondition.VERY_GOOD,
  "slightly damaged/dirty": ItemCondition.GOOD,
  "a little damaged/dirty": ItemCondition.GOOD,
  "some scratches or stains": ItemCondition.GOOD,
  "damaged/dirty": ItemCondition.FAIR,
  "overall condition bad": ItemCondition.POOR,
  "for parts": ItemCondition.FOR_PARTS,
}


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("yahoo_japan")
class YahooJapanScraper(BaseScraper):
  """Scraper for Yahoo Japan Auctions via the Buyee proxy service.

  Buyee provides an English-language interface to Yahoo Japan Auctions.
  All HTTP requests go through ``buyee.jp``.
  """

  def __init__(self, config: ScrapingConfig) -> None:
    super().__init__(config)
    self._cffi_session = cffi_requests.Session(impersonate="chrome")

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "Yahoo Japan"

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
      has_estimates=False,
      has_reserve_price=False,
      has_lot_numbers=False,
      has_auction_house_info=False,
      # Fixed end time, no extensions.  Periodic snapshots + final
      # check at auction end, just like eBay.
      monitoring_strategy="snapshot",
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
    """Search Yahoo Japan Auctions via Buyee."""
    params: dict[str, str] = {"translationType": "1"}
    if page > 1:
      params["page"] = str(page)

    url = f"{_BASE_URL}{_SEARCH_PATH}/{_url_encode_query(query)}"
    html = self._get_html(url, params=params)
    if html is None:
      return []

    return self._parse_search_results(html, query, page)

  def _parse_search_results(
    self,
    html: str,
    query: str,
    page: int,
  ) -> list[SearchResult]:
    """Parse item cards from Buyee search results."""
    results: list[SearchResult] = []

    # Split by item card markers.
    cards = html.split('class="itemCard">')
    # First chunk is before the first card.
    for card_html in cards[1:]:
      # Extract the auction link and ID.
      link_match = re.search(
        r'href="(/item/[^"]+/auction/([a-zA-Z0-9]+))"',
        card_html,
      )
      if not link_match:
        continue

      item_path = link_match.group(1)
      auction_id = link_match.group(2)
      item_url = f"{_BASE_URL}{item_path}"

      # Title from the alt attribute of the image or the item name span.
      title = ""
      alt_match = re.search(r'alt="([^"]+)"', card_html)
      if alt_match:
        title = html_lib.unescape(alt_match.group(1)).strip()
      if not title:
        name_match = re.search(
          r'class="[^"]*itemCard__itemName[^"]*"[^>]*>(.*?)<',
          card_html, re.DOTALL,
        )
        if name_match:
          title = re.sub(r'<[^>]+>', '', name_match.group(1)).strip()

      # Price (in JPY).
      price = None
      price_match = re.search(r'([\d,]+)\s*(?:YEN|JPY|円)', card_html)
      if price_match:
        price = _decimal_or_none(price_match.group(1))

      # Image.
      image_url = None
      img_match = re.search(
        r'(?:src|data-src)\s*=\s*"(https?://[^"]*(?:auctions\.c\.yimg\.jp|cdnyauction)[^"]*)"',
        card_html,
      )
      if img_match:
        image_url = html_lib.unescape(img_match.group(1))

      # Time remaining (coarse: "4Days", "23Hours", "1Day 5Hours").
      # We cannot reliably derive an exact end_time from this.
      # The full item page has the exact closing time.

      results.append(SearchResult(
        external_id=auction_id,
        url=item_url,
        title=title,
        current_price=price,
        currency=_DEFAULT_CURRENCY,
        image_url=image_url,
        end_time=None,  # Not available in search cards.
        listing_type=ListingType.AUCTION,
        status=ListingStatus.ACTIVE,
      ))

    logger.info(
      "Yahoo Japan search '%s' page %d: %d results.",
      query, page, len(results),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch the full details of a Yahoo Japan Auction listing via Buyee."""
    url = self._normalise_url(url_or_external_id)
    auction_id = self._extract_auction_id(url_or_external_id)

    html = self._get_html(url)
    if html is None:
      raise ValueError(
        f"Could not fetch Yahoo Japan listing: {url}"
      )

    return self._parse_item_page(html, auction_id, url)

  def _parse_item_page(
    self,
    html: str,
    auction_id: str,
    url: str,
  ) -> ScrapedListing:
    """Extract listing data from the Buyee item detail page."""

    # --- Title ---
    title = self._extract_title(html)

    # --- Current price ---
    current_price = None
    price_match = re.search(
      r'class="current_price".*?<dd[^>]*>.*?([\d,]+)\s*YEN',
      html, re.DOTALL,
    )
    if price_match:
      current_price = _decimal_or_none(price_match.group(1))

    # --- Seller section (all item metadata lives here on Buyee) ---
    seller_info = self._extract_seller_section(html)

    # --- Seller ---
    seller_name = seller_info.get("seller", "")
    seller = None
    if seller_name:
      rating_good = _int_or_none(seller_info.get("good"))
      rating_bad = _int_or_none(seller_info.get("bad"))
      rating_pct_str = seller_info.get("percentage_of_good_ratings", "")
      rating_pct = None
      if rating_pct_str:
        pct_match = re.search(r'([\d.]+)', rating_pct_str)
        if pct_match:
          rating_pct = float(pct_match.group(1))

      feedback_count = None
      if rating_good is not None:
        feedback_count = rating_good
        if rating_bad is not None:
          feedback_count += rating_bad

      seller = ScrapedSeller(
        external_id=seller_name,
        username=seller_name,
        display_name=seller_name,
        country="JP",
        rating=rating_pct,
        feedback_count=feedback_count,
      )

    # --- Starting price ---
    starting_price = None
    starting_str = seller_info.get("starting_price", "")
    sp_match = re.search(r'([\d,]+)\s*YEN', starting_str)
    if sp_match:
      starting_price = _decimal_or_none(sp_match.group(1))

    # --- Condition ---
    condition_label = seller_info.get("item_condition", "").lower().strip()
    condition = _CONDITION_MAP.get(condition_label, ItemCondition.UNKNOWN)

    # --- Bid count ---
    bid_count = _int_or_none(seller_info.get("number_of_bids")) or 0

    # --- Watcher count ---
    watcher_count = None
    watch_match = re.search(
      r'class="[^"]*g-feather-bidding[^"]*"[^>]*>\s*</i>\s*(\d+)',
      html,
    )
    if watch_match:
      watcher_count = int(watch_match.group(1))

    # --- Opening and closing times (JST) ---
    start_time = _parse_buyee_datetime(
      seller_info.get("opening_time_(jst)", "")
    )
    end_time = _parse_buyee_datetime(
      seller_info.get("closing_time_(jst)", "")
    )

    # --- Status ---
    status = self._derive_status(html, bid_count, end_time)

    # --- Listing type ---
    # Buyee does not clearly surface whether a listing has a buy-it-now
    # option.  The "bidorbuy hidden" element always shows "0 YEN" and is
    # present on all pages.  We look for a non-hidden, non-zero buyout
    # price to detect it.
    buy_now_price = None
    bin_match = re.search(
      r'class="[^"]*bidorbuy(?!.*hidden)[^"]*"[^>]*>.*?([\d,]+)\s*YEN',
      html, re.DOTALL,
    )
    if bin_match:
      candidate = _decimal_or_none(bin_match.group(1))
      if candidate and candidate > 0:
        buy_now_price = candidate

    listing_type = ListingType.AUCTION
    if buy_now_price is not None:
      listing_type = ListingType.HYBRID

    # --- Images ---
    images = self._extract_images(html)

    # --- Description (embedded in an iframe on Buyee) ---
    description = self._extract_description(html)

    # --- Attributes ---
    attributes: dict[str, str] = {}
    if seller_info.get("item_condition"):
      attributes["condition_label"] = seller_info["item_condition"]
    if seller_info.get("early_finish"):
      attributes["early_finish"] = seller_info["early_finish"]
    if seller_info.get("bidder_rating_restriction"):
      attributes["bidder_rating_restriction"] = (
        seller_info["bidder_rating_restriction"]
      )
    if seller_info.get("item_quantity"):
      attributes["item_quantity"] = seller_info["item_quantity"]
    domestic_shipping = seller_info.get(
      "domestic_shipping_fee_responsibility", ""
    )
    if domestic_shipping:
      attributes["domestic_shipping"] = domestic_shipping[:200]

    # --- Final price (for ended auctions) ---
    final_price = None
    if status in (ListingStatus.SOLD, ListingStatus.UNSOLD):
      final_price = current_price

    return ScrapedListing(
      external_id=auction_id,
      url=url,
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency=_DEFAULT_CURRENCY,
      starting_price=starting_price,
      buy_now_price=buy_now_price,
      current_price=current_price,
      final_price=final_price,
      shipping_from_country="JP",
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
  # HTML fetching
  # ------------------------------------------------------------------

  def _get_html(
    self,
    url: str,
    *,
    params: Optional[dict[str, str]] = None,
  ) -> Optional[str]:
    """Fetch a page from Buyee using curl_cffi for TLS impersonation."""
    # --- Browser path ---
    if self._browser_enabled:
      try:
        if params:
          from urllib.parse import urlencode
          full_url = f"{url}?{urlencode(params)}"
        else:
          full_url = url
        return self._get_html_via_browser(full_url)
      except Exception as exc:
        logger.debug("Browser fetch failed for %s (%s), falling back to curl_cffi.", url, exc)

    try:
      self._rate_limit()
      logger.debug("GET %s (params=%s)", url, params)
      response = self._cffi_session.get(
        url,
        params=params,
        timeout=self.config.timeout,
      )
      response.encoding = "utf-8"

      if response.status_code == 404:
        logger.warning("404 for %s", url)
        return None

      if response.status_code != 200:
        logger.warning(
          "HTTP %d for %s", response.status_code, url,
        )
        return None

      return response.text

    except Exception as exception:
      logger.error("Request to %s failed: %s", url, exception)
      return None

  # ------------------------------------------------------------------
  # Parsing helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _extract_title(html: str) -> str:
    """Extract the listing title from the page."""
    # Buyee uses <h1 class="itemInformation__itemName">…</h1>.
    title_match = re.search(
      r'class="itemInformation__itemName"[^>]*>(.*?)</h1>',
      html, re.DOTALL,
    )
    if title_match:
      title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
      if title:
        return html_lib.unescape(title)

    # Fallback: <title> tag (strip the Buyee suffix).
    title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
    if title_match:
      title = title_match.group(1).strip()
      # Remove "  /【Buyee】 …" suffix.
      title = re.sub(r'\s*/\s*【Buyee】.*$', '', title).strip()
      if title:
        return html_lib.unescape(title)

    return "Untitled Yahoo Japan listing"

  @staticmethod
  def _extract_seller_section(html: str) -> dict[str, str]:
    """Parse the seller/item info section into a key-value dict.

    On Buyee, the ``class="itemSeller"`` section contains all the
    important metadata (seller, condition, starting price, bid count,
    opening/closing times, etc.) as label/value text pairs.
    """
    result: dict[str, str] = {}

    seller_match = re.search(
      r'class="itemSeller"(.*?)(?=class="(?:itemDescription|recommend|footer|cookiePolicyModal)"|</body>)',
      html, re.DOTALL,
    )
    if not seller_match:
      return result

    section = seller_match.group(1)
    # Strip HTML tags, keep line breaks between elements.
    clean = re.sub(r'<[^>]+>', '\n', section)
    lines = [line.strip() for line in clean.split('\n') if line.strip()]

    # The section contains label lines followed by value lines.
    # We use a simple heuristic: known labels map to the next line.
    _KNOWN_LABELS = {
      "Seller", "Percentage of good ratings", "Good", "Bad",
      "Item Condition", "Starting Price", "Item Quantity",
      "Domestic Shipping Fee Responsibility", "Winner",
      "Estimated Shipping Days (to Buyee Warehouse)",
      "International Shipping Fees and Delivery Timelines",
      "Auction ID", "Bidder Rating Restriction",
      "Number of Bids", "Highest Bidder",
      "Opening Time (JST)", "Closing Time (JST)",
      "Current Japan Time", "Early Finish",
    }

    current_label = None
    for line in lines:
      if line in _KNOWN_LABELS:
        current_label = line.lower().replace(" ", "_")
      elif current_label is not None:
        # Skip multi-line explanations (they start a new label).
        if line.startswith("If this is set") or line.startswith("The shipping"):
          # Explanatory text — skip it, keep the same label for
          # the *next* actual value line.
          continue
        result[current_label] = html_lib.unescape(line)
        current_label = None

    return result

  @staticmethod
  def _extract_images(html: str) -> list[ScrapedImage]:
    """Extract listing image URLs from the Buyee item page.

    The actual item images live inside the ``flexslider`` gallery and
    are served from ``cdnyauction.buyee.jp`` (without ``-pctr``).
    Images on ``cdnyauction-pctr.buyee.jp`` are recommendation
    thumbnails for *other* listings and must be excluded.

    Inside the gallery each slide contains a lazy-loaded ``<img>``
    with a ``data-src`` attribute pointing to the real image URL.
    """
    images: list[ScrapedImage] = []

    # Locate the flexslider (image gallery) section.
    gallery_match = re.search(
      r'class="flexslider"(.*?)(?:</ul>\s*</div>)',
      html, re.DOTALL,
    )
    gallery_html = gallery_match.group(1) if gallery_match else ""

    if gallery_html:
      # Extract data-src URLs from the gallery (lazy-loaded images).
      img_urls = re.findall(
        r'data-src\s*=\s*"(https?://cdnyauction\.buyee\.jp[^"]+)"',
        gallery_html,
      )
    else:
      # Fallback: look for js-smartPhoto anchors which wrap each
      # gallery slide and carry the full-size URL in their href.
      img_urls = re.findall(
        r'class="js-smartPhoto"\s+href="(https?://cdnyauction\.buyee\.jp[^"]+)"',
        html,
      )

    if not img_urls:
      # Last-resort fallback: any cdnyauction.buyee.jp image on
      # the page (excluding the -pctr recommendation thumbnails).
      img_urls = re.findall(
        r'(?:src|data-src)\s*=\s*"(https?://cdnyauction\.buyee\.jp[^"]+)"',
        html,
      )

    # De-duplicate while preserving order.
    seen: set[str] = set()
    unique_urls: list[str] = []
    for raw_url in img_urls:
      url = html_lib.unescape(raw_url)
      base_url = re.sub(r'\?.*$', '', url)
      if base_url not in seen:
        seen.add(base_url)
        unique_urls.append(url)

    for position, url in enumerate(unique_urls):
      images.append(ScrapedImage(source_url=url, position=position))

    return images

  @staticmethod
  def _extract_description(html: str) -> Optional[str]:
    """Extract the item description.

    On Buyee, the description is rendered inside an iframe
    (``#item_description_viewer``) using inline HTML from the seller.
    We can sometimes extract the raw description from the script that
    populates the iframe.
    """
    # The description is injected via JS into an iframe.
    desc_match = re.search(
      r'\.append\(\s*"([^"]*(?:出品物|商品内容|商品詳細|Description)[^"]*)"',
      html, re.DOTALL,
    )
    if desc_match:
      raw = desc_match.group(1)
      # Unescape JavaScript string escapes.
      raw = raw.replace('\\"', '"').replace("\\'", "'")
      # Strip HTML tags.
      text = re.sub(r'<[^>]+>', ' ', raw)
      text = re.sub(r'\s+', ' ', text).strip()
      if len(text) > 20:
        return text[:5000]

    return None

  @staticmethod
  def _derive_status(
    html: str,
    bid_count: int,
    end_time: Optional[datetime],
  ) -> ListingStatus:
    """Derive the listing status from page content and timing.

    The checks are ordered from most specific to least specific:

    1. The ``timeRemaining`` element in the page header — this is the
       most reliable indicator on Buyee.
    2. Explicit "auction has ended" text — but only in the page
       header area (before the item description), to avoid false
       positives from seller descriptions that mention
       "オークション終了時間" ("auction ending time").
    3. The parsed end time compared to the current time.
    """
    # Check time remaining display (most reliable Buyee indicator).
    time_match = re.search(
      r'class="itemInformation__timeRemaining"[^>]*>(.*?)<',
      html, re.DOTALL,
    )
    if time_match:
      remaining_text = time_match.group(1).strip()
      if remaining_text.lower() in ("ended", "終了", "closed"):
        return ListingStatus.SOLD if bid_count > 0 else ListingStatus.UNSOLD

    # Check for explicit ended indicators in the page header only.
    # Limit the search to the first part of the page (before the
    # item description section) to avoid matching seller text like
    # "オークション終了時間" (auction ending time).
    description_boundary = re.search(
      r'class="(?:itemDescription|flexslider)"', html,
    )
    header_html = html[:description_boundary.start()] if description_boundary else html[:30000]
    if re.search(r'This\s+auction\s+has\s+ended', header_html, re.I):
      return ListingStatus.SOLD if bid_count > 0 else ListingStatus.UNSOLD
    # Match "オークション終了" only as a standalone status label, not
    # when followed by characters that form a longer phrase such as
    # "オークション終了時間" (auction ending time).
    if re.search(r'オークション終了(?![時間前後まで])', header_html):
      return ListingStatus.SOLD if bid_count > 0 else ListingStatus.UNSOLD

    # Check end time.
    if end_time:
      now = datetime.now(timezone.utc)
      if end_time < now:
        return ListingStatus.SOLD if bid_count > 0 else ListingStatus.UNSOLD
      return ListingStatus.ACTIVE

    return ListingStatus.ACTIVE

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_url(url_or_id: str) -> str:
    """Accept a full Buyee URL or a bare auction ID."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare auction ID (e.g. "m1175690842").
    if re.match(r'^[a-zA-Z]\d{5,}$', url_or_id):
      return f"{_BASE_URL}{_ITEM_PATH}/{url_or_id}"
    raise ValueError(
      f"Cannot normalise Yahoo Japan URL: {url_or_id}"
    )

  @staticmethod
  def _extract_auction_id(url_or_id: str) -> str:
    """Extract the Yahoo auction ID from a URL or bare ID."""
    # From URL path.
    match = re.search(r'/auction/([a-zA-Z0-9]+)', url_or_id)
    if match:
      return match.group(1)
    # Bare ID.
    if re.match(r'^[a-zA-Z]\d{5,}$', url_or_id):
      return url_or_id
    raise ValueError(
      f"Cannot extract Yahoo Japan auction ID from: {url_or_id}"
    )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _url_encode_query(query: str) -> str:
  """URL-encode a search query for Buyee's path-based search.

  Buyee puts the query in the URL path (not a query parameter), so we
  need to percent-encode it.
  """
  from urllib.parse import quote
  return quote(query, safe="")


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
    return int(value.replace(",", ""))
  except (ValueError, TypeError):
    return None


def _parse_buyee_datetime(value: str) -> Optional[datetime]:
  """Parse a Buyee-formatted JST datetime string.

  Buyee shows dates like ``7 Feb 2026 21:41:41`` (in JST).
  """
  if not value:
    return None

  # Try common formats.
  for fmt in (
    "%d %b %Y %H:%M:%S",   # "7 Feb 2026 21:41:41"
    "%d %B %Y %H:%M:%S",   # "7 February 2026 21:41:41"
    "%Y-%m-%d %H:%M:%S",   # "2026-02-07 21:41:41"
    "%Y/%m/%d %H:%M:%S",   # "2026/02/07 21:41:41"
  ):
    try:
      naive = datetime.strptime(value.strip(), fmt)
      # The time is in JST (UTC+9).  Convert to UTC.
      jst_aware = naive.replace(tzinfo=_JST)
      return jst_aware.astimezone(timezone.utc)
    except ValueError:
      continue

  logger.debug("Could not parse Buyee datetime: '%s'", value)
  return None
