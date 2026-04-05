"""Yahoo Japan Auctions parser (via Buyee proxy).

Yahoo Japan blocks EEA/UK access for GDPR compliance. International
users must use a proxy service such as Buyee (buyee.jp), which provides
English-language access to Yahoo Japan Auction listings.

Key technical facts:

* All URLs point to ``buyee.jp``.
* Search: ``/item/search/query/{query}?translationType=1&page={n}``.
* Item detail: ``/item/yahoo/auction/{auction_id}``.
* The item page embeds all data directly in HTML (no JSON APIs or
  embedded state blobs). Data is extracted with regex.
* Data available: title, current price (JPY), starting price, seller
  name/rating, condition, bid count, watcher count, opening/closing
  times (JST), images, description (best-effort from iframe script).
* No bid history is available through Buyee.
* Fixed end times (no extensions). Monitoring is ``snapshot`` strategy.
* Currency is always JPY.
"""

from __future__ import annotations

import html as html_module
import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

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

_BASE_URL = "https://buyee.jp"
_SEARCH_PATH = "/item/search/query"
_ITEM_PATH = "/item/yahoo/auction"

_JST = timezone(timedelta(hours=9))

_CONDITION_MAP: dict[str, str] = {
  "new": "new",
  "unused": "new",
  "close to unused": "like_new",
  "no noticeable scratches or stains": "very_good",
  "slightly damaged/dirty": "good",
  "a little damaged/dirty": "good",
  "some scratches or stains": "good",
  "damaged/dirty": "fair",
  "overall condition bad": "poor",
  "for parts": "for_parts",
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class YahooJapanParser(Parser):
  """Parser for Yahoo Japan Auctions via the Buyee proxy."""

  @property
  def website_name(self) -> str:
    return "yahoo_japan"

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

  def build_search_url(self, query: str, **kwargs) -> str:
    page = int(kwargs.get("page", 1))
    encoded = quote(query, safe="")
    params = "translationType=1"
    if page > 1:
      params += f"&page={page}"
    return f"{_BASE_URL}{_SEARCH_PATH}/{encoded}?{params}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/auction/([a-zA-Z0-9]+)", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url=url)
    results: list[ScrapedSearchResult] = []
    cards = html.split('class="itemCard">')

    for card_html in cards[1:]:
      link_match = re.search(
        r'href="(/item/[^"]+/auction/([a-zA-Z0-9]+))"',
        card_html,
      )
      if not link_match:
        continue

      item_path = link_match.group(1)
      auction_id = link_match.group(2)
      item_url = f"{_BASE_URL}{item_path}"

      title = _extract_card_title(card_html)

      price = None
      price_match = re.search(r"([\d,]+)\s*(?:YEN|JPY)", card_html)
      if price_match:
        price = _decimal_or_none(price_match.group(1))

      image_url = None
      img_match = re.search(
        r'(?:src|data-src)\s*=\s*"(https?://[^"]*(?:auctions\.c\.yimg\.jp|cdnyauction)[^"]*)"',
        card_html,
      )
      if img_match:
        image_url = html_module.unescape(img_match.group(1))

      results.append(ScrapedSearchResult(
        external_id=auction_id,
        url=item_url,
        title=title,
        current_price=price,
        currency="JPY",
        image_url=image_url,
        end_time=None,
        listing_type="auction",
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url=url)
    auction_id = _extract_auction_id_from_url(url) if url else ""

    title = _extract_title(html)
    current_price = _extract_current_price(html)
    seller_info = _extract_seller_section(html)

    seller = _parse_seller(seller_info)
    starting_price = _extract_starting_price(seller_info)
    condition_label = seller_info.get("item_condition", "").lower().strip()
    condition = _CONDITION_MAP.get(condition_label)
    bid_count = _int_or_none(seller_info.get("number_of_bids")) or 0
    watcher_count = _extract_watcher_count(html)

    start_time = _parse_buyee_datetime(
      seller_info.get("opening_time_(jst)", ""),
    )
    end_time = _parse_buyee_datetime(
      seller_info.get("closing_time_(jst)", ""),
    )

    status = _derive_status(html, bid_count, end_time)
    buy_now_price = _extract_buy_now_price(html)

    listing_type = "auction"
    if buy_now_price is not None:
      listing_type = "hybrid"

    image_urls = _extract_image_urls(html)
    description = _extract_description(html)

    final_price = current_price if status in ("sold", "unsold") else None

    attributes: dict[str, str] = {}
    if seller_info.get("item_condition"):
      attributes["condition_label"] = seller_info["item_condition"]
    if seller_info.get("early_finish"):
      attributes["early_finish"] = seller_info["early_finish"]
    if seller_info.get("item_quantity"):
      attributes["item_quantity"] = seller_info["item_quantity"]

    return ScrapedListing(
      external_id=auction_id,
      url=url or f"{_BASE_URL}{_ITEM_PATH}/{auction_id}",
      title=title,
      description=description,
      listing_type=listing_type,
      condition=condition,
      currency="JPY",
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
      image_urls=image_urls,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# HTML extraction helpers
# ------------------------------------------------------------------


def _extract_card_title(card_html: str) -> str:
  """Extract title from a search result card."""
  alt_match = re.search(r'alt="([^"]+)"', card_html)
  if alt_match:
    title = html_module.unescape(alt_match.group(1)).strip()
    if title:
      return title
  name_match = re.search(
    r'class="[^"]*itemCard__itemName[^"]*"[^>]*>(.*?)<',
    card_html, re.DOTALL,
  )
  if name_match:
    return re.sub(r"<[^>]+>", "", name_match.group(1)).strip()
  return ""


def _extract_title(html: str) -> str:
  """Extract listing title from the detail page."""
  title_match = re.search(
    r'class="itemInformation__itemName"[^>]*>(.*?)</h1>',
    html, re.DOTALL,
  )
  if title_match:
    title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
    if title:
      return html_module.unescape(title)

  title_match = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
  if title_match:
    title = title_match.group(1).strip()
    title = re.sub(r"\s*/\s*【Buyee】.*$", "", title).strip()
    if title:
      return html_module.unescape(title)

  return "Untitled Yahoo Japan listing"


def _extract_current_price(html: str) -> Decimal | None:
  """Extract the current price from the detail page."""
  price_match = re.search(
    r'class="current_price".*?<dd[^>]*>.*?([\d,]+)\s*YEN',
    html, re.DOTALL,
  )
  if price_match:
    return _decimal_or_none(price_match.group(1))
  return None


def _extract_seller_section(html: str) -> dict[str, str]:
  """Parse the seller/item metadata section into key-value pairs.

  The ``class="itemSeller"`` section contains metadata as
  label/value text pairs.
  """
  result: dict[str, str] = {}

  seller_match = re.search(
    r'class="itemSeller"(.*?)(?=class="(?:itemDescription|recommend|footer|cookiePolicyModal)"|</body>)',
    html, re.DOTALL,
  )
  if not seller_match:
    return result

  section = seller_match.group(1)
  clean = re.sub(r"<[^>]+>", "\n", section)
  lines = [line.strip() for line in clean.split("\n") if line.strip()]

  known_labels = {
    "Seller", "Percentage of good ratings", "Good", "Bad",
    "Item Condition", "Starting Price", "Item Quantity",
    "Domestic Shipping Fee Responsibility", "Winner",
    "Auction ID", "Bidder Rating Restriction",
    "Number of Bids", "Highest Bidder",
    "Opening Time (JST)", "Closing Time (JST)",
    "Current Japan Time", "Early Finish",
  }

  current_label = None
  for line in lines:
    if line in known_labels:
      current_label = line.lower().replace(" ", "_")
    elif current_label is not None:
      if line.startswith("If this is set") or line.startswith("The shipping"):
        continue
      result[current_label] = html_module.unescape(line)
      current_label = None

  return result


def _extract_starting_price(seller_info: dict[str, str]) -> Decimal | None:
  """Extract starting price from seller info."""
  starting_str = seller_info.get("starting_price", "")
  sp_match = re.search(r"([\d,]+)\s*YEN", starting_str)
  if sp_match:
    return _decimal_or_none(sp_match.group(1))
  return None


def _extract_watcher_count(html: str) -> int | None:
  """Extract watcher count from the page."""
  watch_match = re.search(
    r'class="[^"]*g-feather-bidding[^"]*"[^>]*>\s*</i>\s*(\d+)',
    html,
  )
  if watch_match:
    return int(watch_match.group(1))
  return None


def _extract_buy_now_price(html: str) -> Decimal | None:
  """Extract buy-now price, ignoring hidden/zero elements."""
  bin_match = re.search(
    r'class="[^"]*bidorbuy(?!.*hidden)[^"]*"[^>]*>.*?([\d,]+)\s*YEN',
    html, re.DOTALL,
  )
  if bin_match:
    candidate = _decimal_or_none(bin_match.group(1))
    if candidate and candidate > 0:
      return candidate
  return None


def _extract_image_urls(html: str) -> list[str]:
  """Extract listing image URLs from the Buyee item page.

  Real images live on ``cdnyauction.buyee.jp`` (not ``-pctr`` which
  is recommendation thumbnails). De-duplicated by base URL.
  """
  gallery_match = re.search(
    r'class="flexslider"(.*?)(?:</ul>\s*</div>)',
    html, re.DOTALL,
  )
  gallery_html = gallery_match.group(1) if gallery_match else ""

  if gallery_html:
    img_urls = re.findall(
      r'data-src\s*=\s*"(https?://cdnyauction\.buyee\.jp[^"]+)"',
      gallery_html,
    )
  else:
    img_urls = re.findall(
      r'class="js-smartPhoto"\s+href="(https?://cdnyauction\.buyee\.jp[^"]+)"',
      html,
    )

  if not img_urls:
    img_urls = re.findall(
      r'(?:src|data-src)\s*=\s*"(https?://cdnyauction\.buyee\.jp[^"]+)"',
      html,
    )

  seen: set[str] = set()
  unique: list[str] = []
  for raw_url in img_urls:
    url = html_module.unescape(raw_url)
    base_url = re.sub(r"\?.*$", "", url)
    if base_url not in seen:
      seen.add(base_url)
      unique.append(url)

  return unique


def _extract_description(html: str) -> str | None:
  """Extract description from the iframe injection script."""
  desc_match = re.search(
    r'\.append\(\s*"([^"]*(?:\u51fa\u54c1\u7269|\u5546\u54c1\u5185\u5bb9|\u5546\u54c1\u8a73\u7d30|Description)[^"]*)"',
    html, re.DOTALL,
  )
  if desc_match:
    raw = desc_match.group(1)
    raw = raw.replace('\\"', '"').replace("\\'", "'")
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 20:
      return text[:5000]
  return None


def _extract_auction_id_from_url(url: str) -> str:
  """Extract auction ID from a Buyee URL."""
  match = re.search(r"/auction/([a-zA-Z0-9]+)", url)
  if match:
    return match.group(1)
  return ""


# ------------------------------------------------------------------
# Status derivation
# ------------------------------------------------------------------


def _derive_status(
  html: str, bid_count: int, end_time: datetime | None,
) -> str:
  """Derive listing status from page content and timing."""
  time_match = re.search(
    r'class="itemInformation__timeRemaining"[^>]*>(.*?)<',
    html, re.DOTALL,
  )
  if time_match:
    remaining_text = time_match.group(1).strip().lower()
    if remaining_text in ("ended", "\u7d42\u4e86", "closed"):
      return "sold" if bid_count > 0 else "unsold"

  description_boundary = re.search(
    r'class="(?:itemDescription|flexslider)"', html,
  )
  header_html = (
    html[:description_boundary.start()]
    if description_boundary
    else html[:30000]
  )
  if re.search(r"This\s+auction\s+has\s+ended", header_html, re.I):
    return "sold" if bid_count > 0 else "unsold"

  if end_time:
    now = datetime.now(timezone.utc)
    if end_time < now:
      return "sold" if bid_count > 0 else "unsold"
    return "active"

  return "active"


# ------------------------------------------------------------------
# Seller
# ------------------------------------------------------------------


def _parse_seller(seller_info: dict[str, str]) -> ScrapedSeller | None:
  """Build a ScrapedSeller from the seller section."""
  seller_name = seller_info.get("seller", "")
  if not seller_name:
    return None

  rating_pct = None
  pct_str = seller_info.get("percentage_of_good_ratings", "")
  if pct_str:
    pct_match = re.search(r"([\d.]+)", pct_str)
    if pct_match:
      rating_pct = float(pct_match.group(1))

  rating_good = _int_or_none(seller_info.get("good"))
  rating_bad = _int_or_none(seller_info.get("bad"))
  feedback_count = None
  if rating_good is not None:
    feedback_count = rating_good
    if rating_bad is not None:
      feedback_count += rating_bad

  return ScrapedSeller(
    external_id=seller_name,
    username=seller_name,
    display_name=seller_name,
    country="JP",
    rating=rating_pct,
    feedback_count=feedback_count,
  )


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _decimal_or_none(value: str | None) -> Decimal | None:
  """Convert a string to Decimal."""
  if not value:
    return None
  try:
    return Decimal(value.replace(",", ""))
  except (InvalidOperation, ValueError):
    return None


def _int_or_none(value: str | None) -> int | None:
  """Convert a string to int."""
  if not value:
    return None
  try:
    return int(value.replace(",", ""))
  except (ValueError, TypeError):
    return None


def _parse_buyee_datetime(value: str) -> datetime | None:
  """Parse a Buyee-formatted JST datetime string.

  Buyee shows dates like ``7 Feb 2026 21:41:41`` in JST (UTC+9).
  """
  if not value:
    return None

  for fmt in (
    "%d %b %Y %H:%M:%S",
    "%d %B %Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
  ):
    try:
      naive = datetime.strptime(value.strip(), fmt)
      jst_aware = naive.replace(tzinfo=_JST)
      return jst_aware.astimezone(timezone.utc)
    except ValueError:
      continue

  return None
