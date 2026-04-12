"""Ricardo parser.

Ricardo (ricardo.ch) is Switzerland's largest online marketplace.
Listings may be auctions (with bids and optional ``Sofort kaufen`` /
buy-now) or fixed-price classifieds.  Search result pages are largely
server-rendered HTML with stacked product cards inside a
``regular-results`` container.

* **Search URL**: ``GET /{locale}/s/{query}/`` where ``query`` uses
  hyphen-separated words (e.g. ``fountain-pen``).  Optional
  ``?page=N`` for pagination.
* **Listing URL**: ``/{locale}/a/{slug-with-id}/`` where the numeric
  article id is the trailing ``-1234567890`` segment before ``/``.
* **Currency**: CHF.
"""

from __future__ import annotations

import json
import logging
import re
from decimal import Decimal
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

_BASE_HOST = "https://www.ricardo.ch"
_DEFAULT_CURRENCY = "CHF"


def _slug_query(query: str) -> str:
  """Turn a free-text query into Ricardo's hyphenated path segment."""
  cleaned = query.strip().lower()
  cleaned = re.sub(r"[^\w\s-]", "", cleaned, flags=re.UNICODE)
  cleaned = re.sub(r"[\s_]+", "-", cleaned)
  cleaned = re.sub(r"-+", "-", cleaned).strip("-")
  return cleaned or "suche"


def _extract_jsonld_product(html: str) -> dict | None:
  blocks = re.findall(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    html, re.DOTALL | re.IGNORECASE,
  )
  for block in blocks:
    try:
      data = json.loads(block)
    except json.JSONDecodeError:
      continue
    if isinstance(data, dict) and data.get("@type") == "Product":
      return data
  return None


def _parse_chf_amount(text: str) -> Decimal | None:
  if not text:
    return None
  cleaned = re.sub(r"(CHF|Fr\.|CHf)\s*", "", text, flags=re.IGNORECASE)
  cleaned = cleaned.replace("'", "").strip()
  cleaned = cleaned.replace(",", ".")
  try:
    return Decimal(cleaned)
  except Exception:
    return None


@ParserRegistry.register
class RicardoParser(Parser):
  """Parser for ricardo.ch marketplace listings."""

  @property
  def website_name(self) -> str:
    return "ricardo"

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

  def build_search_url(self, query: str, **kwargs) -> str:
    locale = str(kwargs.get("locale", "de")).lower()
    if len(locale) != 2:
      locale = "de"
    slug = _slug_query(query)
    page = int(kwargs.get("page", 1))
    base = f"{_BASE_HOST}/{locale}/s/{quote(slug, safe='-')}/"
    if page > 1:
      return f"{base}?page={page}"
    return base

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/a/[^/]+-(\d+)/?(?:\?|$|#)", url)
    if match:
      return match.group(1)
    match = re.search(r"-(\d+)/?(?:\?|$|#)", url)
    return match.group(1) if match else None

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)
    results: list[ScrapedSearchResult] = []
    seen: set[str] = set()

    matches = list(re.finditer(r'href="(/[a-z]{2}/a/[^"]+)"', html))
    for index, match in enumerate(matches):
      href = match.group(1)
      if not re.search(r"/a/.+-\d+/?$", href):
        continue
      slug_part = href.rstrip("/").split("/a/", 1)[-1]
      id_match = re.search(r"-(\d+)$", slug_part)
      if not id_match:
        continue
      external_id = id_match.group(1)
      if external_id in seen:
        continue
      seen.add(external_id)

      item_url = href if href.startswith("http") else _BASE_HOST + href

      end = matches[index + 1].start() if index + 1 < len(matches) else match.start() + 12000
      chunk = html[match.start() : end]

      title = ""
      title_match = re.search(
        r'class="[^"]*mui-knftza[^"]*"[^>]*>([^<]+)<',
        chunk,
      )
      if title_match:
        title = title_match.group(1).strip()
      if not title:
        alt_match = re.search(r'alt="([^"]+)"', chunk)
        if alt_match:
          title = alt_match.group(1).strip()
      if not title:
        continue

      current_price, buy_now_price, listing_type = _parse_ricardo_card_prices(chunk)

      image_url = None
      image_match = re.search(
        r'src="(https://img\.ricardostatic\.ch/[^"]+)"',
        chunk,
      )
      if image_match:
        image_url = image_match.group(1)

      bid_count = None
      bid_match = re.search(r"\((\d+)\s*Gebote\)", chunk)
      if bid_match:
        bid_count = int(bid_match.group(1))

      results.append(
        ScrapedSearchResult(
          external_id=external_id,
          url=item_url,
          title=title,
          current_price=current_price or buy_now_price,
          currency=_DEFAULT_CURRENCY,
          listing_type=listing_type,
          image_url=image_url,
          bid_count=bid_count,
        ),
      )

    logger.info("Ricardo search: parsed %d results", len(results))
    return results

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)

    product = _extract_jsonld_product(html)
    title = ""
    description = None
    current_price = None
    buy_now_price = None
    currency = _DEFAULT_CURRENCY
    image_urls: list[str] = []

    if product:
      title = product.get("name", "") or ""
      description = product.get("description")
      offers = product.get("offers") or {}
      if isinstance(offers, dict):
        raw_price = offers.get("price")
        currency = offers.get("priceCurrency", _DEFAULT_CURRENCY) or _DEFAULT_CURRENCY
        if raw_price is not None:
          try:
            current_price = Decimal(str(raw_price))
          except Exception:
            current_price = None
      for image_value in product.get("image") or []:
        if isinstance(image_value, str) and image_value not in image_urls:
          fixed = image_value if image_value.startswith("http") else "https:" + image_value
          image_urls.append(fixed)

    if not title:
      heading = tree.css_first("h1")
      if heading:
        title = heading.text(strip=True)
    if not title:
      meta_title = re.search(
        r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"',
        html,
        re.IGNORECASE,
      )
      if meta_title:
        title = meta_title.group(1).strip()

    if not title:
      raise ValueError("No listing title found on page")

    external_id = self.extract_external_id(url)
    if not external_id:
      match = re.search(r"/a/[^/]+-(\d+)/", html)
      external_id = match.group(1) if match else ""
    if not external_id:
      raise ValueError("Could not determine Ricardo article id")

    if current_price is None:
      body_text = tree.body.text() if tree.body else ""
      chf_match = re.search(
        r"(?:Sofort\s+kaufen|Kaufpreis|Preis)[^\d]{0,40}([\d']+(?:\.\d{2})?)\s*CHF",
        body_text,
        re.IGNORECASE,
      )
      if chf_match:
        current_price = _parse_chf_amount(chf_match.group(1))

    seller = _extract_ricardo_seller(tree, html)

    listing_type = "buy_now"
    if re.search(r"Gebot|Gebote|Auktion", tree.body.text() if tree.body else "", re.I):
      listing_type = "auction"

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type=listing_type,
      currency=currency,
      current_price=current_price,
      buy_now_price=buy_now_price or current_price,
      status="active",
      image_urls=image_urls,
      seller=seller,
    )


def _parse_ricardo_card_prices(
  card_html: str,
) -> tuple[Decimal | None, Decimal | None, str]:
  """Extract current bid, buy-now, and listing type from a search card."""
  plain = re.sub(r"<[^>]+>", " ", card_html)
  plain = re.sub(r"\s+", " ", plain)

  current = None
  buy_now = None
  bid_match = re.search(
    r"([\d']+(?:\.\d{2})?)\s*\(\s*(\d+)\s*Gebote\s*\)",
    plain,
  )
  if bid_match:
    current = _parse_chf_amount(bid_match.group(1))

  instant_match = re.search(
    r"([\d']+(?:\.\d{2})?)\s*Sofort\s*kaufen",
    plain,
    re.IGNORECASE,
  )
  if instant_match:
    buy_now = _parse_chf_amount(instant_match.group(1))

  if bid_match or re.search(r"\(\s*\d+\s*Gebote\s*\)", plain, re.IGNORECASE):
    listing_type = "auction"
  else:
    listing_type = "buy_now"

  if current is None and buy_now is None:
    loose = re.search(r"([\d']+(?:\.\d{2})?)\s*CHF", plain, re.IGNORECASE)
    if loose:
      buy_now = _parse_chf_amount(loose.group(1))

  return current, buy_now, listing_type


def _extract_ricardo_seller(tree: HTMLParser, html: str) -> ScrapedSeller | None:
  body = tree.body.text() if tree.body else ""
  name_match = re.search(
    r"(?:Verkäufer|Verkaufer|Vendeur)\s*[:\s]+([^\n\r]{2,60})",
    body,
    re.IGNORECASE,
  )
  if name_match:
    name = name_match.group(1).strip()
    return ScrapedSeller(
      external_id=name,
      username=name,
      display_name=name,
      country="CH",
    )
  return None
