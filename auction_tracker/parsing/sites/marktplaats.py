"""Marktplaats parser.

Marktplaats (marktplaats.nl) is the Netherlands' largest classifieds
marketplace.  Listings are fixed-price (with optional negotiation).
Search is served as server-rendered HTML with listing cards; detail
pages embed a ``window.__CONFIG__`` JSON object with structured
listing data (``itemId``, ``title``, ``priceInfo``, ``seller``,
``gallery``) and Schema.org ``Product`` JSON-LD.

* **Search URL**: ``GET /q/QUERY/`` with ``/p/N/`` for page ``N`` (page
  ``1`` omits the ``/p/`` segment).
* **Listing URL**: ``/v/{category}/{subcategory}/a{ID}-{slug}``
* **External ID**: digits after the ``a`` prefix in the path segment
  (e.g. ``a1524398513`` → ``1524398513``).
* **Currency**: EUR (prices in ``priceInfo`` are cents).
"""

from __future__ import annotations

import json
import logging
import re
from decimal import Decimal
from urllib.parse import quote_plus

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

_BASE_URL = "https://www.marktplaats.nl"
_DEFAULT_CURRENCY = "EUR"


def _extract_braced_json_object(html: str, prefix: str) -> dict | None:
  """Parse the first balanced JSON object after ``prefix``."""
  index = html.find(prefix)
  if index < 0:
    return None
  start = index + len(prefix)
  text = html[start:].lstrip()
  if not text.startswith("{"):
    return None
  depth = 0
  for offset, char in enumerate(text):
    if char == "{":
      depth += 1
    elif char == "}":
      depth -= 1
      if depth == 0:
        try:
          return json.loads(text[: offset + 1])
        except json.JSONDecodeError:
          return None
  return None


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


@ParserRegistry.register
class MarktplaatsParser(Parser):
  """Parser for marktplaats.nl classified listings."""

  @property
  def website_name(self) -> str:
    return "marktplaats"

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
    encoded = quote_plus(query)
    page = int(kwargs.get("page", 1))
    if page > 1:
      return f"{_BASE_URL}/q/{encoded}/p/{page}/"
    return f"{_BASE_URL}/q/{encoded}/"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/a(\d+)-", url)
    return match.group(1) if match else None

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)
    results: list[ScrapedSearchResult] = []
    seen: set[str] = set()

    for anchor in tree.css('a[href^="/v/"]'):
      href = anchor.attributes.get("href", "")
      if not re.search(r"/a\d+-", href):
        continue
      external_id = self.extract_external_id(href)
      if not external_id or external_id in seen:
        continue
      seen.add(external_id)

      item_url = href if href.startswith("http") else f"{_BASE_URL}{href}"

      title_node = anchor.css_first('span[class*="Listing-title"]')
      if title_node is None:
        title_node = anchor.css_first('span[class*="ListingTitle"]')
      title = title_node.text(strip=True) if title_node else ""
      if not title:
        img_tag = anchor.css_first("img")
        if img_tag:
          title = img_tag.attributes.get("title") or img_tag.attributes.get("alt") or ""
      if not title:
        continue

      price = None
      price_node = anchor.css_first('h5[class*="ListingPrice"]')
      if price_node is None:
        price_node = anchor.css_first('h5[class*="Listing-price"]')
      if price_node:
        price = _parse_nl_price_text(price_node.text(strip=True))

      image_url = None
      img_tag = anchor.css_first("img[src]")
      if img_tag:
        image_url = img_tag.attributes.get("src") or img_tag.attributes.get("data-src")

      results.append(
        ScrapedSearchResult(
          external_id=external_id,
          url=item_url,
          title=title,
          current_price=price,
          currency=_DEFAULT_CURRENCY,
          listing_type="buy_now",
          image_url=image_url,
        ),
      )

    logger.info("Marktplaats search: parsed %d results", len(results))
    return results

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)
    tree = HTMLParser(html)

    config = _extract_braced_json_object(html, "window.__CONFIG__ = ")
    if not config or "listing" not in config:
      raise ValueError("No window.__CONFIG__.listing found on page")

    listing = config["listing"]
    item_id = listing.get("itemId", "")
    external_id = re.sub(r"^a", "", str(item_id)) if item_id else ""
    if not external_id:
      raise ValueError("No listing itemId in __CONFIG__")

    title = listing.get("title", "")
    if not title:
      raise ValueError("No listing title in __CONFIG__")

    price_info = listing.get("priceInfo") or {}
    cents = price_info.get("priceCents")
    price = None
    if cents is not None:
      price = Decimal(cents) / Decimal(100)
    price_type = price_info.get("priceType")

    description = None
    product = _extract_jsonld_product(html)
    if product:
      description = product.get("description")

    seller = _extract_seller_from_config(listing.get("seller") or {})

    image_urls: list[str] = []
    gallery = listing.get("gallery") or {}
    for image_url in gallery.get("imageUrls") or []:
      if image_url.startswith("//"):
        image_urls.append("https:" + image_url)
      elif image_url.startswith("http"):
        image_urls.append(image_url)
      else:
        image_urls.append(_BASE_URL + image_url)

    shipping_cost = _extract_shipping_eur(listing.get("shippingInformation"))

    attributes: dict[str, str] = {}
    if price_type:
      attributes["price_type"] = str(price_type)
    if listing.get("adType"):
      attributes["ad_type"] = str(listing["adType"])

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type="buy_now",
      currency=_DEFAULT_CURRENCY,
      current_price=price,
      buy_now_price=price,
      shipping_cost=shipping_cost,
      status="active",
      image_urls=image_urls,
      seller=seller,
      attributes=attributes,
    )


def _parse_nl_price_text(text: str) -> Decimal | None:
  """Parse Dutch price text like ``€ 12,50`` or ``€\xa012,50``."""
  if not text:
    return None
  cleaned = re.sub(r"[^\d,.]", "", text.replace("\xa0", " "))
  cleaned = cleaned.replace(".", "").replace(",", ".")
  if not cleaned:
    return None
  try:
    return Decimal(cleaned)
  except Exception:
    return None


def _extract_seller_from_config(seller: dict) -> ScrapedSeller | None:
  seller_id = seller.get("id")
  name = seller.get("name")
  if seller_id is None and not name:
    return None
  external_id = str(seller_id) if seller_id is not None else str(name)
  page_url = seller.get("pageUrl", "")
  profile_url = None
  if page_url:
    profile_url = page_url if page_url.startswith("http") else _BASE_URL + page_url

  return ScrapedSeller(
    external_id=external_id,
    username=name or external_id,
    display_name=name,
    country="NL",
    profile_url=profile_url,
  )


def _extract_shipping_eur(shipping: object) -> Decimal | None:
  """Best-effort shipping price from ``shippingInformation``."""
  if not isinstance(shipping, dict):
    return None
  for key in ("priceCents", "shippingPriceCents", "costCents"):
    cents = shipping.get(key)
    if isinstance(cents, int):
      return Decimal(cents) / Decimal(100)
  return None
