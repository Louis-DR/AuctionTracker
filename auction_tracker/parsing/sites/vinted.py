"""Vinted parser.

Vinted is a peer-to-peer fashion marketplace. Listings have a fixed
price (sometimes negotiable) and no end date. Items disappear when
the seller marks them as sold, the buyer completes a transaction, or
the item is deleted.

Key technical facts used by this parser:

* The site is a client-side React SPA. Camoufox navigates to the web
  page URL, waits for networkidle, and returns ``page.content()`` — a
  fully-rendered DOM including all data the React app has populated.
  This parser extracts data directly from that rendered HTML.
* ``<meta property="og:*">`` tags are emitted server-side and are the
  most reliable source for title, description, and image.
* Item images are also preloaded in ``<head>`` with
  ``<link rel="preload" as="image" href="…">`` pointing to the Vinted
  CDN — they are always present even before React runs.
* Prices appear in JSON-LD ``<script type="application/ld+json">``
  blocks (schema.org Product) when present, and in rendered price
  elements otherwise.
* Search results are rendered by React into item-card ``<article>``
  elements. Each card contains a link whose ``href`` includes the item
  slug (``/items/{id}-{slug}``), from which the ID is extracted.
* Prices are in the seller's local currency (usually EUR).
* Condition vocabulary: new_with_tags, new_without_tags, very_good,
  good, satisfactory. Detected from rendered ``<span>`` text.
* A "Vendu" / "Sold" / "Verkauft" … badge in the DOM signals a sold
  listing; otherwise the listing is treated as active.
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

from selectolax.parser import HTMLParser

from auction_tracker.parsing.base import (
  Parser,
  ParserBlocked,
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

_DEFAULT_DOMAIN = "www.vinted.fr"
_ITEMS_PER_PAGE = 96

# Sold labels across all Vinted locales.
_SOLD_LABELS = frozenset({
  "vendu", "sold", "verkauft", "vendido", "venduto",
  "verkocht", "prodano", "eladva", "solgt",
})

# Vinted CDN hostname for item photos.
_VINTED_CDN_HOSTS = ("images1.vinted.net", "images2.vinted.net",
                     "images3.vinted.net", "images4.vinted.net",
                     "images.vinted.net")

# Condition labels in French/English/German/Spanish/Italian/Dutch.
_CONDITION_TEXT_MAP: dict[str, str] = {
  # French
  "neuf avec étiquettes": "new",
  "neuf sans étiquettes": "like_new",
  "très bon état": "very_good",
  "bon état": "good",
  "état satisfaisant": "fair",
  # English
  "new with tags": "new",
  "new without tags": "like_new",
  "very good": "very_good",
  "good": "good",
  "satisfactory": "fair",
  # German
  "neu mit etikett": "new",
  "neu ohne etikett": "like_new",
  "sehr gut": "very_good",
  "gut": "good",
  "befriedigend": "fair",
  # Spanish
  "nuevo con etiquetas": "new",
  "nuevo sin etiquetas": "like_new",
  "muy buen estado": "very_good",
  "buen estado": "good",
  "estado aceptable": "fair",
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
    """Build a Vinted catalog search page URL.

    Optional kwargs:
      domain: Regional domain (e.g. ``"vinted.fr"``).
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
    return f"https://{domain}/catalog?{urlencode(params)}"

  def build_fetch_url(self, url: str) -> str:
    """Return the canonical HTML item page URL.

    Rewrites any previously-stored internal ``/api/v2/`` URLs back to
    the regular ``/items/{id}`` web-page URL so that Camoufox navigates
    to the rendered HTML page rather than a JSON endpoint.
    """
    api_match = re.search(r"/api/v2/items/(\d+)", url)
    if api_match:
      item_id = api_match.group(1)
      parsed = urlparse(url)
      return f"https://{parsed.netloc}/items/{item_id}"
    return url

  def extract_external_id(self, url: str) -> str | None:
    return _extract_id_from_url(url)

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(
    self, html: str, url: str = "",
  ) -> list[ScrapedSearchResult]:
    """Parse search results from a fully-rendered catalog HTML page.

    Camoufox navigates to the catalog URL and waits for networkidle,
    so every item card is present in the rendered DOM.  We locate all
    links whose ``href`` targets a Vinted item page (``/items/…``),
    extract the item ID, and build basic search results from whatever
    data is available within each card.

    If the response looks like a raw JSON API payload (HTTP transport
    fallback), we parse it directly.  If the HTML is an un-rendered SPA
    shell with no item links, we raise ``ParserBlocked``.
    """
    domain = _domain_from_url(url)

    # Detect raw JSON API response (HTTP fallback hitting old API URL).
    stripped = html.strip()
    if stripped.startswith("{"):
      return _parse_search_from_json(html, url, domain)

    check_html_for_blocking(html, url)

    tree = HTMLParser(html)
    results = _extract_search_results_from_dom(tree, domain)

    if not results:
      # If no items found at all but the page looks like a full render,
      # return an empty list (search may have genuinely zero results).
      body_text_len = len(tree.body.text() if tree.body else "")
      if body_text_len < 2000:
        # Very little body text → un-rendered SPA shell.
        raise ParserBlocked(
          "Vinted catalog page appears to be an unrendered SPA shell "
          "— Camoufox browser transport required",
          url=url,
        )

    logger.info(
      "Vinted search (DOM): parsed %d results from %s", len(results), url,
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    """Parse a listing from a fully-rendered Vinted item HTML page.

    Camoufox navigates to the canonical ``/items/{id}-{slug}`` URL and
    returns ``page.content()`` after networkidle — a complete rendered
    DOM.  We parse:
    * ``og:`` meta tags (server-emitted, always present) for title,
      description, canonical URL, and primary image.
    * ``<link rel="preload" as="image">`` tags in ``<head>`` for all
      item photos preloaded by the server.
    * JSON-LD ``<script type="application/ld+json">`` for price.
    * Rendered price, seller, and condition elements in the body.

    If the HTML is an un-rendered SPA shell (Camoufox transport not
    used, or page failed to render), we raise ``ParserBlocked``.

    If the response is a raw JSON API payload, we parse it directly
    (HTTP fallback path).
    """
    # Detect raw JSON API response (HTTP fallback).
    stripped = html.strip()
    if stripped.startswith("{"):
      with contextlib.suppress(Exception):
        data = json.loads(stripped)
        item = data.get("item")
        if isinstance(item, dict) and item.get("id"):
          return _parse_item_detail_from_json(item, url)
        if "message_code" in stripped or "invalid_authentication" in stripped:
          raise ParserBlocked(
            "Vinted API returned an authentication error — "
            "Camoufox browser transport required",
            url=url,
          )

    check_html_for_blocking(html, url)

    tree = HTMLParser(html)

    # Determine item ID.
    item_id = _extract_id_from_url(url)
    if not item_id:
      og_url = _meta_content(tree, "og:url")
      if og_url:
        item_id = _extract_id_from_url(og_url)
    if not item_id:
      # Last resort: any link in the page targeting /items/{id}.
      for node in tree.css('link[rel="canonical"]'):
        canonical = node.attributes.get("href", "")
        item_id = _extract_id_from_url(canonical)
        if item_id:
          break

    if not item_id:
      raise ValueError("Could not determine Vinted item ID from the page")

    # Title.
    title = _meta_content(tree, "og:title") or ""
    title = re.sub(
      r"\s*[|\u2014\-]\s*Vinted\s*$", "", title, flags=re.IGNORECASE,
    ).strip()
    if not title:
      h1 = tree.css_first("h1")
      if h1:
        title = h1.text(strip=True)

    # Description.
    description = _meta_content(tree, "og:description") or None

    # Canonical URL.
    canonical_url = _meta_content(tree, "og:url") or url

    # Images: server-preloaded CDN links, then og:image.
    image_urls = _extract_preloaded_images(tree)
    if not image_urls:
      og_image = _meta_content(tree, "og:image")
      if og_image:
        image_urls = [og_image]

    # Price and currency.
    price, currency = _extract_price_from_dom(tree)

    # Status: look for "sold" / "vendu" etc. in the rendered body.
    status = _detect_status(tree, html)

    # Condition: scan rendered badge/span text.
    condition = _extract_condition_from_dom(tree)

    # Seller: find the seller profile link.
    seller = _extract_seller_from_dom(tree, _domain_from_url(canonical_url))

    # If title is empty and there are no images, the page probably
    # didn't render — raise ParserBlocked so Camoufox retries.
    if not title and not image_urls:
      body_text = tree.body.text() if tree.body else ""
      if len(body_text) < 1000:
        raise ParserBlocked(
          "Vinted item page appears to be an unrendered SPA shell "
          "— Camoufox browser transport required",
          url=url,
        )
      raise ValueError(
        f"Could not extract title or images from Vinted item page for {url}",
      )

    return ScrapedListing(
      external_id=item_id,
      url=canonical_url,
      title=title,
      description=description,
      listing_type="buy_now",
      condition=condition,
      currency=currency,
      buy_now_price=price,
      current_price=price,
      status=status,
      image_urls=image_urls,
      seller=seller,
    )


# ------------------------------------------------------------------
# DOM extraction helpers — item detail
# ------------------------------------------------------------------


def _meta_content(tree: HTMLParser, prop: str) -> str:
  """Return the ``content`` of an ``og:`` meta tag, or empty string."""
  node = tree.css_first(f'meta[property="{prop}"]')
  if node is not None:
    return (node.attributes.get("content") or "").strip()
  # Also check ``name`` attribute (used by some tags like description).
  node = tree.css_first(f'meta[name="{prop}"]')
  if node is not None:
    return (node.attributes.get("content") or "").strip()
  return ""


def _extract_preloaded_images(tree: HTMLParser) -> list[str]:
  """Return Vinted CDN images preloaded in the document head."""
  urls: list[str] = []
  for link in tree.css('link[rel="preload"][as="image"]'):
    href = (link.attributes.get("href") or "").strip()
    if any(host in href for host in _VINTED_CDN_HOSTS):
      urls.append(href)
  return urls


def _extract_price_from_dom(tree: HTMLParser) -> tuple[Decimal | None, str]:
  """Return ``(price, currency)`` extracted from the rendered page.

  Tries in order:
  1. JSON-LD ``<script type="application/ld+json">`` with schema.org
     ``Product`` or ``Offer`` types — most reliable when present.
  2. ``[itemprop="price"]`` element.
  3. Elements with ``data-testid`` containing ``"price"``.
  4. Any ``<span>`` or ``<p>`` whose text matches a price pattern.
  """
  currency = "EUR"

  # 1. JSON-LD.
  for script in tree.css('script[type="application/ld+json"]'):
    with contextlib.suppress(Exception):
      data = json.loads(script.text() or "")
      if not isinstance(data, dict):
        continue
      # Handle list of items (``@graph`` pattern).
      if "@graph" in data:
        items = data["@graph"]
      elif isinstance(data, list):
        items = data
      else:
        items = [data]
      for item in items:
        if not isinstance(item, dict):
          continue
        offers = item.get("offers")
        if isinstance(offers, dict):
          price_val = offers.get("price")
          currency = offers.get("priceCurrency") or currency
          price = _decimal_or_none(price_val)
          if price is not None:
            return price, currency
        # Direct price on the item.
        price_val = item.get("price")
        if price_val is not None:
          price = _decimal_or_none(price_val)
          if price is not None:
            return price, currency

  # 2. Microdata.
  node = tree.css_first('[itemprop="price"]')
  if node is not None:
    price_val = (
      node.attributes.get("content")
      or node.text(strip=True)
    )
    price = _parse_price_text(price_val)
    currency_node = tree.css_first('[itemprop="priceCurrency"]')
    if currency_node is not None:
      currency = (
        currency_node.attributes.get("content")
        or currency_node.text(strip=True)
        or currency
      )
    if price is not None:
      return price, currency

  # 3. data-testid containing "price".
  for selector in [
    '[data-testid="item-price"]',
    '[data-testid*="price"]',
  ]:
    node = tree.css_first(selector)
    if node is not None:
      price = _parse_price_text(node.text(strip=True))
      if price is not None:
        currency = _detect_currency(node.text(strip=True)) or currency
        return price, currency

  # 4. Text-pattern scan: walk ``<span>`` and ``<p>`` looking for a
  #    price pattern like "15,00 €" or "15.00 CHF".
  price_re = re.compile(
    r"""
    (?:^|[\s(])
    (\d{1,6}(?:[.,]\d{1,3})?)   # numeric amount
    \s*
    (€|EUR|CHF|£|GBP|\$|USD|PLN|CZK|HUF|SEK|DKK|NOK)
    |
    (€|EUR|CHF|£|GBP|\$|USD|PLN|CZK|HUF|SEK|DKK|NOK)
    \s*
    (\d{1,6}(?:[.,]\d{1,3})?)
    """,
    re.VERBOSE,
  )
  for node in tree.css("span, p, div"):
    text = node.text(strip=True)
    if not text or len(text) > 30:
      continue
    match = price_re.search(text)
    if match:
      amount_str = match.group(1) or match.group(4) or ""
      symbol = match.group(2) or match.group(3) or ""
      price = _decimal_or_none(amount_str.replace(",", "."))
      if price is not None:
        currency = _symbol_to_currency(symbol) or currency
        return price, currency

  return None, currency


def _detect_status(tree: HTMLParser, html: str) -> str:
  """Return ``"sold"`` if the page shows a sold indicator, else ``"active"``.

  Vinted pages always contain words like "vendu" or "sold" in seller
  stats, navigation, and other page chrome.  A naive body-text scan
  would flag every listing as sold.  Instead we check:

  1. JSON-LD ``offers.availability`` — ``SoldOut`` / ``Discontinued``
     / ``OutOfStock`` signals a sold item.
  2. Specific UI badge/overlay elements that Vinted renders on top of
     a sold item's images.
  3. The ``is_closed`` / ``can_buy`` flags emitted in inline JSON.
  """
  # 1. JSON-LD availability (most reliable when present).
  for script in tree.css('script[type="application/ld+json"]'):
    with contextlib.suppress(Exception):
      data = json.loads(script.text() or "")
      if not isinstance(data, dict):
        continue
      items = data.get("@graph", [data])
      for item in items:
        if not isinstance(item, dict):
          continue
        offers = item.get("offers")
        if isinstance(offers, dict):
          availability = (offers.get("availability") or "").lower()
          if any(keyword in availability
                 for keyword in ("soldout", "discontinued", "outofstock")):
            return "sold"

  # 2. Dedicated sold-overlay / badge elements.  Vinted wraps the sold
  #    indicator in elements with data-testid or class containing "sold"
  #    or "overlay". Only accept short text that matches known labels.
  for selector in (
    '[data-testid*="sold"]',
    '[data-testid*="overlay"]',
    '[class*="ItemStatus"]',
    '[class*="item-status"]',
    '.overlay--sold',
  ):
    for node in tree.css(selector):
      text = node.text(strip=True).lower()
      if text in _SOLD_LABELS:
        return "sold"

  # 3. Inline JSON flags (React hydration data, __NEXT_DATA__, etc.).
  if re.search(r'"is_closed"\s*:\s*true', html):
    return "sold"
  if re.search(r'"can_buy"\s*:\s*false', html):
    # "can_buy: false" with a loaded item page means the listing closed.
    # Verify the item is actually present to avoid false positives on
    # error pages.
    if tree.css_first('meta[property="og:title"]'):
      return "sold"

  return "active"


def _extract_condition_from_dom(tree: HTMLParser) -> str | None:
  """Return a normalised condition key by scanning the rendered body text."""
  body = tree.body
  if not body:
    return None
  text = body.text().lower()
  for label, condition in _CONDITION_TEXT_MAP.items():
    if label in text:
      return condition
  return None


def _extract_seller_from_dom(
  tree: HTMLParser, domain: str,
) -> ScrapedSeller | None:
  """Extract seller info from the rendered page.

  Looks for a link whose ``href`` contains ``/member/`` — which Vinted
  uses for seller profile URLs — and extracts the username from the
  link text or the URL slug.
  """
  for link in tree.css('a[href*="/member/"]'):
    href = link.attributes.get("href", "")
    if not href:
      continue

    # Extract member ID or slug from path: /member/{id}-{username}.
    member_match = re.search(r"/member/(\d+)(?:-([^/?#]+))?", href)
    if not member_match:
      continue

    member_id = member_match.group(1)
    slug = member_match.group(2) or ""
    username = link.text(strip=True) or slug or member_id

    # Build full profile URL.
    if href.startswith("/"):
      profile_url = f"https://{domain}{href}"
    else:
      profile_url = href

    return ScrapedSeller(
      external_id=member_id,
      username=username,
      display_name=username,
      profile_url=profile_url,
    )

  return None


# ------------------------------------------------------------------
# DOM extraction helpers — search results
# ------------------------------------------------------------------


def _extract_search_results_from_dom(
  tree: HTMLParser, domain: str,
) -> list[ScrapedSearchResult]:
  """Build search results by scanning all item links in the rendered DOM."""
  results: list[ScrapedSearchResult] = []
  seen_ids: set[str] = set()

  for link in tree.css('a[href*="/items/"]'):
    href = link.attributes.get("href", "")
    if not href:
      continue

    item_id = _extract_id_from_url(href)
    if not item_id or item_id in seen_ids:
      continue
    seen_ids.add(item_id)

    # Build absolute URL.
    if href.startswith("/"):
      item_url = f"https://{domain}{href}"
    elif href.startswith("http"):
      item_url = href
    else:
      continue

    # Title: prefer the image alt text (usually the item title on
    # Vinted), fall back to the full link text.
    img = link.css_first("img")
    title = ""
    if img is not None:
      title = (img.attributes.get("alt") or "").strip()
    if not title:
      title = link.text(strip=True)

    # Price within the card element.
    price: Decimal | None = None
    currency = "EUR"
    for price_selector in [
      '[data-testid*="price"]',
      '[itemprop="price"]',
      '[class*="price"]',
    ]:
      price_node = link.css_first(price_selector)
      if price_node is not None:
        text = price_node.text(strip=True)
        price = _parse_price_text(text)
        if price is not None:
          currency = _detect_currency(text) or currency
          break

    # Image src.
    image_url: str | None = None
    if img is not None:
      image_url = (
        img.attributes.get("src")
        or img.attributes.get("data-src")
        or None
      )

    results.append(ScrapedSearchResult(
      external_id=item_id,
      url=item_url,
      title=title or f"Vinted item {item_id}",
      current_price=price,
      currency=currency,
      listing_type="buy_now",
      image_url=image_url,
    ))

  return results


# ------------------------------------------------------------------
# JSON parsing fallback (HTTP transport or direct API response)
# ------------------------------------------------------------------


def _parse_search_from_json(
  html: str, url: str, domain: str,
) -> list[ScrapedSearchResult]:
  """Parse a JSON API catalog response (HTTP transport fallback)."""
  with contextlib.suppress(Exception):
    data = json.loads(html)
    items = data.get("items")
    if isinstance(items, list):
      results: list[ScrapedSearchResult] = []
      for item in items:
        result = _item_to_search_result(item, domain)
        if result is not None:
          results.append(result)
      logger.info(
        "Vinted search (JSON API fallback): %d results", len(results),
      )
      return results
  if "message_code" in html or "invalid_authentication" in html:
    raise ParserBlocked(
      "Vinted API returned an authentication error — "
      "Camoufox browser transport required",
      url=url,
    )
  raise ValueError("Could not parse Vinted search JSON response")


def _item_to_search_result(
  item: dict, domain: str,
) -> ScrapedSearchResult | None:
  """Convert a JSON API search-result item dict to a ScrapedSearchResult."""
  item_id = item.get("id")
  if item_id is None:
    return None

  title = item.get("title", "")
  item_url = item.get("url", "")
  if item_url and not item_url.startswith("http"):
    item_url = f"https://{domain}{item_url}"
  if not item_url:
    item_url = f"https://{domain}/items/{item_id}"

  price = _extract_price_from_json(item.get("price"))
  currency = _extract_currency_from_json(item.get("price"))

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


def _parse_item_detail_from_json(item: dict, url: str) -> ScrapedListing:
  """Parse a full item details JSON object into a ScrapedListing."""
  item_id = str(item.get("id", ""))
  title = item.get("title", "")
  description = item.get("description") or None

  item_url = item.get("url", "") or url
  if item_url and not item_url.startswith("http"):
    domain = _domain_from_url(url)
    item_url = f"https://{domain}{item_url}"

  price = _extract_price_from_json(item.get("price"))
  currency = _extract_currency_from_json(item.get("price"))
  total_price = _extract_price_from_json(item.get("total_item_price"))

  shipping_cost = None
  if total_price is not None and price is not None and total_price > price:
    shipping_cost = total_price - price

  status = "sold" if item.get("is_closed") or item.get("can_buy") is False \
    else "active"

  # Condition from status_id.
  _condition_id_map: dict[int, str] = {
    6: "new", 1: "like_new", 2: "very_good", 3: "good", 4: "fair",
  }
  condition = _condition_id_map.get(item.get("status_id") or -1)

  # Seller.
  seller: ScrapedSeller | None = None
  user = item.get("user")
  if isinstance(user, dict):
    user_id = user.get("id")
    login = user.get("login", "")
    if user_id or login:
      rating = None
      rep = user.get("feedback_reputation")
      if rep is not None:
        with contextlib.suppress(ValueError, TypeError):
          rating = round(float(rep) * 20, 1)
      seller = ScrapedSeller(
        external_id=str(user_id or login),
        username=login,
        display_name=login,
        country=(user.get("country_iso_code") or "").upper() or None,
        rating=rating,
        feedback_count=_safe_int(user.get("feedback_count")),
        profile_url=user.get("profile_url"),
      )

  # Images.
  photos = item.get("photos") or []
  image_urls = [
    (p.get("full_size_url") or p.get("url"))
    for p in photos
    if isinstance(p, dict) and (p.get("full_size_url") or p.get("url"))
  ]

  # Counters.
  favourite_count = _safe_int(item.get("favourite_count"))
  view_count = _safe_int(item.get("view_count"))

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
  )


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _extract_id_from_url(url: str) -> str | None:
  """Extract the numeric item ID from a Vinted item URL."""
  match = re.search(r"/items/(\d+)", url)
  return match.group(1) if match else None


def _domain_from_url(url: str) -> str:
  """Extract the domain from a URL, falling back to the default."""
  if url:
    parsed = urlparse(url)
    if parsed.netloc:
      return parsed.netloc
  return _DEFAULT_DOMAIN


def _parse_price_text(text: str) -> Decimal | None:
  """Parse a human-readable price string such as ``"15,00 €"``."""
  if not text:
    return None
  # Remove currency symbols, non-breaking spaces, and other noise.
  cleaned = re.sub(r"[€£$\u00a0\u202f\s]", "", text)
  cleaned = re.sub(r"[A-Za-z]", "", cleaned)
  # Normalise comma-as-decimal (e.g. "15,00" → "15.00") but handle
  # thousands separators (e.g. "1.500,00" or "1,500.00").
  if "," in cleaned and "." in cleaned:
    # Both present: the last of the two is the decimal separator.
    if cleaned.rfind(",") > cleaned.rfind("."):
      cleaned = cleaned.replace(".", "").replace(",", ".")
    else:
      cleaned = cleaned.replace(",", "")
  elif "," in cleaned:
    cleaned = cleaned.replace(",", ".")
  match = re.search(r"\d+(?:\.\d+)?", cleaned)
  if match:
    return _decimal_or_none(match.group())
  return None


def _detect_currency(text: str) -> str | None:
  """Return an ISO currency code detected in a price string."""
  for symbol, code in [
    ("€", "EUR"), ("£", "GBP"), ("CHF", "CHF"),
    ("zł", "PLN"), ("Kč", "CZK"), ("Ft", "HUF"),
    ("kr", "SEK"), ("$", "USD"),
  ]:
    if symbol in text:
      return code
  return None


def _symbol_to_currency(symbol: str) -> str | None:
  """Map a currency symbol or code to an ISO code."""
  mapping = {
    "€": "EUR", "EUR": "EUR", "£": "GBP", "GBP": "GBP",
    "CHF": "CHF", "$": "USD", "USD": "USD",
    "PLN": "PLN", "CZK": "CZK", "HUF": "HUF",
    "SEK": "SEK", "DKK": "DKK", "NOK": "NOK",
  }
  return mapping.get(symbol.strip())


def _extract_price_from_json(price_data: dict | str | None) -> Decimal | None:
  """Extract price from a Vinted JSON price object or string."""
  if price_data is None:
    return None
  if isinstance(price_data, str):
    return _decimal_or_none(price_data)
  if isinstance(price_data, dict):
    return _decimal_or_none(price_data.get("amount"))
  return None


def _extract_currency_from_json(price_data: dict | str | None) -> str:
  """Extract currency code from a Vinted JSON price object."""
  if isinstance(price_data, dict):
    return price_data.get("currency_code", "EUR")
  return "EUR"


def _decimal_or_none(value: str | int | float | None) -> Decimal | None:
  """Convert a value to Decimal.  Returns ``None`` on failure."""
  if value is None:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None


def _safe_int(value: int | str | None) -> int | None:
  """Convert a value to int.  Returns ``None`` on failure."""
  if value is None:
    return None
  if isinstance(value, int):
    return value
  try:
    return int(value)
  except (ValueError, TypeError):
    return None
