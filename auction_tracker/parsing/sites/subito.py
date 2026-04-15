"""Subito parser.

Subito (subito.it) is Italy's largest classifieds marketplace.  The
site is a Next.js application that historically embedded a
``<script id="__NEXT_DATA__">`` JSON blob with hydrated React state.
This parser tries ``__NEXT_DATA__`` first; if absent (the app may have
dropped SSR hydration), it falls back to parsing the fully-rendered DOM
returned by the Camoufox transport after ``networkidle``.

When ``__NEXT_DATA__`` is present:
  Search results: ``props.pageProps.initialState.items.list``
  Listing detail: ``props.pageProps.item`` (or similar key)

When falling back to rendered DOM:
  * Title from ``<meta property="og:title">`` or ``<h1>``.
  * Price from JSON-LD, ``<meta property="og:price:amount">``, or
    ``[itemprop="price"]`` microdata.
  * Description from ``<meta property="og:description">``.
  * Images from ``<meta property="og:image">`` or preloaded CDN links.
  * For search: every ``<a>`` linking to a ``*.htm`` subito.it item.

* **Search URL**:
  ``/annunci-italia/vendita/usato/?q=QUERY`` with ``&start=OFFSET``
  for pagination (30 items per page, ``start`` is zero-based).
* **Listing URLs**: absolute paths ending in ``.htm`` with a numeric
  id (extracted via ``extract_external_id``).
* **Currency**: EUR.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, urlencode

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

_BASE_URL = "https://www.subito.it"
_DEFAULT_CURRENCY = "EUR"
_PAGE_SIZE = 30


def _load_next_data(html: str) -> dict | None:
  match = re.search(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    html, re.DOTALL | re.IGNORECASE,
  )
  if not match:
    return None
  try:
    return json.loads(match.group(1))
  except json.JSONDecodeError:
    return None


def _find_listing_item_payload(data: object) -> dict | None:
  """Locate a Subito ``item`` dict inside arbitrary Next.js JSON.

  Prefers objects that include a non-trivial ``body`` or ``description``
  string so slim search-row payloads are not mistaken for VIP data.
  """
  if isinstance(data, dict):
    if "subject" in data and "urls" in data:
      body = data.get("body") or data.get("description")
      if isinstance(body, str) and len(body.strip()) >= 10:
        return data
    for value in data.values():
      found = _find_listing_item_payload(value)
      if found is not None:
        return found
  elif isinstance(data, list):
    for element in data:
      found = _find_listing_item_payload(element)
      if found is not None:
        return found
  return None


def _item_price_eur(item: dict) -> Decimal | None:
  features = item.get("features") or {}
  price_block = features.get("/price") or {}
  values = price_block.get("values") or []
  if not values:
    return None
  raw = values[0].get("key")
  if raw is None:
    return None
  try:
    return Decimal(str(raw))
  except Exception:
    return None


def _item_external_id(item: dict) -> str | None:
  urn = item.get("urn") or ""
  digits = re.search(r"(\d{6,})", str(urn))
  if digits:
    return digits.group(1)
  default_url = (item.get("urls") or {}).get("default", "")
  match = re.search(r"[-/](\d+)\.htm", default_url)
  return match.group(1) if match else None


@ParserRegistry.register
class SubitoParser(Parser):
  """Parser for subito.it classified listings."""

  @property
  def website_name(self) -> str:
    return "subito"

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
    page = int(kwargs.get("page", 1))
    params: dict[str, str | int] = {"q": query}
    if page > 1:
      params["start"] = (page - 1) * _PAGE_SIZE
    return f"{_BASE_URL}/annunci-italia/vendita/usato/?{urlencode(params, quote_via=quote_plus)}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"[-/](\d+)\.htm", url)
    return match.group(1) if match else None

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)

    # Primary: __NEXT_DATA__ (present in older/server-rendered Subito pages).
    payload = _load_next_data(html)
    if payload is not None:
      with contextlib.suppress(Exception):
        items_list = (
          payload["props"]["pageProps"]["initialState"]["items"]["list"]
        )
        results: list[ScrapedSearchResult] = []
        for wrapper in items_list:
          item = wrapper.get("item") if isinstance(wrapper, dict) else None
          if not isinstance(item, dict):
            continue
          external_id = _item_external_id(item)
          if not external_id:
            continue
          title = item.get("subject", "")
          if not title:
            continue
          link = (item.get("urls") or {}).get("default", "")
          item_url = link if link.startswith("http") else f"{_BASE_URL}{link}"
          price = _item_price_eur(item)
          image_url = None
          images = item.get("images") or item.get("pictures") or []
          if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, dict):
              image_url = (
                first.get("cdn") or first.get("url") or first.get("baseUrl")
              )
          results.append(ScrapedSearchResult(
            external_id=external_id,
            url=item_url,
            title=title,
            current_price=price,
            currency=_DEFAULT_CURRENCY,
            listing_type="buy_now",
            image_url=image_url,
          ))
        if results:
          logger.info("Subito search (__NEXT_DATA__): parsed %d results", len(results))
          return results

    # Fallback: parse the fully-rendered DOM (Camoufox returns page.content()
    # after networkidle, so every item card is present in the HTML).
    tree = HTMLParser(html)
    results = _parse_search_from_dom(tree)
    if results:
      logger.info("Subito search (DOM): parsed %d results", len(results))
      return results

    # If neither path yielded anything, check for an unrendered SPA shell.
    body_text = tree.body.text() if tree.body else ""
    if len(body_text) < 2000:
      raise ParserBlocked(
        "Subito search page appears to be an unrendered SPA shell — "
        "Camoufox browser transport required",
        url=url,
      )
    logger.warning("Subito search: no items found in DOM or __NEXT_DATA__ for %s", url)
    return []

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)

    # Primary: __NEXT_DATA__ (present in older/server-rendered Subito pages).
    payload = _load_next_data(html)
    if payload is not None:
      page_props = payload.get("props", {}).get("pageProps", {})
      item = None
      for key in ("item", "advert", "ad", "listing"):
        candidate = page_props.get(key)
        if isinstance(candidate, dict) and candidate.get("subject"):
          item = candidate
          break
      if item is None:
        item = _find_listing_item_payload(payload)
      if item is not None:
        external_id = _item_external_id(item)
        title = item.get("subject", "")
        if external_id and title:
          description = item.get("body") or item.get("description")
          price = _item_price_eur(item)
          seller = _extract_subito_seller(
            item.get("advertiser") or item.get("user") or {}
          )
          image_urls = _extract_subito_images(item)
          attributes: dict[str, str] = {}
          geo = item.get("geo") or {}
          town = (geo.get("town") or {}).get("value")
          city = (geo.get("city") or {}).get("shortName")
          if town or city:
            attributes["location"] = ", ".join(
              part for part in (town, city) if part
            )
          return ScrapedListing(
            external_id=external_id,
            url=url,
            title=title,
            description=description,
            listing_type="buy_now",
            currency=_DEFAULT_CURRENCY,
            current_price=price,
            buy_now_price=price,
            status="active",
            image_urls=image_urls,
            seller=seller,
            attributes=attributes,
          )

    # Fallback: parse the fully-rendered DOM returned by Camoufox.
    tree = HTMLParser(html)
    return _parse_listing_from_dom(tree, url)


# ------------------------------------------------------------------
# DOM parsing helpers (fallback when __NEXT_DATA__ is absent)
# ------------------------------------------------------------------

# Item URL pattern: ends with a numeric ID followed by .htm.
# The ID may be preceded by "/" (old format) or "-" (new slug format,
# e.g. …-perugia-642754252.htm).
_ITEM_URL_RE = re.compile(r"[-/](\d{4,})\.htm")


def _meta_content(tree: HTMLParser, prop: str) -> str:
  """Return the ``content`` of a meta tag matched by ``property`` or ``name``."""
  for attr in ("property", "name"):
    node = tree.css_first(f'meta[{attr}="{prop}"]')
    if node is not None:
      return (node.attributes.get("content") or "").strip()
  return ""


def _parse_listing_from_dom(tree: HTMLParser, url: str) -> ScrapedListing:
  """Parse a Subito item page from its fully-rendered DOM."""
  # Item ID from URL or og:url.
  external_id = None
  match = _ITEM_URL_RE.search(url)
  if match:
    external_id = match.group(1)
  if not external_id:
    og_url = _meta_content(tree, "og:url")
    match = _ITEM_URL_RE.search(og_url)
    if match:
      external_id = match.group(1)
  if not external_id:
    raise ValueError(f"Cannot determine Subito item ID from URL: {url}")

  # Title.
  title = _meta_content(tree, "og:title") or ""
  title = re.sub(r"\s*[|\-]\s*Subito\.it\s*$", "", title, flags=re.IGNORECASE).strip()
  if not title:
    h1 = tree.css_first("h1")
    if h1:
      title = h1.text(strip=True)
  if not title:
    raise ValueError(f"Cannot extract title from Subito item page for {url}")

  # Description.
  description = _meta_content(tree, "og:description") or None

  # Price: try JSON-LD, then og:price:amount, then itemprop microdata.
  price: Decimal | None = None
  currency = _DEFAULT_CURRENCY

  for script in tree.css('script[type="application/ld+json"]'):
    with contextlib.suppress(Exception):
      data = json.loads(script.text() or "")
      if not isinstance(data, dict):
        continue
      for item_data in ([data] if "@type" in data else data.get("@graph", [])):
        if not isinstance(item_data, dict):
          continue
        offers = item_data.get("offers")
        if isinstance(offers, dict):
          price_val = offers.get("price")
          currency = offers.get("priceCurrency") or currency
          with contextlib.suppress(Exception):
            price = Decimal(str(price_val))
          if price is not None:
            break
      if price is not None:
        break

  if price is None:
    og_price = _meta_content(tree, "og:price:amount")
    og_currency = _meta_content(tree, "og:price:currency")
    if og_price:
      with contextlib.suppress(InvalidOperation, ValueError):
        price = Decimal(og_price.replace(",", "."))
    if og_currency:
      currency = og_currency

  if price is None:
    node = tree.css_first('[itemprop="price"]')
    if node is not None:
      raw = node.attributes.get("content") or node.text(strip=True)
      with contextlib.suppress(InvalidOperation, ValueError):
        price = Decimal(raw.replace(",", "."))
      currency_node = tree.css_first('[itemprop="priceCurrency"]')
      if currency_node is not None:
        currency = (
          currency_node.attributes.get("content")
          or currency_node.text(strip=True)
          or currency
        )

  # Images: og:image, then any CDN image link in head preloads.
  image_urls: list[str] = []
  og_image = _meta_content(tree, "og:image")
  if og_image:
    image_urls.append(og_image)
  for link in tree.css('link[rel="preload"][as="image"]'):
    href = (link.attributes.get("href") or "").strip()
    if "subito" in href and href not in image_urls:
      image_urls.append(href)

  # Seller: look for itemprop="seller" or a known seller link pattern.
  seller: ScrapedSeller | None = None
  seller_node = tree.css_first('[itemprop="seller"]')
  if seller_node is None:
    seller_node = tree.css_first('[data-testid*="seller"], [class*="seller"]')
  if seller_node is not None:
    name_node = seller_node.css_first('[itemprop="name"]') or seller_node
    name = name_node.text(strip=True)
    if name:
      seller = ScrapedSeller(
        external_id=name,
        username=name,
        display_name=name,
        country="IT",
      )

  # Status: check for sold indicators via JSON-LD availability or
  # specific badge elements.  A naive body-text scan would false-
  # positive on "venduto" appearing in footers or seller stats.
  status = "active"
  for script in tree.css('script[type="application/ld+json"]'):
    with contextlib.suppress(Exception):
      data = json.loads(script.text() or "")
      if not isinstance(data, dict):
        continue
      for item_data in ([data] if "@type" in data else data.get("@graph", [])):
        if not isinstance(item_data, dict):
          continue
        offers = item_data.get("offers")
        if isinstance(offers, dict):
          availability = (offers.get("availability") or "").lower()
          if any(keyword in availability
                 for keyword in ("soldout", "discontinued", "outofstock")):
            status = "sold"
            break
      if status == "sold":
        break
  if status == "active":
    for selector in (
      '[data-testid*="sold"]',
      '[class*="sold"]',
      '[class*="Sold"]',
      '[class*="venduto"]',
      '[class*="Venduto"]',
    ):
      node = tree.css_first(selector)
      if node is not None:
        text = node.text(strip=True).lower()
        if text and ("venduto" in text or "sold" in text):
          status = "sold"
          break

  return ScrapedListing(
    external_id=external_id,
    url=url,
    title=title,
    description=description,
    listing_type="buy_now",
    currency=currency,
    current_price=price,
    buy_now_price=price,
    status=status,
    image_urls=image_urls,
    seller=seller,
  )


def _parse_search_from_dom(tree: HTMLParser) -> list[ScrapedSearchResult]:
  """Extract search result items from a rendered Subito catalog page."""
  results: list[ScrapedSearchResult] = []
  seen_ids: set[str] = set()

  for link in tree.css("a[href]"):
    href = link.attributes.get("href", "")
    match = _ITEM_URL_RE.search(href)
    if not match:
      continue
    external_id = match.group(1)
    if external_id in seen_ids:
      continue
    seen_ids.add(external_id)

    item_url = href if href.startswith("http") else f"{_BASE_URL}{href}"

    # Title from alt text of contained image or link text.
    img = link.css_first("img")
    title = ""
    if img is not None:
      title = (img.attributes.get("alt") or "").strip()
    if not title:
      title = link.text(strip=True)
    if not title:
      continue

    # Price within the card.
    price: Decimal | None = None
    currency = _DEFAULT_CURRENCY
    for sel in ['[itemprop="price"]', '[class*="price"]', '[data-testid*="price"]']:
      price_node = link.css_first(sel)
      if price_node is not None:
        raw = (
          price_node.attributes.get("content")
          or price_node.text(strip=True)
        )
        with contextlib.suppress(InvalidOperation, ValueError):
          price = Decimal(raw.replace(".", "").replace(",", "."))
        break

    # Image.
    image_url = None
    if img is not None:
      image_url = img.attributes.get("src") or img.attributes.get("data-src")

    results.append(ScrapedSearchResult(
      external_id=external_id,
      url=item_url,
      title=title,
      current_price=price,
      currency=currency,
      listing_type="buy_now",
      image_url=image_url,
    ))

  return results


def _extract_subito_seller(advertiser: dict) -> ScrapedSeller | None:
  if not advertiser:
    return None
  name = advertiser.get("name") or advertiser.get("nickname")
  user_id = advertiser.get("userId") or advertiser.get("id")
  if not name and user_id is None:
    return None
  external_id = str(user_id) if user_id is not None else str(name)
  profile_url = advertiser.get("profileUrl") or advertiser.get("url")
  if profile_url and not profile_url.startswith("http"):
    profile_url = _BASE_URL + profile_url

  return ScrapedSeller(
    external_id=external_id,
    username=name or external_id,
    display_name=name,
    country="IT",
    profile_url=profile_url,
  )


def _extract_subito_images(item: dict) -> list[str]:
  urls: list[str] = []
  seen: set[str] = set()
  for key in ("images", "pictures"):
    for entry in item.get(key) or []:
      if not isinstance(entry, dict):
        continue
      raw = entry.get("cdn") or entry.get("url") or entry.get("baseUrl")
      if not raw or raw in seen:
        continue
      seen.add(raw)
      urls.append(raw if raw.startswith("http") else "https:" + raw)
  return urls
