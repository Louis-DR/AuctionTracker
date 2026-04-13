"""Subito parser.

Subito (subito.it) is Italy's largest classifieds marketplace.  The
site is a Next.js application: search and listing pages embed a
``<script id="__NEXT_DATA__">`` JSON blob with hydrated React state.

Search results live at ``props.pageProps.initialState.items.list``
(each element wraps an ``item`` object).  Listing detail pages embed
the same pattern with a nested ``item`` payload (title ``subject``,
body text, ``features`` map for price, ``urls`` for canonical links).

* **Search URL**:
  ``/annunci-italia/vendita/usato/?q=QUERY`` with ``&start=OFFSET``
  for pagination (30 items per page, ``start`` is zero-based).
* **Listing URLs**: absolute paths ending in ``.htm`` with a numeric
  id (extracted via ``extract_external_id``).
* **Currency**: EUR.
"""

from __future__ import annotations

import json
import logging
import re
from decimal import Decimal
from urllib.parse import quote_plus, urlencode

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
  match = re.search(r"/(\d+)\.htm", default_url)
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
    match = re.search(r"/(\d+)\.htm", url)
    return match.group(1) if match else None

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    check_html_for_blocking(html, url)
    payload = _load_next_data(html)
    if payload is None:
      raise ParserBlocked(
        "No __NEXT_DATA__ script — likely a JS challenge page",
        url=url,
      )

    try:
      items_list = (
        payload["props"]["pageProps"]["initialState"]["items"]["list"]
      )
    except (KeyError, TypeError) as exc:
      raise ValueError("Unrecognised Subito __NEXT_DATA__ shape") from exc

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
          image_url = first.get("cdn") or first.get("url") or first.get("baseUrl")

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

    logger.info("Subito search: parsed %d results", len(results))
    return results

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    check_html_for_blocking(html, url)
    payload = _load_next_data(html)
    if payload is None:
      raise ParserBlocked(
        "No __NEXT_DATA__ script — likely a JS challenge page",
        url=url,
      )

    page_props = payload.get("props", {}).get("pageProps", {})
    item = None
    for key in ("item", "advert", "ad", "listing"):
      candidate = page_props.get(key)
      if isinstance(candidate, dict) and candidate.get("subject"):
        item = candidate
        break
    if item is None:
      item = _find_listing_item_payload(payload)
    if item is None:
      raise ValueError("No listing item payload in __NEXT_DATA__")

    external_id = _item_external_id(item)
    if not external_id:
      raise ValueError("Could not derive listing id from item payload")

    title = item.get("subject", "")
    if not title:
      raise ValueError("No listing subject in item payload")

    description = item.get("body") or item.get("description")
    price = _item_price_eur(item)

    seller = _extract_subito_seller(item.get("advertiser") or item.get("user") or {})

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
