"""Drouot parser.

Drouot is the leading French auction marketplace, aggregating sales
from hundreds of auction houses across France and abroad. Their website
is built with SvelteKit and embeds structured data in inline script
tags as JavaScript object literals (not JSON).

Unlike Catawiki, Drouot does not require active bid monitoring:

- Live sales happen in person; the website shows only the hammer
  result once the sale is over.
- Online sales have a fixed close time without last-minute extensions.

The intended monitoring workflow is post-auction: daily search to
discover upcoming lots, then a post-auction fetch to record results.

Key Drouot facts:
- Buyer premium (``saleFees``) varies per auction house and sale,
  typically 22-33%. Always stored per-listing.
- Estimates (``lowEstim`` / ``highEstim``) are almost always provided.
- Images served from ``cdn.drouot.com/d/image/lot?size=ftall&path=...``
- All prices default to EUR.
- Each lot belongs to a sale managed by an auctioneer.
- No bid history is available (live auctions last seconds to minutes).
- Results may be hidden until the user "favorites" the lot (requires
  browser interaction at the transport level, outside this parser).
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, urlencode

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

_BASE_URL = "https://drouot.com"
_CDN_BASE = "https://cdn.drouot.com/d/image/lot"
_SEARCH_URL = f"{_BASE_URL}/en/s"
_DEFAULT_CURRENCY = "EUR"
_IMAGE_SIZE = "ftall"

# Drouot uses numeric country IDs. Most common mappings.
_COUNTRY_CODE_MAP: dict[int, str] = {
  75: "FR", 18: "BE", 44: "GB", 49: "DE", 34: "ES",
  39: "IT", 41: "CH", 1: "US", 31: "NL", 43: "AT",
  351: "PT", 352: "LU", 33: "FR", 963: "FR",
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class DrouotParser(Parser):
  """Pure parser for Drouot search results and lot detail pages."""

  @property
  def website_name(self) -> str:
    return "drouot"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=True,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
      has_auction_house_info=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    page = kwargs.get("page", 1)
    params: dict[str, str | int] = {"query": query}
    if page > 1:
      params["page"] = page
    return f"{_SEARCH_URL}?{urlencode(params)}"

  def extract_external_id(self, url: str) -> str | None:
    return _extract_lot_id(url)

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    """Parse Drouot search page (SvelteKit SSR with embedded JS)."""
    check_html_for_blocking(html, url=url)
    lots = _extract_search_lots(html)
    results: list[ScrapedSearchResult] = []

    for lot in lots:
      lot_id = str(lot.get("id", ""))
      if not lot_id:
        continue

      slug = lot.get("slug", "")
      lot_url = (
        f"{_BASE_URL}/en/l/{lot_id}-{slug}"
        if slug
        else f"{_BASE_URL}/en/l/{lot_id}"
      )

      current_bid = lot.get("currentBid", 0) or 0
      next_bid = lot.get("nextBid", 0) or 0
      current_price = (
        _decimal_or_none(current_bid)
        if current_bid > 0
        else _decimal_or_none(next_bid)
      )

      end_time = _timestamp_to_datetime(
        lot.get("bidEndDate") or lot.get("date")
      )

      photo = lot.get("photo") or {}
      image_url = (
        _build_image_url(photo["path"]) if photo.get("path") else None
      )

      description = lot.get("description", "")
      title = _build_title(description, lot.get("num"))

      results.append(ScrapedSearchResult(
        external_id=lot_id,
        url=lot_url,
        title=title,
        current_price=current_price,
        currency=lot.get("currencyId", _DEFAULT_CURRENCY),
        listing_type="auction",
        end_time=end_time,
        image_url=image_url,
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    """Parse a Drouot lot detail page.

    Tries the SvelteKit embedded ``lot:{...}`` first, then falls back
    to JSON-LD structured data.
    """
    check_html_for_blocking(html, url=url)
    lot = _extract_lot_detail(html)

    if lot is not None:
      return self._parse_from_svelte(lot, html, url)

    # Fallback to JSON-LD.
    listing = _parse_from_json_ld(html, url)
    if listing is not None:
      return listing

    raise ValueError(f"Could not extract lot data from Drouot page: {url}")

  def _parse_from_svelte(
    self,
    lot: dict,
    html: str,
    url: str,
  ) -> ScrapedListing:
    """Build a ScrapedListing from the parsed SvelteKit lot object."""
    external_id = str(lot.get("id", ""))
    description = lot.get("description", "")
    sale_info = lot.get("saleInfo") or {}

    # -- Prices --
    current_bid = lot.get("currentBid", 0) or 0
    next_bid = lot.get("nextBid", 0) or 0
    result = _get_result_value(lot, sale_info)
    if result == 0:
      result = _find_result_in_dict(lot)
    if result == 0 and sale_info:
      result = _find_result_in_dict(sale_info)

    # Fall back to raw lot string and other HTML sources.
    if result == 0:
      result = _extract_result_from_html(html)

    current_price = (
      _decimal_or_none(current_bid) if current_bid > 0 else None
    )
    starting_price = (
      _decimal_or_none(next_bid) if next_bid > 0 else None
    )
    estimate_low = _decimal_or_none(lot.get("lowEstim"))
    estimate_high = _decimal_or_none(lot.get("highEstim"))
    sale_fees = _normalize_percent_value(
      _decimal_or_none(lot.get("saleFees") or lot.get("fees"))
    )

    # -- Timing --
    start_time = _timestamp_to_datetime(lot.get("date"))
    bid_end_date = lot.get("bidEndDate", 0) or 0
    end_time = (
      _timestamp_to_datetime(bid_end_date) if bid_end_date > 0 else start_time
    )

    # -- Status --
    status = _derive_status(lot, sale_info)
    final_price = None
    if result > 0:
      final_price = _decimal_or_none(result)
      if status == "unknown":
        status = "sold"

    if lot.get("reserveNotReached"):
      status = "unsold"

    # Check HTML for explicit "not sold" text.
    if status == "unknown" and _html_says_unsold(html):
      status = "unsold"

    # -- Auctioneer / seller --
    auctioneer_card = sale_info.get("auctioneerCard") or {}
    auctioneer_link = auctioneer_card.get("link") or {}
    auction_house_name = auctioneer_link.get("auctioneerName")
    sale_name = sale_info.get("title")

    address_info = sale_info.get("address") or {}
    sale_city = address_info.get("city")
    sale_country_code = _country_id_to_code(address_info.get("country"))

    seller = None
    auctioneer_id = str(lot.get("auctioneerId", ""))
    if auctioneer_id and auction_house_name:
      auctioneer_slug = auctioneer_link.get("auctioneerSlug", "")
      seller = ScrapedSeller(
        external_id=auctioneer_id,
        username=auction_house_name,
        display_name=auction_house_name,
        country=sale_country_code,
        profile_url=(
          f"{_BASE_URL}/en/cp/{auctioneer_id}-{auctioneer_slug}"
          if auctioneer_slug else None
        ),
      )

    # -- Images --
    image_urls = _parse_images(lot)

    # -- Attributes --
    attributes: dict[str, str] = {}
    if sale_name:
      attributes["sale_name"] = sale_name
    if sale_city:
      attributes["sale_city"] = sale_city
    if sale_country_code:
      attributes["sale_country"] = sale_country_code
    if address_info.get("name"):
      attributes["sale_venue"] = address_info["name"]
    if address_info.get("hotel"):
      attributes["hotel_drouot"] = "True"

    sale_type = lot.get("saleType", "")
    if sale_type:
      attributes["sale_type"] = sale_type

    if lot.get("reserveNotReached"):
      attributes["reserve_not_reached"] = "True"

    original_description = lot.get("originalDescription", "")
    if original_description and original_description != description:
      attributes["original_description"] = original_description

    transport_size = lot.get("transportSize")
    if transport_size and transport_size != "NO_SIZE":
      attributes["transport_size"] = transport_size

    categories = lot.get("categories") or []
    if categories:
      attributes["category_ids"] = ",".join(str(cid) for cid in categories)

    title = _build_title(description, lot.get("num"))
    lot_number = str(lot.get("num", "")) if lot.get("num") else None

    return ScrapedListing(
      external_id=external_id,
      url=url or f"{_BASE_URL}/en/l/{external_id}",
      title=title,
      description=description,
      listing_type="auction",
      condition=None,
      currency=lot.get("currencyId", _DEFAULT_CURRENCY),
      starting_price=starting_price,
      reserve_price=None,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=None,
      current_price=current_price if current_price else starting_price,
      final_price=final_price,
      buyer_premium_percent=sale_fees,
      buyer_premium_fixed=None,
      shipping_cost=None,
      shipping_from_country=sale_country_code,
      ships_internationally=None,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=1 if current_bid > 0 else 0,
      watcher_count=None,
      view_count=None,
      lot_number=lot_number,
      auction_house_name=auction_house_name,
      sale_name=sale_name,
      sale_date=start_time.date() if start_time else None,
      seller=seller,
      image_urls=image_urls,
      bids=[],
      attributes=attributes,
    )


# ------------------------------------------------------------------
# SvelteKit data extraction
# ------------------------------------------------------------------


def _extract_search_lots(html: str) -> list[dict]:
  """Extract the ``lots`` array from SvelteKit SSR data."""
  start_marker = "lots:["
  start = html.find(start_marker)
  if start == -1:
    return []
  start += len(start_marker)

  raw = _extract_balanced_bracket(html, start - 1, "[", "]")
  if raw is None:
    return []

  inner = raw[1:-1].strip()
  if not inner:
    return []

  return _parse_js_object_array(inner)


def _extract_lot_detail_raw(html: str) -> str | None:
  """Extract the raw lot object string from a detail page."""
  match = re.search(r"lot:\{", html)
  if match is None:
    return None
  start = match.start() + len("lot:")
  return _extract_balanced_bracket(html, start, "{", "}")


def _extract_lot_detail(html: str | None) -> dict | None:
  """Extract the lot detail object from a Drouot lot page."""
  if not html:
    return None
  raw = _extract_lot_detail_raw(html)
  if raw is None:
    return None
  return _parse_js_object(raw)


# ------------------------------------------------------------------
# JSON-LD fallback
# ------------------------------------------------------------------


def _parse_from_json_ld(html: str, url: str) -> ScrapedListing | None:
  """Construct a ScrapedListing from JSON-LD structured data."""
  match = re.search(
    r'<script type="application/ld\+json">(.+?)</script>',
    html, re.DOTALL,
  )
  if not match:
    return None

  try:
    data = json.loads(match.group(1))
  except (json.JSONDecodeError, ValueError, TypeError):
    return None

  external_id = str(data.get("sku", ""))
  if not external_id:
    return None

  title = data.get("name", "")
  description = data.get("description", "")

  offers = data.get("offers", {})
  if isinstance(offers, list):
    offers = offers[0] if offers else {}

  price = 0
  currency = "EUR"
  if isinstance(offers, dict):
    with contextlib.suppress(ValueError, TypeError):
      price = int(float(offers.get("price", 0)))
    currency = offers.get("priceCurrency", "EUR")

  # Parse end time.
  end_time = None
  for date_field in ("priceValidUntil", "endDate"):
    raw_date = (
      offers.get(date_field) if isinstance(offers, dict) else None
    ) or data.get(date_field)
    if raw_date:
      try:
        end_time = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        break
      except (ValueError, TypeError):
        continue

  # Determine status.
  is_unsold = _html_says_unsold(html)
  status = "unknown"
  final_price = None
  current_price = _decimal_or_none(price) if price > 0 else None

  if is_unsold:
    status = "unsold"
  elif end_time and end_time < datetime.now(UTC):
    if price > 0:
      status = "sold"
      final_price = current_price
  elif end_time and end_time > datetime.now(UTC):
    status = "upcoming"

  # Image.
  image_urls: list[str] = []
  raw_image = data.get("image")
  if raw_image:
    image_urls.append(_drouot_image_url_to_high_res(raw_image))

  return ScrapedListing(
    external_id=external_id,
    url=url or f"{_BASE_URL}/en/l/{external_id}",
    title=title,
    description=description,
    listing_type="auction",
    condition=None,
    currency=currency,
    current_price=current_price,
    final_price=final_price,
    start_time=None,
    end_time=end_time,
    status=status,
    image_urls=image_urls,
    bids=[],
    attributes={"source": "json_ld_fallback"},
  )


# ------------------------------------------------------------------
# Result / hammer price extraction
# ------------------------------------------------------------------


def _get_result_value(lot: dict, sale_info: dict | None = None) -> int:
  """Get hammer/result price from lot or saleInfo."""
  for source in (lot, sale_info or {}):
    for key in ("result", "hammerPrice", "soldPrice", "winningBid"):
      value = source.get(key)
      if value is not None and value != 0:
        try:
          number = int(value) if isinstance(value, (int, float)) else int(float(str(value)))
          if 0 < number < 100_000_000:
            return number
        except (TypeError, ValueError):
          pass
  return 0


def _find_result_in_dict(
  obj: dict,
  keys: tuple[str, ...] = (
    "result", "hammerPrice", "soldPrice", "winningBid", "price", "priceRealized",
  ),
) -> int:
  """Recursively search a dict for result-like keys with positive values."""
  if not isinstance(obj, dict):
    return 0
  for key in keys:
    value = obj.get(key)
    if value is not None:
      try:
        number = (
          int(value) if isinstance(value, (int, float))
          else int(float(str(value).replace(" ", "").replace("\u202f", "")))
        )
        if 0 < number < 100_000_000:
          return number
      except (TypeError, ValueError):
        pass
  for value in obj.values():
    if isinstance(value, dict):
      found = _find_result_in_dict(value, keys)
      if found > 0:
        return found
    elif isinstance(value, list):
      for item in value:
        if isinstance(item, dict):
          found = _find_result_in_dict(item, keys)
          if found > 0:
            return found
  return 0


def _extract_result_from_html(html: str) -> int:
  """Try to find the hammer price from raw HTML patterns.

  Searches for the result in the raw lot:{...} string, then in
  DOM text patterns like "Result: 1 700 EUR".
  """
  raw_lot = _extract_lot_detail_raw(html)
  if raw_lot:
    for pattern in (
      r'"result"\s*:\s*(\d+(?:\.\d+)?)',
      r"result\s*:\s*(\d+(?:\.\d+)?)",
      r'"hammerPrice"\s*:\s*(\d+(?:\.\d+)?)',
      r"hammerPrice\s*:\s*(\d+(?:\.\d+)?)",
      r'"soldPrice"\s*:\s*(\d+(?:\.\d+)?)',
    ):
      match = re.search(pattern, raw_lot)
      if match:
        try:
          value = int(float(match.group(1)))
          if 0 < value < 100_000_000:
            return value
        except ValueError:
          pass

  # DOM text patterns.
  for pattern in (
    r"(?:result|resultat|adjug)[:\s]+([\d\s\u202f]+)",
    r"([\d\s\u202f]+)\s*[€EUR]",
  ):
    match = re.search(pattern, html, re.IGNORECASE)
    if match:
      try:
        raw = (
          match.group(1)
          .replace(" ", "")
          .replace("\u202f", "")
          .replace("\xa0", "")
          .replace(",", ".")
          .strip()
        )
        value = int(float(raw))
        if 1 < value < 100_000_000:
          return value
      except ValueError:
        pass

  return 0


def _html_says_unsold(html: str) -> bool:
  """Check if the HTML contains explicit unsold indicators."""
  lower = html.lower() if html else ""
  return "lot not sold" in lower or "lot non vendu" in lower


# ------------------------------------------------------------------
# Status derivation
# ------------------------------------------------------------------


def _get_sale_status(lot: dict, sale_info: dict | None = None) -> str:
  """Get sale status string from lot or sale-level data."""
  status = (lot.get("saleStatus") or "").strip().upper()
  if status:
    return status
  if sale_info:
    status = (
      sale_info.get("saleStatus") or sale_info.get("status") or ""
    ).strip().upper()
    if status:
      return status
  return ""


def _derive_status(lot: dict, sale_info: dict | None = None) -> str:
  """Map Drouot sale status to our status strings."""
  sale_status = _get_sale_status(lot, sale_info)
  result = _get_result_value(lot, sale_info)

  if result > 0:
    return "sold"
  if lot.get("reserveNotReached"):
    return "unsold"
  if sale_status in ("ENDED", "CLOSED"):
    return "unsold"
  if sale_status == "IN_PROGRESS":
    return "active"
  if sale_status == "CREATED":
    return "upcoming"
  if sale_status in ("CANCELLED", "SUSPENDED"):
    return "cancelled"
  return "unknown"


# ------------------------------------------------------------------
# Title building
# ------------------------------------------------------------------


def _build_title(description: str, lot_number: int | None = None) -> str:
  """Build a concise title from the description text.

  Drouot lots have no separate title field; the description serves as
  both. Takes the first meaningful lines up to ~120 characters and
  optionally prepends the lot number.
  """
  if not description:
    return "(no description)"

  lines = [line.strip() for line in description.split("\\n") if line.strip()]
  if not lines:
    lines = [line.strip() for line in description.split("\n") if line.strip()]

  title_parts: list[str] = []
  total_length = 0
  for line in lines:
    if total_length + len(line) > 120:
      if not title_parts:
        title_parts.append(line[:117] + "...")
      break
    title_parts.append(line)
    total_length += len(line) + 3

  title = " \u2014 ".join(title_parts) if title_parts else "(no description)"

  if lot_number is not None:
    return f"{lot_number} - {title}"
  return title


# ------------------------------------------------------------------
# Image handling
# ------------------------------------------------------------------


def _parse_images(lot: dict) -> list[str]:
  """Build image URL list from the lot data."""
  urls: list[str] = []
  seen_paths: set[str] = set()

  photos = lot.get("photos") or []
  if not photos:
    single_photo = lot.get("photo")
    if single_photo and single_photo.get("path"):
      photos = [single_photo]

  for photo in photos:
    path = photo.get("path")
    if not path or path in seen_paths:
      continue
    seen_paths.add(path)
    urls.append(_build_image_url(path))

  return urls


def _build_image_url(path: str) -> str:
  """Construct a full CDN image URL from a path."""
  encoded_path = quote_plus(path)
  return f"{_CDN_BASE}?size={_IMAGE_SIZE}&path={encoded_path}"


def _drouot_image_url_to_high_res(url: str) -> str:
  """Ensure a Drouot CDN image URL uses high-resolution size."""
  if not url or "cdn.drouot.com" not in url:
    return url
  if "size=" in url:
    return re.sub(r"size=[^&]+", f"size={_IMAGE_SIZE}", url)
  separator = "&" if "?" in url else "?"
  return f"{url}{separator}size={_IMAGE_SIZE}"


# ------------------------------------------------------------------
# Country codes
# ------------------------------------------------------------------


def _country_id_to_code(country_id: int | None) -> str | None:
  if country_id is None:
    return None
  return _COUNTRY_CODE_MAP.get(country_id)


# ------------------------------------------------------------------
# Value helpers
# ------------------------------------------------------------------


def _decimal_or_none(value) -> Decimal | None:
  if value is None:
    return None
  try:
    decimal_value = Decimal(str(value))
    if decimal_value == 0:
      return None
    return decimal_value
  except (InvalidOperation, ValueError, TypeError):
    return None


def _normalize_percent_value(value: Decimal | None) -> Decimal | None:
  """Normalize percent values that are sometimes scaled by 100 or 1000.

  Drouot usually exposes buyer fees as a percent (e.g. 26 or 25.5). In some
  cases we have observed values that look like an already-percent value that
  was multiplied again (e.g. 26000 instead of 26).
  """
  if value is None:
    return None

  if value <= 0:
    return None

  if value <= 100:
    return value

  normalized = value

  for divisor in (Decimal(1000), Decimal(100)):
    candidate = normalized / divisor
    if candidate <= 100:
      normalized = candidate
      break

  if normalized > 100:
    logger.warning(
      "Ignoring implausible percent value: %s",
      value,
    )
    return None

  return normalized


def _timestamp_to_datetime(timestamp: int | None) -> datetime | None:
  """Convert a Unix timestamp (seconds) to a UTC datetime."""
  if timestamp is None or timestamp == 0:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp), tz=UTC)
  except (ValueError, TypeError, OSError):
    return None


def _extract_lot_id(url: str) -> str | None:
  """Extract the numeric lot ID from a Drouot URL.

  URLs look like ``/en/l/12345678-some-description``.
  """
  match = re.search(r"/l/(\d+)", url)
  if match:
    return match.group(1)
  return None


# ------------------------------------------------------------------
# Bracket-matching helper
# ------------------------------------------------------------------


def _extract_balanced_bracket(
  text: str,
  start: int,
  open_char: str,
  close_char: str,
) -> str | None:
  """Extract a balanced bracket-delimited expression.

  ``start`` must point to the opening bracket. Returns the full
  substring including both brackets, or None if unbalanced.
  Handles nesting and skips quoted strings.
  """
  if start >= len(text) or text[start] != open_char:
    return None

  depth = 0
  in_string = False
  string_char: str | None = None
  index = start

  while index < len(text):
    character = text[index]

    if in_string:
      if character == "\\" and index + 1 < len(text):
        index += 2
        continue
      if character == string_char:
        in_string = False
    else:
      if character in ('"', "'"):
        in_string = True
        string_char = character
      elif character == open_char:
        depth += 1
      elif character == close_char:
        depth -= 1
        if depth == 0:
          return text[start:index + 1]

    index += 1

  return None


# ------------------------------------------------------------------
# JavaScript-to-JSON conversion
# ------------------------------------------------------------------


def _js_to_json(raw: str) -> str:
  """Convert a JavaScript object/array literal to valid JSON.

  Character-by-character walk that never modifies text inside string
  literals. Handles void 0, new Date(N), new Map(...), unquoted
  property names, and trailing commas.
  """
  out: list[str] = []
  index = 0
  length = len(raw)

  while index < length:
    character = raw[index]

    # String literals: copy verbatim.
    if character in ('"', "'"):
      quote = character
      end = index + 1
      while end < length:
        if raw[end] == "\\" and end + 1 < length:
          end += 2
          continue
        if raw[end] == quote:
          end += 1
          break
        end += 1
      out.append(raw[index:end])
      index = end
      continue

    # void 0 -> null
    if raw[index:index + 6] == "void 0":
      out.append("null")
      index += 6
      continue

    # new Date(N) -> N
    date_match = re.match(r"new\s+Date\((\d+)\)", raw[index:])
    if date_match:
      out.append(date_match.group(1))
      index += date_match.end()
      continue

    # new Map(...) -> null
    if raw[index:index + 7] == "new Map":
      depth = 0
      end = index + 7
      while end < length:
        if raw[end] == "(":
          depth += 1
        elif raw[end] == ")":
          depth -= 1
          if depth == 0:
            end += 1
            break
        end += 1
      out.append("null")
      index = end
      continue

    # Trailing commas before } or ] — check before key-quoting.
    if character == ",":
      end = index + 1
      while end < length and raw[end] in (" ", "\t", "\n", "\r"):
        end += 1
      if end < length and raw[end] in ("}", "]"):
        index += 1
        continue

    # Unquoted property names after { or ,
    if character in ("{", ","):
      out.append(character)
      index += 1
      while index < length and raw[index] in (" ", "\t", "\n", "\r"):
        out.append(raw[index])
        index += 1
      key_match = re.match(
        r"([A-Za-z_$][A-Za-z0-9_$]*)(\s*:\s*)", raw[index:],
      )
      if key_match:
        out.append('"')
        out.append(key_match.group(1))
        out.append('"')
        out.append(":")
        index += key_match.end()
      continue

    out.append(character)
    index += 1

  return "".join(out)


def _parse_js_object(raw: str) -> dict | None:
  """Parse a single JavaScript object literal into a Python dict."""
  sanitised = _js_to_json(raw)
  try:
    return json.loads(sanitised)
  except json.JSONDecodeError as error:
    logger.warning(
      "Failed to parse Drouot JS object: %s (near position %d)",
      error.msg, error.pos,
    )
    return None


def _parse_js_object_array(raw: str) -> list[dict]:
  """Parse comma-separated JS objects into a list of dicts."""
  sanitised = _js_to_json("[" + raw + "]")
  try:
    parsed = json.loads(sanitised)
    if isinstance(parsed, list):
      return parsed
  except json.JSONDecodeError as error:
    logger.warning(
      "Failed to parse Drouot JS array: %s (near position %d)",
      error.msg, error.pos,
    )
  return []
