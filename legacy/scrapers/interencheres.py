"""Interenchères scraper.

Interenchères is one of the main French online auction platforms,
aggregating sales from hundreds of French auction houses.  Their
website is built with Nuxt.js (Vue SSR) and embeds structured data
in a ``window.__NUXT__`` self-executing function.

Key facts used by this scraper:

* The ``__NUXT__`` payload is a function ``(function(a,b,...){return
  {...}}(val_a, val_b, ...))`` whose body contains the page data with
  short variable names as placeholders.  To extract usable data we
  parse the function arguments and substitute them back.
* Search results (``/recherche/lots?search=...``) contain up to 30
  items per page, stored as array elements (e.g. ``var[0]={...}``).
* Lot detail pages (``/.../lot-{id}.html``) embed the full lot data
  under the ``saleItem`` key in the Nuxt state.
* Pricing data lives at ``pricing.estimates.{min,max}``,
  ``pricing.starting_price``, ``pricing.reserve_price``, and
  ``pricing.auctioned`` (the hammer price).
* Buyer premium (``commission_rate``) is per-organisation and per
  sale type (voluntary / judicial) and stored in the sale's
  ``options.commission_rate`` object.
* Images are served from the Thumbor CDN at
  ``thumbor-indbupload.interencheres.com``.
* All prices are in **EUR**.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence
from urllib.parse import quote_plus

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

_BASE_URL = "https://www.interencheres.com"
_SEARCH_URL = f"{_BASE_URL}/recherche/lots"
_DEFAULT_CURRENCY = "EUR"


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("interencheres")
class InterencheresScraper(BaseScraper):
  """Scraper for interencheres.com."""

  def __init__(self, config: ScrapingConfig) -> None:
    super().__init__(config)
    self._cffi_session = cffi_requests.Session(impersonate="chrome")

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "Interenchères"

  @property
  def _browser_locale(self) -> str:
    return "fr-FR"

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
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=False,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
      has_auction_house_info=True,
      monitoring_strategy="post_auction",  # Only check after end.
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
    """Search Interenchères by parsing the Nuxt SSR data."""
    params = {"search": query}
    if page > 1:
      params["offset"] = str((page - 1) * 30)

    html = self._get_html(f"{_SEARCH_URL}", params=params)
    nuxt_data = _parse_nuxt_payload(html)
    if nuxt_data is None:
      logger.warning("Could not parse Nuxt data from Interenchères search page.")
      return []

    body, sub_map, param_names = nuxt_data
    items = _extract_search_items(body, sub_map, param_names)

    results: list[SearchResult] = []
    for item in items:
      external_id = str(item.get("id", ""))
      if not external_id:
        continue

      title = _extract_title(item)
      lot_url = _build_lot_url(item)

      # Status from pricing.
      pricing = item.get("pricing") or {}
      auctioned = pricing.get("auctioned")
      status = _derive_status(item)

      # Current price is the auctioned price if sold, otherwise
      # the starting price or estimate.
      current_price = None
      if auctioned is not None:
        current_price = _decimal_or_none(auctioned)
      elif pricing.get("starting_price") is not None:
        current_price = _decimal_or_none(pricing["starting_price"])

      # End time from the sale datetime.
      sale = item.get("sale") or {}
      end_time_str = sale.get("datetime") or sale.get("start_at")
      end_time = _parse_iso_datetime(end_time_str)

      # Image.
      image_url = _extract_first_image_url(item)

      results.append(SearchResult(
        external_id=external_id,
        url=lot_url,
        title=title,
        current_price=current_price,
        currency=_DEFAULT_CURRENCY,
        image_url=image_url,
        end_time=end_time,
        listing_type=_derive_listing_type(item),
        status=status,
      ))

    logger.info(
      "Interenchères search '%s' page %d: %d results.",
      query, page, len(results),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch a lot page and extract data from the Nuxt SSR state."""
    url = self._normalise_lot_url(url_or_external_id)
    html = self._get_html(url)

    nuxt_data = _parse_nuxt_payload(html)
    if nuxt_data is None:
      raise ValueError(
        f"Could not parse Nuxt data from Interenchères lot page: {url}"
      )

    body, sub_map, param_names = nuxt_data
    item = _extract_lot_detail(body, sub_map, param_names)
    if item is None:
      raise ValueError(
        f"Could not extract lot data from Interenchères page: {url}"
      )

    external_id = str(item.get("id", ""))
    title = _extract_title(item)
    description = _extract_description(item)

    # ----- Pricing -----
    pricing = item.get("pricing") or {}
    estimates = pricing.get("estimates") or {}

    estimate_low = _decimal_or_none(estimates.get("min"))
    estimate_high = _decimal_or_none(estimates.get("max"))
    starting_price = _decimal_or_none(pricing.get("starting_price"))
    reserve_price = _decimal_or_none(pricing.get("reserve_price"))
    auctioned = pricing.get("auctioned")
    is_confirmed = pricing.get("is_confirmed")

    # Final price is the auctioned amount when confirmed.
    final_price = _decimal_or_none(auctioned) if auctioned else None
    current_price = final_price or starting_price

    # ----- Status -----
    status = _derive_status(item)

    # ----- Sale info -----
    sale = item.get("sale") or {}
    sale_live = sale.get("live") or {}
    organisation = item.get("organization") or {}
    sale_type = item.get("type", "voluntary")

    org_names = organisation.get("names") or {}
    # Pick the name matching the sale type first; fall back to the other.
    if sale_type == "judicial":
      auction_house_name = org_names.get("judicial") or org_names.get("voluntary")
    else:
      auction_house_name = org_names.get("voluntary") or org_names.get("judicial")
    # Some auction houses have placeholder values like "00" for unused
    # sale types; fall back to the address name if it looks wrong.
    if auction_house_name and len(auction_house_name) <= 2:
      fallback_name = (organisation.get("address") or {}).get("name")
      auction_house_name = fallback_name or auction_house_name

    # Sale dates.
    sale_datetime_str = sale.get("datetime") or sale.get("start_at")
    start_time = _parse_iso_datetime(sale_datetime_str)
    end_time = start_time  # Most sales are one-shot; no separate end.

    # If the sale has started/ended flags, use them.
    if sale_live.get("has_ended"):
      status = ListingStatus.ENDED if status == ListingStatus.UNKNOWN else status

    # ----- Buyer premium -----
    # The commission rate is per-organisation and per sale type.
    commission_rate_data = (organisation.get("options") or {}).get("commission_rate")
    buyer_premium = None
    if isinstance(commission_rate_data, dict):
      buyer_premium = _decimal_or_none(
        commission_rate_data.get(sale_type)
      )
    elif isinstance(commission_rate_data, (int, float)):
      buyer_premium = _decimal_or_none(commission_rate_data)

    # ----- Seller (auction house) -----
    seller = None
    org_id = str(organisation.get("id", ""))
    org_address = organisation.get("address") or {}
    if org_id and auction_house_name:
      seller = ScrapedSeller(
        external_id=org_id,
        username=organisation.get("login", org_id),
        display_name=auction_house_name,
        country=org_address.get("country"),
        profile_url=f"{_BASE_URL}/commissaire-priseur/{org_id}",
      )

    # ----- Images -----
    images = _parse_images(item)

    # ----- Lot number -----
    meta = item.get("meta") or {}
    order_number = meta.get("order_number") or {}
    lot_number_int = order_number.get("primary")
    lot_number = str(lot_number_int) if lot_number_int is not None else None

    # ----- Sale name -----
    sale_name = sale.get("name") or sale.get("title")
    # Also try the meta on the sale
    if not sale_name:
      sale_source_lang = sale.get("source_lang", "fr-FR")
      sale_name_translations = sale.get("name_translations") or {}
      sale_name = sale_name_translations.get(sale_source_lang)

    # ----- Attributes -----
    attributes: dict[str, str] = {}
    if sale_type:
      attributes["sale_type"] = sale_type
    if sale_name:
      attributes["sale_name"] = sale_name

    sale_address = (sale.get("address") or org_address)
    if sale_address.get("city"):
      attributes["sale_city"] = sale_address["city"]
    if sale_address.get("country"):
      attributes["sale_country"] = sale_address["country"]

    if item.get("highlight"):
      attributes["highlight"] = "True"
    if item.get("refundable_vat"):
      attributes["refundable_vat"] = "True"

    category = item.get("category") or {}
    if category.get("name"):
      attributes["category"] = category["name"].strip()

    if reserve_price is not None:
      attributes["has_reserve_price"] = "True"
    if is_confirmed:
      attributes["result_confirmed"] = "True"

    # Sale conditions PDF link.
    sale_conditions = sale.get("sale_conditions") or {}
    if sale_conditions.get("url"):
      conditions_url = sale_conditions["url"]
      if conditions_url.startswith("//"):
        conditions_url = f"https:{conditions_url}"
      attributes["sale_conditions_url"] = conditions_url

    # Shipping / withdrawal conditions.
    shipping = item.get("shipping") or sale.get("shipping") or {}
    withdrawal = (
      (shipping.get("withdrawal_conditions_translations") or {}).get("fr-FR")
      or shipping.get("withdrawal_conditions")
    )
    if withdrawal:
      attributes["withdrawal_conditions"] = withdrawal[:500]

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type=_derive_listing_type(item),
      condition=ItemCondition.UNKNOWN,
      currency=_DEFAULT_CURRENCY,
      starting_price=starting_price,
      reserve_price=reserve_price,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      buy_now_price=None,
      current_price=current_price,
      final_price=final_price,
      buyer_premium_percent=buyer_premium,
      buyer_premium_fixed=None,
      shipping_cost=None,
      shipping_from_country=org_address.get("country"),
      ships_internationally=None,
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=0,
      watcher_count=None,
      view_count=item.get("favoritesCount"),
      lot_number=lot_number,
      auction_house_name=auction_house_name,
      sale_name=sale_name,
      sale_date=(
        start_time.strftime("%Y-%m-%d") if start_time else None
      ),
      seller=seller,
      images=images,
      bids=[],
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # HTTP helpers
  # ------------------------------------------------------------------

  def _get_html(self, url: str, params: Optional[dict] = None) -> str:
    """Perform a rate-limited GET using curl_cffi (or browser) and return the body.

    Interenchères does not require heavy anti-bot bypass, but
    curl_cffi gives us a realistic TLS fingerprint for reliability.
    Falls back to curl_cffi if the browser fetch fails.
    """
    self._rate_limit()
    if self._browser_enabled:
      try:
        if params:
          from urllib.parse import urlencode
          full_url = f"{url}?{urlencode(params)}"
        else:
          full_url = url
        return self._get_html_via_browser(full_url)
      except Exception as exc:
        logger.warning(
          "Interenchères browser fetch failed for %s, falling back to curl_cffi: %s",
          url, exc,
        )
    logger.debug("GET %s (params=%s)", url, params)
    response = self._cffi_session.get(
      url,
      params=params,
      timeout=self.config.timeout,
    )
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_lot_url(url_or_id: str) -> str:
    """Accept a full URL or a lot ID and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID - we can't reconstruct the full slug-based URL
    # from just an ID, so raise an error.
    raise ValueError(
      f"Interenchères requires a full URL, not a bare ID: {url_or_id}"
    )


# ------------------------------------------------------------------
# Nuxt payload parsing
# ------------------------------------------------------------------

def _parse_nuxt_payload(
  html: str,
) -> Optional[tuple[str, dict[str, str], list[str]]]:
  """Parse the ``window.__NUXT__`` function from the HTML page.

  Returns ``(function_body, substitution_map, param_names)`` or
  ``None`` if the payload cannot be found.
  """
  match = re.search(r'window\.__NUXT__\s*=\s*\(function\((.*?)\)\{', html)
  if not match:
    return None

  param_names = match.group(1).split(",")
  func_body_start = match.end()
  end_script = html.find("</script>", func_body_start)
  if end_script == -1:
    return None

  raw = html[match.start():end_script]
  close_func = raw.rfind("}(")
  last_paren = raw.rfind(")")
  if close_func == -1 or last_paren == -1:
    return None

  args_raw = raw[close_func + 2:last_paren]

  # Parse the argument values (respecting quoted strings).
  args = _parse_function_args(args_raw)

  sub_map = dict(zip(param_names, args))
  body = raw[func_body_start - match.start():close_func]

  return body, sub_map, param_names


def _parse_function_args(args_raw: str) -> list[str]:
  """Split a comma-separated JavaScript argument list into values.

  Correctly handles quoted strings that may contain commas.
  """
  args: list[str] = []
  index = 0
  length = len(args_raw)

  while index < length:
    char = args_raw[index]

    if char in ('"', "'"):
      quote = char
      end = index + 1
      while end < length:
        if args_raw[end] == "\\" and end + 1 < length:
          end += 2
          continue
        if args_raw[end] == quote:
          end += 1
          break
        end += 1
      args.append(args_raw[index:end])
      index = end
    elif char == ",":
      index += 1
    elif char in (" ", "\n", "\r", "\t"):
      index += 1
    else:
      end = index
      while end < length and args_raw[end] not in (",", " ", "\n", "\r", "\t"):
        end += 1
      args.append(args_raw[index:end])
      index = end

  return args


def _substitute_vars(
  text: str,
  sub_map: dict[str, str],
  param_names: list[str],
) -> str:
  """Replace variable placeholders with their actual values.

  Variables are sorted longest-first to prevent partial matches
  (e.g. ``aa`` before ``a``).  Unicode escapes ``\\u002F`` are
  converted to ``/``.

  The substitution walks character by character to skip over
  quoted string literals, ensuring that variable names inside
  strings (e.g. inside titles or descriptions) are not modified.
  """
  # Build a fast lookup of all param patterns, longest first.
  sorted_params = sorted(param_names, key=len, reverse=True)
  identifier_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$")

  # First pass: tokenise into strings and non-strings.
  tokens: list[tuple[str, bool]] = []  # (text, is_string)
  index = 0
  length = len(text)
  non_string_start = 0

  while index < length:
    char = text[index]
    if char in ('"', "'"):
      # Save non-string segment before this string.
      if index > non_string_start:
        tokens.append((text[non_string_start:index], False))
      # Find end of string.
      quote = char
      end = index + 1
      while end < length:
        if text[end] == "\\" and end + 1 < length:
          end += 2
          continue
        if text[end] == quote:
          end += 1
          break
        end += 1
      tokens.append((text[index:end], True))
      index = end
      non_string_start = index
    else:
      index += 1

  # Don't forget trailing non-string content.
  if non_string_start < length:
    tokens.append((text[non_string_start:], False))

  # Second pass: substitute only in non-string tokens.
  output_parts: list[str] = []
  for token_text, is_string in tokens:
    if is_string:
      output_parts.append(token_text)
    else:
      result = token_text
      for param in sorted_params:
        if param not in sub_map:
          continue
        # Build a pattern with custom identifier boundaries.
        escaped = re.escape(param)
        pattern = (
          r"(?<![A-Za-z0-9_$])" + escaped + r"(?![A-Za-z0-9_$])"
        )
        result = re.sub(
          pattern,
          lambda match, val=sub_map[param]: val,
          result,
        )
      output_parts.append(result)

  return "".join(output_parts).replace("\\u002F", "/")


# ------------------------------------------------------------------
# Data extraction from the Nuxt body
# ------------------------------------------------------------------

def _extract_search_items(
  body: str,
  sub_map: dict[str, str],
  param_names: list[str],
) -> list[dict]:
  """Extract individual search result items from the Nuxt body.

  On search pages the items are stored as numbered array assignments
  like ``varName[0]={...}``, ``varName[1]={...}``, etc.
  """
  # Find the array variable name.  The first assignment sets it.
  match = re.search(r"(\w+)\[0\]=\{isFavorite", body)
  if not match:
    logger.warning("Could not find item array in Interenchères search data.")
    return []

  array_var = match.group(1)
  items: list[dict] = []

  # Iterate through array assignments.
  pattern = re.compile(re.escape(array_var) + r"\[(\d+)\]=\{")
  positions = [(m.start(), int(m.group(1))) for m in pattern.finditer(body)]

  for pos_index, (start_pos, _item_index) in enumerate(positions):
    # Determine the end of this item's data.
    if pos_index + 1 < len(positions):
      end_pos = positions[pos_index + 1][0]
    else:
      # Last item: take a generous chunk.
      end_pos = min(len(body), start_pos + 15000)

    chunk = body[start_pos:end_pos]

    # Remove the array assignment prefix.
    eq_index = chunk.find("={")
    if eq_index == -1:
      continue
    obj_str = chunk[eq_index + 1:]

    # Find the balanced closing brace.
    obj_raw = _extract_balanced_bracket(obj_str, 0, "{", "}")
    if obj_raw is None:
      continue

    # Substitute variables and parse.
    substituted = _substitute_vars(obj_raw, sub_map, param_names)
    item = _parse_nuxt_object(substituted)
    if item is not None:
      items.append(item)

  return items


def _extract_lot_detail(
  body: str,
  sub_map: dict[str, str],
  param_names: list[str],
) -> Optional[dict]:
  """Extract the main lot/item data from a lot detail page body.

  On lot pages the item is under ``saleItem:{saleItem:{...}}``.
  """
  # Find saleItem:{saleItem:{
  match = re.search(r"saleItem:\{saleItem:\{", body)
  if not match:
    # Fallback: try ``saleItem:{ ``
    match = re.search(r"saleItem:\{", body)
    if not match:
      logger.warning(
        "Could not find saleItem in Interenchères lot page data."
      )
      return None

  # Point to the inner saleItem object.
  inner_match = re.search(r"saleItem:\{", body[match.start() + 9:])
  if inner_match:
    obj_start = match.start() + 9 + inner_match.start() + len("saleItem:")
  else:
    obj_start = match.start() + len("saleItem:")

  obj_raw = _extract_balanced_bracket(body, obj_start, "{", "}")
  if obj_raw is None:
    logger.warning(
      "Could not find end of saleItem in Interenchères lot page data."
    )
    return None

  substituted = _substitute_vars(obj_raw, sub_map, param_names)
  return _parse_nuxt_object(substituted)


# ------------------------------------------------------------------
# Bracket-matching helper (shared with Drouot scraper pattern)
# ------------------------------------------------------------------

def _extract_balanced_bracket(
  text: str,
  start: int,
  open_char: str,
  close_char: str,
) -> Optional[str]:
  """Extract a balanced bracket-delimited expression.

  *start* must point to the opening bracket.  Returns the full
  substring including brackets, or ``None`` if unmatched.
  """
  if start >= len(text) or text[start] != open_char:
    return None

  depth = 0
  in_string = False
  string_char: Optional[str] = None
  index = start

  while index < len(text):
    char = text[index]

    if in_string:
      if char == "\\" and index + 1 < len(text):
        index += 2
        continue
      if char == string_char:
        in_string = False
    else:
      if char in ('"', "'"):
        in_string = True
        string_char = char
      elif char == open_char:
        depth += 1
      elif char == close_char:
        depth -= 1
        if depth == 0:
          return text[start:index + 1]

    index += 1

  return None


# ------------------------------------------------------------------
# JavaScript-to-JSON conversion for Nuxt data
# ------------------------------------------------------------------

def _nuxt_js_to_json(raw: str) -> str:
  """Convert substituted Nuxt JS object to valid JSON.

  Handles:
  - ``void`` and ``void 0`` → ``null``
  - ``new Date(N)`` → ``N``
  - ``new Map(...)`` → ``null``
  - Unquoted property names → quoted
  - Trailing commas before ``}`` / ``]``
  - JavaScript single-quoted strings → double-quoted
  """
  out: list[str] = []
  index = 0
  length = len(raw)

  while index < length:
    char = raw[index]

    # ------ String literals: copy, converting single to double quotes ------
    if char in ('"', "'"):
      quote = char
      end = index + 1
      while end < length:
        if raw[end] == "\\" and end + 1 < length:
          end += 2
          continue
        if raw[end] == quote:
          end += 1
          break
        end += 1
      content = raw[index + 1:end - 1]
      # Escape embedded double quotes if we're converting from single.
      if quote == "'":
        content = content.replace('"', '\\"')
      out.append('"')
      out.append(content)
      out.append('"')
      index = end
      continue

    # ------ ``void 0`` or ``void`` → ``null`` ------
    if raw[index:index + 6] == "void 0":
      out.append("null")
      index += 6
      continue
    if raw[index:index + 4] == "void":
      out.append("null")
      index += 4
      continue

    # ------ ``new Date(N)`` → ``N`` ------
    date_match = re.match(r"new\s+Date\((\d+)\)", raw[index:])
    if date_match:
      out.append(date_match.group(1))
      index += date_match.end()
      continue

    # ------ ``new Map(...)`` → ``null`` ------
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

    # ------ Unquoted property names ------
    if char in ("{", ","):
      out.append(char)
      index += 1
      # Skip whitespace.
      while index < length and raw[index] in (" ", "\t", "\n", "\r"):
        out.append(raw[index])
        index += 1
      # Check for an unquoted identifier followed by ``:``.
      key_match = re.match(
        r"([A-Za-z_$][A-Za-z0-9_$]*)(\s*:\s*)", raw[index:]
      )
      if key_match:
        out.append('"')
        out.append(key_match.group(1))
        out.append('"')
        out.append(":")
        index += key_match.end()
      continue

    # ------ Trailing commas ------
    if char == ",":
      end = index + 1
      while end < length and raw[end] in (" ", "\t", "\n", "\r"):
        end += 1
      if end < length and raw[end] in ("}", "]"):
        index += 1
        continue

    out.append(char)
    index += 1

  return "".join(out)


def _parse_nuxt_object(raw: str) -> Optional[dict]:
  """Parse a substituted Nuxt JS object into a Python dict."""
  sanitised = _nuxt_js_to_json(raw)
  try:
    return __import__("json").loads(sanitised)
  except __import__("json").JSONDecodeError as error:
    logger.debug(
      "Failed to parse Interenchères Nuxt object: %s (pos %d, context: ...%s...)",
      error.msg, error.pos,
      sanitised[max(0, error.pos - 40):error.pos + 40],
    )
    return None


# ------------------------------------------------------------------
# Pure parsing helpers
# ------------------------------------------------------------------

def _extract_title(item: dict) -> str:
  """Extract a readable title from an item dict."""
  translations = item.get("title_translations") or {}
  # Prefer French, fall back to English.
  title = translations.get("fr-FR") or translations.get("en-US") or ""
  if not title:
    # Some items only have description_translations.
    desc_translations = item.get("description_translations") or {}
    title = desc_translations.get("fr-FR") or desc_translations.get("en-US") or ""
  # Truncate overly long titles.
  if len(title) > 200:
    title = title[:197] + "..."
  return title or "(sans titre)"


def _extract_description(item: dict) -> Optional[str]:
  """Extract the full description from an item dict."""
  translations = item.get("description_translations") or {}
  description = translations.get("fr-FR") or translations.get("en-US")
  if description:
    # Clean up escaped newlines.
    description = description.replace("\\n", "\n")
  return description


def _build_lot_url(item: dict) -> str:
  """Construct the lot URL from the item data.

  Interenchères URLs follow the pattern:
  ``/<category-slug>/<sale-slug>-<sale_id>/lot-<item_id>.html``

  The server only validates:
  - A valid top-level category slug (any valid one works).
  - The sale-ID suffix of the sale slug segment.
  - The item ID.

  So we can use a fixed category and a minimal sale slug.
  """
  item_id = item.get("id", "")
  sale = item.get("sale") or {}
  sale_id = sale.get("id", "")

  if sale_id:
    return f"{_BASE_URL}/art-decoration/s-{sale_id}/lot-{item_id}.html"
  # Fallback: we don't have a sale ID; this will only work for
  # full-URL lookups later.
  return f"{_BASE_URL}/art-decoration/lot-{item_id}.html"


def _derive_status(item: dict) -> ListingStatus:
  """Derive the listing status from item data."""
  pricing = item.get("pricing") or {}
  auctioned = pricing.get("auctioned")
  is_confirmed = pricing.get("is_confirmed")

  # Check if the sale has ended via the live section.
  sale = item.get("sale") or {}
  sale_live = sale.get("live") or {}

  if auctioned is not None and auctioned is not False:
    if is_confirmed:
      return ListingStatus.SOLD
    # Auctioned but not yet confirmed by the auction house — the
    # hammer has fallen, so treat as SOLD (confirmation is bookkeeping).
    return ListingStatus.SOLD

  status = item.get("status", "")
  if status == "confirmed":
    # Check if the sale is in the past.
    if sale_live.get("has_ended"):
      # Sale ended but no auctioned price recorded yet — results may
      # not be published.  Return UNKNOWN so the monitor keeps
      # polling until the final outcome is available.
      return ListingStatus.UNKNOWN
    if sale_live.get("has_started"):
      return ListingStatus.ACTIVE
    return ListingStatus.UPCOMING

  if status in ("cancelled", "suspended"):
    return ListingStatus.CANCELLED

  return ListingStatus.UNKNOWN


def _derive_listing_type(item: dict) -> ListingType:
  """Determine the listing type from the item data."""
  sale = item.get("sale") or {}
  # Check for online sale indicators.
  sites = sale.get("sites") or {}
  ie_site = sites.get("interencheres") or {}
  if ie_site.get("type") == "online":
    return ListingType.AUCTION

  # Everything else is also an auction (just live).
  return ListingType.AUCTION


def _parse_images(item: dict) -> list[ScrapedImage]:
  """Extract images from the item data."""
  images: list[ScrapedImage] = []
  medias = item.get("medias") or []

  for position, media in enumerate(medias):
    rewrite = media.get("rewriteImgUrl") or {}
    # Prefer the original size; fall back to large.
    image_path = rewrite.get("original") or rewrite.get("lg") or rewrite.get("md")
    if not image_path:
      # Try the base URL.
      image_path = media.get("url")

    if image_path:
      # Ensure the URL is absolute.
      if image_path.startswith("//"):
        image_path = f"https:{image_path}"
      elif not image_path.startswith("http"):
        image_path = f"https://{image_path}"

      images.append(ScrapedImage(source_url=image_path, position=position))

  return images


def _extract_first_image_url(item: dict) -> Optional[str]:
  """Extract the URL of the first image for search results."""
  medias = item.get("medias") or []
  if not medias:
    return None

  media = medias[0]
  rewrite = media.get("rewriteImgUrl") or {}
  image_path = rewrite.get("lg") or rewrite.get("md") or rewrite.get("original")
  if not image_path:
    image_path = media.get("url")

  if image_path:
    if image_path.startswith("//"):
      return f"https:{image_path}"
    if not image_path.startswith("http"):
      return f"https://{image_path}"
    return image_path

  return None


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
  """Parse an ISO 8601 datetime string into a timezone-aware datetime."""
  if not value:
    return None
  try:
    # Handle various ISO formats.
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)
  except (ValueError, TypeError):
    return None


def _decimal_or_none(value) -> Optional[Decimal]:
  """Safely convert a numeric value to Decimal."""
  if value is None or value is False:
    return None
  try:
    decimal_value = Decimal(str(value))
    return decimal_value
  except (InvalidOperation, ValueError, TypeError):
    return None
