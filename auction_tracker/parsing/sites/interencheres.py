"""Interencheres parser.

Interencheres is one of the main French online auction platforms,
aggregating sales from hundreds of French auction houses. The website
is built with Nuxt.js (Vue SSR) and embeds structured data in a
``window.__NUXT__`` self-executing function.

Key technical facts:

* The ``__NUXT__`` payload is a function
  ``(function(a,b,...){return {...}}(val_a, val_b, ...))`` whose body
  contains page data with short variable names as placeholders. To
  extract usable data we parse the function arguments and substitute
  them back into the body.
* Search results (``/recherche/lots?search=...``) contain up to 30
  items per page, stored as array elements (``var[0]={...}``).
* Lot detail pages (``/.../lot-{id}.html``) embed the full lot data
  under the ``saleItem`` key in the Nuxt state.
* Prices are in ``pricing.estimates``, ``pricing.starting_price``,
  ``pricing.reserve_price``, and ``pricing.auctioned`` (hammer price).
* Buyer premium is per-organisation and per sale type (voluntary /
  judicial) via ``organization.options.commission_rate``.
* All prices are in EUR.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from auction_tracker.parsing.base import (
  Parser,
  ParserCapabilities,
  ParserRegistry,
)
from auction_tracker.parsing.models import (
  ScrapedListing,
  ScrapedSearchResult,
  ScrapedSeller,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.interencheres.com"
_SEARCH_URL = f"{_BASE_URL}/recherche/lots"


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class InterencheresParser(Parser):
  """Parser for interencheres.com auction lots."""

  @property
  def website_name(self) -> str:
    return "interencheres"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=True,
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=False,
      has_estimates=True,
      has_reserve_price=True,
      has_lot_numbers=True,
      has_auction_house_info=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    from urllib.parse import urlencode
    page = int(kwargs.get("page", 1))
    params: dict = {"search": query}
    if page > 1:
      params["offset"] = (page - 1) * 30
    return f"{_SEARCH_URL}?{urlencode(params)}"

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/lot-(\d+)\.html", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    nuxt_data = _parse_nuxt_payload(html)
    if nuxt_data is None:
      raise ValueError(
        "Could not extract __NUXT__ payload from Interencheres search page"
      )

    body, sub_map, param_names = nuxt_data
    items = _extract_search_items(body, sub_map, param_names)

    results: list[ScrapedSearchResult] = []
    for item in items:
      external_id = str(item.get("id", ""))
      if not external_id:
        continue

      title = _extract_title(item)
      lot_url = _build_lot_url(item)
      pricing = item.get("pricing") or {}
      auctioned = pricing.get("auctioned")

      current_price = None
      if auctioned is not None:
        current_price = _decimal_or_none(auctioned)
      elif pricing.get("starting_price") is not None:
        current_price = _decimal_or_none(pricing["starting_price"])

      sale = item.get("sale") or {}
      end_time_str = sale.get("datetime") or sale.get("start_at")
      end_time = _parse_iso_datetime(end_time_str)

      image_url = _extract_first_image_url(item)

      results.append(ScrapedSearchResult(
        external_id=external_id,
        url=lot_url,
        title=title,
        current_price=current_price,
        currency="EUR",
        image_url=image_url,
        end_time=end_time,
        listing_type="auction",
      ))

    logger.info(
      "Interencheres search: parsed %d results from %d items",
      len(results), len(items),
    )
    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    nuxt_data = _parse_nuxt_payload(html)
    if nuxt_data is None:
      raise ValueError(
        "Could not extract __NUXT__ payload from Interencheres lot page"
      )

    body, sub_map, param_names = nuxt_data
    item = _extract_lot_detail(body, sub_map, param_names)
    if item is None:
      raise ValueError(
        f"Could not extract lot data from Interencheres page: {url}"
      )

    external_id = str(item.get("id", ""))
    title = _extract_title(item)
    description = _extract_description(item)

    # Pricing.
    pricing = item.get("pricing") or {}
    estimates = pricing.get("estimates") or {}
    estimate_low = _decimal_or_none(estimates.get("min"))
    estimate_high = _decimal_or_none(estimates.get("max"))
    starting_price = _decimal_or_none(pricing.get("starting_price"))
    reserve_price = _decimal_or_none(pricing.get("reserve_price"))
    auctioned = pricing.get("auctioned")
    final_price = _decimal_or_none(auctioned) if auctioned else None
    current_price = final_price or starting_price

    # Status.
    status = _derive_status(item)

    # Sale info.
    sale = item.get("sale") or {}
    sale_live = sale.get("live") or {}
    organisation = item.get("organization") or {}
    sale_type = item.get("type", "voluntary")

    auction_house_name = _extract_auction_house_name(organisation, sale_type)

    sale_datetime_str = sale.get("datetime") or sale.get("start_at")
    start_time = _parse_iso_datetime(sale_datetime_str)
    end_time = start_time

    # Refine status with sale live flags.
    if sale_live.get("has_ended") and status == "active":
      status = "unsold"

    # Buyer premium.
    buyer_premium = _extract_buyer_premium(organisation, sale_type)

    # Seller (auction house).
    seller = _extract_seller(organisation, auction_house_name)

    # Images.
    image_urls = _parse_image_urls(item)

    # Lot number.
    meta = item.get("meta") or {}
    order_number = meta.get("order_number") or {}
    lot_number_int = order_number.get("primary")
    lot_number = str(lot_number_int) if lot_number_int is not None else None

    # Sale name.
    sale_name = _extract_sale_name(sale)

    # Sale date.
    sale_date = start_time.date() if start_time else None

    # Attributes.
    attributes = _build_attributes(item, sale, organisation, sale_type, sale_name, reserve_price)

    return ScrapedListing(
      external_id=external_id,
      url=url or _build_lot_url(item),
      title=title,
      description=description,
      listing_type="auction",
      currency="EUR",
      starting_price=starting_price,
      reserve_price=reserve_price,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      current_price=current_price,
      final_price=final_price,
      buyer_premium_percent=buyer_premium,
      shipping_from_country=(organisation.get("address") or {}).get("country"),
      start_time=start_time,
      end_time=end_time,
      status=status,
      bid_count=0,
      lot_number=lot_number,
      auction_house_name=auction_house_name,
      sale_name=sale_name,
      sale_date=sale_date,
      seller=seller,
      image_urls=image_urls,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# __NUXT__ payload parsing
# ------------------------------------------------------------------


def _parse_nuxt_payload(
  html: str,
) -> tuple[str, dict[str, str], list[str]] | None:
  """Parse the ``window.__NUXT__`` function from the HTML page.

  Returns ``(function_body, substitution_map, param_names)`` or
  ``None`` if the payload cannot be found.
  """
  match = re.search(r"window\.__NUXT__\s*=\s*\(function\((.*?)\)\{", html)
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
  args = _parse_function_args(args_raw)
  sub_map = dict(zip(param_names, args))
  body = raw[func_body_start - match.start():close_func]

  return body, sub_map, param_names


def _parse_function_args(args_raw: str) -> list[str]:
  """Split a comma-separated JS argument list, handling quoted strings."""
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

  Variables are sorted longest-first to prevent partial matches.
  Substitution skips over quoted string literals so that variable
  names inside strings are not modified.
  """
  sorted_params = sorted(param_names, key=len, reverse=True)

  # Tokenise into strings and non-strings.
  tokens: list[tuple[str, bool]] = []
  index = 0
  length = len(text)
  non_string_start = 0

  while index < length:
    char = text[index]
    if char in ('"', "'"):
      if index > non_string_start:
        tokens.append((text[non_string_start:index], False))
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

  if non_string_start < length:
    tokens.append((text[non_string_start:], False))

  # Substitute only in non-string tokens.
  output_parts: list[str] = []
  for token_text, is_string in tokens:
    if is_string:
      output_parts.append(token_text)
    else:
      result = token_text
      for param in sorted_params:
        if param not in sub_map:
          continue
        escaped = re.escape(param)
        pattern = r"(?<![A-Za-z0-9_$])" + escaped + r"(?![A-Za-z0-9_$])"
        result = re.sub(pattern, lambda _m, val=sub_map[param]: val, result)
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

  Items are stored as numbered array assignments like
  ``varName[0]={...}``, identified by the ``isFavorite`` marker.
  """
  match = re.search(r"(\w+)\[0\]=\{isFavorite", body)
  if not match:
    return []

  array_var = match.group(1)
  items: list[dict] = []
  pattern = re.compile(re.escape(array_var) + r"\[(\d+)\]=\{")
  positions = [(m.start(), int(m.group(1))) for m in pattern.finditer(body)]

  for pos_index, (start_pos, _item_index) in enumerate(positions):
    if pos_index + 1 < len(positions):
      end_pos = positions[pos_index + 1][0]
    else:
      end_pos = min(len(body), start_pos + 15000)

    chunk = body[start_pos:end_pos]
    eq_index = chunk.find("={")
    if eq_index == -1:
      continue
    obj_str = chunk[eq_index + 1:]

    obj_raw = _extract_balanced_bracket(obj_str, 0, "{", "}")
    if obj_raw is None:
      continue

    substituted = _substitute_vars(obj_raw, sub_map, param_names)
    item = _parse_nuxt_object(substituted)
    if item is not None:
      items.append(item)

  return items


def _extract_lot_detail(
  body: str,
  sub_map: dict[str, str],
  param_names: list[str],
) -> dict | None:
  """Extract the main lot data from a lot detail page body.

  The item lives under ``saleItem:{saleItem:{...}}``.
  """
  match = re.search(r"saleItem:\{saleItem:\{", body)
  if not match:
    match = re.search(r"saleItem:\{", body)
    if not match:
      return None

  inner_match = re.search(r"saleItem:\{", body[match.start() + 9:])
  if inner_match:
    obj_start = match.start() + 9 + inner_match.start() + len("saleItem:")
  else:
    obj_start = match.start() + len("saleItem:")

  obj_raw = _extract_balanced_bracket(body, obj_start, "{", "}")
  if obj_raw is None:
    return None

  substituted = _substitute_vars(obj_raw, sub_map, param_names)
  return _parse_nuxt_object(substituted)


# ------------------------------------------------------------------
# Bracket matching
# ------------------------------------------------------------------


def _extract_balanced_bracket(
  text: str,
  start: int,
  open_char: str,
  close_char: str,
) -> str | None:
  """Extract a balanced bracket-delimited expression from *start*."""
  if start >= len(text) or text[start] != open_char:
    return None

  depth = 0
  in_string = False
  string_char: str | None = None
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
# JS-to-JSON conversion
# ------------------------------------------------------------------


def _nuxt_js_to_json(raw: str) -> str:
  """Convert substituted Nuxt JS object to valid JSON.

  Handles void 0, new Date(N), new Map(...), unquoted property
  names, trailing commas, and single-quoted strings.
  """
  out: list[str] = []
  index = 0
  length = len(raw)

  while index < length:
    char = raw[index]

    # String literals: copy, converting single to double quotes.
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
      if quote == "'":
        content = content.replace('"', '\\"')
      out.append('"')
      out.append(content)
      out.append('"')
      index = end
      continue

    # void 0 / void -> null
    if raw[index:index + 6] == "void 0":
      out.append("null")
      index += 6
      continue
    if raw[index:index + 4] == "void":
      out.append("null")
      index += 4
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

    # Trailing commas: skip comma before } or ].
    if char == ",":
      end = index + 1
      while end < length and raw[end] in (" ", "\t", "\n", "\r"):
        end += 1
      if end < length and raw[end] in ("}", "]"):
        index = end
        continue

    # Unquoted property names after { or ,.
    if char in ("{", ","):
      out.append(char)
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

    out.append(char)
    index += 1

  return "".join(out)


def _parse_nuxt_object(raw: str) -> dict | None:
  """Parse a substituted Nuxt JS object into a Python dict."""
  sanitised = _nuxt_js_to_json(raw)
  try:
    return json.loads(sanitised)
  except json.JSONDecodeError as error:
    logger.debug(
      "Failed to parse Interencheres Nuxt object: %s (pos %d)",
      error.msg, error.pos,
    )
    return None


# ------------------------------------------------------------------
# Field extraction helpers
# ------------------------------------------------------------------


def _extract_title(item: dict) -> str:
  """Extract a readable title from an item dict."""
  translations = item.get("title_translations") or {}
  title = translations.get("fr-FR") or translations.get("en-US") or ""
  if not title:
    desc_translations = item.get("description_translations") or {}
    title = desc_translations.get("fr-FR") or desc_translations.get("en-US") or ""
  if len(title) > 200:
    title = title[:197] + "..."
  return title or "(sans titre)"


def _extract_description(item: dict) -> str | None:
  """Extract the full description from an item dict."""
  translations = item.get("description_translations") or {}
  description = translations.get("fr-FR") or translations.get("en-US")
  if description:
    description = description.replace("\\n", "\n")
  return description


def _build_lot_url(item: dict) -> str:
  """Construct the lot URL using a fixed category slug."""
  item_id = item.get("id", "")
  sale = item.get("sale") or {}
  sale_id = sale.get("id", "")
  if sale_id:
    return f"{_BASE_URL}/art-decoration/s-{sale_id}/lot-{item_id}.html"
  return f"{_BASE_URL}/art-decoration/lot-{item_id}.html"


def _derive_status(item: dict) -> str:
  """Derive listing status from item data."""
  pricing = item.get("pricing") or {}
  auctioned = pricing.get("auctioned")
  sale = item.get("sale") or {}
  sale_live = sale.get("live") or {}

  if auctioned is not None and auctioned is not False:
    return "sold"

  status = item.get("status", "")
  if status == "confirmed":
    if sale_live.get("has_ended"):
      return "active"
    if sale_live.get("has_started"):
      return "active"
    return "active"

  if status in ("cancelled", "suspended"):
    return "cancelled"

  return "active"


def _extract_auction_house_name(
  organisation: dict, sale_type: str,
) -> str | None:
  """Extract the auction house name, preferring the matching sale type."""
  org_names = organisation.get("names") or {}
  if sale_type == "judicial":
    name = org_names.get("judicial") or org_names.get("voluntary")
  else:
    name = org_names.get("voluntary") or org_names.get("judicial")
  if name and len(name) <= 2:
    fallback_name = (organisation.get("address") or {}).get("name")
    name = fallback_name or name
  return name or None


def _extract_buyer_premium(
  organisation: dict, sale_type: str,
) -> Decimal | None:
  """Extract buyer premium percentage from organisation config."""
  commission_rate_data = (
    (organisation.get("options") or {}).get("commission_rate")
  )
  if isinstance(commission_rate_data, dict):
    return _decimal_or_none(commission_rate_data.get(sale_type))
  if isinstance(commission_rate_data, (int, float)):
    return _decimal_or_none(commission_rate_data)
  return None


def _extract_seller(
  organisation: dict, auction_house_name: str | None,
) -> ScrapedSeller | None:
  """Build a ScrapedSeller from the organisation data."""
  org_id = str(organisation.get("id", ""))
  if not org_id or not auction_house_name:
    return None
  org_address = organisation.get("address") or {}
  return ScrapedSeller(
    external_id=org_id,
    username=organisation.get("login", org_id),
    display_name=auction_house_name,
    country=org_address.get("country"),
    profile_url=f"{_BASE_URL}/commissaire-priseur/{org_id}",
  )


def _parse_image_urls(item: dict) -> list[str]:
  """Extract image URLs from the item's medias array."""
  urls: list[str] = []
  for media in item.get("medias") or []:
    rewrite = media.get("rewriteImgUrl") or {}
    image_path = (
      rewrite.get("original")
      or rewrite.get("lg")
      or rewrite.get("md")
      or media.get("url")
    )
    if image_path:
      urls.append(_ensure_absolute_url(image_path))
  return urls


def _extract_first_image_url(item: dict) -> str | None:
  """Extract the URL of the first image for search results."""
  medias = item.get("medias") or []
  if not medias:
    return None
  media = medias[0]
  rewrite = media.get("rewriteImgUrl") or {}
  image_path = (
    rewrite.get("lg")
    or rewrite.get("md")
    or rewrite.get("original")
    or media.get("url")
  )
  if image_path:
    return _ensure_absolute_url(image_path)
  return None


def _ensure_absolute_url(path: str) -> str:
  """Ensure a URL/path is absolute HTTPS."""
  if path.startswith("//"):
    return f"https:{path}"
  if not path.startswith("http"):
    return f"https://{path}"
  return path


def _extract_sale_name(sale: dict) -> str | None:
  """Extract the sale name from various fields."""
  name = sale.get("name") or sale.get("title")
  if not name:
    source_lang = sale.get("source_lang", "fr-FR")
    name_translations = sale.get("name_translations") or {}
    name = name_translations.get(source_lang)
  return name


def _build_attributes(
  item: dict,
  sale: dict,
  organisation: dict,
  sale_type: str,
  sale_name: str | None,
  reserve_price: Decimal | None,
) -> dict[str, str]:
  """Build the attributes dict from various item fields."""
  attributes: dict[str, str] = {}
  if sale_type:
    attributes["sale_type"] = sale_type
  if sale_name:
    attributes["sale_name"] = sale_name

  sale_address = sale.get("address") or organisation.get("address") or {}
  if sale_address.get("city"):
    attributes["sale_city"] = sale_address["city"]
  if sale_address.get("country"):
    attributes["sale_country"] = sale_address["country"]

  if item.get("highlight"):
    attributes["highlight"] = "true"

  category = item.get("category") or {}
  if category.get("name"):
    attributes["category"] = category["name"].strip()

  if reserve_price is not None:
    attributes["has_reserve_price"] = "true"

  pricing = item.get("pricing") or {}
  if pricing.get("is_confirmed"):
    attributes["result_confirmed"] = "true"

  sale_conditions = sale.get("sale_conditions") or {}
  conditions_url = sale_conditions.get("url")
  if conditions_url:
    if conditions_url.startswith("//"):
      conditions_url = f"https:{conditions_url}"
    attributes["sale_conditions_url"] = conditions_url

  return attributes


# ------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------


def _parse_iso_datetime(value: str | None) -> datetime | None:
  """Parse an ISO 8601 datetime string."""
  if not value:
    return None
  try:
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)
  except (ValueError, TypeError):
    return None


def _decimal_or_none(value) -> Decimal | None:
  """Safely convert a numeric value to Decimal."""
  if value is None or value is False:
    return None
  try:
    return Decimal(str(value))
  except (InvalidOperation, ValueError, TypeError):
    return None
