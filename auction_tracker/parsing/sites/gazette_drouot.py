"""Gazette Drouot parser.

Gazette Drouot (gazette-drouot.com) is a subscription-based website
providing historical auction results from French auction houses.

Key facts:

* Only contains past auction data (not live bidding).
* Protected by Cloudflare -- browser transport is required.
* Historical-only: designed for manual backfilling, not automated
  monitoring.
* All prices default to EUR.
* Lot pages use CSS classes like ``.lotArtisteFiche``,
  ``.lotDescriptionFiche``, ``.lotResulatListe`` (sic), etc.
* Images are extracted from OpenSeadragon-style JS config rather than
  ``<img>`` tags.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote_plus, urljoin

from selectolax.parser import HTMLParser

from auction_tracker.parsing.base import (
  Parser,
  ParserCapabilities,
  ParserRegistry,
)
from auction_tracker.parsing.models import (
  ScrapedListing,
  ScrapedSearchResult,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.gazette-drouot.com"
_DEFAULT_CURRENCY = "EUR"

_FRENCH_MONTHS: dict[str, int] = {
  "janvier": 1, "janv": 1,
  "fevrier": 2, "février": 2, "fev": 2,
  "mars": 3, "mar": 3,
  "avril": 4, "avr": 4,
  "mai": 5,
  "juin": 6,
  "juillet": 7, "juil": 7,
  "aout": 8, "août": 8,
  "septembre": 9, "sept": 9,
  "octobre": 10, "oct": 10,
  "novembre": 11, "nov": 11,
  "decembre": 12, "décembre": 12, "dec": 12,
}


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------


@ParserRegistry.register
class GazetteDrouotParser(Parser):
  """Parser for gazette-drouot.com (historical auction results)."""

  @property
  def website_name(self) -> str:
    return "gazette_drouot"

  @property
  def capabilities(self) -> ParserCapabilities:
    return ParserCapabilities(
      can_search=True,
      can_parse_listing=True,
      has_bid_history=False,
      has_seller_info=False,
      has_watcher_count=False,
      has_view_count=False,
      has_buy_now=False,
      has_estimates=True,
      has_reserve_price=False,
      has_lot_numbers=True,
      has_auction_house_info=True,
    )

  def build_search_url(self, query: str, **kwargs) -> str:
    page = int(kwargs.get("page", 1))
    encoded = quote_plus(query)
    return (
      f"{_BASE_URL}/recherche/lot/{encoded}"
      f"?type=result&exactMatch=false&page={page}&lang=fr"
    )

  def extract_external_id(self, url: str) -> str | None:
    match = re.search(r"/lots/(\d+)", url)
    return match.group(1) if match else None

  # ----------------------------------------------------------------
  # Search
  # ----------------------------------------------------------------

  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    tree = HTMLParser(html)
    results: list[ScrapedSearchResult] = []

    for node in tree.css(".lotsListe .Lot"):
      link_node = node.css_first(".imgLot a")
      if not link_node:
        continue
      href = link_node.attributes.get("href", "")
      if not href:
        continue

      full_url = urljoin(_BASE_URL, href)
      id_match = re.search(r"/lots/(\d+)", href)
      if not id_match:
        continue
      item_id = id_match.group(1)

      title_parts: list[str] = []
      artist_node = node.css_first(".lotArtisteListe")
      if artist_node:
        title_parts.append(artist_node.text(strip=True))
      desc_node = node.css_first(".lotDescriptionListe")
      if desc_node:
        title_parts.append(desc_node.text(strip=True))
      title = " ".join(title_parts) or "(untitled)"

      image_url = None
      img_div = node.css_first(".imgLot")
      if img_div:
        style = img_div.attributes.get("style", "")
        if "background-image" in style:
          url_match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
          if url_match:
            image_url = url_match.group(1)

      current_price = None
      currency = _DEFAULT_CURRENCY
      price_node = node.css_first(".lotEstimationListe .fontRadikalBold")
      if price_node:
        price_text = price_node.text(strip=True)
        price_value, parsed_currency = _parse_gazette_price(price_text)
        if price_value is not None:
          current_price = price_value
          currency = parsed_currency

      end_time = None
      date_node = node.css_first(".dateVenteLot")
      if date_node:
        end_time = _parse_french_date(date_node.text(strip=True))

      results.append(ScrapedSearchResult(
        external_id=item_id,
        url=full_url,
        title=title,
        current_price=current_price,
        currency=currency,
        image_url=image_url,
        end_time=end_time,
        listing_type="auction",
      ))

    return results

  # ----------------------------------------------------------------
  # Listing
  # ----------------------------------------------------------------

  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    tree = HTMLParser(html)

    id_match = re.search(r"/lots/(\d+)", url)
    external_id = id_match.group(1) if id_match else "unknown"

    # Title.
    title = "(untitled)"
    title_node = tree.css_first(".lotArtisteFiche")
    if title_node:
      raw = title_node.text(strip=True)
      raw = re.sub(r"\s*Lot\s+\d+\s*$", "", raw).strip()
      if raw:
        title = raw

    # Description.
    description = _extract_description(tree)
    if not description and title != "(untitled)":
      description = title

    # Images from OpenSeadragon JS config.
    image_urls = _extract_image_urls(html)

    # Result price.
    price, currency = _extract_result_price(tree)
    status = _derive_listing_status(tree, html, price)

    final_price = price if status == "sold" else None

    # Estimates.
    estimate_low, estimate_high = _extract_estimates(tree)

    # Sale date.
    end_time = _extract_sale_date(tree)

    # If still unknown and date is past, force unsold.
    if status == "active" and end_time:
      now = datetime.now(timezone.utc)
      if (now - end_time).total_seconds() > 86400:
        status = "unsold"

    # Auction house.
    auction_house_name = _extract_auction_house(tree)

    # Lot number.
    lot_number = _extract_lot_number(tree)

    # Attributes.
    attributes: dict[str, str] = {"source": "gazette_drouot"}
    location = _extract_location(tree)
    if location:
      attributes["location"] = location
    if lot_number:
      attributes["lot_number"] = lot_number

    sale_type_node = tree.css_first(".typeVente")
    if sale_type_node:
      attributes["sale_type"] = sale_type_node.text(strip=True)

    sale_name_node = tree.css_first(".venteNomFiche a")
    if sale_name_node:
      attributes["sale_name"] = sale_name_node.text(strip=True)

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type="auction",
      currency=currency,
      current_price=price,
      final_price=final_price,
      estimate_low=estimate_low,
      estimate_high=estimate_high,
      end_time=end_time,
      status=status,
      auction_house_name=auction_house_name,
      lot_number=lot_number,
      image_urls=image_urls,
      attributes=attributes,
    )


# ------------------------------------------------------------------
# Extraction helpers
# ------------------------------------------------------------------


def _extract_description(tree: HTMLParser) -> str | None:
  """Extract description text from .lotDescriptionFiche."""
  desc_node = tree.css_first(".lotDescriptionFiche")
  if not desc_node:
    return None
  text = desc_node.text(strip=True)
  # Remove embedded sale info that starts with known class prefixes.
  text = re.sub(r"\s*(Vente|Estimation|Résultat).*$", "", text).strip()
  return text if text else None


def _extract_image_urls(html: str) -> list[str]:
  """Extract image URLs from OpenSeadragon JS config."""
  return [
    url.replace("&amp;", "&")
    for url in re.findall(
      r"""url:\s*['"]([^'"]+cdn\.drouot\.com[^'"]+)['"]""",
      html,
    )
  ]


def _extract_result_price(tree: HTMLParser) -> tuple[Decimal | None, str]:
  """Extract the result/hammer price from the lot page."""
  result_node = tree.css_first(".lotResulatListe .fontRadikalBold")
  if result_node:
    price_text = result_node.text(strip=True)
    price_value, currency = _parse_gazette_price(price_text)
    if price_value is not None:
      return price_value, currency
  return None, _DEFAULT_CURRENCY


def _extract_estimates(tree: HTMLParser) -> tuple[Decimal | None, Decimal | None]:
  """Extract estimate range from .lotEstimationFiche."""
  estimate_node = tree.css_first(".lotEstimationFiche .fontRadikalBold")
  if not estimate_node:
    return None, None

  estimate_text = estimate_node.text(strip=True)
  est_match = re.search(
    r"([\d\s\xa0.,]+)\s*[-/\u2013]\s*([\d\s\xa0.,]+)",
    estimate_text,
  )
  if not est_match:
    return None, None

  try:
    low_str = _clean_number(est_match.group(1))
    high_str = _clean_number(est_match.group(2))
    return Decimal(low_str), Decimal(high_str)
  except (InvalidOperation, ValueError):
    return None, None


def _extract_sale_date(tree: HTMLParser) -> datetime | None:
  """Extract sale date from .venteDateFiche."""
  date_node = tree.css_first(".venteDateFiche")
  if date_node:
    return _parse_french_date(date_node.text(strip=True))
  return None


def _extract_auction_house(tree: HTMLParser) -> str | None:
  """Extract auction house name from .infoVenteContent."""
  for info_el in tree.css(".infoVenteContent"):
    link = info_el.css_first("a")
    if link:
      return link.text(strip=True)
  return None


def _extract_lot_number(tree: HTMLParser) -> str | None:
  """Extract lot number from .lotNumFiche."""
  lot_node = tree.css_first(".lotNumFiche")
  if lot_node:
    num_match = re.search(r"\d+", lot_node.text(strip=True))
    if num_match:
      return num_match.group(0)
  return None


def _extract_location(tree: HTMLParser) -> str | None:
  """Extract sale location from .venteLieuFiche."""
  lieu_node = tree.css_first(".venteLieuFiche")
  if not lieu_node:
    return None
  text = lieu_node.text(strip=True)
  text = re.sub(r",\s*,", ",", text).strip(", ")
  return text if text else None


def _derive_listing_status(
  tree: HTMLParser, html: str, price: Decimal | None,
) -> str:
  """Derive status from price and unsold markers."""
  lower_html = html.lower()
  if not price and ("invendu" in lower_html or "lot non vendu" in lower_html):
    return "unsold"
  if price:
    return "sold"
  return "active"


# ------------------------------------------------------------------
# Price and date parsing
# ------------------------------------------------------------------


def _parse_gazette_price(price_text: str) -> tuple[Decimal | None, str]:
  """Parse a Gazette price string like ``4 000 EUR`` or ``150``."""
  if not price_text:
    return None, _DEFAULT_CURRENCY

  text = price_text.replace("\xa0", " ").strip()
  match = re.search(r"^([\d\s.,]+)\s*([A-Za-z\u20ac$\u00a3]+)?", text)
  if not match:
    return None, _DEFAULT_CURRENCY

  num_str, currency_code = match.groups()
  currency = _DEFAULT_CURRENCY
  if currency_code:
    symbol_map = {"\u20ac": "EUR", "$": "USD", "\u00a3": "GBP"}
    upper = currency_code.upper()
    currency = symbol_map.get(currency_code, upper)

  clean_num = _clean_number(num_str)
  try:
    return Decimal(clean_num), currency
  except (InvalidOperation, ValueError):
    return None, _DEFAULT_CURRENCY


def _clean_number(raw: str) -> str:
  """Clean a French-formatted number string."""
  clean = raw.replace(" ", "").replace("\xa0", "")
  if "," in clean:
    clean = clean.replace(".", "")
    clean = clean.replace(",", ".")
  return clean


def _parse_french_date(date_str: str) -> datetime | None:
  """Parse a French date string like ``26 mars 2025``."""
  if not date_str:
    return None

  clean_str = " ".join(date_str.lower().split())

  # Try dd/mm/yyyy first.
  slash_match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", clean_str)
  if slash_match:
    day, month, year = map(int, slash_match.groups())
    try:
      return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
      pass

  # Try dd month yyyy.
  match = re.search(r"(\d{1,2}|1er)\s+([a-z\u00e9\u00e2\u00e4\u00e0]+)\s+(\d{4})", clean_str)
  if not match:
    return None

  day_str, month_str, year_str = match.groups()
  day = 1 if day_str == "1er" else int(day_str)
  year = int(year_str)

  month = _FRENCH_MONTHS.get(month_str)
  if month is None:
    for name, month_num in _FRENCH_MONTHS.items():
      if month_str.startswith(name):
        month = month_num
        break

  if month is None:
    return None

  try:
    return datetime(year, month, day, tzinfo=timezone.utc)
  except ValueError:
    return None
