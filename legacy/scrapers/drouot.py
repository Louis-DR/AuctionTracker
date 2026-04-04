"""Drouot scraper.

Drouot is the leading French auction marketplace, aggregating sales
from hundreds of auction houses across France and abroad.  Their
website is built with SvelteKit and embeds structured data in inline
``<script>`` tags, similar to Catawiki's ``__NEXT_DATA__``.

Unlike Catawiki, Drouot does **not** require active bid monitoring
because:

* **Live sales** happen in person; the website shows only the hammer
  result once the sale is over.
* **Online sales** have a fixed close time without the Catawiki-style
  last-minute extension.

The intended workflow is:

1. **Daily search** to discover upcoming lots.
2. **Post-auction fetch** on each lot URL to record the result.

Key Drouot facts used in the scraper:

* Buyer premium (``saleFees``) varies per auction house and per sale,
  typically between 22 % and 33 %.  It is always available in the lot
  data and stored per-listing.
* Estimates (``lowEstim`` / ``highEstim``) are almost always provided.
* Images are served from the CDN at
  ``https://cdn.drouot.com/d/image/lot?size=ftall&path=…``.
* All prices default to **EUR**.
* Each lot belongs to a *sale* managed by an *auctioneer*.  The sale
  has its own date, location, and title.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence
from urllib.parse import parse_qs, quote_plus, urlencode, urljoin, urlparse, urlunparse

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

_BASE_URL = "https://drouot.com"
_GAZETTE_BASE = "https://www.gazette-drouot.com"
_CDN_BASE = "https://cdn.drouot.com/d/image/lot"
_SEARCH_URL = f"{_BASE_URL}/en/s"
_LOT_URL_PATTERN = f"{_BASE_URL}/en/l/{{lot_id}}-{{slug}}"
_DEFAULT_CURRENCY = "EUR"

# CDN image size parameter.  ``ftall`` gives high-resolution images
# suitable for archiving.
_IMAGE_SIZE = "ftall"


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("drouot")
class DrouotScraper(BaseScraper):
  """Scraper for drouot.com."""

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "Drouot"

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
      can_search_history=True,
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
    """Search Drouot by parsing the SvelteKit SSR data."""
    params = f"query={_url_encode(query)}"
    if page > 1:
      params += f"&page={page}"
    url = f"{_SEARCH_URL}?{params}"
    html = self._get_html(url, use_browser=False)

    lots = _extract_search_lots(html)
    results: list[SearchResult] = []
    for lot in lots:
      lot_id = str(lot.get("id", ""))
      slug = lot.get("slug", "")
      lot_url = f"{_BASE_URL}/en/l/{lot_id}-{slug}" if slug else f"{_BASE_URL}/en/l/{lot_id}"

      # Derive status.
      status = _derive_status(lot)

      # Current price is the current bid, or the starting price if no bids.
      current_bid = lot.get("currentBid", 0) or 0
      next_bid = lot.get("nextBid", 0) or 0
      current_price = (
        _decimal_or_none(current_bid)
        if current_bid > 0
        else _decimal_or_none(next_bid)
      )

      # End time: for online sales use bidEndDate; for live sales use date.
      end_time = _timestamp_to_datetime(lot.get("bidEndDate") or lot.get("date"))

      # Image.
      photo = lot.get("photo") or {}
      image_url = _build_image_url(photo.get("path")) if photo.get("path") else None

      # Build a readable title from the description.
      description = lot.get("description", "")
      title = _build_title(description, lot.get("num"))

      results.append(SearchResult(
        external_id=lot_id,
        url=lot_url,
        title=title,
        current_price=current_price,
        currency=lot.get("currencyId", _DEFAULT_CURRENCY),
        image_url=image_url,
        end_time=end_time,
        listing_type=_derive_listing_type(lot),
        status=status,
      ))

    logger.info(
      "Drouot search '%s' page %d: %d results.",
      query, page, len(results),
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch a lot page and extract data (trying fast curl_cffi first, then browser)."""
    url = self._normalise_lot_url(url_or_external_id)
    html = None
    lot = None

    today = datetime.now(timezone.utc)
    used_browser_with_favorites = False

    # 1. Try fast fetch (curl_cffi)
    try:
        html = self._get_html(url, use_browser=False)
    except Exception as e:
        logger.debug(f"Fast fetch failed for {url}: {e}")

    lot = _extract_lot_detail(html)

    # 2. If fast fetch failed (or Svelte extraction failed), try browser
    if lot is None and self.config.browser_profile:
        logger.info(f"Fast fetch incomplete for {url}. Switching to Browser.")
        try:
             html = self._get_html(url, use_browser=True, check_favorites=True)
             used_browser_with_favorites = True
             lot = _extract_lot_detail(html)
        except Exception as e:
             logger.error(f"Browser fetch failed for {url}: {e}")

    if lot is None:
      # Fallback: Try to build from JSON-LD
      logger.warning(f"Svelte data extraction failed for {url}. Attempting JSON-LD fallback.")

      # Check for "Lot not sold" or "Auction ended" in static HTML too just in case
      is_unsold = False
      if "Lot not sold" in html or "Lot non vendu" in html:
          is_unsold = True


      listing = self._extract_listing_from_json_ld(html, url, is_unsold=is_unsold)
      if listing:
           return listing

      logger.error("Failed to extract lot data from %s", url)
      raise ValueError(f"Could not extract lot data from {url}")

    external_id = str(lot.get("id", ""))
    description = lot.get("description", "")
    original_description = lot.get("originalDescription", "")

    # ----- Sale and auctioneer info (needed for status/result fallbacks) -----
    sale_info = lot.get("saleInfo") or {}

    # ----- Prices (result = hammer price; try lot, saleInfo, deep dict, then raw lot string) -----
    current_bid = lot.get("currentBid", 0) or 0
    next_bid = lot.get("nextBid", 0) or 0
    result_raw = _get_result_value(lot, sale_info)
    result = result_raw if isinstance(result_raw, (int, float)) else 0
    result = result or 0
    if result == 0:
      result = _find_result_in_dict(lot)
    if result == 0 and sale_info:
      result = _find_result_in_dict(sale_info)

    # 2b. SMART BROWSER RETRY:
    # If we used fast fetch (or browser without favorite?), and result is 0,
    # and the item seems to be closed/sold (status ambiguity), try browser with favorites.
    # We do this BEFORE the fallback chain.
    status_hint = _derive_status(lot, sale_info)
    if result == 0 and status_hint in (ListingStatus.UNSOLD, ListingStatus.UNKNOWN) and self.config.browser_profile:
        # Avoid infinite loop if we already used browser with favorites
        if not used_browser_with_favorites:
             logger.info(f"Lot {external_id}: Result 0, status {status_hint}. Retrying with Browser+Favorites.")
             try:
                 html = self._get_html(url, use_browser=True, check_favorites=True)
                 lot_browser = _extract_lot_detail(html)
                 if lot_browser:
                     lot = lot_browser
                     sale_info = lot.get("saleInfo") or {} # Update sale info

                     # Re-extract result
                     result_raw = _get_result_value(lot, sale_info)
                     result = result_raw if isinstance(result_raw, (int, float)) else 0
                     result = result or 0
                     if result == 0:
                         result = _find_result_in_dict(lot)
                     if result == 0 and sale_info:
                         result = _find_result_in_dict(sale_info)

                     logger.info(f"Lot {external_id}: Browser Retry Result: {result}")
             except Exception as e:
                 logger.error(f"Browser retry failed: {e}")

    # Re-derive status from raw lot data, but only override if the
    # fallback chain hasn't already determined a definitive status
    # (e.g. SOLD from JSON-LD/DOM regex/Gazette price-finding).
    status = ListingStatus.UNKNOWN
    derived = _derive_status(lot, sale_info)
    if derived != ListingStatus.UNKNOWN:
      status = derived

    # --- STRATEGY: BROWSER / FAVORITE (Consolidated) ---
    # If we have a browser profile, we might as well use it immediately if the standard request failed
    # OR we can be smart and try standard first (fast) then browser (slow).
    # But user wants single-pass if we are going to use the browser.

    # Actually, the user said: "it opens the same drouot listing twice, once to do nothing, and the second time to favorite".
    # This implies we should try to do it all in one go if we are using the browser.

    # New Logic:
    # 1. Try standard request (requests/curl_cffi) FIRST because it is much faster (hundreds of ms vs seconds).
    # 2. If standard fails (price 0) AND we have profile -> Open Browser ONCE with check_favorites=True.

    # Browser/Favorite strategy already applied via _get_html(check_favorites=True) above.

    # --- Result Fallbacks ---
    if result == 0:
        # Check for explicit "Lot not sold" text or "Auction ended" without a result
        is_unsold = False

        if "Lot not sold" in html or "Lot non vendu" in html:
            is_unsold = True
            logger.info(f"Lot {external_id}: Detected 'Lot not sold' text. Marking as UNSOLD.")



        if is_unsold:
            result = 0
            # Status will be derived as UNKNOWN/UNSOLD later.

        # If not unsold, try JSON-LD price extraction
        # CRITICAL FIX: Only try JSON-LD if status allows it.
        # If status is UNSOLD (e.g. CLOSED) or UPCOMING, JSON-LD price is likely starting/estimate.
        elif status not in (ListingStatus.UNSOLD, ListingStatus.UPCOMING, ListingStatus.CANCELLED):
             ld_data = _extract_json_ld_data(html)
             ld_price = _get_price_from_json_ld(ld_data)
             if ld_price > 0:
                 result = ld_price
                 logger.info(f"Lot {external_id}: JSON-LD fallback SUCCESS! Found price: {result}")
                 # If we found a price, it's likely SOLD now
                 status = ListingStatus.SOLD

        # DOM fallback (regex)
        if result == 0:
             # 1. Specific fallback (Result/Adjugé) - Safe to run even if status is UNSOLD
             # because it looks for explicit "Result:" labels.
             # (Note: html is already the interactive one from _get_html)
             raw_price_match = re.search(r'(?:result|resultat|résultat|adjugé|sold)[:\s]+([\d\s\u202f]+)', html, re.IGNORECASE)

             # 2. Generic fallback (Any price) - ONLY if status suggests it should be there (not UNSOLD/UPCOMING)
             if not raw_price_match and status not in (ListingStatus.UNSOLD, ListingStatus.UPCOMING):
                  raw_price_match = re.search(r'([\d\s\u202f]+)\s*€', html)

             if raw_price_match:
                 try:
                      raw_val = raw_price_match.group(1).replace(" ", "").replace("\u202f", "").replace("\xa0", "").strip()
                      # Filter out tiny or huge numbers (year?)
                      # Also filter out strict integer checks if decimal is allowed
                      clean_val = raw_val.replace(",", ".")
                      val_float = float(clean_val)
                      val_int = int(val_float)

                      if 1 < val_int < 100_000_000:
                          result = val_int
                          logger.info(f"Lot {external_id}: DOM Regex fallback SUCCESS! Found price: {result}")
                          # If we found a result, it is SOLD (override UNSOLD status)
                          status = ListingStatus.SOLD
                 except ValueError:
                      pass

    # --- STRATEGY 4: GAZETTE FALLBACK ---
    if result == 0:
      raw_lot_str = _extract_lot_detail_raw(html)
      if raw_lot_str:
        result = _extract_result_from_raw_lot(raw_lot_str)
        if result > 0:
          logger.debug("Lot %s: result %s from raw lot string", external_id, result)
    if result == 0 and external_id:
      result = self._fetch_result_from_gazette(external_id)
      if result > 0:
        logger.debug("Lot %s: result %s from Gazette Drouot", external_id, result)

    current_price = _decimal_or_none(current_bid) if current_bid > 0 else None
    starting_price = _decimal_or_none(next_bid) if next_bid > 0 else None

    estimate_low = _decimal_or_none(lot.get("lowEstim"))
    estimate_high = _decimal_or_none(lot.get("highEstim"))

    # ----- Fees (variable per auction house and sale) -----
    sale_fees = _decimal_or_none(lot.get("saleFees") or lot.get("fees"))

    # ----- Timing (needed before status fallback for ended sales) -----
    sale_date_timestamp = lot.get("date")
    start_time = _timestamp_to_datetime(sale_date_timestamp)
    bid_end_date = lot.get("bidEndDate", 0) or 0
    end_time = _timestamp_to_datetime(bid_end_date) if bid_end_date > 0 else start_time


    # ----- Status (lot and saleInfo; if sale ended and still unknown, treat as UNSOLD) -----
    status = _derive_status(lot, sale_info)
    final_price = None
    if result > 0:
      final_price = _decimal_or_none(result)
      # Only mark as SOLD if we are not explicitly in another valid state (UPCOMING, ACTIVE, UNSOLD)
      if status == ListingStatus.UNKNOWN:
        status = ListingStatus.SOLD

    if lot.get("reserveNotReached"):
      status = ListingStatus.UNSOLD

    # ----- Sale and auctioneer info (continued) -----
    auctioneer_card = sale_info.get("auctioneerCard") or {}
    auctioneer_link = auctioneer_card.get("link") or {}

    auction_house_name = auctioneer_link.get("auctioneerName")
    sale_name = sale_info.get("title")

    # Sale address.
    address_info = sale_info.get("address") or {}
    sale_city = address_info.get("city")
    sale_country_code = _country_id_to_code(address_info.get("country"))

    # ----- Seller (auction house acts as the seller) -----
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

    # ----- Images -----
    images = _parse_images(lot)

    # ----- Attributes -----
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

    if original_description and original_description != description:
      attributes["original_description"] = original_description

    # Transport size.
    transport_size = lot.get("transportSize")
    if transport_size and transport_size != "NO_SIZE":
      attributes["transport_size"] = transport_size

    # Categories from the lot data.
    categories = lot.get("categories") or []
    if categories:
      attributes["category_ids"] = ",".join(str(cat_id) for cat_id in categories)

    # Build title from description.
    title = _build_title(description, lot.get("num"))
    lot_number = str(lot.get("num", "")) if lot.get("num") else None

    return ScrapedListing(
      external_id=external_id,
      url=url,
      title=title,
      description=description,
      listing_type=_derive_listing_type(lot),
      condition=ItemCondition.UNKNOWN,
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
      bid_count=_count_bids(lot),
      watcher_count=None,
      view_count=None,
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

  def _get_html(self, url: str, use_browser: Optional[bool] = None, **kwargs) -> str:
    """Perform a rate-limited GET and return the response body.

    If ``use_browser`` is True (or None and browser is enabled), uses
    the shared Camoufox browser.  Otherwise uses curl_cffi/requests.
    """
    should_use_browser = use_browser if use_browser is not None else self._browser_enabled

    if should_use_browser:
      try:
        check_favorites = kwargs.get("check_favorites", False)
        if check_favorites:
          return self._run_on_browser_thread(
            self._browser_fetch_with_favorites, url,
          )
        return self._get_html_via_browser(url)
      except Exception as e:
        logger.error(f"Browser scrape failed for {url}: {e}")
        raise

    response = self._get(url)
    response.encoding = "utf-8"
    return response.text

  def _browser_fetch_with_favorites(self, url: str) -> str:
    """Browser fetch with favorite-click to reveal hidden prices.

    Runs on the dedicated browser thread.
    """
    self._ensure_browser()

    page = self._browser_page
    if not page:
      page = self._browser_context.new_page()
      self._browser_page = page

    nav_timeout = int(self.config.browser_nav_timeout * 1000)
    pre_min, pre_max = self.config.browser_pre_nav_delay

    logger.debug("Navigating to %s …", url)
    time.sleep(random.uniform(pre_min, pre_max))

    try:
      page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
    except Exception as exc:
      logger.error("Browser navigation failed for %s: %s", url, exc)
      raise

    try:
      page.wait_for_load_state(
        "networkidle",
        timeout=int(self.config.browser_idle_timeout * 1000),
      )
    except Exception:
      pass

    if self.config.browser_human_behavior:
      self._browser_human_behavior(page)
    post_min, post_max = self.config.browser_post_nav_delay
    time.sleep(random.uniform(post_min, post_max))

    # --- Favorite click to reveal hidden result prices ---
    fav_selector = "[data-cy='icon-favorite']"
    try:
      # Wait for client hydration.
      page.wait_for_timeout(int(self.config.browser_post_click_delay * 1000))

      if page.locator(fav_selector).count() > 0:
        logger.debug("Clicking 'Favorite' to reveal price…")
        page.click(fav_selector)
        page.wait_for_timeout(int(self.config.browser_post_click_delay * 1000))

        # Cleanup: un-favorite to avoid cluttering account.
        if page.locator(fav_selector).count() > 0:
          logger.debug("Cleaning up: clicking 'Favorite' again to toggle off…")
          page.click(fav_selector)
          page.wait_for_timeout(int(self.config.browser_post_click_delay * 500))
      else:
        logger.warning("Favorite button not found during check_favorites strategy.")
    except Exception as e:
      logger.error(f"Error during interactive favorite check: {e}")

    self._last_request_time = time.time()
    return page.content()

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_lot_url(url_or_id: str) -> str:
    """Accept a full URL or a numeric lot ID and return a full URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID.
    return f"{_BASE_URL}/en/l/{url_or_id}"

  def _fetch_result_from_gazette(self, external_id: str) -> int:
    """Fetch the lot result page on Gazette Drouot and try to extract hammer price.

    Used when the main drouot.com lot page does not contain the result (e.g. it
    is loaded only on the results site).
    """
    url = f"{_GAZETTE_BASE}/lots/{external_id}"
    try:
      response = self._get(url)
      response.encoding = "utf-8"
      html = response.text
    except Exception as error:
      logger.debug("Could not fetch Gazette Drouot lot %s: %s", external_id, error)
      return 0
    # Look for price: "1 700 €", "1700 EUR", "1 700 EUR", "result":1700, etc.
    patterns = [
      r'"result"\s*:\s*(\d+(?:\.\d+)?)',
      r'"price"\s*:\s*(\d+(?:\.\d+)?)',
      r'"hammerPrice"\s*:\s*(\d+(?:\.\d+)?)',
      r'(\d[\d\s\u202f]*\d?)\s*[€EUR]',
      r'[€EUR]\s*(\d[\d\s\u202f]*\d?)',
    ]
    for pattern in patterns:
      for match in re.finditer(pattern, html, re.IGNORECASE):
        try:
          raw = match.group(1).replace(" ", "").replace("\u202f", "").replace("\xa0", "")
          value = int(float(raw))
          if 0 < value < 100_000_000:
            return value
        except (ValueError, IndexError):
          continue
    return 0


  def _extract_listing_from_json_ld(self, html: str, url: str, is_unsold: bool = False) -> Optional[ScrapedListing]:
      """Construct a ScrapedListing purely from JSON-LD data."""
      data = _extract_json_ld_data(html)
      if not data:
          return None

      try:
          external_id = str(data.get("sku", ""))
          if not external_id:
              return None

          title = data.get("name", "Unknown Title")
          description = data.get("description", "")

          offers = data.get("offers", {})
          price = 0
          currency = "EUR"

          if isinstance(offers, dict):
              price = int(float(offers.get("price", 0)))
              currency = offers.get("priceCurrency", "EUR")
          elif isinstance(offers, list) and offers:
               price = int(float(offers[0].get("price", 0)))
               currency = offers[0].get("priceCurrency", "EUR")

          image_url = data.get("image")
          images = []
          if image_url:
              image_url = _drouot_image_url_to_high_res(image_url)
              images.append(ScrapedImage(source_url=image_url, position=0))

          # If we have a price in JSON-LD here, it's likely the result/current price
          final_price = None
          status = ListingStatus.UNKNOWN
          current_price = None


          # Date parsing from JSON-LD or HTML
          # Try "priceValidUntil" in offers, which often indicates auction end
          end_time = None
          valid_until = offers.get("priceValidUntil") if isinstance(offers, dict) else None
          if valid_until:
              try:
                  # ISO format: 2025-03-15 or 2025-03-15T14:00:00
                  end_time = datetime.fromisoformat(valid_until.replace("Z", "+00:00"))
              except ValueError:
                  pass

          # Try other fields for date
          if not end_time:
              # Sometimes endDate is at top level
              et = data.get("endDate")
              if et:
                  try:
                      end_time = datetime.fromisoformat(et.replace("Z", "+00:00"))
                  except ValueError:
                      pass

          logger.info(f"JSON-LD Date Parsed: {end_time} (valid_until={valid_until})")

          # If no date in JSON, maybe we can guess from context or just be safe
          is_future = end_time and end_time > datetime.now(timezone.utc)

          if is_unsold:
               status = ListingStatus.UNSOLD
               current_price = Decimal(price)
               final_price = None
          elif is_future:
               # Definitely upcoming
               status = ListingStatus.UPCOMING
               current_price = Decimal(price) # Estimate or start price
               final_price = None
          else:
               # Date unknown or past.
               # Checking "availability"
               availability = offers.get("availability", "") if isinstance(offers, dict) else ""

               if "InStock" in availability:
                   # Likely active/upcoming
                   status = ListingStatus.ACTIVE
                   current_price = Decimal(price)
                   final_price = None
               elif price > 0:
                   # Only if we are SURE it's past, mark as SOLD.
                   # But without a date, assuming SOLD is dangerous (as proven by the bug).
                   # Safer to assume ACTIVE/UNKNOWN so it gets checked again.
                   if end_time and end_time < datetime.now(timezone.utc):
                       status = ListingStatus.SOLD
                       current_price = Decimal(price)
                       final_price = current_price

          logger.debug("JSON-LD status decision: is_unsold=%s, is_future=%s, end_time=%s, price=%s, status=%s", is_unsold, is_future, end_time, price, status)

          return ScrapedListing(
              external_id=external_id,
              url=url,
              title=title,
              description=description,
              listing_type=ListingType.AUCTION,
              condition=ItemCondition.UNKNOWN,
              currency=currency,
              starting_price=None,
              reserve_price=None,
              estimate_low=None,
              estimate_high=None,
              buy_now_price=None,
              current_price=current_price,
              final_price=final_price,
              buyer_premium_percent=None,
              buyer_premium_fixed=None,
              shipping_cost=None,
              shipping_from_country=None, # Unknown
              ships_internationally=None,
              start_time=None,
              end_time=end_time,
              status=status,
              bid_count=None,
              watcher_count=None,
              view_count=None,
              lot_number=None,
              auction_house_name=None,
              sale_name=None,
              sale_date=None,
              seller=None,
              images=images,
              bids=[],
              attributes={"source": "json-ld_fallback"},
          )
      except Exception as e:
          logger.error(f"JSON-LD listing construction failed: {e}")
          return None





def _extract_json_ld_data(html: str) -> Optional[dict]:
  """Extract dict from JSON-LD schema in the page."""
  match = re.search(r'<script type="application/ld\+json">(.+?)</script>', html, re.DOTALL)
  if match:
    try:
      return json.loads(match.group(1))
    except (json.JSONDecodeError, ValueError, TypeError):
      pass
  return None

def _get_price_from_json_ld(data: Optional[dict]) -> int:
    if not data:
        return 0
    offers = data.get("offers")
    if isinstance(offers, dict):
        price = offers.get("price")
        if price is not None:
             return int(float(price))
    elif isinstance(offers, list):
        for offer in offers:
            price = offer.get("price")
            if price is not None:
                return int(float(price))
    return 0





# ------------------------------------------------------------------
# SvelteKit data extraction
# ------------------------------------------------------------------

def _extract_search_lots(html: str) -> list[dict]:
  """Extract the ``lots`` array from the SvelteKit SSR data in a
  search results page.

  The SvelteKit boot script contains a ``data:`` property with an
  array of data blocks.  The search results are in the last block
  under ``data.lots``.
  """
  # Find the start of the lots array.
  start_marker = "lots:["
  start = html.find(start_marker)
  if start == -1:
    logger.warning("Could not find lots array in Drouot search page.")
    return []
  start += len(start_marker)

  # Find the matching ``]`` by counting braces.
  raw = _extract_balanced_bracket(html, start - 1, "[", "]")
  if raw is None:
    logger.warning("Could not find end of lots array in Drouot search page.")
    return []

  # ``raw`` includes the outer brackets; strip them.
  inner = raw[1:-1].strip()
  if not inner:
    return []

  return _parse_js_object_array(inner)


def _extract_lot_detail_raw(html: str) -> Optional[str]:
  """Extract the raw lot object string (balanced braces) from a Drouot lot page."""
  match = re.search(r'lot:\{', html)
  if match is None:
    return None
  start = match.start() + len("lot:")
  return _extract_balanced_bracket(html, start, "{", "}")


def _extract_lot_detail(html: str) -> Optional[dict]:
  """Extract the lot detail object from a Drouot lot page.

  On lot pages, the SvelteKit data contains ``lot:{...}`` with
  all the lot fields.
  """
  raw = _extract_lot_detail_raw(html)
  if raw is None:
    logger.warning("Could not find lot data in Drouot lot page.")
    return None
  return _parse_js_object(raw)


def _find_result_in_dict(obj, keys: Sequence[str] = ("result", "hammerPrice", "soldPrice", "winningBid", "price", "priceRealized")) -> int:
  """Recursively search a dict for any of the given keys with a positive numeric value."""
  if not isinstance(obj, dict):
    return 0
  for key in keys:
    value = obj.get(key)
    if value is not None:
      try:
        number = int(value) if isinstance(value, (int, float)) else int(float(str(value).replace(" ", "").replace("\u202f", "")))
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


def _extract_result_from_raw_lot(raw_lot_str: str) -> int:
  """Find hammer/result price in the raw lot object string (before JSON parse).

  Handles unquoted keys and decimal values. Only searches within the
  given string (the lot object) to avoid picking up wrong numbers.
  """
  patterns = [
    r'"result"\s*:\s*(\d+(?:\.\d+)?)',
    r"'result'\s*:\s*(\d+(?:\.\d+)?)",
    r'\bresult\s*:\s*(\d+(?:\.\d+)?)',
    r'"hammerPrice"\s*:\s*(\d+(?:\.\d+)?)',
    r'"soldPrice"\s*:\s*(\d+(?:\.\d+)?)',
    r'"priceRealized"\s*:\s*(\d+(?:\.\d+)?)',
    r'\bhammerPrice\s*:\s*(\d+(?:\.\d+)?)',
  ]
  for pattern in patterns:
    match = re.search(pattern, raw_lot_str)
    if match:
      try:
        value = float(match.group(1))
        if 0 < value < 100_000_000:
          return int(value)
      except ValueError:
        pass
  return 0


# ------------------------------------------------------------------
# Bracket-matching helper
# ------------------------------------------------------------------

def _extract_balanced_bracket(
  text: str,
  start: int,
  open_char: str,
  close_char: str,
) -> Optional[str]:
  """Extract a balanced bracket-delimited expression from *text*.

  *start* must point to the opening bracket character.  Returns the
  full substring from ``open_char`` to the matching ``close_char``
  (inclusive), or ``None`` if no match is found.  Handles nesting and
  skips over quoted strings.
  """
  if start >= len(text) or text[start] != open_char:
    return None

  depth = 0
  in_string = False
  string_char: Optional[str] = None
  i = start

  while i < len(text):
    ch = text[i]

    if in_string:
      if ch == "\\\\" and i + 1 < len(text):
        i += 2  # Skip escaped character.
        continue
      if ch == string_char:
        in_string = False
    else:
      if ch in ('"', "'"):
        in_string = True
        string_char = ch
      elif ch == open_char:
        depth += 1
      elif ch == close_char:
        depth -= 1
        if depth == 0:
          return text[start : i + 1]

    i += 1

  return None


# ------------------------------------------------------------------
# JavaScript-to-JSON parsing helpers
# ------------------------------------------------------------------

def _js_to_json(raw: str) -> str:
  """Convert a JavaScript object/array literal to valid JSON.

  This does a character-by-character walk so it never modifies text
  inside string literals (which would corrupt descriptions that
  contain commas, colons, etc.).

  Handles:
  - ``void 0`` → ``null``
  - ``new Date(N)`` → ``N``
  - ``new Map(...)`` → ``null``
  - Unquoted property names → quoted
  - Trailing commas before ``}`` / ``]``
  """
  out: list[str] = []
  i = 0
  n = len(raw)

  while i < n:
    ch = raw[i]

    # ------ String literals: copy verbatim ------
    if ch in ('"', "'"):
      quote = ch
      j = i + 1
      while j < n:
        if raw[j] == "\\" and j + 1 < n:
          j += 2
          continue
        if raw[j] == quote:
          j += 1
          break
        j += 1
      out.append(raw[i:j])
      i = j
      continue

    # ------ ``void 0`` → ``null`` ------
    if raw[i:i + 6] == "void 0":
      out.append("null")
      i += 6
      continue

    # ------ ``new Date(N)`` → ``N`` ------
    m = re.match(r'new\s+Date\((\d+)\)', raw[i:])
    if m:
      out.append(m.group(1))
      i += m.end()
      continue

    # ------ ``new Map(...)`` → ``null`` ------
    if raw[i:i + 7] == "new Map":
      # Skip until the closing paren.
      depth = 0
      j = i + 7
      while j < n:
        if raw[j] == "(":
          depth += 1
        elif raw[j] == ")":
          depth -= 1
          if depth == 0:
            j += 1
            break
        j += 1
      out.append("null")
      i = j
      continue

    # ------ Unquoted property names ------
    # After ``{`` or ``,`` (ignoring whitespace), an identifier
    # followed by ``:`` is an unquoted key.
    if ch in ("{", ","):
      out.append(ch)
      i += 1
      # Skip whitespace.
      while i < n and raw[i] in (" ", "\t", "\n", "\r"):
        out.append(raw[i])
        i += 1
      # Check for an unquoted identifier followed by ``:``.
      km = re.match(r'([A-Za-z_$][A-Za-z0-9_$]*)(\s*:\s*)', raw[i:])
      if km:
        out.append('"')
        out.append(km.group(1))
        out.append('"')
        out.append(":")
        i += km.end()
      continue

    # ------ Trailing commas ------
    if ch == ",":
      # Peek ahead for ``}`` or ``]`` after optional whitespace.
      j = i + 1
      while j < n and raw[j] in (" ", "\t", "\n", "\r"):
        j += 1
      if j < n and raw[j] in ("}", "]"):
        # Skip the trailing comma.
        i += 1
        continue

    out.append(ch)
    i += 1

  return "".join(out)


def _parse_js_object(raw: str) -> Optional[dict]:
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
  """Parse a comma-separated series of JS objects into a list of dicts.

  The input is the *inner* content of an array literal (without the
  surrounding ``[`` and ``]``).
  """
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


# ------------------------------------------------------------------
# Pure parsing helpers
# ------------------------------------------------------------------

def _get_sale_status(lot: dict, sale_info: Optional[dict] = None) -> str:
  """Get sale status from lot or sale-level data (Drouot may put it on either)."""
  status = (lot.get("saleStatus") or "").strip().upper()
  if status:
    return status
  if sale_info:
    status = (sale_info.get("saleStatus") or sale_info.get("status") or "").strip().upper()
    if status:
      return status
  return ""


def _get_result_value(lot: dict, sale_info: Optional[dict] = None):
  """Get hammer/result price from lot or sale (tries common field names)."""
  for source in (lot, sale_info or {}):
    if source is None:
      continue
    # Priority: result > hammerPrice > soldPrice > winningBid > price
    # BUT sometimes 'currentBid' or 'nextBid' might be there.
    # Check explicitly for result fields.
    for key in ("result", "hammerPrice", "soldPrice", "winningBid", "price"):
      value = source.get(key)
      if value is not None and value != 0:
        try:
          return int(value) if isinstance(value, (int, float)) else value
        except (TypeError, ValueError):
          pass
  return 0


def _derive_status(lot: dict, sale_info: Optional[dict] = None) -> ListingStatus:
  """Map Drouot sale status to our listing status enum.

  Status and result may be on the lot or on saleInfo depending on page structure.
  """
  sale_status = _get_sale_status(lot, sale_info)
  result = _get_result_value(lot, sale_info)
  if not isinstance(result, (int, float)):
    result = 0
  result = result or 0

  logger.debug("_derive_status: sale_status=%s, result=%s", sale_status, result)

  if result > 0:
    return ListingStatus.SOLD
  if lot.get("reserveNotReached"):
    return ListingStatus.UNSOLD
  if sale_status == "ENDED":
    return ListingStatus.UNSOLD
  if sale_status == "CLOSED":
    return ListingStatus.UNSOLD
  if sale_status == "IN_PROGRESS":
    return ListingStatus.ACTIVE
  if sale_status == "CREATED":
    return ListingStatus.UPCOMING
  if sale_status in ("CANCELLED", "SUSPENDED"):
    return ListingStatus.CANCELLED
  return ListingStatus.UNKNOWN


def _derive_listing_type(lot: dict) -> ListingType:
  """Determine listing type from the sale type."""
  sale_type = (lot.get("saleType") or "").upper()
  if sale_type == "ONLINE":
    return ListingType.AUCTION
  # "LIVE" sales are also auctions, just in person.
  return ListingType.AUCTION


def _build_title(description: str, lot_number: Optional[int] = None) -> str:
  """Build a concise title from the description text.

  Drouot lots don't have a separate title field; the description
  serves as both.  We take the first meaningful lines (up to ~120
  characters) as the title and optionally prepend the lot number.
  """
  if not description:
    return "(no description)"

  # Collect lines until we have a reasonable title length.
  lines = [l.strip() for l in description.split("\\n") if l.strip()]
  title_parts: list[str] = []
  total_len = 0
  for line in lines:
    if total_len + len(line) > 120:
      # If we haven't collected anything yet, truncate this line.
      if not title_parts:
        title_parts.append(line[:117] + "...")
      break
    title_parts.append(line)
    total_len += len(line) + 2  # +2 for the separator

  title = " — ".join(title_parts) if title_parts else "(no description)"

  if lot_number is not None:
    return f"{lot_number} - {title}"
  return title


def _parse_images(lot: dict) -> list[ScrapedImage]:
  """Build the list of images from the lot data."""
  result: list[ScrapedImage] = []
  seen_paths: set[str] = set()

  # Detail pages have a ``photos`` array; search results only have ``photo``.
  photos = lot.get("photos") or []
  if not photos:
    single_photo = lot.get("photo")
    if single_photo and single_photo.get("path"):
      photos = [single_photo]

  for position, photo in enumerate(photos):
    path = photo.get("path")
    if not path or path in seen_paths:
      continue
    seen_paths.add(path)
    image_url = _build_image_url(path)
    result.append(ScrapedImage(source_url=image_url, position=position))

  return result


def _build_image_url(path: str) -> str:
  """Construct a full CDN image URL from a path.

  Uses high-resolution size ``ftall`` and URL-encodes the path so the
  query string is valid.
  """
  encoded_path = quote_plus(path)
  return f"{_CDN_BASE}?size={_IMAGE_SIZE}&path={encoded_path}"


def _drouot_image_url_to_high_res(url: str) -> str:
  """Ensure a Drouot CDN image URL uses high-resolution (size=ftall).

  Use for any image URL taken from the page (e.g. JSON-LD or HTML) so we
  store and display high-quality images instead of thumbnails.
  """
  if not url or "cdn.drouot.com" not in url:
    return url
  if "size=" in url:
    url = re.sub(r"size=[^&]+", f"size={_IMAGE_SIZE}", url)
  else:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["size"] = [_IMAGE_SIZE]
    new_query = urlencode(query, doseq=True)
    url = urlunparse(parsed._replace(query=new_query))
  return url


def _count_bids(lot: dict) -> int:
  """Estimate the number of bids from available data.

  Drouot does not expose a bid count directly.  If there is a
  ``currentBid`` greater than zero, at least one bid has been placed.
  """
  current_bid = lot.get("currentBid", 0) or 0
  return 1 if current_bid > 0 else 0


def _timestamp_to_datetime(timestamp: Optional[int]) -> Optional[datetime]:
  """Convert a Unix timestamp (seconds) to a timezone-aware datetime."""
  if timestamp is None or timestamp == 0:
    return None
  try:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
  except (ValueError, TypeError, OSError):
    return None


_COUNTRY_CODE_MAP: dict[int, str] = {
  # Drouot uses numeric country IDs.  The most common ones:
  75: "FR",
  18: "BE",
  44: "GB",
  49: "DE",
  34: "ES",
  39: "IT",
  41: "CH",
  1: "US",
  31: "NL",
  43: "AT",
  351: "PT",
  352: "LU",
  33: "FR",
  963: "FR",
}


def _country_id_to_code(country_id: Optional[int]) -> Optional[str]:
  """Convert a Drouot numeric country ID to an ISO 3166-1 alpha-2 code."""
  if country_id is None:
    return None
  return _COUNTRY_CODE_MAP.get(country_id)


def _decimal_or_none(value) -> Optional[Decimal]:
  """Safely convert a numeric value to Decimal."""
  if value is None:
    return None
  try:
    decimal_value = Decimal(str(value))
    # Ignore zero values for prices.
    if decimal_value == 0:
      return None
    return decimal_value
  except (InvalidOperation, ValueError, TypeError):
    return None


def _url_encode(text: str) -> str:
  """Minimal URL encoding for query strings."""
  from urllib.parse import quote_plus
  return quote_plus(text)
