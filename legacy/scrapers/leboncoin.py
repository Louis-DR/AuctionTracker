"""LeBonCoin scraper.

LeBonCoin is France's largest classified-ads marketplace.  Unlike
auction sites, listings have a fixed asking price (sometimes
negotiable) and no end date.  Items simply disappear when the seller
removes them — whether because the item was sold, the ad expired, or
for any other reason.

Key LeBonCoin facts used in the scraper:

* The site is a **Next.js** application.  Both search result pages and
  individual ad pages embed a ``__NEXT_DATA__`` JSON blob in an inline
  ``<script>`` tag.
* **DataDome** anti-bot protection is in place, but ``curl_cffi`` with
  Chrome impersonation bypasses it reliably.
* Search URL pattern:
  ``https://www.leboncoin.fr/recherche?text=QUERY&page=N``
  Results are in ``pageProps.searchData.ads`` (up to ~35 per page).
* Ad detail URL pattern:
  ``https://www.leboncoin.fr/ad/{category_slug}/{list_id}``
  Data is in ``pageProps.ad``.
* When an ad is removed the server returns **HTTP 410** and
  ``pageProps.ad`` is ``None``.
* Prices are in euros.  ``price`` is a list (usually one element) in
  whole euros, ``price_cents`` is the precise amount in cents (which
  may include LeBonCoin's buyer protection fee).
* The ``buyer_fee`` field, when present, gives the platform fee in
  cents.
* Condition values: ``etatneuf`` (new), ``tresbonetat`` (very good),
  ``bonetat`` (good), ``etatsatisfaisant`` (fair).
* The ``owner`` object contains ``store_id``, ``user_id``, ``name``,
  and ``type`` (``"private"`` or ``"pro"``).
* ``counters.favorites`` gives the number of users who bookmarked the
  listing.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Sequence

from curl_cffi import requests as cffi_requests

from auction_tracker.config import ScrapingConfig
from auction_tracker.database.models import (
  ItemCondition,
  ListingStatus,
  ListingType,
)
from auction_tracker.scrapers.base import (
  BaseScraper,
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

_BASE_URL = "https://www.leboncoin.fr"

# Map LeBonCoin condition slugs to our enum.
_CONDITION_MAP: dict[str, ItemCondition] = {
  "etatneuf": ItemCondition.NEW,
  "tresbonetat": ItemCondition.VERY_GOOD,
  "bonetat": ItemCondition.GOOD,
  "etatsatisfaisant": ItemCondition.FAIR,
}


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("leboncoin")
class LeBonCoinScraper(BaseScraper):
  """Scraper for LeBonCoin classified ads.

  LeBonCoin is a fixed-price marketplace.  There are no auctions, no
  bids, and no end times.  Listings are considered *active* until they
  disappear (HTTP 410).  The monitoring strategy is ``snapshot``:
  periodic checks to detect price changes and removal.
  """

  def __init__(self, config: ScrapingConfig) -> None:
    super().__init__(config)
    self._cffi_session = cffi_requests.Session(impersonate="chrome")
    # Set headers to mimic a real browser session
    self._cffi_session.headers.update({
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
      "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
      "Accept-Encoding": "gzip, deflate, br",
      "DNT": "1",
      "Connection": "keep-alive",
      "Upgrade-Insecure-Requests": "1",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
    })
    self._browser_warmed_up = False

  @property
  def _browser_locale(self) -> str:
    return "fr-FR"

  # ------------------------------------------------------------------
  # Browser anti-detection (LeBonCoin / DataDome specific)
  # ------------------------------------------------------------------

  def _browser_fetch(self, url: str) -> str:
    """LeBonCoin-specific browser fetch with DataDome handling.

    - On first use, visits the homepage to warm up cookies.
    - Accepts cookie consent banner (DataDome tracks this).
    - Detects and waits for DataDome challenges.
    """
    self._ensure_browser()

    page = self._browser_page
    if not page:
      page = self._browser_context.new_page()
      self._browser_page = page

    nav_timeout = int(self.config.browser_nav_timeout * 1000)
    pre_min, pre_max = self.config.browser_pre_nav_delay
    post_min, post_max = self.config.browser_post_nav_delay

    # --- First visit: warm up by visiting the homepage ---
    if not self._browser_warmed_up:
      self._browser_warmed_up = True
      logger.info("LeBonCoin: warming up browser session (visiting homepage)…")
      try:
        page.goto("https://www.leboncoin.fr", timeout=30_000, wait_until="domcontentloaded")
        time.sleep(random.uniform(1.0, 2.0))
        self._accept_cookie_consent(page)
        if self.config.browser_human_behavior:
          self._browser_human_behavior(page)
        time.sleep(random.uniform(0.5, 1.0))
      except Exception as exc:
        logger.warning("Homepage warm-up warning: %s", exc)

    # --- Navigate to the actual URL ---
    logger.debug("Browser navigating to %s …", url)
    time.sleep(random.uniform(pre_min, pre_max))

    try:
      page.goto(url, timeout=nav_timeout, wait_until="domcontentloaded")
    except Exception as exc:
      logger.error("Browser navigation failed for %s: %s", url, exc)
      raise

    # Wait for dynamic content.
    try:
      page.wait_for_load_state(
        "networkidle",
        timeout=int(self.config.browser_idle_timeout * 1000),
      )
    except Exception:
      pass

    # Check for and handle DataDome challenge.
    self._wait_for_datadome(page)

    # Accept cookie consent if it reappears.
    try:
      self._accept_cookie_consent(page)
    except Exception:
      pass

    # Simulate realistic human behaviour.
    if self.config.browser_human_behavior:
      try:
        self._browser_human_behavior(page)
      except Exception:
        pass

    time.sleep(random.uniform(post_min, post_max))
    self._last_request_time = time.time()

    return page.content()

  def _accept_cookie_consent(self, page) -> None:
    """Click the cookie consent 'Accept' button if present."""
    try:
      # LeBonCoin cookie consent selectors (Didomi-based).
      consent_selectors = [
        "#didomi-notice-agree-button",
        "button[aria-label='Accepter & Fermer']",
        "button:has-text('Accepter')",
        "button:has-text('Tout accepter')",
        "#consent-page button",
      ]
      for selector in consent_selectors:
        try:
          btn = page.locator(selector).first
          if btn.is_visible(timeout=500):
            time.sleep(random.uniform(0.5, 1.5))
            btn.click()
            logger.info("LeBonCoin: accepted cookie consent.")
            time.sleep(random.uniform(0.5, 1.0))
            return
        except Exception:
          continue
    except Exception:
      pass  # No consent banner found, that's fine.

  def _wait_for_datadome(self, page) -> None:
    """Detect DataDome interstitial/challenge and wait for it to resolve."""
    if not self._is_datadome_challenge(page):
      return

    logger.info("DataDome challenge detected — waiting for resolution…")
    start = time.monotonic()
    max_wait = 15  # seconds

    while time.monotonic() - start < max_wait:
      time.sleep(2.0)
      if not self._is_datadome_challenge(page):
        logger.info("DataDome challenge resolved (%.1fs).", time.monotonic() - start)
        return

    logger.warning("DataDome challenge did not resolve within %ds.", max_wait)

  @staticmethod
  def _is_datadome_challenge(page) -> bool:
    """Return True if the page is showing a DataDome challenge."""
    try:
      content = page.content()
      return (
        "geo.captcha-delivery.com" in content
        or "datadome" in content.lower()[:3000]
        or "dd.js" in content[:5000]
      ) and len(content) < 10_000  # Real pages are much larger.
    except Exception:
      return False

  # ------------------------------------------------------------------
  # Metadata
  # ------------------------------------------------------------------

  @property
  def website_name(self) -> str:
    return "LeBonCoin"

  @property
  def website_base_url(self) -> str:
    return _BASE_URL

  @property
  def capabilities(self) -> ScraperCapabilities:
    return ScraperCapabilities(
      can_search=True,
      can_fetch_listing=True,
      can_fetch_bids=False,
      can_fetch_seller=True,
      has_bid_history=False,
      has_watcher_count=True,  # favorites count
      has_view_count=False,
      has_buy_now=True,  # All listings are fixed-price.
      has_estimates=False,
      has_reserve_price=False,
      has_lot_numbers=False,
      has_auction_house_info=False,
      monitoring_strategy="snapshot",
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
    """Search LeBonCoin and return summarised results."""
    params: dict[str, str] = {"text": query}
    if category:
      params["category"] = category
    if page > 1:
      params["page"] = str(page)

    next_data = self._get_next_data(
      f"{_BASE_URL}/recherche",
      params=params,
    )
    if next_data is None:
      return []

    search_data = (
      next_data
      .get("props", {})
      .get("pageProps", {})
      .get("searchData", {})
    )
    ads = search_data.get("ads") or []
    total = search_data.get("total", 0)

    results: list[SearchResult] = []
    for ad in ads:
      result = self._ad_to_search_result(ad)
      if result is not None:
        results.append(result)

    logger.info(
      "LeBonCoin search '%s' page %d: %d results (total %s).",
      query, page, len(results), total,
    )
    return results

  # ------------------------------------------------------------------
  # Fetch listing
  # ------------------------------------------------------------------

  def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
    """Fetch the full details of a LeBonCoin listing."""
    url = self._normalise_url(url_or_external_id)
    list_id = self._extract_list_id(url_or_external_id)

    # Set Referer header to mimic browser navigation from search results
    original_headers = self._cffi_session.headers.copy()
    try:
      self._cffi_session.headers["Referer"] = f"{_BASE_URL}/recherche"
      next_data = self._get_next_data(url)
    finally:
      # Restore original headers
      self._cffi_session.headers = original_headers

    if next_data is None:
      raise ValueError(f"Could not fetch LeBonCoin listing: {url}")

    ad = (
      next_data
      .get("props", {})
      .get("pageProps", {})
      .get("ad")
    )

    if ad is None:
      # The listing has been removed (HTTP 410).
      logger.info("Listing %s has been removed (410).", list_id)
      return ScrapedListing(
        external_id=str(list_id),
        url=url,
        title=f"[Removed] LeBonCoin #{list_id}",
        status=ListingStatus.SOLD,
        listing_type=ListingType.BUY_NOW,
        currency="EUR",
      )

    return self._parse_ad(ad)

  # ------------------------------------------------------------------
  # Internal helpers
  # ------------------------------------------------------------------

  def _get_next_data(
    self,
    url: str,
    *,
    params: Optional[dict[str, str]] = None,
  ) -> Optional[dict]:
    """Fetch a LeBonCoin page and extract the __NEXT_DATA__ JSON."""
    try:
      self._rate_limit()

      # --- Browser path (bypasses DataDome natively) ---
      if self._browser_enabled:
        # Build the full URL with params for browser navigation.
        if params:
          from urllib.parse import urlencode
          full_url = f"{url}?{urlencode(params)}"
        else:
          full_url = url

        html = self._get_html_via_browser(full_url)

        # Check for 410-style removal (page shows no ad data).
        if len(html) < 1000:
          logger.warning(
            "LeBonCoin browser returned short HTML (%d bytes) for %s",
            len(html), url,
          )

        return _extract_next_data(html)

      # --- curl_cffi path (original) ---
      logger.debug("GET %s (params=%s)", url, params)
      response = self._cffi_session.get(
        url,
        params=params,
        timeout=self.config.timeout,
      )
      response.encoding = "utf-8"
      html = response.text

      # Check HTTP status code
      if response.status_code != 200:
        logger.warning(
          "LeBonCoin returned HTTP %d for %s (expected 200)",
          response.status_code, url,
        )
        if response.status_code == 410:
          # Listing removed - this is expected, return special dict to signal removal
          return {"status_code": 410}

      # Check for DataDome challenge.
      if len(html) < 5000 and "datadome" in html.lower():
        logger.warning("DataDome challenge detected on %s (HTML length: %d)", url, len(html))
        return None

      # Check if HTML looks suspiciously short (might be a redirect or error page)
      if len(html) < 1000:
        logger.warning(
          "LeBonCoin returned suspiciously short HTML (%d bytes) for %s. "
          "First 500 chars: %s",
          len(html), url, html[:500],
        )

      return _extract_next_data(html)

    except Exception as error:
      logger.error("Failed to fetch %s: %s", url, error, exc_info=True)
      return None

  def _fetch_seller_profile(self, user_id: str) -> Optional[dict]:
    """Fetch a seller's profile page to get registration date and badges.

    Args:
      user_id: The seller's UUID (from owner.user_id in the ad).

    Returns:
      A dict with profileInfo data, or None if the profile can't be fetched.
    """
    profile_url = f"{_BASE_URL}/profile/{user_id}/offers"
    try:
      self._rate_limit()
      logger.debug("GET %s (seller profile)", profile_url)
      response = self._cffi_session.get(profile_url, timeout=self.config.timeout)
      response.encoding = "utf-8"
      html = response.text

      profile_data = _extract_next_data(html)
      if not profile_data:
        return None

      page_props = profile_data.get("props", {}).get("pageProps", {})
      return page_props.get("profileInfo")

    except Exception as error:
      logger.debug("Failed to fetch seller profile %s: %s", user_id, error)
      return None

  def _ad_to_search_result(self, ad: dict) -> Optional[SearchResult]:
    """Convert a search-result ad dict to a SearchResult DTO."""
    list_id = ad.get("list_id")
    if list_id is None:
      return None

    title = ad.get("subject", "")
    url = ad.get("url", f"{_BASE_URL}/ad/{list_id}")
    price = _extract_price(ad)

    # First image.
    image_url = None
    images_block = ad.get("images") or {}
    urls = images_block.get("urls") or images_block.get("urls_large") or []
    if urls:
      image_url = urls[0]

    return SearchResult(
      external_id=str(list_id),
      url=url,
      title=title,
      current_price=price,
      currency="EUR",
      image_url=image_url,
      listing_type=ListingType.BUY_NOW,
      status=ListingStatus.ACTIVE,
    )

  def _parse_ad(self, ad: dict) -> ScrapedListing:
    """Parse a full ad dict into a ScrapedListing."""
    list_id = str(ad.get("list_id", ""))
    title = ad.get("subject", "")
    url = ad.get("url", f"{_BASE_URL}/ad/{list_id}")
    body = ad.get("body") or None
    price = _extract_price(ad)

    # Status.
    raw_status = (ad.get("status") or "").lower()
    if raw_status == "active":
      status = ListingStatus.ACTIVE
    elif raw_status in ("expired", "deleted"):
      status = ListingStatus.SOLD
    else:
      status = ListingStatus.UNKNOWN

    # Images.
    images = _extract_images(ad)

    # Seller / owner – first extract from ad, then enrich with profile.
    seller = _extract_seller(ad)

    # Fetch seller profile to get registration date and badges.
    owner = ad.get("owner") or {}
    user_id = owner.get("user_id")
    if user_id and seller:
      profile_info = self._fetch_seller_profile(user_id)
      if profile_info:
        seller.member_since = profile_info.get("registered_at")
        # Store additional profile info as seller attributes.
        badges = profile_info.get("badges", [])
        if badges:
          seller.display_name = f"{seller.username} ({', '.join(b.get('name', '') for b in badges)})"
        # Update feedback if available from profile.
        total_ads = profile_info.get("total_ads")
        if total_ads is not None and seller.feedback_count is None:
          seller.feedback_count = total_ads

    # Condition.
    condition = _extract_condition(ad)

    # Location.
    location = ad.get("location", {})
    city = location.get("city_label") or location.get("city", "")
    region = location.get("region_name", "")
    department = location.get("department_name", "")
    country = location.get("country_id", "FR")

    # Shipping.
    shipping_type = _get_attribute(ad, "shipping_type")
    shippable = _get_attribute(ad, "shippable") == "true"

    # Buyer fee.
    buyer_fee_data = ad.get("buyer_fee")
    buyer_fee = None
    if isinstance(buyer_fee_data, dict) and buyer_fee_data.get("amount"):
      buyer_fee = Decimal(buyer_fee_data["amount"]) / Decimal(100)

    # Dates.
    publication_date = _parse_datetime(ad.get("first_publication_date"))
    index_date = _parse_datetime(ad.get("index_date"))

    # Counters.
    counters = ad.get("counters") or {}
    watcher_count = counters.get("favorites")
    if isinstance(watcher_count, str):
      watcher_count = int(watcher_count) if watcher_count.isdigit() else None

    # Attributes.
    attributes: dict[str, str] = {}
    attributes["category"] = ad.get("category_name", "")
    attributes["category_id"] = str(ad.get("category_id", ""))
    if city:
      attributes["city"] = city
    if region:
      attributes["region"] = region
    if department:
      attributes["department"] = department
    if shipping_type:
      attributes["shipping_type"] = shipping_type

    # Seller type (private vs professional).
    owner = ad.get("owner") or {}
    seller_type = owner.get("type", "")
    if seller_type:
      attributes["seller_type"] = seller_type

    # Ad type (offer, demand, etc.).
    ad_type = ad.get("ad_type", "")
    if ad_type:
      attributes["ad_type"] = ad_type

    # Negotiation possible?
    negotiable = _get_attribute(ad, "negotiation_cta_visible")
    if negotiable == "true":
      attributes["negotiable"] = "true"

    # Collect remaining useful attributes.
    for attr_dict in ad.get("attributes", []):
      key = attr_dict.get("key", "")
      if key in (
        "ean", "isbn", "brand", "model",
        "energy_rate", "real_estate_type",
      ):
        label = attr_dict.get("value_label") or attr_dict.get("value", "")
        if label:
          attributes[key] = label

    return ScrapedListing(
      external_id=list_id,
      url=url,
      title=title,
      description=body,
      listing_type=ListingType.BUY_NOW,
      condition=condition,
      currency="EUR",
      buy_now_price=price,
      current_price=price,
      buyer_premium_fixed=buyer_fee,
      shipping_from_country=country,
      ships_internationally=shippable if shippable else None,
      start_time=publication_date,
      status=status,
      watcher_count=watcher_count,
      seller=seller,
      images=images,
      attributes=attributes,
    )

  # ------------------------------------------------------------------
  # URL helpers
  # ------------------------------------------------------------------

  @staticmethod
  def _normalise_url(url_or_id: str) -> str:
    """Accept a full URL or bare ID and return a canonical URL."""
    if url_or_id.startswith("http"):
      return url_or_id
    # Bare numeric ID — we don't know the category slug, so use a
    # generic search redirect that LeBonCoin resolves.
    if url_or_id.isdigit():
      return f"{_BASE_URL}/ad/offres/{url_or_id}"
    raise ValueError(f"Cannot normalise LeBonCoin URL/ID: {url_or_id}")

  @staticmethod
  def _extract_list_id(url_or_id: str) -> str:
    """Extract the numeric listing ID from a URL or bare ID."""
    if url_or_id.isdigit():
      return url_or_id
    match = re.search(r'/(\d{8,12})(?:\?|$|#)', url_or_id)
    if match:
      return match.group(1)
    # Fallback: last numeric segment in the path.
    match2 = re.search(r'/(\d+)$', url_or_id.split("?")[0])
    if match2:
      return match2.group(1)
    raise ValueError(f"Cannot extract LeBonCoin list ID from: {url_or_id}")


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _extract_next_data(html: str) -> Optional[dict]:
  """Extract the __NEXT_DATA__ JSON from a LeBonCoin HTML page."""
  # Try multiple patterns to find __NEXT_DATA__ script tag.
  # Pattern 1: Modern format with id attribute (most common)
  pattern1 = re.search(
    r'<script\s+id=["\']__NEXT_DATA__["\']\s+type=["\']application/json["\']\s*>(.*?)</script>',
    html,
    re.DOTALL | re.IGNORECASE,
  )
  if pattern1:
    try:
      json_str = pattern1.group(1).strip()
      return json.loads(json_str)
    except json.JSONDecodeError as error:
      logger.error("Failed to parse __NEXT_DATA__ JSON (pattern1): %s", error)

  # Pattern 2: Look for script tag with __NEXT_DATA__ id (flexible attribute order)
  pattern2 = re.search(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
    html,
    re.DOTALL | re.IGNORECASE,
  )
  if pattern2:
    try:
      json_str = pattern2.group(1).strip()
      return json.loads(json_str)
    except json.JSONDecodeError as error:
      logger.error("Failed to parse __NEXT_DATA__ JSON (pattern2): %s", error)

  # Pattern 3: Fallback to simple string search (old format with double quotes)
  marker = 'id="__NEXT_DATA__" type="application/json">'
  start = html.find(marker)
  if start >= 0:
    start += len(marker)
    end = html.find("</script>", start)
    if end >= 0:
      try:
        json_str = html[start:end].strip()
        return json.loads(json_str)
      except json.JSONDecodeError as error:
        logger.error("Failed to parse __NEXT_DATA__ JSON (pattern3): %s", error)

  # Pattern 4: Try with single quotes
  marker = "id='__NEXT_DATA__' type='application/json'>"
  start = html.find(marker)
  if start >= 0:
    start += len(marker)
    end = html.find("</script>", start)
    if end >= 0:
      try:
        json_str = html[start:end].strip()
        return json.loads(json_str)
      except json.JSONDecodeError as error:
        logger.error("Failed to parse __NEXT_DATA__ JSON (pattern4): %s", error)

  # Pattern 5: Try without id attribute (very old format)
  marker = '__NEXT_DATA__" type="application/json">'
  start = html.find(marker)
  if start >= 0:
    start += len(marker)
    end = html.find("</script>", start)
    if end >= 0:
      try:
        json_str = html[start:end].strip()
        return json.loads(json_str)
      except json.JSONDecodeError as error:
        logger.error("Failed to parse __NEXT_DATA__ JSON (pattern5): %s", error)

  # If all patterns fail, log diagnostic info
  contains_marker = '__NEXT_DATA__' in html
  logger.warning(
    "No __NEXT_DATA__ found in HTML. HTML length: %d, contains '__NEXT_DATA__': %s",
    len(html), contains_marker,
  )
  # Log a snippet of HTML around where __NEXT_DATA__ might be
  if contains_marker:
    idx = html.find('__NEXT_DATA__')
    snippet_start = max(0, idx - 200)
    snippet_end = min(len(html), idx + 500)
    logger.debug("HTML snippet around __NEXT_DATA__: %s", html[snippet_start:snippet_end])
  else:
    # Log a snippet from the beginning to see what we got
    logger.debug("HTML preview (first 1000 chars): %s", html[:1000])

  return None


def _extract_price(ad: dict) -> Optional[Decimal]:
  """Extract the price from an ad dict.

  LeBonCoin provides ``price_cents`` (precise, in cents) and ``price``
  (a list of whole-euro amounts).  We prefer ``price_cents`` when
  available.
  """
  price_cents = ad.get("price_cents")
  if price_cents is not None:
    try:
      return Decimal(price_cents) / Decimal(100)
    except Exception:
      pass

  price_list = ad.get("price")
  if isinstance(price_list, list) and price_list:
    try:
      return Decimal(str(price_list[0]))
    except Exception:
      pass

  return None


def _extract_images(ad: dict) -> list[ScrapedImage]:
  """Extract image URLs from an ad dict."""
  images_block = ad.get("images") or {}
  # Prefer large URLs, fall back to regular.
  urls = images_block.get("urls_large") or images_block.get("urls") or []
  return [
    ScrapedImage(source_url=url, position=index)
    for index, url in enumerate(urls)
  ]


def _extract_seller(ad: dict) -> Optional[ScrapedSeller]:
  """Extract seller information from an ad dict."""
  owner = ad.get("owner")
  if not owner:
    return None

  store_id = owner.get("store_id") or owner.get("user_id") or ""
  name = owner.get("name", "")
  if not store_id and not name:
    return None

  # Rating and feedback from attributes.
  rating = None
  feedback_count = None
  for attr_dict in ad.get("attributes", []):
    key = attr_dict.get("key", "")
    if key == "rating_score":
      try:
        # LeBonCoin rating is 0–1; scale to 0–5 for consistency.
        raw_rating = float(attr_dict.get("value", "0"))
        rating = round(raw_rating * 5, 2)
      except (ValueError, TypeError):
        pass
    elif key == "rating_count":
      try:
        feedback_count = int(attr_dict.get("value", "0"))
      except (ValueError, TypeError):
        pass

  # Country from attributes or location.
  country = _get_attribute(ad, "country_isocode3166")
  if not country:
    country = (ad.get("location") or {}).get("country_id")

  seller_type = owner.get("type", "")

  return ScrapedSeller(
    external_id=str(store_id),
    username=name,
    display_name=f"{name} ({seller_type})" if seller_type else name,
    country=country,
    rating=rating,
    feedback_count=feedback_count,
  )


def _extract_condition(ad: dict) -> ItemCondition:
  """Extract item condition from an ad's attributes."""
  condition_value = _get_attribute(ad, "condition")
  if condition_value:
    return _CONDITION_MAP.get(condition_value, ItemCondition.UNKNOWN)
  return ItemCondition.UNKNOWN


def _get_attribute(ad: dict, key: str) -> Optional[str]:
  """Get a single attribute value from the ad's attributes list."""
  for attr_dict in ad.get("attributes", []):
    if attr_dict.get("key") == key:
      return attr_dict.get("value")
  return None


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
  """Parse a LeBonCoin datetime string.

  Format is ``"2026-01-20 18:09:27"`` in Paris local time.  We store
  as-is (effectively CET/CEST) since LeBonCoin is France-only.
  """
  if not value:
    return None
  try:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
      tzinfo=timezone.utc,
    )
  except (ValueError, TypeError):
    return None
