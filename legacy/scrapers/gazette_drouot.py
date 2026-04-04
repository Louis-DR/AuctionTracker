"""Gazette Drouot scraper.

Gazette Drouot (gazette-drouot.com) is a subscription-based website
providing historical auction results from French auction houses.  Unlike
the main drouot.com site, Gazette pages:

* Require a **paid subscription** to view lot details and results.
* Are protected by an **anti-bot system** that blocks simple HTTP
  requests — Playwright with a persistent browser profile is required.
* Only contain **past** auction data (results, not live bidding).

This scraper is designed for **manual execution** via ``run_gazette.py``
to backfill the database with historical results.  It reuses the same
saved search queries as the other scrapers.

Key implementation notes:

* Always uses the browser (Playwright) for every request.
* Shares the ``browser_profile`` from ``ScrapingConfig`` with the
  regular Drouot scraper.
* ``search()`` delegates to ``search_past()`` since all Gazette results
  are historical.
"""

from __future__ import annotations

import logging
import random
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence
from urllib.parse import quote_plus, urljoin

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
    ScraperCapabilities,
    SearchResult,
)
from auction_tracker.scrapers.registry import ScraperRegistry

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------

_GAZETTE_BASE = "https://www.gazette-drouot.com"
_SEARCH_PATH = "/recherche/lot/"
_DEFAULT_CURRENCY = "EUR"
_MAX_SEARCH_PAGES = 20  # Safety limit to avoid infinite pagination loops
_CF_CHALLENGE_MAX_WAIT = 30  # seconds to wait for Cloudflare challenge
_CF_POLL_INTERVAL = 2  # seconds between challenge checks

# Chrome launch arguments to reduce automation fingerprints.
_CHROME_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-infobars",
    "--disable-dev-shm-usage",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-extensions",
    "--disable-sync",
    "--metrics-recording-only",
    "--no-first-run",
]

# French month names for date parsing.
_FRENCH_MONTHS: dict[str, int] = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}


# ------------------------------------------------------------------
# Scraper
# ------------------------------------------------------------------

@ScraperRegistry.auto_register("gazette_drouot")
class GazetteDrouotScraper(BaseScraper):
    """Scraper for gazette-drouot.com (historical auction results)."""

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def website_name(self) -> str:
        return "Gazette Drouot"

    @property
    def _browser_locale(self) -> str:
        return "fr-FR"

    @property
    def website_base_url(self) -> str:
        return _GAZETTE_BASE

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
            has_reserve_price=False,
            has_lot_numbers=False,
            has_auction_house_info=True,
            monitoring_strategy="post_auction",
            exclude_from_discover=True,
            is_historical_only=True,
        )

    # ------------------------------------------------------------------
    # Pre-filtering
    # ------------------------------------------------------------------

    @staticmethod
    def is_relevant_title(title: str) -> bool:
        """Return True if the title likely contains a writing instrument.

        Used to pre-filter search results before fetching details, avoiding
        unnecessary page loads (and thus Cloudflare challenges) for
        irrelevant items (e.g. lighters, cufflinks).
        """
        t = title.lower()

        # 1. Block explicit non-writing instruments
        blocklist = (
            "briquet", "lighter", "cufflink", "bouton de manchette",
            "montre", "watch", "sac", "bag", "foulard", "scarf",
            "tie", "cravate", "brooch", "broche", "ring", "bague",
            "earring", "boucle d'oreille",
        )
        if any(w in t for w in blocklist):
            return False

        # 2. Allow explicit writing instruments
        allowlist = (
            "stylo", "plume", "bille", "pencil", "pen", "fountain",
            "ballpoint", "roller", "mine", "criterium", "parure",
            "encrier", "inkwell", "nib", "capsule", "capless",
            "namiki", "montblanc", "pelikan", "parker", "waterman",
            "sheaffer", "pilot", "sailor", "aurora", "visconti",
            "omas", "montegrappa", "dupont", "lamy", "kaweco",
        )
        if any(w in t for w in allowlist):
            return True

        # 3. Default to False for safety (reduce spam), but log it?
        # For now, if it's not a known pen keyword, skip it.
        return False

    # ------------------------------------------------------------------
    # Browser lifecycle — delegates to BaseScraper shared Camoufox
    # ------------------------------------------------------------------

    @property
    def _browser_enabled(self) -> bool:
        """Gazette always uses the browser."""
        return True

    def _browser_fetch(self, url: str) -> str:
        """Override to add Cloudflare Turnstile challenge handling.

        Runs on the dedicated browser thread.
        """
        self._ensure_browser()

        page = self._browser_page
        if not page:
            page = self._browser_context.new_page()
            self._browser_page = page

        logger.debug("Navigating to %s …", url)

        # Random pre-navigation delay.
        time.sleep(self.config.browser_post_goto_delay * 0.5)

        try:
            page.goto(url, timeout=90_000, wait_until="domcontentloaded")
        except Exception as exc:
            logger.warning("Navigation warning (proceeding): %s", exc)

        # Gazette-specific: detect and wait for Cloudflare challenge.
        self._wait_for_cloudflare(page)

        # Human-like behaviour (richer than base class).
        self._human_behavior(page)

        time.sleep(self.config.browser_post_goto_delay)
        self._last_request_time = time.time()
        return page.content()

    # ------------------------------------------------------------------
    # Cloudflare Turnstile bypass (Gazette-specific)
    # ------------------------------------------------------------------

    def _wait_for_cloudflare(self, page) -> None:
        """Detect Cloudflare Turnstile challenge and attempt to solve it.

        Strategies:
        1. Auto-click Turnstile checkbox (using Camoufox's disable_coop).
        2. Exponential backoff for retries.
        3. Page reload if stuck for too long.
        """
        if not self._is_cloudflare_challenge(page):
            return

        logger.info("Cloudflare challenge detected — attempting bypass …")

        start_time = time.monotonic()
        max_duration = 60  # Increased timeout
        reload_threshold = 25 # Reload if stuck for 25s
        next_reload = start_time + reload_threshold

        # Exponential backoff for checks
        attempt = 0

        while time.monotonic() - start_time < max_duration:
            # 1. Check if resolved
            if not self._is_cloudflare_challenge(page):
                logger.info("Cloudflare challenge resolved.")
                return

            # 2. Reload if stuck
            if time.monotonic() > next_reload:
                logger.warning("Stuck on Cloudflare — reloading page …")
                try:
                    page.reload(timeout=30_000, wait_until="domcontentloaded")
                except Exception:
                    pass
                next_reload = time.monotonic() + reload_threshold
                time.sleep(3)
                continue

            # 3. Try to click
            if self._try_click_turnstile(page):
                # If clicked, wait a bit longer for reaction
                time.sleep(random.uniform(2.0, 4.0))
            else:
                # If cookie logic failed, wait before retry
                wait_time = min(1.5 * (1.2 ** attempt), 5.0)
                time.sleep(wait_time)
                attempt += 1

        logger.warning(
            "Cloudflare challenge did not resolve within %ds.",
            max_duration,
        )



    @staticmethod
    def _is_cloudflare_challenge(page) -> bool:
        """Return True if the page is showing a Cloudflare challenge."""
        try:
            title = page.title() or ""
            content = page.content()
            return (
                "Just a moment" in title
                or "Vérification" in title
                or "cf-challenge" in content
                or "cf-turnstile" in content
                or "challenge-platform" in content
                or "Checking your browser" in content
            )
        except Exception:
            return False

    def _try_click_turnstile(self, page) -> bool:
        """Locate the Turnstile iframe and click its checkbox.

        Returns True if a click was performed.
        """
        try:
            # Wait a moment for the Turnstile iframe to render
            time.sleep(1.5)

            # Turnstile iframe has src containing challenges.cloudflare.com
            for frame in page.frames:
                if "challenges.cloudflare.com" not in (frame.url or ""):
                    continue

                logger.debug("Found Turnstile iframe: %s", frame.url)

                # The checkbox is an <input type="checkbox"> or a clickable
                # element inside the iframe.  Try multiple selectors.
                selectors = [
                    "input[type='checkbox']",
                    "#cf-turnstile-response",
                    ".cf-turnstile",
                    "label",
                    "body",  # Last resort: click anywhere in the iframe
                ]

                for selector in selectors:
                    el = frame.query_selector(selector)
                    if el and el.is_visible():
                        # Small random delay to look human
                        time.sleep(random.uniform(0.3, 0.8))

                        # Click with a slight offset for realism
                        box = el.bounding_box()
                        if box:
                            x = box["x"] + box["width"] / 2 + random.randint(-3, 3)
                            y = box["y"] + box["height"] / 2 + random.randint(-3, 3)
                            page.mouse.click(x, y)
                            logger.info(
                                "Clicked Turnstile element '%s' at (%.0f, %.0f).",
                                selector, x, y,
                            )
                        else:
                            el.click()
                            logger.info("Clicked Turnstile element '%s'.", selector)
                        return True

                logger.debug("No clickable element found in Turnstile iframe.")
                return False

            # No Turnstile iframe found — might be a JS challenge instead
            logger.debug("No Turnstile iframe found in page frames.")
            return False

        except Exception as exc:
            logger.debug("Turnstile click attempt failed: %s", exc)
            return False

    def _browser_human_behavior(self, page) -> None:
        """Override base with richer human behavior for Gazette."""
        self._human_behavior(page)

    def _human_behavior(self, page) -> None:
        """Simulate human-like mouse movements and scrolling."""
        try:
            for _ in range(random.randint(2, 5)):
                x = random.randint(100, 1000)
                y = random.randint(100, 600)
                page.mouse.move(x, y, steps=random.randint(5, 20))
                time.sleep(random.uniform(0.1, 0.5))

            page.evaluate(f"window.scrollBy(0, {random.randint(200, 500)})")
            time.sleep(random.uniform(0.5, 1.5))

            page.evaluate(f"window.scrollBy(0, -{random.randint(50, 200)})")
            time.sleep(random.uniform(0.2, 0.8))
        except Exception as exc:
            logger.debug("Human behavior simulation failed: %s", exc)

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
        """Run a search — delegates to ``search_past`` since Gazette
        only has historical results."""
        return self.search_past(query, limit=50)

    def search_past(
        self,
        query: str,
        *,
        limit: int = 50,
    ) -> Sequence[SearchResult]:
        """Search for past auction results on Gazette Drouot.

        Iterates through search result pages until *limit* is reached
        or no more results are found.
        """
        results: list[SearchResult] = []
        page_num = 1

        while len(results) < limit:
            encoded_query = quote_plus(query)
            url = (
                f"{_GAZETTE_BASE}{_SEARCH_PATH}{encoded_query}"
                f"?type=result&exactMatch=false&page={page_num}&lang=fr"
            )

            logger.info("Searching Gazette past results (page %d): %s",
                         page_num, url)

            try:
                html = self._get_html_browser(url)
                page_results = _parse_gazette_search_results(html)

                if not page_results:
                    logger.info("No more results found on page %d.", page_num)
                    break

                results.extend(page_results)
                logger.info("Found %d results on page %d (total: %d).",
                             len(page_results), page_num, len(results))

                page_num += 1
                if page_num > _MAX_SEARCH_PAGES:
                    logger.warning("Hit max search page limit (%d).",
                                    _MAX_SEARCH_PAGES)
                    break

            except Exception as exc:
                logger.error("Error searching Gazette page %d: %s",
                              page_num, exc)
                break

        return results[:limit]

    # ------------------------------------------------------------------
    # Fetch listing
    # ------------------------------------------------------------------

    def fetch_listing(self, url_or_external_id: str) -> ScrapedListing:
        """Fetch a single Gazette lot page and parse it.

        Always uses the browser because Gazette has anti-bot protection.
        """
        url = _normalise_gazette_url(url_or_external_id)
        html = self._get_html_browser(url)
        listing = _parse_gazette_listing(html, url)
        if listing is None:
            raise ValueError(f"Could not parse Gazette listing from {url}")
        return listing


# ------------------------------------------------------------------
# URL helpers
# ------------------------------------------------------------------

def _normalise_gazette_url(url_or_id: str) -> str:
    """Accept a full Gazette URL or a numeric lot ID and return a URL."""
    if url_or_id.startswith("http"):
        return url_or_id
    # Assume it's a numeric lot ID.
    return f"{_GAZETTE_BASE}/lots/{url_or_id}"


# ------------------------------------------------------------------
# Parsing helpers  (module-level for testability)
# ------------------------------------------------------------------

def _parse_french_date(date_str: str) -> Optional[datetime]:
    """Parse a French date string like ``'26 mars 2025'``.

    Handles optional weekday prefixes, time suffixes, and case variations.
    """
    if not date_str:
        return None

    try:
        # Normalize: lowercase, remove extra spaces
        clean_str = " ".join(date_str.lower().split())

        # Regex to find "dd month yyyy" pattern
        # Matches: "26 mars 2025", "1er avril 2024", "vendredi 14 fevrier 2025"
        # Note: "1er" handling
        pattern = r"(\d{1,2}|1er)\s+([a-zéâäà]+)\s+(\d{4})"
        match = re.search(pattern, clean_str)

        if not match:
            # Fallback for "dd/mm/yyyy"
            match_slash = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", clean_str)
            if match_slash:
                day, month, year = map(int, match_slash.groups())
                return datetime(year, month, day, tzinfo=timezone.utc)

            logger.debug("Date parse regex failed for: '%s'", date_str)
            return None

        day_str, month_str, year_str = match.groups()

        # Handle "1er"
        if day_str == "1er":
            day = 1
        else:
            day = int(day_str)

        year = int(year_str)

        # Fuzzy month matching
        month = None
        # Handle "février" / "fevrier", "août" / "aout", "décembre" / "decembre"
        month_map = {
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

        # Try direct match or prefix match
        if month_str in month_map:
            month = month_map[month_str]
        else:
            for name, m_num in month_map.items():
                if month_str.startswith(name):
                    month = m_num
                    break

        if day and month and year:
            return datetime(year, month, day, tzinfo=timezone.utc)

        logger.warning("Could not map month '%s' in date: '%s'", month_str, date_str)

    except Exception as exc:
        logger.warning("Error parsing date '%s': %s", date_str, exc)


def _parse_gazette_price(price_text: str) -> tuple[Optional[Decimal], str]:
    """Parse a Gazette price string like "4 000 CAD" or "150 EUR".

    Returns:
        (price, currency) or (None, default_currency) if parsing fails.
    """
    if not price_text:
        return None, _DEFAULT_CURRENCY

    # Text normalization
    text = price_text.replace("\xa0", " ").strip()

    # Regex to capture numeric part and optional currency
    # Matches: "4 000", "26", "1 200,50", "1.200"
    # followed optionally by currency code
    match = re.search(r"^([\d\s\.,]+)\s*([A-Za-z€$£]+)?", text)
    if not match:
        return None, _DEFAULT_CURRENCY

    num_str, currency_code = match.groups()
    currency = _DEFAULT_CURRENCY
    if currency_code:
        # Map symbol to code if needed, or just use as is if 3-letter
        c = currency_code.upper()
        if c == "€": currency = "EUR"
        elif c == "$": currency = "USD"
        elif c == "£": currency = "GBP"
        else: currency = c

    # Clean up the number string
    # French locale usually: space for thousands, comma for decimals
    clean_num = num_str.replace(" ", "")
    if "," in clean_num:
        clean_num = clean_num.replace(".", "") # assume dot is thousand sep if comma exists
        clean_num = clean_num.replace(",", ".")

    try:
        return Decimal(clean_num), currency
    except (InvalidOperation, ValueError):
        return None, _DEFAULT_CURRENCY



def _parse_gazette_search_results(html: str) -> list[SearchResult]:
    """Parse search-result items from a Gazette Drouot search page."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    items: list[SearchResult] = []
    lot_nodes = soup.select(".lotsListe .Lot")

    for node in lot_nodes:
        try:
            link_node = node.select_one(".imgLot a")
            if not link_node:
                continue
            href = link_node.get("href")
            if not href:
                continue

            full_url = urljoin(_GAZETTE_BASE, href)

            # ID from URL: /lots/21211687-waterman-carene-stylo-plume---
            match = re.search(r"/lots/(\d+)", href)
            item_id = match.group(1) if match else None
            if not item_id:
                continue

            # Title
            title_parts: list[str] = []
            artist_node = node.select_one(".lotArtisteListe")
            if artist_node:
                title_parts.append(artist_node.get_text(strip=True))
            desc_node = node.select_one(".lotDescriptionListe")
            if desc_node:
                title_parts.append(desc_node.get_text(strip=True))
            title = " ".join(title_parts) or "(untitled)"

            # Image
            image_url: Optional[str] = None
            img_div = node.select_one(".imgLot")
            if img_div:
                style = img_div.get("style", "")
                if "background-image" in str(style):
                    m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
                    if m:
                        image_url = m.group(1)

            # Result price
            current_price: Optional[Decimal] = None
            currency = _DEFAULT_CURRENCY
            status = ListingStatus.UNKNOWN

            price_node = node.select_one(
                ".lotEstimationListe .fontRadikalBold"
            )
            if price_node:
                price_text = price_node.get_text(strip=True)
                p_val, p_curr = _parse_gazette_price(price_text)
                if p_val is not None:
                    current_price = p_val
                    currency = p_curr
                    status = ListingStatus.SOLD

            # Date
            end_time: Optional[datetime] = None
            date_node = node.select_one(".dateVenteLot")
            if date_node:
                end_time = _parse_french_date(date_node.get_text(strip=True))

            items.append(SearchResult(
                external_id=item_id,
                url=full_url,
                title=title,
                current_price=current_price,
                currency=currency,
                image_url=image_url,
                end_time=end_time,
                listing_type=ListingType.AUCTION,
                status=status,
            ))

        except Exception as exc:
            logger.warning("Failed to parse Gazette search item: %s", exc)
            continue

    return items


def _parse_gazette_listing(
    html: str, url: str,
) -> Optional[ScrapedListing]:
    """Parse a single Gazette Drouot lot page into a ``ScrapedListing``."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # External ID from URL  (/lots/28599730-...)
    match = re.search(r"/lots/(\d+)", url)
    external_id = match.group(1) if match else "unknown"

    # ------------------------------------------------------------------
    # Title — .lotArtisteFiche contains the short title + lot number
    # ------------------------------------------------------------------
    title = "(untitled)"
    title_node = soup.select_one(".lotArtisteFiche")
    if title_node:
        # The element often has "WATERMAN. Stylo-plume...Lot 233"
        # Remove the trailing "Lot NNN" part
        raw = title_node.get_text(" ", strip=True)
        raw = re.sub(r"\s*Lot\s+\d+\s*$", "", raw).strip()
        if raw:
            title = raw

    # ------------------------------------------------------------------
    # Description — .lotDescriptionFiche has the full text
    # ------------------------------------------------------------------
    description = ""
    desc_container = soup.select_one(".lotDescriptionFiche")
    if desc_container:
        # Only take direct text content, not nested sale info
        # The description text is in the first text nodes before
        # nested divs like .venteNomFiche, .venteDateFiche, etc.
        parts: list[str] = []
        for child in desc_container.children:
            if hasattr(child, "name") and child.name:
                # Stop at block elements (nested sale info)
                if child.get("class") and any(
                    c.startswith("vente") for c in child.get("class", [])
                ):
                    break
                parts.append(child.get_text(" ", strip=True))
            else:
                text = str(child).strip()
                if text:
                    parts.append(text)
        description = " ".join(p for p in parts if p).strip()

    if not description and title != "(untitled)":
        description = title

    # ------------------------------------------------------------------
    # Images — extracted from OpenSeadragon tileSource JS config
    # URLs look like: https://cdn.drouot.com/d/image/lot?size=fullHD&path=...
    # ------------------------------------------------------------------
    images: list[ScrapedImage] = []
    img_urls = re.findall(
        r"""url:\s*['"]([^'"]+cdn\.drouot\.com[^'"]+)['"]""",
        html,
    )
    for position, img_url in enumerate(img_urls):
        # Unescape HTML entities (&amp; -> &)
        img_url = img_url.replace("&amp;", "&")
        images.append(ScrapedImage(source_url=img_url, position=position))

    # ------------------------------------------------------------------
    # Result price — .lotResulatListe .fontRadikalBold  ("150 EUR")
    # ------------------------------------------------------------------
    price: Optional[Decimal] = None
    currency = _DEFAULT_CURRENCY
    is_unsold = False
    status = ListingStatus.UNKNOWN

    result_node = soup.select_one(".lotResulatListe .fontRadikalBold")

    if result_node:
        price_text = result_node.get_text(strip=True)
        p_val, p_curr = _parse_gazette_price(price_text)
        if p_val is not None:
            price = p_val
            currency = p_curr

    # Check for unsold markers
    lower_html = html.lower()
    if not price and ("invendu" in lower_html or "lot non vendu" in lower_html):
        is_unsold = True

    # Determine status based on price and unsold markers
    if is_unsold:
        status = ListingStatus.UNSOLD
    elif price:
        status = ListingStatus.SOLD
    else:
        status = ListingStatus.UNKNOWN

    # Force terminal status for historical items if date is past
    if status == ListingStatus.UNKNOWN:
        # We need the end_time to make this decision.
        # _parse_gazette_listing doesn't parse it directly (it comes from search results usually),
        # but let's try to extract it from the page if possible.
        # Actually, the caller often doesn't pass end_time into here.
        # However, _parse_gazette_listing parses the HTML.
        # Let's try to find the date in the page content.

        # .venteDateFiche usually contains the sale date
        date_node = soup.select_one(".venteDateFiche")
        parsed_end_time = None
        if date_node:
             parsed_end_time = _parse_french_date(date_node.get_text(strip=True))

        if parsed_end_time:
             # Use the parsed date.
             scraped_end_time = parsed_end_time
        else:
             scraped_end_time = None

        # If we found a date and it's > 24h ago, mark as UNSOLD if still Unknown.
        if scraped_end_time:
            now = datetime.now(timezone.utc)
            if (now - scraped_end_time).total_seconds() > 86400:
                status = ListingStatus.UNSOLD
                logger.debug("Forcing UNSOLD status for past Gazette listing without price.")


    # ------------------------------------------------------------------
    # Estimates — .lotEstimationFiche .fontRadikalBold  ("160 - 200 EUR")
    # ------------------------------------------------------------------
    estimate_low: Optional[Decimal] = None
    estimate_high: Optional[Decimal] = None
    estimate_node = soup.select_one(".lotEstimationFiche .fontRadikalBold")
    if estimate_node:
        estimate_text = estimate_node.get_text(strip=True)
        est_match = re.search(
            r"([\d\s\xa0.,]+)\s*[-/–]\s*([\d\s\xa0.,]+)",
            estimate_text,
        )
        if est_match:
            try:
                low_str = (
                    est_match.group(1)
                    .replace(" ", "").replace("\xa0", "")
                    .replace(",", ".").strip()
                )
                high_str = (
                    est_match.group(2)
                    .replace(" ", "").replace("\xa0", "")
                    .replace(",", ".").strip()
                )
                estimate_low = Decimal(low_str)
                estimate_high = Decimal(high_str)
            except (InvalidOperation, ValueError):
                pass

    # ------------------------------------------------------------------
    # Sale date — .venteDateFiche  ("mercredi 26 mars 2025 - 14:00")
    # ------------------------------------------------------------------
    end_date: Optional[datetime] = None
    date_node = soup.select_one(".venteDateFiche")
    if date_node:
        date_text = date_node.get_text(" ", strip=True)
        end_date = _parse_french_date(date_text)

    # ------------------------------------------------------------------
    # Auction house — third .infoVenteContent (contains an <a> tag)
    # ------------------------------------------------------------------
    auction_house_name: Optional[str] = None
    for info_el in soup.select(".infoVenteContent"):
        link = info_el.select_one("a")
        if link:
            auction_house_name = link.get_text(strip=True)
            break

    # ------------------------------------------------------------------
    # Location — .venteLieuFiche
    # ------------------------------------------------------------------
    location: Optional[str] = None
    lieu_node = soup.select_one(".venteLieuFiche")
    if lieu_node:
        # Replace <br> with ", " for readability
        for br in lieu_node.find_all("br"):
            br.replace_with(", ")
        location = lieu_node.get_text(strip=True)
        # Clean up multiple commas/spaces
        location = re.sub(r",\s*,", ",", location).strip(", ")

    # ------------------------------------------------------------------
    # Lot number — .lotNumFiche  ("Lot n° 233")
    # ------------------------------------------------------------------
    lot_number: Optional[str] = None
    lot_node = soup.select_one(".lotNumFiche")
    if lot_node:
        lot_text = lot_node.get_text(strip=True)
        num_match = re.search(r"\d+", lot_text)
        if num_match:
            lot_number = num_match.group(0)

    # ------------------------------------------------------------------
    # Sale type — .typeVente  ("Online", "Live", etc.)
    # ------------------------------------------------------------------
    sale_type: Optional[str] = None
    type_node = soup.select_one(".typeVente")
    if type_node:
        sale_type = type_node.get_text(strip=True)

    # Build attributes dict with extra metadata
    attributes: dict[str, str] = {"source": "gazette_drouot"}
    if location:
        attributes["location"] = location
    if lot_number:
        attributes["lot_number"] = lot_number
    if sale_type:
        attributes["sale_type"] = sale_type

    # Sale name (e.g. "Tabac, Écriture & Coutellerie")
    sale_node = soup.select_one(".venteNomFiche a")
    if sale_node:
        attributes["sale_name"] = sale_node.get_text(strip=True)

    return ScrapedListing(
        external_id=external_id,
        url=url,
        title=title,
        description=description,
        listing_type=ListingType.AUCTION,
        condition=ItemCondition.UNKNOWN,
        currency=currency,
        current_price=price,
        final_price=price if status == ListingStatus.SOLD else None,
        estimate_low=estimate_low,
        estimate_high=estimate_high,
        end_time=end_date,
        status=status,
        images=images,
        bids=[],
        auction_house_name=auction_house_name,
        attributes=attributes,
    )
