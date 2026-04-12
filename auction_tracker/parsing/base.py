"""Abstract parser interface and parser registry.

Parsers are stateless, pure functions that take HTML and return
structured Pydantic models. They never perform HTTP requests.
"""

from __future__ import annotations

import abc
import logging
import re
from dataclasses import dataclass
from typing import ClassVar

from auction_tracker.parsing.models import ScrapedListing, ScrapedSearchResult

logger = logging.getLogger(__name__)


class ParserBlocked(Exception):
  """Raised when the fetched HTML is a bot-detection / consent page.

  Unlike a transport-level block (HTTP 403/429), the server returned
  HTTP 200 with valid HTML but the page content is a challenge,
  sign-in redirect, or cookie-consent gate rather than actual listing
  data. Callers can inspect ``fallback_urls`` to retry the same
  resource on alternative server paths or regional domains.
  """

  def __init__(self, message: str, url: str, fallback_urls: list[str] | None = None) -> None:
    super().__init__(message)
    self.url = url
    self.fallback_urls: list[str] = fallback_urls or []


_BLOCKING_TITLES = (
  # Cloudflare IUAM and challenge pages
  "just a moment",
  "attention required",
  "please wait",
  "checking your browser",
  # Generic access denial
  "access denied",
  "403 forbidden",
  "429 too many requests",
  # Cloudflare-specific error codes
  "error 1015",
  "error 1020",
)

_BLOCKING_BODY_MARKERS = (
  # Cloudflare challenge JS variable and verification classes
  "_cf_chl_opt",
  "cf-browser-verification",
  "__cf_chl_f_tk",
  # Visible Cloudflare identifiers that only appear on error/block pages
  "ray id:",
  # Generic captcha / human-verification
  "verifying you are human",
  "please enable cookies",
)


def check_html_for_blocking(html: str, url: str = "") -> None:
  """Raise ``ParserBlocked`` if the page is a bot-detection challenge.

  Covers Cloudflare IUAM / JS challenge pages and other common
  anti-bot patterns. Safe to call on any HTML response — does nothing
  when the page looks like genuine content.

  Title-based detection is reliable at any page size. Body-marker
  detection is restricted to small pages (< 20 KB) to avoid false
  positives from legitimate content that may contain these strings
  (e.g. a security blog post).
  """
  title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.DOTALL)
  if title_match:
    title_lower = title_match.group(1).lower().strip()
    if any(marker in title_lower for marker in _BLOCKING_TITLES):
      raise ParserBlocked(
        f"Bot-detection page: {title_match.group(1).strip()!r}",
        url=url,
      )

  if len(html) < 20_000:
    lower_html = html.lower()
    if any(marker in lower_html for marker in _BLOCKING_BODY_MARKERS):
      raise ParserBlocked("Bot-detection challenge page", url=url)


def check_json_response_for_blocking(raw: str, url: str = "") -> None:
  """Raise ``ParserBlocked`` if an expected JSON response is actually HTML.

  JSON API endpoints (e.g. Catawiki, Invaluable) normally return a
  JSON object or array. When the endpoint is blocked it returns an
  HTML challenge page instead. Call this when ``json.loads()`` fails
  to distinguish a genuine parse error from a bot-detection redirect.
  """
  stripped = raw.lstrip()
  if stripped.startswith(("{", "[")):
    # Looks like JSON that failed to parse for a different reason.
    return
  # Not JSON — check whether it is an HTML blocking page.
  check_html_for_blocking(raw, url=url)


@dataclass(frozen=True)
class ParserCapabilities:
  """Declares what a parser can extract.

  The orchestrator reads these capabilities to decide what operations
  to perform (e.g. skip bid fetching for sites that don't expose bids).
  """
  can_search: bool = True
  can_parse_listing: bool = True
  has_bid_history: bool = False
  has_seller_info: bool = False
  has_watcher_count: bool = False
  has_view_count: bool = False
  has_buy_now: bool = False
  has_estimates: bool = False
  has_reserve_price: bool = False
  has_lot_numbers: bool = False
  has_auction_house_info: bool = False
  can_search_history: bool = False


class Parser(abc.ABC):
  """Abstract base class for website parsers.

  Subclasses implement parse_search_results and parse_listing to
  extract structured data from raw HTML strings.
  """

  @property
  @abc.abstractmethod
  def website_name(self) -> str:
    """Machine-friendly website identifier (e.g. 'ebay', 'catawiki')."""

  @property
  @abc.abstractmethod
  def capabilities(self) -> ParserCapabilities:
    """What this parser can extract."""

  @abc.abstractmethod
  def parse_search_results(self, html: str, url: str = "") -> list[ScrapedSearchResult]:
    """Parse a search results page into a list of results.

    The optional ``url`` may be used by parsers that embed pagination
    state in the URL. Returns an empty list if no results are found
    (not an error). Raises ValueError if the HTML structure is
    unrecognizable.
    """

  @abc.abstractmethod
  def parse_listing(self, html: str, url: str = "") -> ScrapedListing:
    """Parse a listing detail page into structured data.

    The optional ``url`` is used to populate ``ParserBlocked.fallback_urls``
    when the page is a challenge or consent gate rather than real content.

    Raises ParserBlocked if the HTML is a bot-detection page.
    Raises ValueError if the HTML does not contain a recognizable
    listing (e.g. the page was removed or the structure changed).
    """

  def build_search_url(self, query: str, **kwargs) -> str:
    """Build a search URL for the given query text.

    Override in subclasses to handle website-specific URL patterns,
    pagination, filters, etc.
    """
    raise NotImplementedError(
      f"{self.website_name} parser does not implement build_search_url"
    )

  def build_fetch_url(self, url: str) -> str:
    """Transform a listing's stored URL into the URL to actually fetch.

    Most sites use the same URL for display and fetch.  Override for
    sites where the internal API URL differs from the public URL
    (e.g. Vinted, where the public ``/items/123-slug`` must be
    rewritten to ``/api/v2/items/123/details``).
    """
    return url

  def extract_external_id(self, url: str) -> str | None:
    """Extract the website's listing ID from a listing URL.

    Returns None if the URL format is not recognized.
    """
    return None


class ParserRegistry:
  """Registry of available parsers, keyed by website name.

  Parsers register themselves via the ``register`` class decorator::

      @ParserRegistry.register
      class EbayParser(Parser):
          ...
  """
  _parsers: ClassVar[dict[str, type[Parser]]] = {}

  @classmethod
  def register(cls, parser_class: type[Parser]) -> type[Parser]:
    """Class decorator that registers a parser.

    The parser class must have a ``website_name`` property. To make
    this work at class level (before instantiation), we instantiate
    a temporary instance to read the name.
    """
    instance = parser_class()
    name = instance.website_name.lower()
    if name in cls._parsers:
      logger.warning(
        "Parser for '%s' already registered, overwriting with %s",
        name, parser_class.__name__,
      )
    cls._parsers[name] = parser_class
    logger.debug("Registered parser: %s -> %s", name, parser_class.__name__)
    return parser_class

  @classmethod
  def get(cls, website_name: str) -> Parser:
    """Get an instance of the parser for the given website."""
    name = website_name.lower()
    if name not in cls._parsers:
      available = ", ".join(sorted(cls._parsers.keys()))
      raise KeyError(
        f"No parser registered for '{name}'. Available: {available}"
      )
    return cls._parsers[name]()

  @classmethod
  def list_registered(cls) -> list[str]:
    """Return sorted list of registered website names."""
    return sorted(cls._parsers.keys())

  @classmethod
  def has(cls, website_name: str) -> bool:
    return website_name.lower() in cls._parsers
