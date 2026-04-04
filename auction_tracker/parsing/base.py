"""Abstract parser interface and parser registry.

Parsers are stateless, pure functions that take HTML and return
structured Pydantic models. They never perform HTTP requests.
"""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass
from typing import ClassVar

from auction_tracker.parsing.models import ScrapedListing, ScrapedSearchResult

logger = logging.getLogger(__name__)


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
  def parse_search_results(self, html: str) -> list[ScrapedSearchResult]:
    """Parse a search results page into a list of results.

    Returns an empty list if no results are found (not an error).
    Raises ValueError if the HTML structure is unrecognizable.
    """

  @abc.abstractmethod
  def parse_listing(self, html: str) -> ScrapedListing:
    """Parse a listing detail page into structured data.

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
