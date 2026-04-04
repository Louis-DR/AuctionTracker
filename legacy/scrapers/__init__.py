"""Scrapers package - base classes and registry for website-specific scrapers."""

from auction_tracker.scrapers.base import BaseScraper, ScraperCapabilities
from auction_tracker.scrapers.registry import ScraperRegistry

# Import website-specific scrapers so they auto-register.
import auction_tracker.scrapers.catawiki  # noqa: F401
import auction_tracker.scrapers.drouot  # noqa: F401
import auction_tracker.scrapers.gazette_drouot  # noqa: F401
import auction_tracker.scrapers.invaluable  # noqa: F401
import auction_tracker.scrapers.interencheres  # noqa: F401
import auction_tracker.scrapers.liveauctioneers  # noqa: F401
import auction_tracker.scrapers.ebay  # noqa: F401
import auction_tracker.scrapers.yahoo_japan  # noqa: F401
import auction_tracker.scrapers.leboncoin  # noqa: F401

__all__ = [
  "BaseScraper",
  "ScraperCapabilities",
  "ScraperRegistry",
]
