"""Scraper registry – maps website names to scraper classes.

Website-specific scraper modules register themselves by calling
:func:`ScraperRegistry.register` (typically at import time via the
:func:`ScraperRegistry.auto_register` decorator).
"""

from __future__ import annotations

import logging
from typing import Optional, Type

from auction_tracker.config import ScrapingConfig
from auction_tracker.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class ScraperRegistry:
  """A simple in-process registry of scraper classes keyed by website name."""

  _scrapers: dict[str, Type[BaseScraper]] = {}

  @classmethod
  def register(cls, name: str, scraper_class: Type[BaseScraper]) -> None:
    """Register a scraper class under *name* (case-insensitive)."""
    key = name.lower()
    if key in cls._scrapers:
      logger.warning(
        "Overwriting existing scraper registration for '%s'.", name,
      )
    cls._scrapers[key] = scraper_class
    logger.debug("Registered scraper: %s -> %s", name, scraper_class.__name__)

  @classmethod
  def auto_register(cls, name: str):
    """Class decorator that registers a scraper at import time.

    Usage::

        @ScraperRegistry.auto_register("catawiki")
        class CatawikiScraper(BaseScraper):
            ...
    """
    def decorator(scraper_class: Type[BaseScraper]) -> Type[BaseScraper]:
      cls.register(name, scraper_class)
      return scraper_class
    return decorator

  @classmethod
  def get(cls, name: str) -> Optional[Type[BaseScraper]]:
    """Look up a scraper class by website name (case-insensitive)."""
    return cls._scrapers.get(name.lower())

  @classmethod
  def create(cls, name: str, config: ScrapingConfig) -> BaseScraper:
    """Instantiate a scraper by website name.

    Raises ``KeyError`` if no scraper is registered under *name*.
    
    If the config has per-scraper delays configured, a modified config
    with the appropriate delay for this scraper will be used.
    """
    scraper_class = cls.get(name)
    if scraper_class is None:
      available = ", ".join(sorted(cls._scrapers.keys())) or "(none)"
      raise KeyError(
        f"No scraper registered for '{name}'. Available: {available}"
      )
    
    # Create a config with the scraper-specific delay if configured.
    scraper_delay = config.get_delay_for_scraper(name)
    if scraper_delay != config.request_delay:
      # Create a new config instance with the scraper-specific delay.
      from dataclasses import replace
      config = replace(config, request_delay=scraper_delay)
    
    return scraper_class(config)

  @classmethod
  def list_registered(cls) -> list[str]:
    """Return sorted names of all registered scrapers."""
    return sorted(cls._scrapers.keys())
