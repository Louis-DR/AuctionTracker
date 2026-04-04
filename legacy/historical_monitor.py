"""Historical data monitoring loop.

This module provides a specialized monitor for backfilling past auction results
from sources like Gazette Drouot. It runs on a slower schedule (e.g. weekly)
and focuses on populating the database with historical records.
"""

import logging
import time
from typing import Sequence

from auction_tracker.config import AppConfig
from auction_tracker.database.models import ListingStatus
from auction_tracker.monitor import Monitor
from auction_tracker.scrapers.registry import ScraperRegistry

logger = logging.getLogger(__name__)


class HistoricalMonitor(Monitor):
  """Orchestrates historical data scraping."""

  def __init__(self, config: AppConfig) -> None:
    super().__init__(config)
    self.historical_config = config.historical

  def run_cycle(self) -> int:
    """Run one full cycle of historical queries across all capable scrapers.

    Returns the number of new listings ingested.
    """
    if not self.historical_config.enabled:
      logger.info("Historical scraping is disabled in config.")
      return 0

    queries = self.historical_config.queries
    if not queries:
      logger.info("No historical queries configured.")
      return 0

    # Identify scrapers that support historical search
    scrapers = []
    for name in ScraperRegistry.list_registered():
      scraper = self.get_scraper(name)
      if scraper.capabilities.can_search_history:
        scrapers.append(scraper)

    if not scrapers:
      logger.info("No scrapers support historical search.")
      return 0

    logger.info(
      f"Starting historical cycle: {len(queries)} queries on {len(scrapers)} scrapers."
    )

    total_new = 0

    for scraper in scrapers:
      # Start persistent browser session if supported
      if hasattr(scraper, "start_browser"):
          try:
              scraper.start_browser()
          except Exception as e:
              logger.error(f"Failed to start browser for {scraper.website_name}: {e}")
              continue

      try:
          for query in queries:
            try:
              logger.info(f"Running historical search on {scraper.website_name}: '{query}'")

              # We use a default limit of 50 for now, could be configurable
              results = scraper.search_past(query, limit=50)

              logger.info(f"  Found {len(results)} historical results.")

              for result in results:
                 self.throttle.wait_if_needed(scraper.website_name)
                 try:
                     # Check/store basic result first?
                     # Monitor.ingest_listing fetches full details then stores.
                     # Let's try to fetch if we can.

                     # Using ingest_listing from parent
                     # Note: fetch_past_listing is needed.
                     # BaseScraper.fetch_listing usually fetches active.
                     # Drouot.fetch_listing fetches by ID/URL.
                     # Drouot.fetch_past_listing is separate.
                     # BaseScraper.fetch_listing corresponds to current listings.
                     # We added fetch_past_listing to Drouot but not to BaseScraper interface (yet).
                     # Wait, looking at base.py, I didn't add fetch_past_listing to BaseScraper.
                     # I added search_past.

                     # Scraper interface issue:
                     # Monitor.ingest_listing calls scraper.fetch_listing.
                     # But for Drouot, fetching a PAST listing might require different logic
                     # (flushing caches, different endpoint, or parser logic)
                     # which is why I created fetch_past_listing in Drouot.

                     # If I use scraper.fetch_listing(url) on a past URL, will it work?
                     # Drouot.fetch_listing expects drouot.com URLs.
                     # Search results from Gazette have gazette-drouot.com URLs.

                     # So I should use fetch_past_listing if available.

                     if hasattr(scraper, "fetch_past_listing"):
                          scraped = scraper.fetch_past_listing(result.url)
                     else:
                          # Fallback or assume fetch_listing handles it
                          scraped = scraper.fetch_listing(result.url)

                     if scraped:
                          if scraped.status == ListingStatus.UNKNOWN and result.status != ListingStatus.UNKNOWN:
                              # Trust search result status if detail page is ambiguous
                              scraped.status = result.status

                          # Store it
                          self._store_scraped_listing(scraper, scraped)
                          total_new += 1

                 except Exception as e:
                     logger.error(f"Failed to ingest historical result {result.url}: {e}")

                 self.throttle.mark_used(scraper.website_name)

            except Exception as e:
               logger.error(f"Error querying {scraper.website_name} for '{query}': {e}")

      finally:
          # meaningful finally block to ensure browser is closed
          if hasattr(scraper, "stop_browser"):
              try:
                  scraper.stop_browser()
              except Exception as e:
                  logger.error(f"Failed to stop browser for {scraper.website_name}: {e}")

    logger.info(f"Historical cycle complete. {total_new} listings processed.")
    return total_new

  def run_loop(self) -> None:
      """Run historical scraping in a loop."""
      interval = self.historical_config.interval
      logger.info(f"Starting historical monitor loop (interval: {interval}s)")

      while True:
          try:
              self.run_cycle()
          except Exception as e:
              logger.exception("Error in historical monitor cycle")

          logger.info(f"Sleeping for {interval}s...")
          time.sleep(interval)
