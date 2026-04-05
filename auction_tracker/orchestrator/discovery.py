"""Discovery loop: runs saved searches to find new listings.

The discovery loop iterates over active search queries, fetches the
search results pages via the transport router, parses them, and
ingests any new listings into the database. Newly discovered listings
are returned so the caller can enqueue them for monitoring.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy.orm import Session

from auction_tracker.config import AppConfig
from auction_tracker.database.models import Listing, SearchQuery
from auction_tracker.database.repository import Repository
from auction_tracker.orchestrator.ingest import Ingest
from auction_tracker.orchestrator.utils import fetch_and_parse_listing
from auction_tracker.parsing.base import ParserBlocked, ParserRegistry
from auction_tracker.transport.router import TransportRouter

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryStats:
  """Statistics from a discovery run."""
  searches_run: int = 0
  results_found: int = 0
  new_listings: int = 0
  listings_fetched: int = 0
  listings_classified: int = 0
  listings_rejected: int = 0
  errors: int = 0


class DiscoveryLoop:
  """Runs saved searches and discovers new listings.

  Usage::

      loop = DiscoveryLoop(config, router, repository)
      stats = await loop.run_all(session)
      # or for a specific website:
      stats = await loop.run_for_website(session, "ebay")
  """

  def __init__(
    self,
    config: AppConfig,
    router: TransportRouter,
    repository: Repository,
  ) -> None:
    self._config = config
    self._router = router
    self._repo = repository
    self._ingest = Ingest(repository)

  async def run_all(
    self,
    session: Session,
    website_filter: str | None = None,
  ) -> tuple[DiscoveryStats, list[Listing]]:
    """Run all active searches and return (stats, new_listings)."""
    stats = DiscoveryStats()
    all_new_listings: list[Listing] = []

    searches = self._repo.get_active_searches(session, website_name=website_filter)
    if not searches:
      logger.info("No active searches to run")
      return stats, all_new_listings

    for search_query in searches:
      try:
        new_listings = await self._run_search(session, search_query, stats)
        all_new_listings.extend(new_listings)
        session.commit()
      except Exception as error:
        stats.errors += 1
        session.rollback()
        logger.error(
          "Error running search '%s': %s",
          search_query.name, error,
          exc_info=True,
        )

    logger.info(
      "Discovery complete: %d searches, %d results, %d new listings, %d errors",
      stats.searches_run, stats.results_found, stats.new_listings, stats.errors,
    )
    return stats, all_new_listings

  async def _run_search(
    self,
    session: Session,
    search_query: SearchQuery,
    stats: DiscoveryStats,
  ) -> list[Listing]:
    """Run a single search query and return newly discovered listings."""
    website = search_query.website
    if website is None:
      logger.warning("Search '%s' has no website, skipping", search_query.name)
      return []

    website_name = website.name
    website_config = self._config.website(website_name)
    if not website_config.enabled:
      return []
    if website_config.exclude_from_discovery:
      return []
    if not ParserRegistry.has(website_name):
      logger.warning("No parser for website '%s', skipping search", website_name)
      return []

    parser = ParserRegistry.get(website_name)
    if not parser.capabilities.can_search:
      return []

    search_kwargs: dict = {}
    if website_config.preferred_domain:
      search_kwargs["domain"] = website_config.preferred_domain
    search_url = parser.build_search_url(search_query.query_text, **search_kwargs)
    logger.info(
      "Running search '%s' on %s: %s",
      search_query.name, website_name, search_url,
    )

    result = await self._router.fetch(website_name, search_url)
    try:
      search_results = parser.parse_search_results(result.html, url=search_url)
    except ParserBlocked:
      logger.warning(
        "Search URL blocked on %s: %s — skipping", website_name, search_url,
      )
      return []
    stats.searches_run += 1
    stats.results_found += len(search_results)

    from datetime import datetime
    search_query.last_run_at = datetime.utcnow()
    search_query.result_count = len(search_results)

    new_listings: list[Listing] = []
    for scraped_result in search_results:
      listing, is_new = self._ingest.ingest_search_result(
        session, website.id, scraped_result,
      )
      if is_new:
        stats.new_listings += 1
        new_listings.append(listing)

    return new_listings

  async def fetch_unfetched(
    self,
    session: Session,
    website_filter: str | None = None,
    classify: bool = True,
  ) -> DiscoveryStats:
    """Fetch full details for listings discovered via search.

    Optionally runs image classification to filter out non-pen
    listings. Returns detailed stats.
    """
    from auction_tracker.orchestrator.images import (
      classify_listing as run_classification,
    )
    from auction_tracker.orchestrator.images import (
      download_listing_images,
    )

    stats = DiscoveryStats()
    listings = self._repo.get_listings_needing_fetch(session, website_name=website_filter)
    if not listings:
      return stats

    for listing in listings:
      website_name = listing.website.name
      if not ParserRegistry.has(website_name):
        continue

      parser = ParserRegistry.get(website_name)
      if not parser.capabilities.can_parse_listing:
        continue

      try:
        _result, scraped = await fetch_and_parse_listing(
          self._router, parser, website_name, listing.url,
        )
        self._ingest.ingest_listing(session, listing.website_id, scraped)
        stats.listings_fetched += 1

        # Run classification if enabled and images are available.
        if classify and scraped.image_urls:
          image_paths = await download_listing_images(
            scraped.image_urls,
            listing.id,
            self._config.classifier,
          )
          if image_paths:
            is_relevant, score, top_classes = run_classification(
              image_paths, self._config.classifier,
            )
            stats.listings_classified += 1
            if not is_relevant:
              stats.listings_rejected += 1
              from auction_tracker.database.models import ListingStatus
              self._repo.mark_listing_status(
                session, listing.id, ListingStatus.CANCELLED,
              )
              top_labels = ", ".join(f"{label} ({prob:.0%})" for label, prob in top_classes)
              logger.info(
                "Rejected listing %s (score=%.0f%%): %s",
                listing.external_id, score * 100, top_labels,
              )

        session.commit()
      except Exception:
        session.rollback()
        stats.errors += 1
        logger.error(
          "Error fetching listing %s",
          listing.external_id,
          exc_info=True,
        )

    logger.info(
      "Fetch complete: %d fetched, %d classified, %d rejected, %d errors",
      stats.listings_fetched, stats.listings_classified,
      stats.listings_rejected, stats.errors,
    )
    return stats
