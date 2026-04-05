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
    """Run all active searches across all enabled websites.

    Each saved search is a global query: it is issued against every
    enabled website that has search support. An optional ``website_filter``
    restricts execution to a single website (useful for CLI debugging).
    """
    stats = DiscoveryStats()
    all_new_listings: list[Listing] = []

    searches = self._repo.get_active_searches(session)
    if not searches:
      logger.info("No active searches to run")
      return stats, all_new_listings

    # Collect the list of enabled, searchable websites once.
    all_websites = self._repo.get_active_websites(session)
    target_websites = [
      w for w in all_websites
      if (website_filter is None or w.name == website_filter)
      and ParserRegistry.has(w.name)
      and ParserRegistry.get(w.name).capabilities.can_search
      and self._config.website(w.name).enabled
      and not self._config.website(w.name).exclude_from_discovery
    ]

    if not target_websites:
      logger.info("No searchable websites available")
      return stats, all_new_listings

    for search_query in searches:
      total_results = 0
      for website in target_websites:
        try:
          new_listings, result_count = await self._run_search_on_website(
            session, search_query, website.name, stats,
          )
          all_new_listings.extend(new_listings)
          total_results += result_count
          session.commit()
        except Exception as error:
          stats.errors += 1
          session.rollback()
          logger.error(
            "Error running search '%s' on %s: %s",
            search_query.name, website.name, error,
            exc_info=True,
          )

      from datetime import datetime
      search_query.last_run_at = datetime.utcnow()
      search_query.result_count = total_results
      session.commit()

    logger.info(
      "Discovery complete: %d searches, %d results, %d new listings, %d errors",
      stats.searches_run, stats.results_found, stats.new_listings, stats.errors,
    )
    return stats, all_new_listings

  async def _run_search_on_website(
    self,
    session: Session,
    search_query: SearchQuery,
    website_name: str,
    stats: DiscoveryStats,
  ) -> tuple[list[Listing], int]:
    """Run a single search on one website and return (new_listings, result_count)."""
    parser = ParserRegistry.get(website_name)
    website_config = self._config.website(website_name)

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
    except ParserBlocked as blocked_error:
      fallback_urls = blocked_error.fallback_urls
      if not fallback_urls:
        logger.warning(
          "Search URL blocked on %s: %s — no fallbacks, skipping",
          website_name, search_url,
        )
        return [], 0
      # Try each fallback domain in order (e.g. ebay.fr → ebay.com → ebay.co.uk).
      for fallback_url in fallback_urls:
        logger.info(
          "Search blocked on %s, retrying with fallback: %s",
          website_name, fallback_url,
        )
        try:
          fallback_result = await self._router.fetch(website_name, fallback_url)
          search_results = parser.parse_search_results(
            fallback_result.html, url=fallback_url,
          )
          search_url = fallback_url
          break
        except ParserBlocked:
          continue
      else:
        logger.warning(
          "Search URL blocked on %s: all fallbacks exhausted, skipping",
          website_name,
        )
        return [], 0
    stats.searches_run += 1
    stats.results_found += len(search_results)

    website_obj = self._repo.get_website_by_name(session, website_name)
    if website_obj is None:
      return [], 0

    new_listings: list[Listing] = []
    for scraped_result in search_results:
      listing, is_new = self._ingest.ingest_search_result(
        session, website_obj.id, scraped_result,
      )
      if is_new:
        stats.new_listings += 1
        new_listings.append(listing)

    return new_listings, len(search_results)

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
