"""Shared orchestration utilities."""

from __future__ import annotations

import logging

from auction_tracker.parsing.base import Parser, ParserBlocked
from auction_tracker.parsing.models import ScrapedListing
from auction_tracker.transport.base import FetchResult
from auction_tracker.transport.router import TransportRouter

logger = logging.getLogger(__name__)


async def fetch_and_parse_listing(
  router: TransportRouter,
  parser: Parser,
  website_name: str,
  url: str,
) -> tuple[FetchResult, ScrapedListing]:
  """Fetch a listing URL and parse it, retrying on blocked pages.

  When the parser raises ParserBlocked (the server returned HTTP 200
  but the HTML is a consent/challenge page), each URL in
  ``ParserBlocked.fallback_urls`` is tried in order. This handles
  regional eBay domains that enforce cookie consent — the same item
  is accessible on ebay.com, ebay.co.uk, etc. with the same ID.

  Returns the (FetchResult, ScrapedListing) from the first successful
  attempt. Raises the last ParserBlocked if all fallbacks are
  exhausted, or re-raises any other exception immediately.
  """
  last_blocked: ParserBlocked | None = None

  for attempt_url in [url]:
    try:
      result = await router.fetch(website_name, attempt_url)
      scraped = parser.parse_listing(result.html, url=attempt_url)
      if attempt_url != url:
        logger.info(
          "Fetched %s via fallback domain %s",
          url, attempt_url,
        )
      return result, scraped
    except ParserBlocked as blocked:
      last_blocked = blocked
      fallback_urls = blocked.fallback_urls
      break

  if last_blocked is None:
    raise RuntimeError("Unreachable")

  for fallback_url in last_blocked.fallback_urls:
    logger.info(
      "Retrying blocked URL %s with fallback %s",
      url, fallback_url,
    )
    try:
      result = await router.fetch(website_name, fallback_url)
      scraped = parser.parse_listing(result.html, url=fallback_url)
      logger.info("Fallback succeeded: %s", fallback_url)
      return result, scraped
    except ParserBlocked as blocked:
      last_blocked = blocked
      logger.debug("Fallback %s also blocked", fallback_url)
      continue

  raise last_blocked
