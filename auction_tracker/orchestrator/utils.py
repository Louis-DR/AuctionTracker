"""Shared orchestration utilities."""

from __future__ import annotations

import logging

from auction_tracker.parsing.base import Parser, ParserBlocked
from auction_tracker.parsing.models import ScrapedListing
from auction_tracker.transport.base import FetchResult, TransportError
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
  regional eBay domains that enforce cookie consent -- the same item
  is accessible on ebay.com, ebay.co.uk, etc. with the same ID.

  After successfully parsing the listing, if the parser exposes a
  ``build_bids_url`` method, a second fetch is made to retrieve the
  full bid history and merge it into the scraped listing.

  Returns the (FetchResult, ScrapedListing) from the first successful
  attempt. Raises the last ParserBlocked if all fallbacks are
  exhausted, or re-raises any other exception immediately.
  """
  last_blocked: ParserBlocked | None = None

  for attempt_url in [url]:
    try:
      fetch_url = parser.build_fetch_url(attempt_url)
      result = await router.fetch(website_name, fetch_url)
      scraped = parser.parse_listing(result.html, url=attempt_url)
      if attempt_url != url:
        logger.info(
          "Fetched %s via fallback domain %s",
          url, attempt_url,
        )
      scraped = await _fetch_full_bid_history(router, parser, website_name, scraped)
      return result, scraped
    except ParserBlocked as blocked:
      last_blocked = blocked
      break

  if last_blocked is None:
    raise RuntimeError("Unreachable")

  # Soft-block recovery: if the primary transport returned HTTP 200 with
  # a body that the parser recognised as blocked/stripped, retry the
  # same URL via the website's fallback transport (e.g. camoufox for an
  # HTTP-primary site).  The router does not trigger fallback on its
  # own here because no TransportError was raised — the body just had
  # no usable data.  Tried BEFORE the domain-fallback list so that a
  # Cloudflare-style soft block on ebay.fr does not have to exhaust
  # every regional domain before we switch to a real browser.
  if router.has_fallback_transport(website_name):
    fetch_url = parser.build_fetch_url(url)
    logger.info(
      "Parser reported block on %s (%s) — retrying via fallback transport",
      website_name, url,
    )
    try:
      result = await router.fetch(website_name, fetch_url, force_fallback=True)
      scraped = parser.parse_listing(result.html, url=url)
      logger.info(
        "Fallback transport succeeded for %s on %s", url, website_name,
      )
      scraped = await _fetch_full_bid_history(router, parser, website_name, scraped)
      return result, scraped
    except ParserBlocked as blocked:
      last_blocked = blocked
      logger.debug(
        "Fallback transport also produced a parser block for %s", url,
      )

  for fallback_url in last_blocked.fallback_urls:
    logger.info(
      "Retrying blocked URL %s with fallback %s",
      url, fallback_url,
    )
    try:
      result = await router.fetch(website_name, fallback_url)
      scraped = parser.parse_listing(result.html, url=fallback_url)
      logger.info("Fallback succeeded: %s", fallback_url)
      scraped = await _fetch_full_bid_history(router, parser, website_name, scraped)
      return result, scraped
    except ParserBlocked as blocked:
      last_blocked = blocked
      logger.debug("Fallback %s also blocked", fallback_url)
      continue

  raise last_blocked


async def _fetch_full_bid_history(
  router: TransportRouter,
  parser: Parser,
  website_name: str,
  scraped: ScrapedListing,
) -> ScrapedListing:
  """If the parser supports a dedicated bids API, fetch and merge it.

  Some sites (e.g. Catawiki) embed only a subset of bids in the lot
  page but expose the full history via a separate API endpoint. When
  the parser provides ``build_bids_url`` and ``parse_bid_history``,
  this function fetches the complete set and replaces the embedded
  bids. On failure it keeps the embedded bids as a fallback.
  """
  build_bids_url = getattr(parser, "build_bids_url", None)
  parse_bid_history = getattr(parser, "parse_bid_history", None)
  if build_bids_url is None or parse_bid_history is None:
    return scraped

  bids_url = build_bids_url(scraped.external_id)
  if not bids_url:
    return scraped

  try:
    bids_result = await router.fetch(website_name, bids_url)
    full_bids = parse_bid_history(bids_result.html)
    if full_bids:
      logger.info(
        "Fetched %d bids from API for %s (embedded had %d)",
        len(full_bids), scraped.external_id, len(scraped.bids),
      )
      scraped = scraped.model_copy(update={
        "bids": full_bids,
        "bid_count": len(full_bids),
      })
  except (TransportError, Exception) as error:
    logger.warning(
      "Failed to fetch bid history for %s, keeping %d embedded bids: %s",
      scraped.external_id, len(scraped.bids), error,
    )

  return scraped
