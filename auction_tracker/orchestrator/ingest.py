"""Ingest module: converts scraped data into database records.

This module bridges the parser output (Pydantic models) and the
database layer (SQLAlchemy models). It handles the mapping, dedup,
and update logic so that the orchestrator loops stay clean.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from auction_tracker.database.models import (
  ItemCondition,
  Listing,
  ListingStatus,
  ListingType,
)
from auction_tracker.database.repository import Repository
from auction_tracker.parsing.models import ScrapedListing, ScrapedSearchResult

logger = logging.getLogger(__name__)

_LISTING_TYPE_MAP = {
  "auction": ListingType.AUCTION,
  "buy_now": ListingType.BUY_NOW,
  "hybrid": ListingType.HYBRID,
}

_LISTING_STATUS_MAP = {
  "upcoming": ListingStatus.UPCOMING,
  "active": ListingStatus.ACTIVE,
  "sold": ListingStatus.SOLD,
  "unsold": ListingStatus.UNSOLD,
  "cancelled": ListingStatus.CANCELLED,
  "relisted": ListingStatus.RELISTED,
}

_CONDITION_MAP = {
  "new": ItemCondition.NEW,
  "like_new": ItemCondition.LIKE_NEW,
  "very_good": ItemCondition.VERY_GOOD,
  "good": ItemCondition.GOOD,
  "fair": ItemCondition.FAIR,
  "poor": ItemCondition.POOR,
  "for_parts": ItemCondition.FOR_PARTS,
}


def _safe_enum(mapping: dict, value: str | None, default):
  """Map a string to an enum, returning the default on failure."""
  if value is None:
    return default
  return mapping.get(value.lower(), default)


class Ingest:
  """Converts scraped data into database records."""

  def __init__(self, repository: Repository) -> None:
    self._repo = repository

  def ingest_search_result(
    self,
    session: Session,
    website_id: int,
    result: ScrapedSearchResult,
  ) -> tuple[Listing, bool]:
    """Create or update a listing from a search result.

    Search results contain minimal data, so the listing is marked
    as not fully fetched. Returns (listing, is_new).
    """
    kwargs = {
      "current_price": result.current_price,
      "currency": result.currency,
      "end_time": result.end_time,
      "bid_count": result.bid_count or 0,
      "is_fully_fetched": False,
    }
    if result.listing_type:
      kwargs["listing_type"] = _safe_enum(
        _LISTING_TYPE_MAP, result.listing_type, ListingType.AUCTION,
      )

    listing, is_new = self._repo.upsert_listing(
      session,
      website_id=website_id,
      external_id=result.external_id,
      url=result.url,
      title=result.title,
      **kwargs,
    )

    if is_new:
      logger.info(
        "Discovered new listing: %s [%s]", result.title[:60], result.external_id,
      )
      if result.image_url:
        self._repo.sync_listing_images(session, listing.id, [result.image_url])

    return listing, is_new

  def ingest_listing(
    self,
    session: Session,
    website_id: int,
    scraped: ScrapedListing,
  ) -> tuple[Listing, bool]:
    """Create or update a listing from a full detail page.

    This is the primary ingest path — it updates all fields and
    marks the listing as fully fetched.
    """
    kwargs = {
      "description": scraped.description,
      "listing_type": _safe_enum(
        _LISTING_TYPE_MAP, scraped.listing_type, ListingType.AUCTION,
      ),
      "condition": _safe_enum(
        _CONDITION_MAP, scraped.condition, ItemCondition.UNKNOWN,
      ),
      "currency": scraped.currency,
      "starting_price": scraped.starting_price,
      "reserve_price": scraped.reserve_price,
      "estimate_low": scraped.estimate_low,
      "estimate_high": scraped.estimate_high,
      "buy_now_price": scraped.buy_now_price,
      "current_price": scraped.current_price,
      "final_price": scraped.final_price,
      "buyer_premium_percent": scraped.buyer_premium_percent,
      "buyer_premium_fixed": scraped.buyer_premium_fixed,
      "shipping_cost": scraped.shipping_cost,
      "shipping_from_country": scraped.shipping_from_country,
      "ships_internationally": scraped.ships_internationally,
      "start_time": scraped.start_time,
      "end_time": scraped.end_time,
      "bid_count": scraped.bid_count or 0,
      "watcher_count": scraped.watcher_count,
      "view_count": scraped.view_count,
      "lot_number": scraped.lot_number,
      "auction_house_name": scraped.auction_house_name,
      "sale_name": scraped.sale_name,
      "sale_date": scraped.sale_date,
      "is_fully_fetched": True,
      "last_checked_at": datetime.utcnow(),
    }
    if scraped.status:
      kwargs["status"] = _safe_enum(
        _LISTING_STATUS_MAP, scraped.status, ListingStatus.UNKNOWN,
      )

    listing, is_new = self._repo.upsert_listing(
      session,
      website_id=website_id,
      external_id=scraped.external_id,
      url=scraped.url,
      title=scraped.title,
      **kwargs,
    )

    if scraped.image_urls:
      self._repo.sync_listing_images(session, listing.id, scraped.image_urls)

    if scraped.seller:
      seller = self._repo.get_or_create_seller(
        session,
        website_id=website_id,
        external_id=scraped.seller.external_id,
        username=scraped.seller.username,
        display_name=scraped.seller.display_name,
        country=scraped.seller.country,
        rating=scraped.seller.rating,
        feedback_count=scraped.seller.feedback_count,
        member_since=scraped.seller.member_since,
        profile_url=scraped.seller.profile_url,
      )
      listing.seller_id = seller.id

    if scraped.current_price is not None:
      self._repo.add_price_snapshot(
        session,
        listing_id=listing.id,
        price=float(scraped.current_price),
        currency=scraped.currency,
        bid_count=scraped.bid_count,
        watcher_count=scraped.watcher_count,
        view_count=scraped.view_count,
      )

    if scraped.bids:
      bid_dicts = [
        {
          "amount": bid.amount,
          "currency": bid.currency,
          "bid_time": bid.bid_time,
          "bidder_username": bid.bidder_username,
          "bidder_country": bid.bidder_country,
          "is_automatic": bid.is_automatic,
        }
        for bid in scraped.bids
      ]
      # Mark the highest bid as winning if the listing is sold.
      if scraped.status == "sold" and bid_dicts:
        bid_dicts[-1]["is_winning"] = True
      self._repo.sync_bid_events(session, listing.id, bid_dicts)

    log_action = "Ingested new" if is_new else "Updated"
    logger.info(
      "%s listing: %s [%s] — %s %s",
      log_action, scraped.title[:60], scraped.external_id,
      scraped.current_price or "?", scraped.currency,
    )

    return listing, is_new
