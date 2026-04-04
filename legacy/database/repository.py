"""Repository layer – convenience functions for common database operations.

All public functions accept an explicit ``Session`` so callers remain in
control of transaction boundaries.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from auction_tracker.database.models import (
  BidEvent,
  Listing,
  ListingAttribute,
  ListingImage,
  ListingStatus,
  PriceSnapshot,
  SearchQuery,
  Seller,
  Website,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Website helpers
# ------------------------------------------------------------------

def get_or_create_website(
  session: Session,
  *,
  name: str,
  base_url: str,
  **kwargs,
) -> Website:
  """Return the ``Website`` with *name*, creating it if necessary."""
  website = session.execute(
    select(Website).where(Website.name == name)
  ).scalar_one_or_none()

  if website is not None:
    return website

  website = Website(name=name, base_url=base_url, **kwargs)
  session.add(website)
  session.flush()
  logger.info("Created website: %s", name)
  return website


def list_websites(session: Session, *, active_only: bool = True) -> Sequence[Website]:
  """Return all registered websites, optionally filtered to active ones."""
  statement = select(Website).order_by(Website.name)
  if active_only:
    statement = statement.where(Website.is_active.is_(True))
  return session.execute(statement).scalars().all()


# ------------------------------------------------------------------
# Seller helpers
# ------------------------------------------------------------------

def get_or_create_seller(
  session: Session,
  *,
  website_id: int,
  external_id: str,
  username: str,
  **kwargs,
) -> Seller:
  """Return the ``Seller`` identified by *website_id* + *external_id*,
  creating it if necessary.
  """
  seller = session.execute(
    select(Seller).where(
      Seller.website_id == website_id,
      Seller.external_id == external_id,
    )
  ).scalar_one_or_none()

  if seller is not None:
    return seller

  seller = Seller(
    website_id=website_id,
    external_id=external_id,
    username=username,
    **kwargs,
  )
  session.add(seller)
  session.flush()
  logger.info("Created seller: %s (website_id=%d)", username, website_id)
  return seller


# ------------------------------------------------------------------
# Listing helpers
# ------------------------------------------------------------------

def get_or_create_listing(
  session: Session,
  *,
  website_id: int,
  external_id: str,
  defaults: Optional[dict] = None,
) -> tuple[Listing, bool]:
  """Return ``(listing, created)`` for *website_id* + *external_id*.

  If the listing already exists the second element is ``False`` and the
  caller should decide whether to update it.  *defaults* are only used
  when creating a new row.
  """
  listing = session.execute(
    select(Listing).where(
      Listing.website_id == website_id,
      Listing.external_id == external_id,
    )
  ).scalar_one_or_none()

  if listing is not None:
    return listing, False

  if defaults is None:
    defaults = {}

  listing = Listing(website_id=website_id, external_id=external_id, **defaults)
  session.add(listing)
  session.flush()
  logger.info("Created listing: %s (website_id=%d)", external_id, website_id)
  return listing, True


def update_listing_price(
  session: Session,
  listing: Listing,
  *,
  price: Decimal,
  bid_count: Optional[int] = None,
) -> None:
  """Update the current price (and optionally bid count) of a listing."""
  listing.current_price = price
  if bid_count is not None:
    listing.bid_count = bid_count
  listing.last_checked_at = datetime.utcnow()
  session.flush()


def mark_listing_sold(
  session: Session,
  listing: Listing,
  *,
  final_price: Decimal,
) -> None:
  """Mark a listing as sold and record the final hammer price."""
  listing.status = ListingStatus.SOLD
  listing.final_price = final_price
  listing.current_price = final_price
  listing.last_checked_at = datetime.utcnow()
  session.flush()


def mark_listing_unsold(session: Session, listing: Listing) -> None:
  """Mark a listing as unsold (ended without a sale)."""
  listing.status = ListingStatus.UNSOLD
  listing.last_checked_at = datetime.utcnow()
  session.flush()


def get_active_listings(
  session: Session,
  *,
  website_id: Optional[int] = None,
  join_website: bool = False,
  include_unknown: bool = False,
) -> Sequence[Listing]:
  """Return listings that the watch loop should track.

  By default returns ACTIVE and UPCOMING only. If *include_unknown* is
  True, also returns UNKNOWN (e.g. Drouot listings marked for re-fetch
  by fix-database so they get re-fetched and updated to SOLD/UNSOLD).

  If *join_website* is True, the website relation is eager-loaded.
  """
  statuses = [ListingStatus.ACTIVE, ListingStatus.UPCOMING]
  if include_unknown:
    statuses.append(ListingStatus.UNKNOWN)
  statement = select(Listing).where(
    Listing.status.in_(statuses)
  ).order_by(Listing.end_time)

  if join_website:
    statement = statement.options(joinedload(Listing.website))

  if website_id is not None:
    statement = statement.where(Listing.website_id == website_id)

  result = session.execute(statement)
  if join_website:
    return result.unique().scalars().all()
  return result.scalars().all()


def get_unfetched_active_listings(
  session: Session,
) -> Sequence[Listing]:
  """Return active/upcoming/unknown listings that have never been fully fetched.

  These are listings discovered by a search but whose detail page has
  not yet been scraped (``is_fully_fetched = False``).  Includes
  ``UNKNOWN`` status since that is what search results are marked with
  before being fully processed.
  """
  statement = (
    select(Listing)
    .options(joinedload(Listing.website))
    .where(Listing.status.in_([
      ListingStatus.ACTIVE,
      ListingStatus.UPCOMING,
      ListingStatus.UNKNOWN,
    ]))
    .where(Listing.is_fully_fetched.is_(False))
    .order_by(Listing.end_time)
  )
  return session.execute(statement).scalars().unique().all()


def search_listings(
  session: Session,
  *,
  title_contains: Optional[str] = None,
  website_id: Optional[int] = None,
  status: Optional[ListingStatus] = None,
  limit: int = 100,
) -> Sequence[Listing]:
  """Flexible listing search with optional filters."""
  statement = select(Listing)

  if title_contains:
    statement = statement.where(Listing.title.ilike(f"%{title_contains}%"))
  if website_id is not None:
    statement = statement.where(Listing.website_id == website_id)
  if status is not None:
    statement = statement.where(Listing.status == status)

  statement = statement.order_by(Listing.created_at.desc()).limit(limit)
  return session.execute(statement).scalars().all()


# ------------------------------------------------------------------
# BidEvent helpers
# ------------------------------------------------------------------

def record_bid(
  session: Session,
  *,
  listing_id: int,
  amount: Decimal,
  currency: str,
  amount_eur: Optional[Decimal] = None,
  bid_time: datetime,
  bidder_username: Optional[str] = None,
  bidder_country: Optional[str] = None,
  is_automatic: bool = False,
) -> BidEvent:
  """Record a new bid, avoiding duplicates based on listing + time + amount."""
  existing = session.execute(
    select(BidEvent).where(
      BidEvent.listing_id == listing_id,
      BidEvent.bid_time == bid_time,
      BidEvent.amount == amount,
    )
  ).scalar_one_or_none()

  if existing is not None:
    # Back-fill country and EUR amount if they were missing before.
    if existing.bidder_country is None and bidder_country:
      existing.bidder_country = bidder_country
    if existing.amount_eur is None and amount_eur is not None:
      existing.amount_eur = amount_eur
    session.flush()
    return existing

  bid = BidEvent(
    listing_id=listing_id,
    amount=amount,
    currency=currency,
    amount_eur=amount_eur,
    bid_time=bid_time,
    bidder_username=bidder_username,
    bidder_country=bidder_country,
    is_automatic=is_automatic,
  )
  session.add(bid)
  session.flush()
  return bid


# ------------------------------------------------------------------
# PriceSnapshot helpers
# ------------------------------------------------------------------

def take_price_snapshot(
  session: Session,
  listing: Listing,
  *,
  price_eur: Optional[Decimal] = None,
) -> Optional[PriceSnapshot]:
  """Create a price snapshot from the listing's current state.

  Returns ``None`` if no price data is available.
  """
  price = listing.current_price
  if price is None:
    return None

  snapshot = PriceSnapshot(
    listing_id=listing.id,
    price=price,
    currency=listing.currency,
    price_eur=price_eur,
    bid_count=listing.bid_count,
    watcher_count=listing.watcher_count,
    view_count=listing.view_count,
  )
  session.add(snapshot)
  session.flush()
  return snapshot


# ------------------------------------------------------------------
# ListingImage helpers
# ------------------------------------------------------------------

def add_listing_image(
  session: Session,
  *,
  listing_id: int,
  source_url: str,
  position: int = 0,
) -> ListingImage:
  """Add an image record to a listing, or return the existing one."""
  existing = session.execute(
    select(ListingImage).where(
      ListingImage.listing_id == listing_id,
      ListingImage.source_url == source_url,
    )
  ).scalar_one_or_none()

  if existing is not None:
    return existing

  image = ListingImage(
    listing_id=listing_id,
    source_url=source_url,
    position=position,
  )
  session.add(image)
  session.flush()
  return image


# ------------------------------------------------------------------
# ListingAttribute helpers
# ------------------------------------------------------------------

def set_listing_attribute(
  session: Session,
  *,
  listing_id: int,
  attribute_name: str,
  attribute_value: str,
) -> ListingAttribute:
  """Set (insert or update) a free-form attribute on a listing."""
  existing = session.execute(
    select(ListingAttribute).where(
      ListingAttribute.listing_id == listing_id,
      ListingAttribute.attribute_name == attribute_name,
    )
  ).scalar_one_or_none()

  if existing is not None:
    existing.attribute_value = attribute_value
    session.flush()
    return existing

  attribute = ListingAttribute(
    listing_id=listing_id,
    attribute_name=attribute_name,
    attribute_value=attribute_value,
  )
  session.add(attribute)
  session.flush()
  return attribute


def add_listing_search_source(
  session: Session,
  *,
  listing_id: int,
  search_query_text: str,
) -> ListingAttribute:
  """Record that a listing was discovered by a specific search query.

  The source is stored as a ``source_search:<query_text>`` attribute
  so that each unique search query creates a separate, idempotent
  entry for the listing.
  """
  attribute_name = f"source_search:{search_query_text}"
  return set_listing_attribute(
    session,
    listing_id=listing_id,
    attribute_name=attribute_name,
    attribute_value="true",
  )


# ------------------------------------------------------------------
# SearchQuery helpers
# ------------------------------------------------------------------

def get_or_create_search_query(
  session: Session,
  *,
  name: str,
  query_text: str,
  website_id: Optional[int] = None,
  **kwargs,
) -> SearchQuery:
  """Return a search query by name, creating it when absent."""
  existing = session.execute(
    select(SearchQuery).where(SearchQuery.name == name)
  ).scalar_one_or_none()

  if existing is not None:
    return existing

  query = SearchQuery(
    name=name,
    query_text=query_text,
    website_id=website_id,
    **kwargs,
  )
  session.add(query)
  session.flush()
  logger.info("Created search query: %s", name)
  return query


def get_active_search_queries(
  session: Session,
  *,
  website_id: Optional[int] = None,
) -> Sequence[SearchQuery]:
  """Return all active search queries, optionally for a specific website."""
  statement = select(SearchQuery).where(SearchQuery.is_active.is_(True))
  if website_id is not None:
    statement = statement.where(SearchQuery.website_id == website_id)
  return session.execute(statement).scalars().all()
