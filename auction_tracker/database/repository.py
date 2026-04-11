"""Repository layer providing clean database operations.

All database reads and writes go through this class so that the
rest of the application never directly constructs SQLAlchemy queries.
This makes the data access easy to mock in tests and keeps SQL
concerns in one place.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

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


class Repository:
  """Stateless helper that operates on a provided session.

  Every public method takes a ``Session`` so the caller controls
  transaction boundaries. Typical usage::

      with db.session() as session:
          repo = Repository()
          listing = repo.get_listing_by_external_id(session, "ebay", "12345")
          session.commit()
  """

  # -------------------------------------------------------------------
  # Websites
  # -------------------------------------------------------------------

  def get_website_by_name(self, session: Session, name: str) -> Website | None:
    statement = select(Website).where(Website.name == name)
    return session.scalars(statement).first()

  def get_or_create_website(
    self,
    session: Session,
    name: str,
    base_url: str,
    **kwargs,
  ) -> Website:
    website = self.get_website_by_name(session, name)
    if website is not None:
      return website
    website = Website(name=name, base_url=base_url, **kwargs)
    session.add(website)
    session.flush()
    logger.info("Created website: %s", name)
    return website

  def get_active_websites(self, session: Session) -> Sequence[Website]:
    statement = select(Website).where(Website.is_active.is_(True))
    return session.scalars(statement).all()

  # -------------------------------------------------------------------
  # Sellers
  # -------------------------------------------------------------------

  def get_or_create_seller(
    self,
    session: Session,
    website_id: int,
    external_id: str,
    username: str,
    **kwargs,
  ) -> Seller:
    statement = select(Seller).where(
      Seller.website_id == website_id,
      Seller.external_id == external_id,
    )
    seller = session.scalars(statement).first()
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
    return seller

  # -------------------------------------------------------------------
  # Listings
  # -------------------------------------------------------------------

  def get_listing_by_external_id(
    self,
    session: Session,
    website_name: str,
    external_id: str,
  ) -> Listing | None:
    statement = (
      select(Listing)
      .join(Website)
      .where(Website.name == website_name, Listing.external_id == external_id)
    )
    return session.scalars(statement).first()

  def get_listing_by_id(self, session: Session, listing_id: int) -> Listing | None:
    return session.get(Listing, listing_id)

  def upsert_listing(
    self,
    session: Session,
    website_id: int,
    external_id: str,
    url: str,
    title: str,
    **kwargs,
  ) -> tuple[Listing, bool]:
    """Insert or update a listing. Returns (listing, is_new)."""
    statement = select(Listing).where(
      Listing.website_id == website_id,
      Listing.external_id == external_id,
    )
    listing = session.scalars(statement).first()
    is_new = listing is None
    if is_new:
      listing = Listing(
        website_id=website_id,
        external_id=external_id,
        url=url,
        title=title,
        **kwargs,
      )
      session.add(listing)
    else:
      listing.url = url
      listing.title = title
      for key, value in kwargs.items():
        if value is not None:
          setattr(listing, key, value)
      listing.last_checked_at = datetime.now(UTC).replace(tzinfo=None)
    session.flush()
    return listing, is_new

  def get_active_listings(
    self,
    session: Session,
    website_name: str | None = None,
  ) -> Sequence[Listing]:
    """Get all listings that are not in a terminal state."""
    terminal_statuses = (ListingStatus.SOLD, ListingStatus.UNSOLD, ListingStatus.CANCELLED)
    statement = select(Listing).where(Listing.status.not_in(terminal_statuses))
    if website_name is not None:
      statement = statement.join(Website).where(Website.name == website_name)
    statement = statement.order_by(Listing.end_time.asc().nullslast())
    return session.scalars(statement).all()

  def get_listings_needing_fetch(
    self,
    session: Session,
    website_name: str | None = None,
  ) -> Sequence[Listing]:
    """Get listings discovered but not yet fully fetched."""
    statement = select(Listing).where(Listing.is_fully_fetched.is_(False))
    if website_name is not None:
      statement = statement.join(Website).where(Website.name == website_name)
    return session.scalars(statement).all()

  def mark_listing_status(
    self,
    session: Session,
    listing_id: int,
    status: ListingStatus,
    final_price: float | None = None,
  ) -> None:
    values: dict = {"status": status, "last_checked_at": datetime.now(UTC).replace(tzinfo=None)}
    if final_price is not None:
      values["final_price"] = final_price
    session.execute(
      update(Listing).where(Listing.id == listing_id).values(**values)
    )

  def count_listings(
    self,
    session: Session,
    website_name: str | None = None,
    status: ListingStatus | None = None,
  ) -> int:
    statement = select(Listing)
    if website_name is not None:
      statement = statement.join(Website).where(Website.name == website_name)
    if status is not None:
      statement = statement.where(Listing.status == status)
    return len(session.scalars(statement).all())

  # -------------------------------------------------------------------
  # Price snapshots
  # -------------------------------------------------------------------

  def add_price_snapshot(
    self,
    session: Session,
    listing_id: int,
    price: float,
    currency: str,
    bid_count: int | None = None,
    watcher_count: int | None = None,
    view_count: int | None = None,
    price_eur: float | None = None,
    exchange_rate: float | None = None,
  ) -> PriceSnapshot:
    snapshot = PriceSnapshot(
      listing_id=listing_id,
      price=price,
      currency=currency,
      price_eur=price_eur,
      exchange_rate=exchange_rate,
      bid_count=bid_count,
      watcher_count=watcher_count,
      view_count=view_count,
    )
    session.add(snapshot)
    session.flush()
    return snapshot

  # -------------------------------------------------------------------
  # Bid events
  # -------------------------------------------------------------------

  def sync_bid_events(
    self,
    session: Session,
    listing_id: int,
    bids: list[dict],
  ) -> int:
    """Synchronise bid events for a listing.

    Each bid is identified by (listing_id, amount, bid_time) to avoid
    duplicates on repeated fetches. Only new bids are inserted.
    Returns the number of newly added bids.
    """
    existing = session.scalars(
      select(BidEvent).where(BidEvent.listing_id == listing_id)
    ).all()
    existing_keys = {
      (bid.amount, bid.bid_time) for bid in existing
    }

    added = 0
    for bid_data in bids:
      key = (bid_data["amount"], bid_data["bid_time"])
      if key in existing_keys:
        continue
      session.add(BidEvent(listing_id=listing_id, **bid_data))
      existing_keys.add(key)
      added += 1

    if added > 0:
      session.flush()
      logger.debug("Added %d new bid events for listing %d", added, listing_id)
    return added

  # -------------------------------------------------------------------
  # Images
  # -------------------------------------------------------------------

  def sync_listing_images(
    self,
    session: Session,
    listing_id: int,
    image_urls: list[str],
  ) -> None:
    """Ensure the listing has exactly the given image URLs.

    Adds missing images and removes stale ones so that repeated
    fetches converge to the correct set.
    """
    existing = (
      session.scalars(
        select(ListingImage)
        .where(ListingImage.listing_id == listing_id)
        .order_by(ListingImage.position)
      )
      .all()
    )
    existing_urls = {image.source_url for image in existing}
    wanted_urls = set(image_urls)

    for image in existing:
      if image.source_url not in wanted_urls:
        session.delete(image)

    for position, url in enumerate(image_urls):
      if url not in existing_urls:
        session.add(ListingImage(
          listing_id=listing_id,
          source_url=url,
          position=position,
        ))

  # -------------------------------------------------------------------
  # Search queries
  # -------------------------------------------------------------------

  def upsert_listing_attribute(
    self,
    session: Session,
    listing_id: int,
    name: str,
    value: str,
  ) -> None:
    """Create or update a single key/value attribute on a listing."""
    statement = select(ListingAttribute).where(
      ListingAttribute.listing_id == listing_id,
      ListingAttribute.attribute_name == name,
    )
    attr = session.scalars(statement).first()
    if attr is None:
      session.add(ListingAttribute(
        listing_id=listing_id,
        attribute_name=name,
        attribute_value=value,
      ))
    else:
      attr.attribute_value = value

  def get_active_searches(self, session: Session) -> Sequence[SearchQuery]:
    statement = select(SearchQuery).where(SearchQuery.is_active.is_(True))
    return session.scalars(statement).all()

  def upsert_search_query(
    self,
    session: Session,
    name: str,
    query_text: str,
    **kwargs,
  ) -> SearchQuery:
    """Insert or update a global saved search (matched by name)."""
    statement = select(SearchQuery).where(SearchQuery.name == name)
    query = session.scalars(statement).first()
    if query is None:
      query = SearchQuery(name=name, query_text=query_text, **kwargs)
      session.add(query)
    else:
      query.query_text = query_text
      for key, value in kwargs.items():
        if value is not None:
          setattr(query, key, value)
    session.flush()
    return query
