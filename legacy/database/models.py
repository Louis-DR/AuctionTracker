"""SQLAlchemy declarative models for the auction tracking database.

Every table uses auto-incrementing integer primary keys.  Timestamps
(``created_at``, ``updated_at``) are managed automatically so callers
never need to set them by hand.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
  Boolean,
  Date,
  DateTime,
  Enum,
  Float,
  ForeignKey,
  Index,
  Integer,
  Numeric,
  String,
  Text,
  UniqueConstraint,
  func,
)
from sqlalchemy.orm import (
  DeclarativeBase,
  Mapped,
  mapped_column,
  relationship,
)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ListingType(enum.Enum):
  """How the item is being sold."""
  AUCTION = "auction"
  BUY_NOW = "buy_now"
  HYBRID = "hybrid"


class ListingStatus(enum.Enum):
  """Lifecycle state of a listing."""
  UPCOMING = "upcoming"
  ACTIVE = "active"
  SOLD = "sold"
  UNSOLD = "unsold"
  CANCELLED = "cancelled"
  RELISTED = "relisted"
  UNKNOWN = "unknown"


class ItemCondition(enum.Enum):
  """Physical condition of the item."""
  NEW = "new"
  LIKE_NEW = "like_new"
  VERY_GOOD = "very_good"
  GOOD = "good"
  FAIR = "fair"
  POOR = "poor"
  FOR_PARTS = "for_parts"
  UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
  """Shared declarative base for all models."""
  pass


# ---------------------------------------------------------------------------
# Website
# ---------------------------------------------------------------------------

class Website(Base):
  """An online auction house or marketplace (e.g. eBay, Catawiki)."""

  __tablename__ = "websites"

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
  base_url: Mapped[str] = mapped_column(String(500), nullable=False)

  # Default buyer fees – can be overridden per listing.
  default_buyer_premium_percent: Mapped[Optional[Decimal]] = mapped_column(
    Numeric(8, 4), nullable=True,
  )
  default_buyer_premium_fixed: Mapped[Optional[Decimal]] = mapped_column(
    Numeric(12, 2), nullable=True,
  )
  default_currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")

  # Name of the scraper class that handles this website.
  scraper_class_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

  is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
  notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  # Relationships
  sellers: Mapped[List[Seller]] = relationship(back_populates="website", cascade="all, delete-orphan")
  listings: Mapped[List[Listing]] = relationship(back_populates="website", cascade="all, delete-orphan")
  search_queries: Mapped[List[SearchQuery]] = relationship(back_populates="website", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# Seller
# ---------------------------------------------------------------------------

class Seller(Base):
  """A seller or auction house account on a specific website."""

  __tablename__ = "sellers"
  __table_args__ = (
    UniqueConstraint("website_id", "external_id", name="uq_seller_website_external"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  website_id: Mapped[int] = mapped_column(ForeignKey("websites.id"), nullable=False)
  external_id: Mapped[str] = mapped_column(String(300), nullable=False)

  username: Mapped[str] = mapped_column(String(300), nullable=False)
  display_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
  country: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
  rating: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
  feedback_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  member_since: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
  profile_url: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  # Relationships
  website: Mapped[Website] = relationship(back_populates="sellers")
  listings: Mapped[List[Listing]] = relationship(back_populates="seller")


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

class Listing(Base):
  """A single item listed for sale on a tracked website.

  This is the central entity of the tracker.  It captures all
  metadata about an item at a given moment and links to its price
  history, images, and custom attributes.
  """

  __tablename__ = "listings"
  __table_args__ = (
    UniqueConstraint("website_id", "external_id", name="uq_listing_website_external"),
    Index("ix_listing_status", "status"),
    Index("ix_listing_end_time", "end_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  website_id: Mapped[int] = mapped_column(ForeignKey("websites.id"), nullable=False)
  seller_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sellers.id"), nullable=True)
  external_id: Mapped[str] = mapped_column(String(300), nullable=False)
  url: Mapped[str] = mapped_column(String(2000), nullable=False)

  # Content
  title: Mapped[str] = mapped_column(String(1000), nullable=False)
  description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

  # Sale format
  listing_type: Mapped[ListingType] = mapped_column(
    Enum(ListingType), nullable=False, default=ListingType.AUCTION,
  )
  condition: Mapped[ItemCondition] = mapped_column(
    Enum(ItemCondition), nullable=False, default=ItemCondition.UNKNOWN,
  )

  # Currency (ISO 4217)
  currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")

  # Prices – all stored in the listing's own currency.
  starting_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  reserve_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  estimate_low: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  estimate_high: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  buy_now_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  current_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  final_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)

  # EUR-converted prices (computed at the time the price is recorded,
  # using the exchange rate of the day).  Always in EUR regardless of
  # the listing's native currency.  NULL when the native currency is
  # already EUR (just read the original column) or when the rate was
  # unavailable.
  current_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  final_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)

  # Buyer fees – override the website defaults when they differ per
  # listing or per auction house (e.g. Drouot).
  buyer_premium_percent: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
  buyer_premium_fixed: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)

  # Shipping
  shipping_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2), nullable=True)
  shipping_from_country: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
  ships_internationally: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

  # Timing
  start_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
  end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

  # Lifecycle
  status: Mapped[ListingStatus] = mapped_column(
    Enum(ListingStatus), nullable=False, default=ListingStatus.UNKNOWN,
  )

  # Counters
  bid_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
  watcher_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  view_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

  # Auction-house specific fields (Drouot, Invaluable, LiveAuctioneers…)
  lot_number: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
  auction_house_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
  sale_name: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
  sale_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

  # Tracking timestamps
  first_seen_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  last_checked_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  # Whether the listing has been fully fetched from its detail page
  # (as opposed to only being discovered via a search result).
  is_fully_fetched: Mapped[bool] = mapped_column(
    Boolean, nullable=False, server_default="1", default=True,
  )

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  # Relationships
  website: Mapped[Website] = relationship(back_populates="listings")
  seller: Mapped[Optional[Seller]] = relationship(back_populates="listings")
  images: Mapped[List[ListingImage]] = relationship(
    back_populates="listing", cascade="all, delete-orphan", order_by="ListingImage.position",
  )
  bids: Mapped[List[BidEvent]] = relationship(
    back_populates="listing", cascade="all, delete-orphan", order_by="BidEvent.amount",
  )
  price_snapshots: Mapped[List[PriceSnapshot]] = relationship(
    back_populates="listing", cascade="all, delete-orphan", order_by="PriceSnapshot.snapshot_time",
  )
  attributes: Mapped[List[ListingAttribute]] = relationship(
    back_populates="listing", cascade="all, delete-orphan",
  )

  # ------------------------------------------------------------------
  # Helpers
  # ------------------------------------------------------------------

  @property
  def effective_buyer_premium_percent(self) -> Optional[Decimal]:
    """Return the buyer premium, falling back to the website default."""
    if self.buyer_premium_percent is not None:
      return self.buyer_premium_percent
    if self.website is not None:
      return self.website.default_buyer_premium_percent
    return None

  @property
  def effective_buyer_premium_fixed(self) -> Optional[Decimal]:
    """Return the fixed buyer fee, falling back to the website default."""
    if self.buyer_premium_fixed is not None:
      return self.buyer_premium_fixed
    if self.website is not None:
      return self.website.default_buyer_premium_fixed
    return None

  @property
  def total_buyer_cost(self) -> Optional[Decimal]:
    """Estimate the total cost to the buyer (price + fees + shipping).

    Uses *final_price* if the item is sold, otherwise *current_price*.
    Returns ``None`` when no price information is available.
    """
    price = self.final_price or self.current_price
    if price is None:
      return None

    total = price
    premium_percent = self.effective_buyer_premium_percent
    if premium_percent is not None:
      total += price * premium_percent / Decimal(100)
    premium_fixed = self.effective_buyer_premium_fixed
    if premium_fixed is not None:
      total += premium_fixed
    if self.shipping_cost is not None:
      total += self.shipping_cost
    return total


# ---------------------------------------------------------------------------
# ListingImage
# ---------------------------------------------------------------------------

class ListingImage(Base):
  """An image associated with a listing."""

  __tablename__ = "listing_images"
  __table_args__ = (
    Index("ix_image_listing_position", "listing_id", "position"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  source_url: Mapped[str] = mapped_column(String(2000), nullable=False)
  local_path: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
  position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

  width: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  height: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  file_size_bytes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

  downloaded_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  # Relationships
  listing: Mapped[Listing] = relationship(back_populates="images")


# ---------------------------------------------------------------------------
# BidEvent
# ---------------------------------------------------------------------------

class BidEvent(Base):
  """A single bid placed on a listing.

  Stores each observed bid so the full bidding history can be
  reconstructed and analysed later.
  """

  __tablename__ = "bid_events"
  __table_args__ = (
    Index("ix_bid_listing_time", "listing_id", "bid_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
  currency: Mapped[str] = mapped_column(String(3), nullable=False)
  # EUR-converted amount at the time of the bid.
  amount_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)
  bid_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)

  bidder_username: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
  bidder_country: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
  is_automatic: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
  is_winning: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

  recorded_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  # Relationships
  listing: Mapped[Listing] = relationship(back_populates="bids")


# ---------------------------------------------------------------------------
# PriceSnapshot
# ---------------------------------------------------------------------------

class PriceSnapshot(Base):
  """A periodic snapshot of a listing's price and engagement counters.

  While ``BidEvent`` captures individual bids, snapshots record the
  state at regular intervals so that time-series analysis is possible
  even when individual bids are not visible (e.g. some websites only
  show the current price).
  """

  __tablename__ = "price_snapshots"
  __table_args__ = (
    Index("ix_snapshot_listing_time", "listing_id", "snapshot_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  price: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
  currency: Mapped[str] = mapped_column(String(3), nullable=False)
  # EUR-converted price at the time of the snapshot.
  price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(14, 2), nullable=True)

  bid_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  watcher_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
  view_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

  snapshot_time: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  # Relationships
  listing: Mapped[Listing] = relationship(back_populates="price_snapshots")


# ---------------------------------------------------------------------------
# ListingAttribute
# ---------------------------------------------------------------------------

class ListingAttribute(Base):
  """A free-form key/value attribute attached to a listing.

  This allows storing domain-specific metadata (e.g. pen brand, nib
  size, filling system) without altering the schema.
  """

  __tablename__ = "listing_attributes"
  __table_args__ = (
    Index("ix_attribute_listing_name", "listing_id", "attribute_name"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  attribute_name: Mapped[str] = mapped_column(String(200), nullable=False)
  attribute_value: Mapped[str] = mapped_column(Text, nullable=False)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  # Relationships
  listing: Mapped[Listing] = relationship(back_populates="attributes")


# ---------------------------------------------------------------------------
# SearchQuery
# ---------------------------------------------------------------------------

class SearchQuery(Base):
  """A saved search that the tracker runs periodically to discover new
  listings.
  """

  __tablename__ = "search_queries"

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  website_id: Mapped[Optional[int]] = mapped_column(ForeignKey("websites.id"), nullable=True)

  name: Mapped[str] = mapped_column(String(300), nullable=False)
  query_text: Mapped[str] = mapped_column(String(1000), nullable=False)
  category: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

  # Extra filters stored as JSON so they can be website-specific
  # without schema changes.
  filters_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

  is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
  last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
  result_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  # Relationships
  website: Mapped[Optional[Website]] = relationship(back_populates="search_queries")
