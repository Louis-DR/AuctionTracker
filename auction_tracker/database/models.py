"""SQLAlchemy 2.0 declarative models for the auction tracking database.

Every table uses auto-incrementing integer primary keys. Timestamps
(created_at, updated_at) are managed automatically. The schema is
intentionally close to v1 since the data model was solid.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal

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
# Base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
  """Shared declarative base for all models."""


# ---------------------------------------------------------------------------
# Website
# ---------------------------------------------------------------------------


class Website(Base):
  """An online auction house or marketplace."""

  __tablename__ = "websites"

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
  base_url: Mapped[str] = mapped_column(String(500), nullable=False)

  default_buyer_premium_percent: Mapped[Decimal | None] = mapped_column(
    Numeric(8, 4), nullable=True,
  )
  default_buyer_premium_fixed: Mapped[Decimal | None] = mapped_column(
    Numeric(12, 2), nullable=True,
  )
  default_currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")

  is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
  notes: Mapped[str | None] = mapped_column(Text, nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  sellers: Mapped[list[Seller]] = relationship(
    back_populates="website", cascade="all, delete-orphan",
  )
  listings: Mapped[list[Listing]] = relationship(
    back_populates="website", cascade="all, delete-orphan",
  )


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
  display_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
  country: Mapped[str | None] = mapped_column(String(2), nullable=True)
  rating: Mapped[float | None] = mapped_column(Float, nullable=True)
  feedback_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
  member_since: Mapped[date | None] = mapped_column(Date, nullable=True)
  profile_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  website: Mapped[Website] = relationship(back_populates="sellers")
  listings: Mapped[list[Listing]] = relationship(back_populates="seller")


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


class Listing(Base):
  """A single item listed for sale on a tracked website.

  Central entity linking to price history, images, and attributes.
  """

  __tablename__ = "listings"
  __table_args__ = (
    UniqueConstraint("website_id", "external_id", name="uq_listing_website_external"),
    Index("ix_listing_status", "status"),
    Index("ix_listing_end_time", "end_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  website_id: Mapped[int] = mapped_column(ForeignKey("websites.id"), nullable=False)
  seller_id: Mapped[int | None] = mapped_column(ForeignKey("sellers.id"), nullable=True)
  external_id: Mapped[str] = mapped_column(String(300), nullable=False)
  url: Mapped[str] = mapped_column(String(2000), nullable=False)

  title: Mapped[str] = mapped_column(String(1000), nullable=False)
  description: Mapped[str | None] = mapped_column(Text, nullable=True)

  listing_type: Mapped[ListingType] = mapped_column(
    Enum(ListingType), nullable=False, default=ListingType.AUCTION,
  )
  condition: Mapped[ItemCondition] = mapped_column(
    Enum(ItemCondition), nullable=False, default=ItemCondition.UNKNOWN,
  )

  currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")

  starting_price: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  reserve_price: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  estimate_low: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  estimate_high: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  buy_now_price: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  current_price: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  final_price: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)

  current_price_eur: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  final_price_eur: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)

  buyer_premium_percent: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), nullable=True)
  buyer_premium_fixed: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

  shipping_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)
  shipping_from_country: Mapped[str | None] = mapped_column(String(2), nullable=True)
  ships_internationally: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

  start_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
  end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

  status: Mapped[ListingStatus] = mapped_column(
    Enum(ListingStatus), nullable=False, default=ListingStatus.UNKNOWN,
  )

  bid_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
  watcher_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
  view_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

  lot_number: Mapped[str | None] = mapped_column(String(100), nullable=True)
  auction_house_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
  sale_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
  sale_date: Mapped[date | None] = mapped_column(Date, nullable=True)

  first_seen_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  last_checked_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  is_fully_fetched: Mapped[bool] = mapped_column(
    Boolean, nullable=False, server_default="0", default=False,
  )

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )

  website: Mapped[Website] = relationship(back_populates="listings")
  seller: Mapped[Seller | None] = relationship(back_populates="listings")
  images: Mapped[list[ListingImage]] = relationship(
    back_populates="listing", cascade="all, delete-orphan",
    order_by="ListingImage.position",
  )
  bids: Mapped[list[BidEvent]] = relationship(
    back_populates="listing", cascade="all, delete-orphan",
    order_by="BidEvent.amount",
  )
  price_snapshots: Mapped[list[PriceSnapshot]] = relationship(
    back_populates="listing", cascade="all, delete-orphan",
    order_by="PriceSnapshot.snapshot_time",
  )
  attributes: Mapped[list[ListingAttribute]] = relationship(
    back_populates="listing", cascade="all, delete-orphan",
  )

  @property
  def effective_buyer_premium_percent(self) -> Decimal | None:
    if self.buyer_premium_percent is not None:
      return self.buyer_premium_percent
    if self.website is not None:
      return self.website.default_buyer_premium_percent
    return None

  @property
  def effective_buyer_premium_fixed(self) -> Decimal | None:
    if self.buyer_premium_fixed is not None:
      return self.buyer_premium_fixed
    if self.website is not None:
      return self.website.default_buyer_premium_fixed
    return None

  @property
  def total_buyer_cost(self) -> Decimal | None:
    """Estimate total cost to buyer (price + fees + shipping)."""
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

  @property
  def is_terminal(self) -> bool:
    """Whether the listing has reached a final state."""
    return self.status in (
      ListingStatus.SOLD,
      ListingStatus.UNSOLD,
      ListingStatus.CANCELLED,
    )


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
  local_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
  position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

  width: Mapped[int | None] = mapped_column(Integer, nullable=True)
  height: Mapped[int | None] = mapped_column(Integer, nullable=True)
  file_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)

  downloaded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  listing: Mapped[Listing] = relationship(back_populates="images")


# ---------------------------------------------------------------------------
# BidEvent
# ---------------------------------------------------------------------------


class BidEvent(Base):
  """A single bid placed on a listing."""

  __tablename__ = "bid_events"
  __table_args__ = (
    Index("ix_bid_listing_time", "listing_id", "bid_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
  currency: Mapped[str] = mapped_column(String(3), nullable=False)
  amount_eur: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
  bid_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)

  bidder_username: Mapped[str | None] = mapped_column(String(300), nullable=True)
  bidder_country: Mapped[str | None] = mapped_column(String(2), nullable=True)
  is_automatic: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
  is_winning: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

  recorded_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  listing: Mapped[Listing] = relationship(back_populates="bids")


# ---------------------------------------------------------------------------
# PriceSnapshot
# ---------------------------------------------------------------------------


class PriceSnapshot(Base):
  """A periodic snapshot of a listing's price and engagement counters.

  Used for time-series analysis even when individual bids are not
  visible (some websites only expose the current price).
  """

  __tablename__ = "price_snapshots"
  __table_args__ = (
    Index("ix_snapshot_listing_time", "listing_id", "snapshot_time"),
  )

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
  listing_id: Mapped[int] = mapped_column(ForeignKey("listings.id"), nullable=False)

  price: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
  currency: Mapped[str] = mapped_column(String(3), nullable=False)
  price_eur: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)

  bid_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
  watcher_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
  view_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

  snapshot_time: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )

  listing: Mapped[Listing] = relationship(back_populates="price_snapshots")


# ---------------------------------------------------------------------------
# ListingAttribute
# ---------------------------------------------------------------------------


class ListingAttribute(Base):
  """Free-form key/value attribute for domain-specific metadata
  (e.g. pen brand, nib size, filling system).
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

  listing: Mapped[Listing] = relationship(back_populates="attributes")


# ---------------------------------------------------------------------------
# SearchQuery
# ---------------------------------------------------------------------------


class SearchQuery(Base):
  """A saved search that the tracker runs periodically to discover new
  listings across all enabled websites.
  """

  __tablename__ = "search_queries"

  id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

  name: Mapped[str] = mapped_column(String(300), nullable=False, unique=True)
  query_text: Mapped[str] = mapped_column(String(1000), nullable=False)
  category: Mapped[str | None] = mapped_column(String(300), nullable=True)
  filters_json: Mapped[str | None] = mapped_column(Text, nullable=True)

  is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
  last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
  result_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

  created_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(),
  )
  updated_at: Mapped[datetime] = mapped_column(
    DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
  )
