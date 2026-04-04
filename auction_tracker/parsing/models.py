"""Pydantic models for scraped data.

These models are the contract between parsers and the rest of the
system. Parsers return these validated objects; the orchestrator
converts them to database models. This separation ensures that
malformed scraper output is caught early with clear error messages.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class ScrapedSeller(BaseModel):
  """Seller information as extracted from a listing page."""
  external_id: str
  username: str
  display_name: str | None = None
  country: str | None = None
  rating: float | None = None
  feedback_count: int | None = None
  member_since: date | None = None
  profile_url: str | None = None


class ScrapedBid(BaseModel):
  """A single bid extracted from bid history."""
  amount: Decimal
  currency: str = "EUR"
  bid_time: datetime
  bidder_username: str | None = None
  bidder_country: str | None = None
  is_automatic: bool = False


class ScrapedSearchResult(BaseModel):
  """A single result from a search results page.

  Contains just enough information to identify and discover the
  listing. Full details come from a separate listing fetch.
  """
  external_id: str
  url: str
  title: str
  current_price: Decimal | None = None
  currency: str = "EUR"
  listing_type: str | None = None
  end_time: datetime | None = None
  image_url: str | None = None
  bid_count: int | None = None


class ScrapedListing(BaseModel):
  """Full listing details as extracted from a detail page.

  This is the primary output of a parser's parse_listing method.
  Every field is optional except the identifiers so that parsers
  only fill in what their website actually provides.
  """
  external_id: str
  url: str
  title: str

  description: str | None = None
  listing_type: str | None = None
  condition: str | None = None

  currency: str = "EUR"
  starting_price: Decimal | None = None
  reserve_price: Decimal | None = None
  estimate_low: Decimal | None = None
  estimate_high: Decimal | None = None
  buy_now_price: Decimal | None = None
  current_price: Decimal | None = None
  final_price: Decimal | None = None

  buyer_premium_percent: Decimal | None = None
  buyer_premium_fixed: Decimal | None = None

  shipping_cost: Decimal | None = None
  shipping_from_country: str | None = None
  ships_internationally: bool | None = None

  start_time: datetime | None = None
  end_time: datetime | None = None

  status: str | None = None

  bid_count: int | None = None
  watcher_count: int | None = None
  view_count: int | None = None

  lot_number: str | None = None
  auction_house_name: str | None = None
  sale_name: str | None = None
  sale_date: date | None = None

  image_urls: list[str] = Field(default_factory=list)
  bids: list[ScrapedBid] = Field(default_factory=list)
  seller: ScrapedSeller | None = None

  attributes: dict[str, str] = Field(default_factory=dict)
