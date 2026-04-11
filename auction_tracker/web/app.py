"""Flask web application for visualizing the AuctionTracker database.

Provides a read-only HTML frontend for browsing listings, sellers,
price history, and images stored by the tracker.
"""

from __future__ import annotations

import contextlib
import json
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from flask import (
  Flask,
  abort,
  redirect,
  render_template,
  request,
  send_from_directory,
  url_for,
)
from markupsafe import Markup
from sqlalchemy import delete, desc, func, select
from sqlalchemy.orm import joinedload

from auction_tracker.config import AppConfig, load_config
from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.models import (
  BidEvent,
  Listing,
  ListingAttribute,
  ListingImage,
  ListingStatus,
  ListingType,
  PipelineEvent,
  SearchQuery,
  Seller,
  Website,
)


class _DecimalDatetimeEncoder(json.JSONEncoder):
  """JSON encoder that handles Decimal and datetime objects."""

  def default(self, obj):
    if isinstance(obj, Decimal):
      return float(obj)
    if isinstance(obj, datetime):
      return obj.isoformat()
    return super().default(obj)


def create_app(config: AppConfig | None = None, config_path: Path | None = None) -> Flask:
  """Flask application factory.

  Accepts either a pre-built AppConfig or a path to a YAML config file.
  If neither is given, loads config.yaml from the current directory
  (or defaults).
  """
  if config is None:
    config = load_config(config_path)

  database = DatabaseEngine(config.database.path)
  database.initialize()

  images_directory = str(config.classifier.images_directory)
  display_tz = ZoneInfo(config.display_timezone)
  display_currency = config.display_currency

  app = Flask(
    __name__,
    template_folder=str(Path(__file__).parent / "templates"),
  )
  app.config["IMAGES_DIR"] = images_directory
  app.config["MANUAL_CANCEL_FILE"] = str(
    config.database.path.parent / "manual_cancel_listings.txt",
  )

  @app.context_processor
  def inject_display_currency():
    return {"display_currency": display_currency}

  # ------------------------------------------------------------------
  # Template filters
  # ------------------------------------------------------------------

  @app.template_filter("format_price")
  def filter_format_price(value, currency=""):
    if value is None:
      return "\u2013"
    formatted = f"{float(value):,.2f}"
    if currency:
      formatted += f"\u00a0{currency}"
    return formatted

  @app.template_filter("format_datetime")
  def filter_format_datetime(value):
    if value is None:
      return "\u2013"
    # Datetimes are stored as UTC-naive; attach UTC then convert to the
    # configured display timezone before formatting.
    if value.tzinfo is None:
      value = value.replace(tzinfo=UTC)
    return value.astimezone(display_tz).strftime("%Y-%m-%d %H:%M")

  @app.template_filter("format_date")
  def filter_format_date(value):
    if value is None:
      return "\u2013"
    # Plain date objects have no tzinfo; just format directly.
    from datetime import date as _date, datetime as _datetime
    if isinstance(value, _datetime):
      if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
      return value.astimezone(display_tz).strftime("%Y-%m-%d")
    if isinstance(value, _date):
      return value.strftime("%Y-%m-%d")
    return str(value)

  @app.template_filter("tojson_safe")
  def filter_tojson_safe(value):
    """Serialize to JSON for embedding in <script> tags."""
    return Markup(json.dumps(value, cls=_DecimalDatetimeEncoder))

  # ------------------------------------------------------------------
  # Routes — Dashboard
  # ------------------------------------------------------------------

  @app.route("/")
  def dashboard():
    with database.session() as session:
      total_listings = session.execute(
        select(func.count(Listing.id))
      ).scalar() or 0

      status_counts = dict(
        session.execute(
          select(Listing.status, func.count(Listing.id))
          .group_by(Listing.status)
        ).all()
      )

      website_stats = session.execute(
        select(Website.name, func.count(Listing.id))
        .outerjoin(Listing)
        .group_by(Website.id)
        .order_by(desc(func.count(Listing.id)))
      ).all()

      total_sellers = session.execute(
        select(func.count(Seller.id))
      ).scalar() or 0

      total_websites = session.execute(
        select(func.count(Website.id))
      ).scalar() or 0

      total_images = session.execute(
        select(func.count(ListingImage.id))
      ).scalar() or 0

      price_stats = session.execute(
        select(
          func.avg(Listing.current_price_eur),
          func.min(Listing.current_price_eur),
          func.max(Listing.current_price_eur),
        ).where(Listing.status == ListingStatus.ACTIVE)
         .where(Listing.current_price_eur.isnot(None))
      ).one()

      sold_price_stats = session.execute(
        select(
          func.avg(Listing.final_price_eur),
          func.min(Listing.final_price_eur),
          func.max(Listing.final_price_eur),
          func.count(Listing.id),
        ).where(Listing.status == ListingStatus.SOLD)
         .where(Listing.final_price_eur.isnot(None))
         .where(Listing.final_price.isnot(None))
      ).one()

      recent_listings = session.execute(
        select(Listing)
        .options(joinedload(Listing.website))
        .order_by(desc(Listing.created_at))
        .limit(20)
      ).scalars().unique().all()

      return render_template(
        "dashboard.html",
        total_listings=total_listings,
        status_counts=status_counts,
        website_stats=website_stats,
        total_sellers=total_sellers,
        total_websites=total_websites,
        total_images=total_images,
        price_avg=price_stats[0],
        price_min=price_stats[1],
        price_max=price_stats[2],
        sold_price_avg=sold_price_stats[0],
        sold_price_min=sold_price_stats[1],
        sold_price_max=sold_price_stats[2],
        sold_count=sold_price_stats[3],
        recent_listings=recent_listings,
        ListingStatus=ListingStatus,
      )

  # ------------------------------------------------------------------
  # Routes — Listings
  # ------------------------------------------------------------------

  @app.route("/listings")
  def listings():
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    search_query = request.args.get("q", "")
    type_filter = request.args.get("type", "")
    source_filter = request.args.get("source", "")
    sort_by = request.args.get("sort", "created_at")
    sort_dir = request.args.get("dir", "desc")

    status_filters = request.args.getlist("status")
    website_filters = request.args.getlist("website")

    is_form_submission = request.args.get("_filtered", "") == "1"
    if not status_filters and not is_form_submission:
      status_filters = [
        status.value
        for status in ListingStatus
        if status not in (ListingStatus.CANCELLED, ListingStatus.UNKNOWN)
      ]

    allowed_sort_columns = {
      "created_at", "title", "current_price", "final_price",
      "bid_count", "end_time", "start_time", "status",
    }
    if sort_by not in allowed_sort_columns:
      sort_by = "created_at"

    price_sort_mapping = {
      "current_price": "current_price_eur",
      "final_price": "final_price_eur",
    }
    db_sort_column = price_sort_mapping.get(sort_by, sort_by)

    with database.session() as session:
      statement = select(Listing).options(
        joinedload(Listing.website),
        joinedload(Listing.images),
      )
      count_statement = select(func.count(Listing.id))

      if search_query:
        filter_clause = Listing.title.ilike(f"%{search_query}%")
        statement = statement.where(filter_clause)
        count_statement = count_statement.where(filter_clause)

      if status_filters:
        valid_statuses = []
        for status_value in status_filters:
          with contextlib.suppress(ValueError):
            valid_statuses.append(ListingStatus(status_value))
        if valid_statuses:
          statement = statement.where(Listing.status.in_(valid_statuses))
          count_statement = count_statement.where(Listing.status.in_(valid_statuses))

      if website_filters:
        valid_website_ids = []
        for website_value in website_filters:
          with contextlib.suppress(ValueError):
            valid_website_ids.append(int(website_value))
        if valid_website_ids:
          statement = statement.where(Listing.website_id.in_(valid_website_ids))
          count_statement = count_statement.where(Listing.website_id.in_(valid_website_ids))

      if type_filter:
        try:
          type_enum = ListingType(type_filter)
          statement = statement.where(Listing.listing_type == type_enum)
          count_statement = count_statement.where(Listing.listing_type == type_enum)
        except ValueError:
          pass

      if source_filter:
        source_attr_name = f"source_search:{source_filter}"
        source_subquery = (
          select(ListingAttribute.listing_id)
          .where(ListingAttribute.attribute_name == source_attr_name)
        )
        statement = statement.where(Listing.id.in_(source_subquery))
        count_statement = count_statement.where(Listing.id.in_(source_subquery))

      sort_column = getattr(Listing, db_sort_column, Listing.created_at)
      if sort_dir == "asc":
        statement = statement.order_by(sort_column.asc())
      else:
        statement = statement.order_by(sort_column.desc())

      total_count = session.execute(count_statement).scalar() or 0
      total_pages = max(1, (total_count + per_page - 1) // per_page)
      page = max(1, min(page, total_pages))

      offset = (page - 1) * per_page
      statement = statement.offset(offset).limit(per_page)

      results = session.execute(statement).scalars().unique().all()

      all_websites = session.execute(
        select(Website).order_by(Website.name)
      ).scalars().all()

      # Build price history chart when filters are active.
      price_history_data = None
      has_filters = (
        search_query or status_filters or website_filters
        or type_filter or source_filter
      )
      if has_filters:
        price_history_data = _build_price_history(
          session, search_query, status_filters, website_filters,
          type_filter, source_filter,
        )

      filter_params = []
      if search_query:
        filter_params.append(("q", search_query))
      for status_value in status_filters:
        filter_params.append(("status", status_value))
      for website_value in website_filters:
        filter_params.append(("website", website_value))
      if type_filter:
        filter_params.append(("type", type_filter))
      if source_filter:
        filter_params.append(("source", source_filter))
      filter_params.append(("_filtered", "1"))
      filter_query_string = urlencode(filter_params)

      return render_template(
        "listings.html",
        listings=results,
        total_count=total_count,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        search_query=search_query,
        status_filters=status_filters,
        website_filters=website_filters,
        type_filter=type_filter,
        source_filter=source_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filter_query_string=filter_query_string,
        websites=all_websites,
        ListingStatus=ListingStatus,
        ListingType=ListingType,
        price_history_data=price_history_data,
        chart_defaults={
          "time_start": config.display.chart_time_start,
          "price_min": config.display.chart_price_min,
          "price_max": config.display.chart_price_max,
        },
      )

  # ------------------------------------------------------------------
  # Routes — Listing detail
  # ------------------------------------------------------------------

  @app.route("/listings/<int:listing_id>")
  def listing_detail(listing_id):
    with database.session() as session:
      listing = session.execute(
        select(Listing)
        .options(
          joinedload(Listing.website),
          joinedload(Listing.seller),
          joinedload(Listing.images),
          joinedload(Listing.bids),
          joinedload(Listing.price_snapshots),
          joinedload(Listing.attributes),
        )
        .where(Listing.id == listing_id)
      ).scalars().unique().first()

      if listing is None:
        abort(404)

      is_non_eur = listing.currency != display_currency
      bid_chart_data = []
      for bid in listing.bids:
        display_time = bid.bid_time
        if bid.is_automatic:
          display_time = bid.bid_time + timedelta(seconds=1)
        entry = {
          "time": display_time.isoformat(),
          "amount": float(bid.amount),
          "bidder": bid.bidder_username or "Anonymous",
          "country": bid.bidder_country or "",
          "automatic": bid.is_automatic,
        }
        if is_non_eur and bid.amount_eur is not None:
          entry["amount_eur"] = float(bid.amount_eur)
        bid_chart_data.append(entry)

      snapshot_chart_data = []
      for snapshot in listing.price_snapshots:
        entry = {
          "time": snapshot.snapshot_time.isoformat(),
          "price": float(snapshot.price),
          "bid_count": snapshot.bid_count,
        }
        if is_non_eur and snapshot.price_eur is not None:
          entry["price_eur"] = float(snapshot.price_eur)
        snapshot_chart_data.append(entry)

      reference_lines = {}
      if listing.reserve_price is not None:
        reference_lines["reserve_price"] = float(listing.reserve_price)
      if listing.estimate_low is not None:
        reference_lines["estimate_low"] = float(listing.estimate_low)
      if listing.estimate_high is not None:
        reference_lines["estimate_high"] = float(listing.estimate_high)

      source_prefix = "source_search:"
      source_searches = []
      attributes_list = []
      for attr in listing.attributes:
        if attr.attribute_name.startswith(source_prefix):
          source_searches.append(attr.attribute_name[len(source_prefix):])
        else:
          attributes_list.append({
            "name": attr.attribute_name,
            "value": attr.attribute_value,
          })

      return render_template(
        "listing_detail.html",
        listing=listing,
        attributes=attributes_list,
        source_searches=source_searches,
        bid_chart_data=bid_chart_data,
        snapshot_chart_data=snapshot_chart_data,
        reference_lines=reference_lines,
      )

  @app.route("/listings/<int:listing_id>/mark-cancel", methods=["POST"])
  def listing_mark_cancel(listing_id):
    """Queue a listing for cancellation via a sidecar file."""
    cancel_file = Path(app.config["MANUAL_CANCEL_FILE"])
    with database.session() as session:
      listing = session.execute(
        select(Listing).where(Listing.id == listing_id),
      ).scalars().first()
      if listing is None:
        abort(404)
      if listing.status == ListingStatus.CANCELLED:
        return redirect(
          url_for("listing_detail", listing_id=listing_id)
          + "?mark_cancel=already_cancelled",
        )
    try:
      cancel_file.parent.mkdir(parents=True, exist_ok=True)
      with open(cancel_file, "a", encoding="utf-8") as handle:
        handle.write(f"{listing_id}\n")
    except OSError:
      abort(500, "Could not write to manual cancel file.")
    return redirect(
      url_for("listing_detail", listing_id=listing_id) + "?mark_cancel=ok",
    )

  @app.route("/listings/<int:listing_id>/mark-confirm", methods=["POST"])
  def listing_mark_confirm(listing_id):
    """Restore a cancelled listing and mark it as a confirmed writing instrument."""
    cancel_file = Path(app.config["MANUAL_CANCEL_FILE"])

    with database.session() as session:
      listing = session.execute(
        select(Listing).where(Listing.id == listing_id),
      ).scalars().first()
      if listing is None:
        abort(404)

      session.execute(
        delete(ListingAttribute)
        .where(ListingAttribute.listing_id == listing_id)
        .where(ListingAttribute.attribute_name.in_([
          "rejected_by_classifier",
          "manually_cancelled",
        ]))
      )

      existing_safelist = session.execute(
        select(ListingAttribute)
        .where(ListingAttribute.listing_id == listing_id)
        .where(ListingAttribute.attribute_name == "confirmed_writing_instrument")
      ).scalars().first()
      if not existing_safelist:
        session.add(
          ListingAttribute(
            listing_id=listing_id,
            attribute_name="confirmed_writing_instrument",
            attribute_value="true",
          )
        )

      # Reset to UNKNOWN + not fully fetched so the discovery loop
      # re-scrapes it and determines the correct status.
      listing.status = ListingStatus.UNKNOWN
      listing.is_fully_fetched = False
      listing.last_checked_at = datetime.fromtimestamp(0, UTC)

      session.commit()

    # Remove from manual cancel file if present.
    if cancel_file.exists():
      try:
        lines = cancel_file.read_text(encoding="utf-8").splitlines()
        if str(listing_id) in lines:
          new_lines = [line for line in lines if line.strip() != str(listing_id)]
          cancel_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
      except OSError:
        pass

    return redirect(
      url_for("listing_detail", listing_id=listing_id) + "?mark_confirm=ok",
    )

  # ------------------------------------------------------------------
  # Routes — Sellers
  # ------------------------------------------------------------------

  @app.route("/sellers")
  def sellers():
    from collections import defaultdict

    with database.session() as session:
      all_sellers = session.execute(
        select(Seller).options(joinedload(Seller.website))
      ).scalars().unique().all()

      # Aggregate listing stats per seller without loading full ORM objects.
      listing_rows = session.execute(
        select(Listing.seller_id, Listing.status, Listing.final_price_eur)
        .where(Listing.seller_id.isnot(None))
      ).all()

      stats_by_seller: dict[int, dict] = defaultdict(
        lambda: {"total": 0, "sold": 0, "active": 0, "volume_eur": 0.0}
      )
      for row in listing_rows:
        entry = stats_by_seller[row.seller_id]
        entry["total"] += 1
        if row.status == ListingStatus.SOLD:
          entry["sold"] += 1
          if row.final_price_eur is not None:
            entry["volume_eur"] += float(row.final_price_eur)
        elif row.status == ListingStatus.ACTIVE:
          entry["active"] += 1

      all_website_names = sorted({s.website.name for s in all_sellers if s.website})
      all_countries = sorted({s.country for s in all_sellers if s.country})

      seller_data = [
        {
          "id": s.id,
          "display_name": s.display_name or s.username,
          "username": s.username,
          "website": s.website.name if s.website else "",
          "country": s.country or "",
          "rating": float(s.rating) if s.rating is not None else None,
          "feedback_count": s.feedback_count or 0,
          "listing_count": stats_by_seller[s.id]["total"],
          "sold_count": stats_by_seller[s.id]["sold"],
          "active_count": stats_by_seller[s.id]["active"],
          "volume_eur": round(stats_by_seller[s.id]["volume_eur"], 2),
        }
        for s in all_sellers
      ]
      seller_data.sort(key=lambda x: x["listing_count"], reverse=True)

      return render_template(
        "sellers.html",
        seller_data=seller_data,
        all_website_names=all_website_names,
        all_countries=all_countries,
      )

  @app.route("/sellers/<int:seller_id>")
  def seller_detail(seller_id):
    with database.session() as session:
      seller = session.execute(
        select(Seller)
        .options(joinedload(Seller.website))
        .where(Seller.id == seller_id)
      ).scalars().first()
      if seller is None:
        abort(404)

      seller_listings = session.execute(
        select(Listing)
        .options(joinedload(Listing.website))
        .where(Listing.seller_id == seller_id)
        .order_by(desc(Listing.created_at))
      ).scalars().unique().all()

      return render_template(
        "seller_detail.html",
        seller=seller,
        listings=seller_listings,
      )

  # ------------------------------------------------------------------
  # Routes — Bidders
  # ------------------------------------------------------------------

  @app.route("/bidders")
  def bidders():
    with database.session() as session:
      max_bid_sub = (
        select(
          BidEvent.listing_id,
          func.max(BidEvent.amount).label("max_amount"),
        )
        .group_by(BidEvent.listing_id)
        .subquery()
      )

      winning_bids = (
        select(
          BidEvent.bidder_username,
          Listing.final_price_eur,
          Listing.final_price,
          Listing.id.label("listing_id"),
        )
        .join(max_bid_sub, (
          (BidEvent.listing_id == max_bid_sub.c.listing_id)
          & (BidEvent.amount == max_bid_sub.c.max_amount)
        ))
        .join(Listing, BidEvent.listing_id == Listing.id)
        .where(Listing.status == ListingStatus.SOLD)
        .where(BidEvent.bidder_username.isnot(None))
        .subquery()
      )

      wins_agg = (
        select(
          winning_bids.c.bidder_username,
          func.count(winning_bids.c.listing_id).label("won_count"),
          func.sum(winning_bids.c.final_price_eur).label("total_spent_eur"),
        )
        .group_by(winning_bids.c.bidder_username)
        .subquery()
      )

      bidder_stats = session.execute(
        select(
          BidEvent.bidder_username,
          BidEvent.bidder_country,
          func.count(func.distinct(BidEvent.listing_id)).label("listing_count"),
          wins_agg.c.won_count,
          wins_agg.c.total_spent_eur,
        )
        .outerjoin(wins_agg, BidEvent.bidder_username == wins_agg.c.bidder_username)
        .where(BidEvent.bidder_username.isnot(None))
        .group_by(BidEvent.bidder_username, BidEvent.bidder_country)
        .order_by(desc(func.count(func.distinct(BidEvent.listing_id))))
      ).all()

      # Build per-bidder website list.
      bidder_website_rows = session.execute(
        select(BidEvent.bidder_username, Website.name.label("website_name"))
        .join(Listing, BidEvent.listing_id == Listing.id)
        .join(Website, Listing.website_id == Website.id)
        .where(BidEvent.bidder_username.isnot(None))
        .distinct()
      ).all()

      websites_by_bidder: dict[str, list[str]] = {}
      all_website_name_set: set[str] = set()
      for row in bidder_website_rows:
        websites_by_bidder.setdefault(row.bidder_username, []).append(row.website_name)
        all_website_name_set.add(row.website_name)

      all_website_names = sorted(all_website_name_set)
      all_countries = sorted({
        row.bidder_country for row in bidder_stats if row.bidder_country
      })

      bidder_data = [
        {
          "username": row.bidder_username,
          "country": row.bidder_country or "",
          "websites": sorted(websites_by_bidder.get(row.bidder_username, [])),
          "listing_count": row.listing_count,
          "won_count": row.won_count or 0,
          "win_rate": (
            round(100.0 * (row.won_count or 0) / row.listing_count, 1)
            if row.listing_count else 0.0
          ),
          "total_spent_eur": (
            round(float(row.total_spent_eur), 2) if row.total_spent_eur else 0.0
          ),
        }
        for row in bidder_stats
      ]

      return render_template(
        "bidders.html",
        bidder_data=bidder_data,
        all_website_names=all_website_names,
        all_countries=all_countries,
      )

  @app.route("/bidders/<username>")
  def bidder_detail(username):
    with database.session() as session:
      latest_bid = session.execute(
        select(BidEvent)
        .where(BidEvent.bidder_username == username)
        .order_by(desc(BidEvent.bid_time))
        .limit(1)
      ).scalars().first()

      if latest_bid is None:
        abort(404)

      bidder_country = latest_bid.bidder_country

      stats = session.execute(
        select(
          func.count(func.distinct(BidEvent.listing_id)).label("listing_count"),
          func.min(BidEvent.bid_time).label("first_bid_time"),
          func.max(BidEvent.bid_time).label("last_bid_time"),
        )
        .where(BidEvent.bidder_username == username)
      ).one()

      max_bid_sub = (
        select(
          BidEvent.listing_id,
          func.max(BidEvent.amount).label("max_amount"),
        )
        .group_by(BidEvent.listing_id)
        .subquery()
      )

      won_listings = session.execute(
        select(Listing)
        .join(BidEvent, BidEvent.listing_id == Listing.id)
        .join(max_bid_sub, (
          (BidEvent.listing_id == max_bid_sub.c.listing_id)
          & (BidEvent.amount == max_bid_sub.c.max_amount)
        ))
        .where(Listing.status == ListingStatus.SOLD)
        .where(BidEvent.bidder_username == username)
        .options(joinedload(Listing.website))
        .order_by(desc(Listing.end_time))
      ).scalars().unique().all()

      total_spent_eur = sum(
        float(listing.final_price_eur)
        for listing in won_listings
        if listing.final_price_eur is not None
      )
      avg_winning_price = (
        total_spent_eur / len(won_listings) if won_listings else 0.0
      )

      spending_timeline = []
      cumulative = 0.0
      for listing in sorted(won_listings, key=lambda listing: listing.end_time or listing.created_at):
        price = float(listing.final_price_eur) if listing.final_price_eur else 0.0
        cumulative += price
        spending_timeline.append({
          "time": (listing.end_time or listing.created_at).isoformat(),
          "total": round(cumulative, 2),
          "item": listing.title[:60] if listing.title else "?",
          "price": round(price, 2),
        })

      all_bids = session.execute(
        select(BidEvent)
        .options(joinedload(BidEvent.listing).joinedload(Listing.website))
        .where(BidEvent.bidder_username == username)
        .order_by(desc(BidEvent.bid_time))
        .limit(500)
      ).scalars().unique().all()

      return render_template(
        "bidder_detail.html",
        username=username,
        bidder_country=bidder_country,
        stats=stats,
        won_listings=won_listings,
        total_spent_eur=total_spent_eur,
        avg_winning_price=avg_winning_price,
        spending_timeline=spending_timeline,
        all_bids=all_bids,
      )

  # ------------------------------------------------------------------
  # Routes — Searches
  # ------------------------------------------------------------------

  @app.route("/searches")
  def searches():
    source_prefix = "source_search:"

    with database.session() as session:
      saved_searches = session.execute(
        select(SearchQuery).order_by(SearchQuery.name)
      ).scalars().unique().all()

      saved_by_query_text: dict[str, list[dict]] = {}
      for search in saved_searches:
        saved_dict = {
          "name": search.name,
          "category": search.category,
          "is_active": search.is_active,
        }
        saved_by_query_text.setdefault(search.query_text, []).append(saved_dict)

      raw_stats = session.execute(
        select(
          ListingAttribute.attribute_name,
          Website.name.label("website_name"),
          Listing.status,
          func.count(Listing.id).label("listing_count"),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .join(Website, Website.id == Listing.website_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .group_by(
          ListingAttribute.attribute_name,
          Website.name,
          Listing.status,
        )
      ).all()

      verified_stats = session.execute(
        select(
          ListingAttribute.attribute_name,
          Website.name.label("website_name"),
          func.count(func.distinct(Listing.id)).label("verified_count"),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .join(Website, Website.id == Listing.website_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .where(
          Listing.id.in_(
            select(ListingAttribute.listing_id).where(
              ListingAttribute.attribute_name.like("classifier_%")
            )
          )
        )
        .group_by(ListingAttribute.attribute_name, Website.name)
      ).all()

      verified_lookup = {}
      for row in verified_stats:
        verified_lookup[(row.attribute_name, row.website_name)] = row.verified_count

      search_data: dict[str, dict] = {}
      for row in raw_stats:
        query_text = row.attribute_name[len(source_prefix):]
        if query_text not in search_data:
          search_data[query_text] = {
            "query_text": query_text,
            "saved_searches": saved_by_query_text.get(query_text, []),
            "websites": {},
            "total": 0,
            "by_status": {},
          }
        entry = search_data[query_text]
        entry["total"] += row.listing_count

        status_label = row.status.value if row.status else "unknown"
        entry["by_status"][status_label] = (
          entry["by_status"].get(status_label, 0) + row.listing_count
        )

        website_name = row.website_name
        if website_name not in entry["websites"]:
          entry["websites"][website_name] = {
            "total": 0,
            "by_status": {},
            "verified": 0,
          }
        website_entry = entry["websites"][website_name]
        website_entry["total"] += row.listing_count
        website_entry["by_status"][status_label] = (
          website_entry["by_status"].get(status_label, 0) + row.listing_count
        )
        website_entry["verified"] = verified_lookup.get(
          (row.attribute_name, website_name), 0,
        )

      rejected_stats = session.execute(
        select(
          ListingAttribute.attribute_name,
          Website.name.label("website_name"),
          func.count(func.distinct(Listing.id)).label("rejected_count"),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .join(Website, Website.id == Listing.website_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .where(Listing.status == ListingStatus.CANCELLED)
        .where(
          Listing.id.in_(
            select(ListingAttribute.listing_id).where(
              ListingAttribute.attribute_name.like("classifier_%")
            )
          )
        )
        .group_by(ListingAttribute.attribute_name, Website.name)
      ).all()

      rejected_lookup = {}
      for row in rejected_stats:
        rejected_lookup[(row.attribute_name, row.website_name)] = row.rejected_count

      for entry in search_data.values():
        entry["verified"] = sum(
          wd["verified"] for wd in entry["websites"].values()
        )
        entry["rejected"] = 0
        for website_name, website_data in entry["websites"].items():
          attr_name = f"{source_prefix}{entry['query_text']}"
          website_rejected = rejected_lookup.get((attr_name, website_name), 0)
          website_data["rejected"] = website_rejected
          website_data["accepted"] = website_data["verified"] - website_rejected
          website_data["acceptance_rate"] = (
            round(100.0 * website_data["accepted"] / website_data["verified"], 1)
            if website_data["verified"] > 0 else 0.0
          )
          website_data["verification_coverage"] = (
            round(100.0 * website_data["verified"] / website_data["total"], 1)
            if website_data["total"] > 0 else 0.0
          )
          entry["rejected"] += website_rejected

        entry["accepted"] = entry["verified"] - entry["rejected"]
        entry["acceptance_rate"] = (
          round(100.0 * entry["accepted"] / entry["verified"], 1)
          if entry["verified"] > 0 else 0.0
        )
        entry["verification_coverage"] = (
          round(100.0 * entry["verified"] / entry["total"], 1)
          if entry["total"] > 0 else 0.0
        )

      sorted_searches = sorted(
        search_data.values(),
        key=lambda entry: entry["total"],
        reverse=True,
      )

      saved_search_list = []
      for search in saved_searches:
        stats = search_data.get(search.query_text, {})
        saved_search_list.append({
          "id": search.id,
          "name": search.name,
          "query_text": search.query_text,
          "category": search.category,
          "is_active": search.is_active,
          "created_at": search.created_at,
          "total": stats.get("total", 0),
          "accepted": stats.get("accepted", 0),
          "verification_coverage": stats.get("verification_coverage", 0.0),
          "acceptance_rate": stats.get("acceptance_rate", 0.0),
        })

    return render_template(
      "searches.html",
      search_stats=sorted_searches,
      saved_searches=saved_search_list,
      ListingStatus=ListingStatus,
    )

  # ------------------------------------------------------------------
  # Routes — Operations dashboard
  # ------------------------------------------------------------------

  def _make_bucket_fn(granularity: str):
    """Return (bucket_fn, step) for the given granularity string."""
    if granularity == "10m":
      def bucket_fn(dt: datetime) -> str:
        return dt.replace(
          minute=(dt.minute // 10) * 10, second=0, microsecond=0,
        ).strftime("%Y-%m-%d %H:%M")
      return bucket_fn, timedelta(minutes=10)
    if granularity == "6h":
      def bucket_fn(dt: datetime) -> str:
        return dt.replace(
          hour=(dt.hour // 6) * 6, minute=0, second=0, microsecond=0,
        ).strftime("%Y-%m-%d %H:00")
      return bucket_fn, timedelta(hours=6)
    if granularity == "1d":
      def bucket_fn(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")
      return bucket_fn, timedelta(days=1)
    if granularity == "1w":
      def bucket_fn(dt: datetime) -> str:
        # Monday of the ISO week that contains dt.
        monday = dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
          days=dt.weekday()
        )
        return monday.strftime("%Y-%m-%d")
      return bucket_fn, timedelta(weeks=1)
    # Default: 1h
    def bucket_fn(dt: datetime) -> str:
      return dt.strftime("%Y-%m-%d %H:00")
    return bucket_fn, timedelta(hours=1)

  def _collect_operations_data(
    hours_back: int,
    granularity: str = "1h",
  ) -> dict:
    """Query the database and return all data needed for the operations page.

    Extracted into a helper so the HTML route and the JSON API route
    can share the same logic without duplication.
    """
    bucket_fn, bucket_step = _make_bucket_fn(granularity)

    now = datetime.now(UTC)
    since = now - timedelta(hours=hours_back)
    since_naive = since.replace(tzinfo=None)

    with database.session() as session:
      last_start = session.execute(
        select(PipelineEvent.timestamp)
        .where(PipelineEvent.event_type == "pipeline_start")
        .order_by(desc(PipelineEvent.timestamp))
        .limit(1)
      ).scalar()
      last_stop = session.execute(
        select(PipelineEvent.timestamp)
        .where(PipelineEvent.event_type == "pipeline_stop")
        .order_by(desc(PipelineEvent.timestamp))
        .limit(1)
      ).scalar()
      pipeline_running = (
        last_start is not None
        and (last_stop is None or last_start > last_stop)
      )
      pipeline_last_activity = session.execute(
        select(func.max(PipelineEvent.timestamp))
      ).scalar()

      events_in_window = session.execute(
        select(PipelineEvent)
        .where(PipelineEvent.timestamp >= since_naive)
        .order_by(desc(PipelineEvent.timestamp))
      ).scalars().all()

      event_type_counts: dict[str, int] = {}
      for event in events_in_window:
        event_type_counts[event.event_type] = event_type_counts.get(event.event_type, 0) + 1

      request_types = {"search_run", "fetch_listing", "watch_check"}
      website_request_counts: dict[str, int] = {}
      for event in events_in_window:
        if event.event_type in request_types and event.website_name:
          website_request_counts[event.website_name] = (
            website_request_counts.get(event.website_name, 0) + 1
          )

      error_events = [event for event in events_in_window if event.event_type == "error"]
      error_by_source: dict[str, int] = {}
      error_by_website: dict[str, int] = {}
      for event in error_events:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        source = detail.get("source", "unknown")
        error_by_source[source] = error_by_source.get(source, 0) + 1
        if event.website_name:
          error_by_website[event.website_name] = (
            error_by_website.get(event.website_name, 0) + 1
          )

      search_events = [event for event in events_in_window if event.event_type == "search_run"]
      search_stats: dict[str, dict] = {}
      for event in search_events:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        website = event.website_name or "unknown"
        if website not in search_stats:
          search_stats[website] = {"runs": 0, "results": 0, "new": 0}
        search_stats[website]["runs"] += 1
        search_stats[website]["results"] += detail.get("results_found", 0)
        search_stats[website]["new"] += detail.get("new_listings", 0)

      watch_cycles = [event for event in events_in_window if event.event_type == "watch_cycle"]
      total_watch_checks = 0
      total_watch_updated = 0
      total_watch_completed = 0
      total_watch_extensions = 0
      for event in watch_cycles:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        total_watch_checks += detail.get("checks", 0)
        total_watch_updated += detail.get("updated", 0)
        total_watch_completed += detail.get("completed", 0)
        total_watch_extensions += detail.get("extensions", 0)

      fetch_batches = [event for event in events_in_window if event.event_type == "fetch_batch"]
      total_fetched = 0
      total_classified = 0
      total_rejected = 0
      for event in fetch_batches:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        total_fetched += detail.get("fetched", 0)
        total_classified += detail.get("classified", 0)
        total_rejected += detail.get("rejected", 0)

      pending_fetches = session.execute(
        select(func.count(Listing.id))
        .where(Listing.is_fully_fetched.is_(False))
        .where(Listing.status != ListingStatus.CANCELLED)
      ).scalar() or 0

      now_naive = now.replace(tzinfo=None)
      upcoming_listings = session.execute(
        select(Listing)
        .options(joinedload(Listing.website))
        .where(Listing.status == ListingStatus.ACTIVE)
        .where(Listing.end_time.isnot(None))
        .where(Listing.end_time > now_naive)
        .order_by(Listing.end_time.asc())
        .limit(50)
      ).scalars().unique().all()

      # Serialise upcoming auctions for the JSON API and template.
      display_tz = ZoneInfo(config.display_timezone)
      upcoming_auctions_json = []
      for listing in upcoming_listings:
        end_utc = listing.end_time.replace(tzinfo=UTC) if listing.end_time else None
        upcoming_auctions_json.append({
          "id": listing.id,
          "title": listing.title or "",
          "current_price": float(listing.current_price) if listing.current_price else None,
          "currency": listing.currency or "",
          "bid_count": listing.bid_count or 0,
          "website_name": listing.website.name if listing.website else "",
          "end_time_iso": end_utc.isoformat() if end_utc else None,
          "end_time_display": (
            end_utc.astimezone(display_tz).strftime("%d/%m %H:%M")
            if end_utc else ""
          ),
        })

      # Request type label mapping.
      request_type_map = {
        "search_run": "search",
        "fetch_listing": "fetch",
        "watch_check": "watch",
      }

      hourly_new_listings: dict[str, int] = {}
      hourly_completed: dict[str, int] = {}
      hourly_updated: dict[str, int] = {}
      hourly_errors: dict[str, int] = {}
      hourly_requests: dict[str, int] = {}
      # {type_label: {bucket: count}} for stacked request-type bar.
      hourly_requests_by_type: dict[str, dict[str, int]] = {
        label: {} for label in request_type_map.values()
      }
      # {website: {bucket: count}} for stacked error-by-website bar.
      hourly_errors_by_website: dict[str, dict[str, int]] = {}
      # {source: {bucket: count}} for stacked error-by-source bar.
      hourly_errors_by_source: dict[str, dict[str, int]] = {}

      # Idle / active accumulators per website per bucket.
      idle_by_website: dict[str, dict[str, float]] = {}
      active_by_website: dict[str, dict[str, float]] = {}
      # Per-bucket average fetch/watch queue depth from worker_utilization samples.
      queue_depth_accum: dict[str, dict[str, dict[str, float | int]]] = {}
      # Late watch checks per website per bucket.
      late_watch_threshold = config.scheduler.late_watch_threshold
      late_watch_by_website: dict[str, dict[str, int]] = {}

      for event in events_in_window:
        bucket = bucket_fn(event.timestamp)
        if event.event_type == "search_run":
          detail = json.loads(event.detail_json) if event.detail_json else {}
          hourly_new_listings[bucket] = (
            hourly_new_listings.get(bucket, 0) + detail.get("new_listings", 0)
          )
        if event.event_type == "watch_cycle":
          detail = json.loads(event.detail_json) if event.detail_json else {}
          hourly_completed[bucket] = (
            hourly_completed.get(bucket, 0) + detail.get("completed", 0)
          )
          hourly_updated[bucket] = (
            hourly_updated.get(bucket, 0) + detail.get("updated", 0)
          )
        if event.event_type == "watch_check":
          detail = json.loads(event.detail_json) if event.detail_json else {}
          delay = detail.get("delay_seconds", 0.0)
          if delay >= late_watch_threshold and event.website_name:
            website = event.website_name
            if website not in late_watch_by_website:
              late_watch_by_website[website] = {}
            late_watch_by_website[website][bucket] = (
              late_watch_by_website[website].get(bucket, 0) + 1
            )
        if event.event_type == "worker_utilization" and event.website_name:
          detail = json.loads(event.detail_json) if event.detail_json else {}
          website = event.website_name
          if website not in idle_by_website:
            idle_by_website[website] = {}
            active_by_website[website] = {}
          idle_by_website[website][bucket] = (
            idle_by_website[website].get(bucket, 0.0)
            + detail.get("idle_seconds", 0.0)
          )
          active_by_website[website][bucket] = (
            active_by_website[website].get(bucket, 0.0)
            + detail.get("active_seconds", 0.0)
          )
          if (
            "fetch_queue" in detail
            or "watch_queue" in detail
            or "search_queue" in detail
          ):
            if website not in queue_depth_accum:
              queue_depth_accum[website] = {}
            if bucket not in queue_depth_accum[website]:
              queue_depth_accum[website][bucket] = {
                "sf": 0, "nf": 0, "sw": 0, "nw": 0, "ss": 0, "ns": 0,
              }
            accum = queue_depth_accum[website][bucket]
            if "fetch_queue" in detail:
              accum["sf"] += int(detail["fetch_queue"])
              accum["nf"] += 1
            if "watch_queue" in detail:
              accum["sw"] += int(detail["watch_queue"])
              accum["nw"] += 1
            if "search_queue" in detail:
              accum["ss"] += int(detail["search_queue"])
              accum["ns"] += 1
        if event.event_type == "error":
          hourly_errors[bucket] = hourly_errors.get(bucket, 0) + 1
          website = event.website_name or "unknown"
          if website not in hourly_errors_by_website:
            hourly_errors_by_website[website] = {}
          hourly_errors_by_website[website][bucket] = (
            hourly_errors_by_website[website].get(bucket, 0) + 1
          )
          detail = json.loads(event.detail_json) if event.detail_json else {}
          source = detail.get("source", "unknown")
          if source not in hourly_errors_by_source:
            hourly_errors_by_source[source] = {}
          hourly_errors_by_source[source][bucket] = (
            hourly_errors_by_source[source].get(bucket, 0) + 1
          )
        if event.event_type in request_types:
          hourly_requests[bucket] = hourly_requests.get(bucket, 0) + 1
          type_label = request_type_map.get(event.event_type, "other")
          hourly_requests_by_type[type_label][bucket] = (
            hourly_requests_by_type[type_label].get(bucket, 0) + 1
          )

      hourly_sold: dict[str, int] = {}
      hourly_unsold: dict[str, int] = {}
      for (end_time,) in session.execute(
        select(Listing.end_time)
        .where(Listing.status == ListingStatus.SOLD)
        .where(Listing.end_time.isnot(None))
        .where(Listing.end_time >= since_naive)
        .where(Listing.end_time <= now_naive)
      ).all():
        bucket = bucket_fn(end_time)
        hourly_sold[bucket] = hourly_sold.get(bucket, 0) + 1
      for (end_time,) in session.execute(
        select(Listing.end_time)
        .where(Listing.status == ListingStatus.UNSOLD)
        .where(Listing.end_time.isnot(None))
        .where(Listing.end_time >= since_naive)
        .where(Listing.end_time <= now_naive)
      ).all():
        bucket = bucket_fn(end_time)
        hourly_unsold[bucket] = hourly_unsold.get(bucket, 0) + 1

      time_labels = []
      if granularity == "10m":
        cursor = since.replace(
          minute=(since.minute // 10) * 10, second=0, microsecond=0,
        )
      elif granularity == "6h":
        cursor = since.replace(
          hour=(since.hour // 6) * 6, minute=0, second=0, microsecond=0,
        )
      elif granularity == "1d":
        cursor = since.replace(hour=0, minute=0, second=0, microsecond=0)
      elif granularity == "1w":
        day_start = since.replace(hour=0, minute=0, second=0, microsecond=0)
        cursor = day_start - timedelta(days=since.weekday())
      else:
        cursor = since.replace(minute=0, second=0, microsecond=0)
      while cursor <= now:
        time_labels.append(bucket_fn(cursor))
        cursor += bucket_step

      def _ts(hourly: dict[str, int]) -> list[int]:
        return [hourly.get(label, 0) for label in time_labels]

      # Load % per website: active / (idle + active) * 100 per bucket (complement of idle %).
      load_pct_by_website: dict[str, list[float | None]] = {}
      for website in idle_by_website:
        load_pct_by_website[website] = []
        for label in time_labels:
          idle = idle_by_website[website].get(label, 0.0)
          active = active_by_website.get(website, {}).get(label, 0.0)
          total = idle + active
          load_pct_by_website[website].append(
            round(100.0 * active / total, 1) if total > 0 else None
          )

      # Late watch counts per website per bucket.
      late_watch_ts: dict[str, list[int]] = {}
      for website, buckets in late_watch_by_website.items():
        late_watch_ts[website] = [
          buckets.get(label, 0) for label in time_labels
        ]

      fetch_queue_by_website: dict[str, list[float | None]] = {}
      watch_queue_by_website: dict[str, list[float | None]] = {}
      search_queue_by_website: dict[str, list[float | None]] = {}
      for website in sorted(queue_depth_accum):
        fetch_queue_by_website[website] = []
        watch_queue_by_website[website] = []
        search_queue_by_website[website] = []
        for label in time_labels:
          cell = queue_depth_accum[website].get(label)
          if cell:
            fetch_queue_by_website[website].append(
              round(cell["sf"] / cell["nf"], 1) if cell["nf"] else None,
            )
            watch_queue_by_website[website].append(
              round(cell["sw"] / cell["nw"], 1) if cell["nw"] else None,
            )
            search_queue_by_website[website].append(
              round(cell["ss"] / cell["ns"], 1) if cell["ns"] else None,
            )
          else:
            fetch_queue_by_website[website].append(None)
            watch_queue_by_website[website].append(None)
            search_queue_by_website[website].append(None)

      timeseries_data = {
        "labels": time_labels,
        "new_listings": _ts(hourly_new_listings),
        "completed": _ts(hourly_completed),
        "updated": _ts(hourly_updated),
        "sold": _ts(hourly_sold),
        "unsold": _ts(hourly_unsold),
        "errors": _ts(hourly_errors),
        "requests": _ts(hourly_requests),
        "requests_by_type": {
          label: _ts(buckets)
          for label, buckets in hourly_requests_by_type.items()
        },
        "errors_by_website": {
          website: _ts(buckets)
          for website, buckets in hourly_errors_by_website.items()
        },
        "errors_by_source": {
          source: _ts(buckets)
          for source, buckets in hourly_errors_by_source.items()
        },
        "error_rate": [
          round(hourly_errors.get(label, 0) / hourly_requests[label] * 100, 2)
          if hourly_requests.get(label, 0) > 0 else 0
          for label in time_labels
        ],
        "load_pct_by_website": load_pct_by_website,
        "late_watch_by_website": late_watch_ts,
        "fetch_queue_by_website": fetch_queue_by_website,
        "watch_queue_by_website": watch_queue_by_website,
        "search_queue_by_website": search_queue_by_website,
      }

      recent_errors = []
      for event in error_events[:50]:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        recent_errors.append({
          "timestamp": event.timestamp,
          "website": event.website_name or "",
          "source": detail.get("source", ""),
          "message": detail.get("message", ""),
        })

      # ---- Search-query charts (not time-windowed) ----

      source_prefix = "source_search:"

      # Listings per search query, broken down by status.
      search_source_rows = session.execute(
        select(
          ListingAttribute.attribute_name,
          Listing.status,
          func.count(Listing.id).label("cnt"),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .group_by(ListingAttribute.attribute_name, Listing.status)
      ).all()

      search_listing_data: dict[str, dict[str, int]] = {}
      for row in search_source_rows:
        query_text = row.attribute_name[len(source_prefix):]
        if query_text not in search_listing_data:
          search_listing_data[query_text] = {}
        status_label = row.status.value if row.status else "unknown"
        search_listing_data[query_text][status_label] = (
          search_listing_data[query_text].get(status_label, 0) + row.cnt
        )

      # Sort by total listings descending.
      sorted_query_keys = sorted(
        search_listing_data,
        key=lambda query: sum(search_listing_data[query].values()),
        reverse=True,
      )
      status_bar_data = [
        {
          "query": query,
          "active": search_listing_data[query].get("active", 0),
          "sold": search_listing_data[query].get("sold", 0),
          "unsold": search_listing_data[query].get("unsold", 0),
        }
        for query in sorted_query_keys
      ]

      # Rejection rate per search query.
      verified_counts = dict(session.execute(
        select(
          ListingAttribute.attribute_name,
          func.count(func.distinct(Listing.id)),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .where(
          Listing.id.in_(
            select(ListingAttribute.listing_id).where(
              ListingAttribute.attribute_name.like("classifier_%")
            )
          )
        )
        .group_by(ListingAttribute.attribute_name)
      ).all())

      rejected_counts = dict(session.execute(
        select(
          ListingAttribute.attribute_name,
          func.count(func.distinct(Listing.id)),
        )
        .join(Listing, Listing.id == ListingAttribute.listing_id)
        .where(ListingAttribute.attribute_name.like(f"{source_prefix}%"))
        .where(Listing.status == ListingStatus.CANCELLED)
        .where(
          Listing.id.in_(
            select(ListingAttribute.listing_id).where(
              ListingAttribute.attribute_name.like("classifier_%")
            )
          )
        )
        .group_by(ListingAttribute.attribute_name)
      ).all())

      rejection_rate_data = []
      for query in sorted_query_keys:
        attr_name = f"{source_prefix}{query}"
        verified = verified_counts.get(attr_name, 0)
        rejected = rejected_counts.get(attr_name, 0)
        rate = round(100.0 * rejected / verified, 1) if verified else 0.0
        rejection_rate_data.append({
          "query": query,
          "rate": rate,
          "rejected": rejected,
          "verified": verified,
        })

      # New listings per search query over time (all-time, daily).
      all_search_events = session.execute(
        select(PipelineEvent)
        .where(PipelineEvent.event_type == "search_run")
        .order_by(PipelineEvent.timestamp)
      ).scalars().all()

      daily_new_by_query: dict[str, dict[str, int]] = {}
      for event in all_search_events:
        detail = json.loads(event.detail_json) if event.detail_json else {}
        query_text = detail.get("query", "")
        new_count = detail.get("new_listings", 0)
        if not query_text or new_count == 0:
          continue
        day = event.timestamp.strftime("%Y-%m-%d")
        if query_text not in daily_new_by_query:
          daily_new_by_query[query_text] = {}
        daily_new_by_query[query_text][day] = (
          daily_new_by_query[query_text].get(day, 0) + new_count
        )

      query_totals = {
        query: sum(days.values())
        for query, days in daily_new_by_query.items()
      }
      top_queries = sorted(
        query_totals, key=query_totals.get, reverse=True,
      )[:10]
      all_days = sorted({
        day
        for days in daily_new_by_query.values()
        for day in days
      })
      new_listings_timeseries = {
        "labels": all_days,
        "datasets": {
          query: [
            daily_new_by_query.get(query, {}).get(day, 0)
            for day in all_days
          ]
          for query in top_queries
        },
      }

      # ---- Classification charts (not time-windowed) ----

      classifier_rows = session.execute(
        select(
          ListingAttribute.listing_id,
          ListingAttribute.attribute_name,
          ListingAttribute.attribute_value,
        )
        .where(ListingAttribute.attribute_name.in_([
          "classifier_score",
          "classifier_accepted",
          "classifier_top_class",
        ]))
      ).all()

      classifier_by_listing: dict[int, dict[str, str]] = {}
      for listing_id, attr_name, attr_value in classifier_rows:
        if listing_id not in classifier_by_listing:
          classifier_by_listing[listing_id] = {}
        classifier_by_listing[listing_id][attr_name] = attr_value

      accepted_scores: list[float] = []
      rejected_scores: list[float] = []
      top_class_counts: dict[str, int] = {}
      for _listing_id, attrs in classifier_by_listing.items():
        score_str = attrs.get("classifier_score")
        accepted_str = attrs.get("classifier_accepted")
        if score_str is None:
          continue
        score = float(score_str)
        if accepted_str == "1":
          accepted_scores.append(score)
        else:
          rejected_scores.append(score)
        top_class = attrs.get("classifier_top_class")
        if top_class and score >= config.classifier.threshold:
          top_class_counts[top_class] = (
            top_class_counts.get(top_class, 0) + 1
          )

      num_bins = 20
      score_histogram = {
        "bin_labels": [
          f"{index * 5}-{index * 5 + 5}%"
          for index in range(num_bins)
        ],
        "accepted": [0] * num_bins,
        "rejected": [0] * num_bins,
      }
      for score in accepted_scores:
        bin_index = min(int(score * num_bins), num_bins - 1)
        score_histogram["accepted"][bin_index] += 1
      for score in rejected_scores:
        bin_index = min(int(score * num_bins), num_bins - 1)
        score_histogram["rejected"][bin_index] += 1

      top_categories = sorted(
        top_class_counts.items(),
        key=lambda pair: pair[1],
        reverse=True,
      )

      classification_events = session.execute(
        select(PipelineEvent)
        .where(PipelineEvent.event_type == "classification")
        .order_by(PipelineEvent.timestamp)
      ).scalars().all()

      daily_classification: dict[str, dict[str, int]] = {}
      for event in classification_events:
        day = event.timestamp.strftime("%Y-%m-%d")
        detail = json.loads(event.detail_json) if event.detail_json else {}
        is_accepted = detail.get("accepted", False)
        if day not in daily_classification:
          daily_classification[day] = {
            "total": 0, "accepted": 0, "rejected": 0,
          }
        daily_classification[day]["total"] += 1
        if is_accepted:
          daily_classification[day]["accepted"] += 1
        else:
          daily_classification[day]["rejected"] += 1

      classification_days = sorted(daily_classification.keys())
      classification_timeseries = {
        "labels": classification_days,
        "total": [
          daily_classification[day]["total"]
          for day in classification_days
        ],
        "accepted": [
          daily_classification[day]["accepted"]
          for day in classification_days
        ],
        "rejected": [
          daily_classification[day]["rejected"]
          for day in classification_days
        ],
      }

    return {
      "pipeline_running": pipeline_running,
      "pipeline_last_activity": pipeline_last_activity,
      "event_type_counts": event_type_counts,
      "request_type_counts": {
        "search": event_type_counts.get("search_run", 0),
        "fetch": event_type_counts.get("fetch_listing", 0),
        "watch": event_type_counts.get("watch_check", 0),
      },
      "website_request_counts": website_request_counts,
      "error_count": len(error_events),
      "error_by_source": error_by_source,
      "error_by_website": error_by_website,
      "search_stats": search_stats,
      "total_watch_checks": total_watch_checks,
      "total_watch_updated": total_watch_updated,
      "total_watch_completed": total_watch_completed,
      "total_watch_extensions": total_watch_extensions,
      "watch_cycle_count": len(watch_cycles),
      "total_fetched": total_fetched,
      "total_classified": total_classified,
      "total_rejected": total_rejected,
      "fetch_batch_count": len(fetch_batches),
      "pending_fetches": pending_fetches,
      "upcoming_auctions": upcoming_auctions_json,
      "timeseries_data": timeseries_data,
      "recent_errors": recent_errors,
      "new_listings_timeseries": new_listings_timeseries,
      "status_bar_data": status_bar_data,
      "rejection_rate_data": rejection_rate_data,
      "score_histogram": score_histogram,
      "top_categories": top_categories,
      "classification_timeseries": classification_timeseries,
    }

  @app.route("/operations")
  def operations():
    hours_back = request.args.get("hours", 24, type=int)
    granularity = request.args.get("granularity", "1h", type=str)
    if granularity not in ("10m", "1h", "6h", "1d", "1w"):
      granularity = "1h"
    data = _collect_operations_data(hours_back, granularity)
    return render_template(
      "operations.html",
      hours_back=hours_back,
      granularity=granularity,
      late_watch_threshold=config.scheduler.late_watch_threshold,
      **data,
    )

  @app.route("/api/operations/stats")
  def operations_stats():
    """Return all operations dashboard data as JSON for live polling."""
    hours_back = request.args.get("hours", 24, type=int)
    granularity = request.args.get("granularity", "1h", type=str)
    if granularity not in ("10m", "1h", "6h", "1d", "1w"):
      granularity = "1h"
    return json.dumps(
      _collect_operations_data(hours_back, granularity),
      cls=_DecimalDatetimeEncoder,
    )

  # ------------------------------------------------------------------
  # Routes — Live status API
  # ------------------------------------------------------------------

  status_file_path = config.database.path.parent / "pipeline_status.json"

  @app.route("/api/operations/live")
  def operations_live():
    """Return the live pipeline status as JSON.

    The ``run`` command writes this file at ~1 Hz.  If the file is
    missing or older than 10 seconds, the pipeline is considered
    offline.
    """
    try:
      if not status_file_path.exists():
        return {"running": False}
      stat = status_file_path.stat()
      if time.time() - stat.st_mtime > 10:
        return {"running": False, "stale": True}
      raw = status_file_path.read_text(encoding="utf-8")
      return json.loads(raw)
    except Exception:
      return {"running": False}

  # ------------------------------------------------------------------
  # Routes — Images
  # ------------------------------------------------------------------

  @app.route("/images/<path:filepath>")
  def serve_image(filepath):
    return send_from_directory(app.config["IMAGES_DIR"], filepath)

  return app


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _build_price_history(
  session,
  search_query: str,
  status_filters: list[str],
  website_filters: list[str],
  type_filter: str,
  source_filter: str,
) -> dict:
  """Build price history scatter chart data from all matching listings.

  Uses stored EUR prices when available. Listings without EUR prices
  are skipped.
  """
  statement = select(Listing).options(
    joinedload(Listing.website),
    joinedload(Listing.images),
  )

  if search_query:
    statement = statement.where(Listing.title.ilike(f"%{search_query}%"))
  if status_filters:
    valid_statuses = []
    for status_value in status_filters:
      with contextlib.suppress(ValueError):
        valid_statuses.append(ListingStatus(status_value))
    if valid_statuses:
      statement = statement.where(Listing.status.in_(valid_statuses))
  if website_filters:
    valid_ids = []
    for website_value in website_filters:
      with contextlib.suppress(ValueError):
        valid_ids.append(int(website_value))
    if valid_ids:
      statement = statement.where(Listing.website_id.in_(valid_ids))
  if type_filter:
    try:
      type_enum = ListingType(type_filter)
      statement = statement.where(Listing.listing_type == type_enum)
    except ValueError:
      pass
  if source_filter:
    source_attr_name = f"source_search:{source_filter}"
    source_subquery = (
      select(ListingAttribute.listing_id)
      .where(ListingAttribute.attribute_name == source_attr_name)
    )
    statement = statement.where(Listing.id.in_(source_subquery))

  all_listings = session.execute(statement).scalars().unique().all()

  sold_data = []
  active_data = []
  now = datetime.now(UTC)

  for listing in all_listings:
    price = listing.final_price or listing.current_price or listing.estimate_low
    if price is None:
      continue

    total_cost = price
    premium_percent = listing.effective_buyer_premium_percent
    if premium_percent is not None:
      total_cost += price * premium_percent / Decimal(100)
    premium_fixed = listing.effective_buyer_premium_fixed
    if premium_fixed is not None:
      total_cost += premium_fixed
    if listing.shipping_cost is not None:
      total_cost += listing.shipping_cost

    if listing.status == ListingStatus.SOLD:
      sale_date = listing.end_time or listing.created_at
      if sale_date and sale_date.tzinfo is None:
        sale_date = sale_date.replace(tzinfo=UTC)
      if sale_date and sale_date > now:
        sale_date = now
    elif listing.listing_type == ListingType.BUY_NOW:
      sale_date = now
    else:
      sale_date = listing.end_time if listing.end_time else now
      if sale_date and sale_date.tzinfo is None:
        sale_date = sale_date.replace(tzinfo=UTC)

    if sale_date is None:
      sale_date = now

    # Use stored EUR prices when available, otherwise skip non-EUR listings.
    if listing.currency == "EUR":
      total_cost_eur = total_cost
    else:
      stored_eur = (
        listing.final_price_eur
        if listing.final_price is not None
        else listing.current_price_eur
      )
      if stored_eur is not None:
        total_cost_eur = stored_eur
        if premium_percent is not None:
          total_cost_eur += stored_eur * premium_percent / Decimal(100)
        if premium_fixed is not None:
          total_cost_eur += premium_fixed
      else:
        continue

    website_name = listing.website.name if listing.website else "Unknown"

    image_url = None
    if listing.images:
      first_image = listing.images[0]
      if first_image.local_path:
        image_url = f"/images/{first_image.local_path}"
      elif first_image.source_url:
        image_url = first_image.source_url

    display_title = listing.title[:60] + ("..." if len(listing.title) > 60 else "")
    if listing.final_price is None and listing.current_price is None:
      display_title += " (Est.)"

    point_data = {
      "x": sale_date.isoformat(),
      "y": float(total_cost_eur),
      "website": website_name,
      "website_id": listing.website.id if listing.website else 0,
      "title": display_title,
      "listing_id": listing.id,
      "image_url": image_url,
    }

    if listing.status == ListingStatus.SOLD:
      sold_data.append(point_data)
    elif listing.status in (ListingStatus.ACTIVE, ListingStatus.UPCOMING):
      active_data.append(point_data)

  return {"sold": sold_data, "active": active_data}
