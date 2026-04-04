"""Flask web application for visualizing the AuctionTracker database.

Provides a read-only HTML frontend for browsing listings, sellers,
price history, and images stored by the tracker.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

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
from sqlalchemy import desc, func, select
from sqlalchemy.orm import joinedload

from auction_tracker.config import load_config
from auction_tracker.currency.converter import CurrencyConverter
from auction_tracker.database.engine import initialize_database, session_scope
from auction_tracker.database.models import (
  BidEvent,
  Listing,
  ListingAttribute,
  ListingImage,
  ListingStatus,
  ListingType,
  PriceSnapshot,
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


def create_app(config_path=None):
  """Flask application factory.

  Loads configuration, initialises the database, registers template
  filters and all routes, then returns the ready-to-run app.
  """
  config = load_config(config_path)
  initialize_database(config.database.resolved_path)

  app = Flask(
    __name__,
    template_folder=str(Path(__file__).parent / "templates"),
  )
  app.config["IMAGES_DIR"] = str(config.images.resolved_directory)
  app.config["MANUAL_CANCEL_FILE"] = str(
    config.database.resolved_path.parent / "manual_cancel_listings.txt",
  )

  # Currency converter for price history charts.
  cache_path = config.database.resolved_path.parent / "exchange_rates.json"
  currency_converter = CurrencyConverter(cache_path=cache_path)

  # ------------------------------------------------------------------
  # Template filters
  # ------------------------------------------------------------------

  @app.template_filter("format_price")
  def filter_format_price(value, currency=""):
    """Format a numeric value as a price string."""
    if value is None:
      return "–"
    formatted = f"{float(value):,.2f}"
    if currency:
      formatted += f"\u00a0{currency}"
    return formatted

  @app.template_filter("format_datetime")
  def filter_format_datetime(value):
    """Format a datetime object to a readable string."""
    if value is None:
      return "–"
    return value.strftime("%Y-%m-%d %H:%M")

  @app.template_filter("format_date")
  def filter_format_date(value):
    """Format a date object to a readable string."""
    if value is None:
      return "–"
    return value.strftime("%Y-%m-%d")

  @app.template_filter("tojson_safe")
  def filter_tojson_safe(value):
    """Serialize a value to JSON, handling Decimal and datetime.

    Returns a ``Markup`` object so Jinja2's auto-escaping does not
    convert quote characters to HTML entities (which would break the
    JSON when embedded in ``<script>`` tags).
    """
    return Markup(json.dumps(value, cls=_DecimalDatetimeEncoder))

  # ------------------------------------------------------------------
  # Routes – Dashboard
  # ------------------------------------------------------------------

  @app.route("/")
  def dashboard():
    """Overview page with aggregate statistics and recent listings."""
    with session_scope() as session:
      total_listings = session.execute(
        select(func.count(Listing.id))
      ).scalar() or 0

      active_cnt = session.execute(
        select(func.count(Listing.id))
        .where(Listing.status == ListingStatus.ACTIVE)
      ).scalar() or 0

      status_counts = dict(
        session.execute(
          select(Listing.status, func.count(Listing.id))
          .group_by(Listing.status)
        ).all()
      )

      # Count rejected listings (those with attribute rejected_by_classifier=true)
      rejected_count = session.execute(
        select(func.count(Listing.id))
        .join(ListingAttribute)
        .where(ListingAttribute.attribute_name == "rejected_by_classifier")
        .where(ListingAttribute.attribute_value == "true")
      ).scalar() or 0

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

      # Current prices (ACTIVE listings only, converted to EUR)
      price_stats = session.execute(
        select(
          func.avg(Listing.current_price_eur),
          func.min(Listing.current_price_eur),
          func.max(Listing.current_price_eur),
        ).where(Listing.status == ListingStatus.ACTIVE)
         .where(Listing.current_price_eur.isnot(None))
      ).one()

      # Sold prices (SOLD listings only, converted to EUR)
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
        rejected_count=rejected_count,
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
  # Routes – Listings
  # ------------------------------------------------------------------

  @app.route("/listings")
  def listings():
    """Searchable, filterable listing browser with pagination."""
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    search_query = request.args.get("q", "")
    type_filter = request.args.get("type", "")
    source_filter = request.args.get("source", "")
    sort_by = request.args.get("sort", "created_at")
    sort_dir = request.args.get("dir", "desc")

    # Multi-value filters: status and website are checklists.
    status_filters = request.args.getlist("status")
    website_filters = request.args.getlist("website")

    # Default status filter: if no statuses were specified in the
    # URL at all, show every status except "cancelled" and "unknown".
    # "Unknown" is for listings found by search but not yet fully
    # fetched.  A hidden form field (_filtered=1) lets us tell "user
    # submitted the form with nothing checked" apart from "first visit".
    is_form_submission = request.args.get("_filtered", "") == "1"
    if not status_filters and not is_form_submission:
      status_filters = [
        status.value
        for status in ListingStatus
        if status not in (ListingStatus.CANCELLED, ListingStatus.UNKNOWN)
      ]

    # Whitelist sortable columns to prevent injection.
    allowed_sort_columns = {
      "created_at", "title", "current_price", "final_price",
      "bid_count", "end_time", "start_time", "status",
    }
    if sort_by not in allowed_sort_columns:
      sort_by = "created_at"

    # Map price sorts to their EUR-converted equivalents so that
    # listings from different currencies (JPY, USD, GBP, etc.) are
    # sorted by their actual value, not nominal amounts.
    price_sort_mapping = {
      "current_price": "current_price_eur",
      "final_price": "final_price_eur",
    }
    db_sort_column = price_sort_mapping.get(sort_by, sort_by)

    with session_scope() as session:
      statement = select(Listing).options(
        joinedload(Listing.website),
        joinedload(Listing.images),
      )
      count_statement = select(func.count(Listing.id))

      if search_query:
        filter_clause = Listing.title.ilike(f"%{search_query}%")
        statement = statement.where(filter_clause)
        count_statement = count_statement.where(filter_clause)

      # Status filter (multi-select).
      if status_filters:
        valid_statuses = []
        for status_value in status_filters:
          try:
            valid_statuses.append(ListingStatus(status_value))
          except ValueError:
            pass
        if valid_statuses:
          statement = statement.where(
            Listing.status.in_(valid_statuses),
          )
          count_statement = count_statement.where(
            Listing.status.in_(valid_statuses),
          )

      # Website filter (multi-select).
      if website_filters:
        valid_website_ids = []
        for website_value in website_filters:
          try:
            valid_website_ids.append(int(website_value))
          except ValueError:
            pass
        if valid_website_ids:
          statement = statement.where(
            Listing.website_id.in_(valid_website_ids),
          )
          count_statement = count_statement.where(
            Listing.website_id.in_(valid_website_ids),
          )

      if type_filter:
        try:
          type_enum = ListingType(type_filter)
          statement = statement.where(Listing.listing_type == type_enum)
          count_statement = count_statement.where(
            Listing.listing_type == type_enum
          )
        except ValueError:
          pass

      # Source search filter — show only listings from a specific search.
      if source_filter:
        source_attr_name = f"source_search:{source_filter}"
        source_subquery = (
          select(ListingAttribute.listing_id)
          .where(ListingAttribute.attribute_name == source_attr_name)
        )
        statement = statement.where(Listing.id.in_(source_subquery))
        count_statement = count_statement.where(
          Listing.id.in_(source_subquery),
        )

      # Sorting — use EUR-converted prices for fair currency comparison.
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

      websites = session.execute(
        select(Website).order_by(Website.name)
      ).scalars().all()

      # Build price history chart data when there are any filters applied.
      price_history_data = None
      has_filters = (
        search_query or
        status_filters or
        website_filters or
        type_filter or
        source_filter
      )
      if has_filters:
        # Query all listings matching the filters (not just paginated results)
        # to build the price history chart.
        history_statement = select(Listing).options(
          joinedload(Listing.website),
          joinedload(Listing.images),
        )

        # Apply the same filters as the main query.
        if search_query:
          history_statement = history_statement.where(
            Listing.title.ilike(f"%{search_query}%")
          )
        if status_filters:
          valid_statuses = []
          for status_value in status_filters:
            try:
              valid_statuses.append(ListingStatus(status_value))
            except ValueError:
              pass
          if valid_statuses:
            history_statement = history_statement.where(
              Listing.status.in_(valid_statuses),
            )
        if website_filters:
          valid_website_ids = []
          for website_value in website_filters:
            try:
              valid_website_ids.append(int(website_value))
            except ValueError:
              pass
          if valid_website_ids:
            history_statement = history_statement.where(
              Listing.website_id.in_(valid_website_ids),
            )
        if type_filter:
          try:
            type_enum = ListingType(type_filter)
            history_statement = history_statement.where(
              Listing.listing_type == type_enum
            )
          except ValueError:
            pass
        if source_filter:
          source_attr_name = f"source_search:{source_filter}"
          source_subquery = (
            select(ListingAttribute.listing_id)
            .where(ListingAttribute.attribute_name == source_attr_name)
          )
          history_statement = history_statement.where(
            Listing.id.in_(source_subquery)
          )

        all_matching_listings = session.execute(
          history_statement
        ).scalars().unique().all()

        # Build chart data: sold listings and active listings.
        sold_data = []
        active_data = []
        from datetime import datetime, timezone

        for listing in all_matching_listings:
          # Calculate total buyer cost (price + fees + shipping).
          # Fallback to estimate_low if no current price is available (for UPCOMING/ACTIVE).
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

          # Determine the date for the chart point.
          # For sold listings, use end_time (sale date).
          # For active Buy It Now, the item is available now — place at today.
          # For active/upcoming auctions, use end_time if available (future), otherwise now.
          if listing.status == ListingStatus.SOLD:
            sale_date = listing.end_time or listing.created_at
            # Ensure sale_date is timezone-aware (DB stores naive UTC).
            if sale_date and sale_date.tzinfo is None:
              sale_date = sale_date.replace(tzinfo=timezone.utc)
            # CLAMP: If sold date is in the future (e.g. sold out GTC item with original end date),
            # clamp it to now so it doesn't mess up the chart scale.
            now = datetime.now(timezone.utc)
            if sale_date and sale_date > now:
                sale_date = now
          elif listing.listing_type == ListingType.BUY_NOW:
            sale_date = datetime.now(timezone.utc)
          else:
            sale_date = listing.end_time if listing.end_time else datetime.now(timezone.utc)
            if sale_date and sale_date.tzinfo is None:
              sale_date = sale_date.replace(tzinfo=timezone.utc)

          if sale_date is None:
            sale_date = datetime.now(timezone.utc)

          total_cost_eur = currency_converter.to_eur(
            total_cost, listing.currency, sale_date
          )

          if total_cost_eur is None:
            # Fallback: use stored EUR value if available.
            if listing.currency == "EUR":
              total_cost_eur = total_cost
            else:
              # Try to use stored EUR-converted price.
              # Note: listing.estimate_low doesn't have a pre-calculated EUR column,
              # so we might miss some estimates if currency conversion fails live.
              stored_eur_price = (
                listing.final_price_eur
                if listing.final_price is not None
                else listing.current_price_eur
              )
              if stored_eur_price is not None:
                # Approximate: use stored EUR price and add fees.
                base_eur = stored_eur_price
                if premium_percent is not None:
                  base_eur += base_eur * premium_percent / Decimal(100)
                if premium_fixed is not None:
                  base_eur += premium_fixed
                if listing.shipping_cost is not None:
                  shipping_eur = currency_converter.to_eur(
                    listing.shipping_cost, listing.currency, sale_date
                  )
                  if shipping_eur is not None:
                    base_eur += shipping_eur
                total_cost_eur = base_eur

              # If we are using estimate_low and conversion failed, we can't show it.
              # Unless we assume 1:1 or just skip. Skipping is safer.
              elif listing.final_price is None and listing.current_price is None:
                 # It was an estimate, but we couldn't convert it.
                 continue
              else:
                continue

          website_name = listing.website.name if listing.website else "Unknown"
          website_id = listing.website.id if listing.website else 0

          # Get first image URL if available.
          image_url = None
          if listing.images:
            first_image = listing.images[0]
            if first_image.local_path:
              image_url = f"/images/{first_image.local_path}"
            elif first_image.source_url:
              image_url = first_image.source_url

          # Title suffix for estimates
          display_title = listing.title[:60] + ("..." if len(listing.title) > 60 else "")
          if listing.final_price is None and listing.current_price is None:
             display_title += " (Est.)"

          point_data = {
            "x": sale_date.isoformat(),
            "y": float(total_cost_eur),
            "website": website_name,
            "website_id": website_id,
            "title": display_title,
            "listing_id": listing.id,
            "image_url": image_url,
          }

          if listing.status == ListingStatus.SOLD:
            sold_data.append(point_data)
          elif listing.status in (ListingStatus.ACTIVE, ListingStatus.UPCOMING):
            active_data.append(point_data)

        price_history_data = {
          "sold": sold_data,
          "active": active_data,
        }

      # Build a query-string fragment containing the current filters
      # (excluding page) for use in pagination and sort links.
      from urllib.parse import urlencode
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
        websites=websites,
        ListingStatus=ListingStatus,
        ListingType=ListingType,
        price_history_data=price_history_data,
      )

  # ------------------------------------------------------------------
  # Routes – Listing detail
  # ------------------------------------------------------------------

  @app.route("/listings/<int:listing_id>")
  def listing_detail(listing_id):
    """Full detail page for a single listing with images and charts."""
    with session_scope() as session:
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

      # Prepare chart data as plain Python lists for JSON embedding.
      # Bids are already sorted by amount (ascending).  For the time-
      # axis chart we nudge automatic bids forward by 1 s so that
      # Chart.js (which internally sorts by x/time) always draws the
      # manual bid *before* the automatic response even when both share
      # the exact same timestamp from Catawiki.
      is_non_eur = listing.currency != "EUR"
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

      # Reference price lines for the chart.
      reference_lines = {}
      if listing.reserve_price is not None:
        reference_lines["reserve_price"] = float(listing.reserve_price)
      if listing.estimate_low is not None:
        reference_lines["estimate_low"] = float(listing.estimate_low)
      if listing.estimate_high is not None:
        reference_lines["estimate_high"] = float(listing.estimate_high)

      # Convert attributes to a plain list while the session is active.
      # This ensures they remain accessible after the session closes.
      # Separate source_search attributes from regular ones.
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
    """Append listing ID to the manual-cancel file for later processing by fix-database.

    Does not change the database. Run ``python -m auction_tracker fix-database``
    to apply pending cancellations.
    """
    cancel_file = Path(app.config["MANUAL_CANCEL_FILE"])
    with session_scope() as session:
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
      with open(cancel_file, "a", encoding="utf-8") as file_handle:
        file_handle.write(f"{listing_id}\n")
    except OSError:
      abort(500, "Could not write to manual cancel file.")
    return redirect(
      url_for("listing_detail", listing_id=listing_id) + "?mark_cancel=ok",
    )

  @app.route("/listings/<int:listing_id>/mark-confirm", methods=["POST"])
  def listing_mark_confirm(listing_id):
    """Mark a listing as a confirmed writing instrument (safelist).

    - Removes 'rejected_by_classifier'.
    - Removes 'manually_cancelled'.
    - Adds 'confirmed_writing_instrument'.
    - Restores status to ACTIVE, UPCOMING, SOLD, or UNSOLD depending on time/price.
    - Removes from manual cancel file if present.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    cancel_file = Path(app.config["MANUAL_CANCEL_FILE"])

    with session_scope() as session:
      listing = session.execute(
        select(Listing).where(Listing.id == listing_id),
      ).scalars().first()

      if listing is None:
        abort(404)

      # 1. Remove negative attributes
      session.execute(
        select(ListingAttribute)
        .where(ListingAttribute.listing_id == listing_id)
        .where(ListingAttribute.attribute_name.in_([
          "rejected_by_classifier",
          "manually_cancelled"
        ]))
      ).scalars().all()
      # SQLAlchemy bulk delete via query is cleaner
      from sqlalchemy import delete
      session.execute(
        delete(ListingAttribute)
        .where(ListingAttribute.listing_id == listing_id)
        .where(ListingAttribute.attribute_name.in_([
          "rejected_by_classifier",
          "manually_cancelled"
        ]))
      )

      # 2. Add safelist attribute
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

      # 3. Restore status -> Force Re-fetch
      # Instead of guessing the status based on stale data, we reset it to UNKNOWN
      # and mark it as not fully fetched. This signals the DiscoveryLoop to
      # re-scrape the listing immediately, ensuring we get the true current state
      # (ACTIVE, SOLD, UNSOLD) and price.

      listing.status = ListingStatus.UNKNOWN
      listing.is_fully_fetched = False

      # We also reset the last_checked_at to ensures it's picked up quickly if sorted by time
      # (though DiscoveryLoop usually prioritizes UNKNOWN regardless).
      listing.last_checked_at = datetime.fromtimestamp(0, timezone.utc)

      # 4. Remove from manual cancel list if present
      if cancel_file.exists():
        try:
           lines = cancel_file.read_text(encoding="utf-8").splitlines()
           if str(listing_id) in lines:
               new_lines = [line for line in lines if line.strip() != str(listing_id)]
               cancel_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        except OSError:
           pass # Non-critical

    return redirect(
      url_for("listing_detail", listing_id=listing_id) + "?mark_confirm=ok",
    )

  # ------------------------------------------------------------------
  # Routes – Sellers
  # ------------------------------------------------------------------

  @app.route("/sellers")
  def sellers():
    """List all sellers with their listing counts."""
    with session_scope() as session:
      all_sellers = session.execute(
        select(Seller)
        .options(
          joinedload(Seller.website),
          joinedload(Seller.listings),
        )
        .order_by(Seller.username)
      ).scalars().unique().all()

      return render_template("sellers.html", sellers=all_sellers)

  @app.route("/sellers/<int:seller_id>")
  def seller_detail(seller_id):
    """Detail page for a specific seller with their listings."""
    with session_scope() as session:
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
  # Routes – Bidders
  # ------------------------------------------------------------------

  @app.route("/bidders")
  def bidders():
    """List all unique bidders with aggregate statistics."""
    with session_scope() as session:
      # Sub-query: for each SOLD listing, find the highest bid amount
      # and the bidder who placed it (the winner).
      max_bid_sub = (
        select(
          BidEvent.listing_id,
          func.max(BidEvent.amount).label("max_amount"),
        )
        .group_by(BidEvent.listing_id)
        .subquery()
      )

      # Winning bids: join back to get the bidder_username of the max bid
      # on sold listings.
      winning_bids = (
        select(
          BidEvent.bidder_username,
          Listing.final_price_eur,
          Listing.final_price,
          Listing.id.label("listing_id"),
        )
        .join(max_bid_sub, (
          (BidEvent.listing_id == max_bid_sub.c.listing_id) &
          (BidEvent.amount == max_bid_sub.c.max_amount)
        ))
        .join(Listing, BidEvent.listing_id == Listing.id)
        .where(Listing.status == ListingStatus.SOLD)
        .where(BidEvent.bidder_username.isnot(None))
        .subquery()
      )

      # Aggregate wins per bidder.
      wins_agg = (
        select(
          winning_bids.c.bidder_username,
          func.count(winning_bids.c.listing_id).label("won_count"),
          func.sum(winning_bids.c.final_price_eur).label("total_spent_eur"),
        )
        .group_by(winning_bids.c.bidder_username)
        .subquery()
      )

      # Main aggregation on all bids.
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

      return render_template(
        "bidders.html",
        bidder_stats=bidder_stats,
      )

  @app.route("/bidders/<username>")
  def bidder_detail(username):
    """Detail page for a single bidder with full statistics."""
    with session_scope() as session:
      # Basic info: country from most recent bid.
      latest_bid = session.execute(
        select(BidEvent)
        .where(BidEvent.bidder_username == username)
        .order_by(desc(BidEvent.bid_time))
        .limit(1)
      ).scalars().first()

      if latest_bid is None:
        abort(404)

      bidder_country = latest_bid.bidder_country

      # Aggregate stats.
      stats = session.execute(
        select(
          func.count(func.distinct(BidEvent.listing_id)).label("listing_count"),
          func.min(BidEvent.bid_time).label("first_bid_time"),
          func.max(BidEvent.bid_time).label("last_bid_time"),
        )
        .where(BidEvent.bidder_username == username)
      ).one()

      # Find items won: listings where this bidder placed the highest bid
      # and the listing is SOLD.
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
          (BidEvent.listing_id == max_bid_sub.c.listing_id) &
          (BidEvent.amount == max_bid_sub.c.max_amount)
        ))
        .where(Listing.status == ListingStatus.SOLD)
        .where(BidEvent.bidder_username == username)
        .options(joinedload(Listing.website))
        .order_by(desc(Listing.end_time))
      ).scalars().unique().all()

      total_spent_eur = sum(
        float(l.final_price_eur) for l in won_listings
        if l.final_price_eur is not None
      )
      avg_winning_price = (
        total_spent_eur / len(won_listings)
        if won_listings else 0.0
      )

      # Spending timeline for chart (cumulative sum of purchases).
      spending_timeline = []
      cumulative = 0.0
      for listing in sorted(won_listings, key=lambda l: l.end_time or l.created_at):
        price = float(listing.final_price_eur) if listing.final_price_eur else 0.0
        cumulative += price
        spending_timeline.append({
          "time": (listing.end_time or listing.created_at).isoformat(),
          "total": round(cumulative, 2),
          "item": listing.title[:60] if listing.title else "?",
          "price": round(price, 2),
        })

      # All bids by this bidder.
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
  # Routes – Searches
  # ------------------------------------------------------------------

  @app.route("/searches")
  def searches():
    """Overview of saved searches with per-search statistics."""
    source_prefix = "source_search:"

    with session_scope() as session:
      # Fetch all saved search queries.
      saved_searches = session.execute(
        select(SearchQuery)
        .options(joinedload(SearchQuery.website))
        .order_by(SearchQuery.name)
      ).scalars().unique().all()

      # Build a lookup from query_text to saved search(es).
      # Convert to plain dicts so they remain accessible after the
      # session closes.
      saved_by_query_text = {}
      for search in saved_searches:
        saved_dict = {
          "name": search.name,
          "category": search.category,
          "website_name": (
            search.website.name if search.website else "All websites"
          ),
          "is_active": search.is_active,
        }
        saved_by_query_text.setdefault(
          search.query_text, [],
        ).append(saved_dict)

      # Aggregate listing stats grouped by source search and website.
      # Each row: (attribute_name, website_name, status, count).
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

      # Also count how many have classifier attributes per source
      # search (to compute verification coverage and acceptance rate).
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
        .group_by(
          ListingAttribute.attribute_name,
          Website.name,
        )
      ).all()

      # Build a verified count lookup.
      verified_lookup = {}
      for row in verified_stats:
        key = (row.attribute_name, row.website_name)
        verified_lookup[key] = row.verified_count

      # Organize stats into a structured dict keyed by query_text.
      search_data = {}
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

        # Per-status totals.
        status_label = row.status.value if row.status else "unknown"
        entry["by_status"][status_label] = (
          entry["by_status"].get(status_label, 0) + row.listing_count
        )

        # Per-website breakdown.
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
          website_entry["by_status"].get(status_label, 0)
          + row.listing_count
        )

        # Verified count for this (search, website) pair.
        verified_key = (row.attribute_name, website_name)
        website_entry["verified"] = verified_lookup.get(
          verified_key, 0,
        )

      # Also count how many classified listings were cancelled
      # (rejected by classifier) per source search and website.
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
        .group_by(
          ListingAttribute.attribute_name,
          Website.name,
        )
      ).all()

      # Build a rejected count lookup.
      rejected_lookup = {}
      for row in rejected_stats:
        key = (row.attribute_name, row.website_name)
        rejected_lookup[key] = row.rejected_count

      # Compute derived metrics for each search.
      # Acceptance rate is based only on verified listings (those
      # that went through the classifier), not all listings.
      # Unverified listings (UNKNOWN, not yet fetched) are excluded
      # from the rate calculation.
      for entry in search_data.values():
        # Compute verified and rejected totals.
        entry["verified"] = sum(
          website_data["verified"]
          for website_data in entry["websites"].values()
        )
        entry["rejected"] = 0
        for website_name, website_data in entry["websites"].items():
          attr_name = f"{source_prefix}{entry['query_text']}"
          rejected_key = (attr_name, website_name)
          website_rejected = rejected_lookup.get(rejected_key, 0)
          website_data["rejected"] = website_rejected
          website_data["accepted"] = (
            website_data["verified"] - website_rejected
          )
          website_data["acceptance_rate"] = (
            round(
              100.0 * website_data["accepted"]
              / website_data["verified"], 1,
            )
            if website_data["verified"] > 0 else 0.0
          )
          website_data["verification_coverage"] = (
            round(
              100.0 * website_data["verified"]
              / website_data["total"], 1,
            )
            if website_data["total"] > 0 else 0.0
          )
          entry["rejected"] += website_rejected

        entry["accepted"] = entry["verified"] - entry["rejected"]
        entry["acceptance_rate"] = (
          round(
            100.0 * entry["accepted"] / entry["verified"], 1,
          )
          if entry["verified"] > 0 else 0.0
        )
        entry["verification_coverage"] = (
          round(
            100.0 * entry["verified"] / entry["total"], 1,
          )
          if entry["total"] > 0 else 0.0
        )

      # Sort by total descending.
      sorted_searches = sorted(
        search_data.values(),
        key=lambda entry: entry["total"],
        reverse=True,
      )

      # Serialize saved searches to plain dicts for the template,
      # enriched with stats from search_data when available.
      saved_search_list = []
      for search in saved_searches:
        stats = search_data.get(search.query_text, {})
        saved_search_list.append({
          "id": search.id,
          "name": search.name,
          "query_text": search.query_text,
          "category": search.category,
          "website_name": (
            search.website.name if search.website else "All websites"
          ),
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
  # Routes – Static assets
  # ------------------------------------------------------------------

  @app.route("/images/<path:filepath>")
  def serve_image(filepath):
    """Serve downloaded listing images from the local storage."""
    return send_from_directory(app.config["IMAGES_DIR"], filepath)

  return app
