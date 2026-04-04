"""CLI entry point for AuctionTracker.

Run with::

    python -m auction_tracker [COMMAND]
"""

from __future__ import annotations

import sys

import click
from rich.console import Console
from rich.table import Table

from auction_tracker import __version__
from auction_tracker.config import load_config, setup_logging
from auction_tracker.database.engine import initialize_database, session_scope
from auction_tracker.database.models import ListingStatus, ListingType
from auction_tracker.database.repository import (
  get_active_listings,
  get_or_create_search_query,
  list_websites,
  search_listings,
)

console = Console()


@click.group()
@click.option("--config", "config_path", default=None, help="Path to config.yaml")
@click.version_option(version=__version__)
@click.pass_context
def cli(context, config_path):
  """AuctionTracker – track prices on online auction websites."""
  config = load_config(config_path)
  setup_logging(config.logging)
  initialize_database(config.database.resolved_path)
  context.ensure_object(dict)
  context.obj["config"] = config


# ------------------------------------------------------------------
# init-db
# ------------------------------------------------------------------

@cli.command("init-db")
@click.pass_context
def init_db(context):
  """Create or verify all database tables."""
  console.print("[green]Database initialised successfully.[/green]")


# ------------------------------------------------------------------
# scrapers
# ------------------------------------------------------------------

@cli.command("scrapers")
def list_scrapers():
  """List all registered scrapers."""
  from auction_tracker.scrapers.registry import ScraperRegistry
  names = ScraperRegistry.list_registered()
  if not names:
    console.print("[yellow]No scrapers registered yet.[/yellow]")
    return
  table = Table(title="Registered Scrapers")
  table.add_column("Website", style="cyan")
  for name in names:
    table.add_row(name)
  console.print(table)


# ------------------------------------------------------------------
# websites
# ------------------------------------------------------------------

@cli.command("websites")
@click.pass_context
def list_websites_cmd(context):
  """List tracked websites in the database."""
  with session_scope() as session:
    websites = list_websites(session, active_only=False)
    if not websites:
      console.print("[yellow]No websites in the database yet.[/yellow]")
      return
    table = Table(title="Websites")
    table.add_column("ID", style="dim")
    table.add_column("Name", style="cyan")
    table.add_column("Base URL")
    table.add_column("Default Premium %")
    table.add_column("Currency")
    table.add_column("Active")
    for website in websites:
      table.add_row(
        str(website.id),
        website.name,
        website.base_url,
        str(website.default_buyer_premium_percent or "–"),
        website.default_currency,
        "✓" if website.is_active else "✗",
      )
    console.print(table)


# ------------------------------------------------------------------
# search
# ------------------------------------------------------------------

@cli.command("search")
@click.argument("query")
@click.option("--website", "-w", multiple=True, help="Scraper name(s) to search on. Can be specified multiple times.")
@click.option("--category", "-c", default=None, help="Category filter.")
@click.option("--save", "-s", default=None, help="Save this search under a name for recurring use.")
@click.option(
  "--fetch", "-f", is_flag=True, default=False,
  help="Also fetch full details (description, images, bids) for every result.",
)
@click.pass_context
def run_search(context, query, website, category, save, fetch):
  """Run a search on one or more registered scrapers.

  With --fetch / -f, also download the full listing details for each
  result in one go (equivalent to running 'search' then 'fetch' on
  every result).

  Examples:
    auction_tracker search "Montblanc" -w ebay -w catawiki
    auction_tracker search "fountain pen" --save "all_pens"
  """
  from auction_tracker.monitor import Monitor
  from auction_tracker.scrapers.registry import ScraperRegistry
  config = context.obj["config"]
  monitor = Monitor(config)

  if save:
    with session_scope() as session:
      get_or_create_search_query(
        session, name=save, query_text=query, category=category,
      )
    console.print(f"[green]Search saved as '{save}'.[/green]")

  scrapers_to_use = []
  if website:
    scrapers_to_use = list(website)
  else:
    scrapers_to_use = ScraperRegistry.list_registered()

  if not scrapers_to_use:
    console.print("[yellow]No scrapers registered. Implement a scraper first.[/yellow]")
    return

  if fetch:
    # Search + fetch full details in one pass.
    total_found = 0
    total_fetched = 0
    total_failures = 0
    for name in scrapers_to_use:
      try:
        scraper = monitor.get_scraper(name)

        def _progress(idx, total, url, success):
          status = "[green]✓[/green]" if success else "[red]✗[/red]"
          console.print(f"  {status} [{idx}/{total}] {url}")

        found, fetched = monitor.search_and_fetch(
          scraper, query, category=category, on_progress=_progress,
        )
        total_found += found
        total_fetched += fetched
        failures = found - fetched
        total_failures += failures
        parts = [f"{found} found", f"{fetched} fetched"]
        if failures > 0:
          parts.append(f"[red]{failures} failed[/red]")
        console.print(f"  {name}: {', '.join(parts)}")
      except KeyError as error:
        total_failures += 1
        console.print(f"  [red]{error}[/red]")
      except Exception as error:
        total_failures += 1
        console.print(f"  [red]{name}: {error}[/red]")

    summary = f"Total: {total_found} found, {total_fetched} fully fetched"
    if total_failures > 0:
      summary += f", [red]{total_failures} failed[/red]"
    console.print(f"\n[bold]{summary}[/bold]")
  else:
    # Search only (existing behaviour).
    total = 0
    total_failures = 0
    for name in scrapers_to_use:
      try:
        scraper = monitor.get_scraper(name)
        count = monitor.run_search(scraper, query, category=category)
        total += count
        console.print(f"  {name}: {count} new listings")
      except KeyError as error:
        total_failures += 1
        console.print(f"  [red]{error}[/red]")
      except Exception as error:
        total_failures += 1
        console.print(f"  [red]{name}: {error}[/red]")

    summary = f"Total new listings: {total}"
    if total_failures > 0:
      summary += f", [red]{total_failures} error(s)[/red]"
    console.print(f"\n[bold]{summary}[/bold]")


# ------------------------------------------------------------------
# fetch
# ------------------------------------------------------------------

@cli.command("fetch")
@click.argument("url")
@click.option("--website", "-w", required=True, help="Scraper name to use.")
@click.pass_context
def fetch_listing(context, url, website):
  """Fetch and store a single listing by URL."""
  from auction_tracker.monitor import Monitor
  config = context.obj["config"]
  monitor = Monitor(config)
  try:
    scraper = monitor.get_scraper(website)
    listing_id = monitor.ingest_listing(scraper, url)
    console.print(f"[green]Listing stored with ID {listing_id}.[/green]")
  except Exception as error:
    console.print(f"[red]Error: {error}[/red]")
    sys.exit(1)


# ------------------------------------------------------------------
# listings
# ------------------------------------------------------------------

@cli.command("listings")
@click.option("--status", "-s", default=None, help="Filter by status (active, sold, unsold, …).")
@click.option("--query", "-q", default=None, help="Filter by title substring.")
@click.option("--limit", "-n", default=50, help="Max number of results.")
@click.pass_context
def show_listings(context, status, query, limit):
  """Show listings in the database."""
  status_filter = None
  if status:
    try:
      status_filter = ListingStatus(status.lower())
    except ValueError:
      console.print(f"[red]Unknown status: {status}[/red]")
      sys.exit(1)

  with session_scope() as session:
    listings = search_listings(
      session,
      title_contains=query,
      status=status_filter,
      limit=limit,
    )

    if not listings:
      console.print("[yellow]No listings found.[/yellow]")
      return

    table = Table(title=f"Listings (showing up to {limit})")
    table.add_column("ID", style="dim")
    table.add_column("Title", max_width=50)
    table.add_column("Price", justify="right")
    table.add_column("Bids", justify="right")
    table.add_column("Status")
    table.add_column("Type")
    table.add_column("Website")

    for listing in listings:
      price_display = "–"
      if listing.final_price is not None:
        price_display = f"{listing.final_price} {listing.currency}"
      elif listing.current_price is not None:
        price_display = f"{listing.current_price} {listing.currency}"

      status_style = {
        ListingStatus.ACTIVE: "green",
        ListingStatus.SOLD: "blue",
        ListingStatus.UNSOLD: "red",
        ListingStatus.UPCOMING: "yellow",
      }.get(listing.status, "white")

      table.add_row(
        str(listing.id),
        listing.title,
        price_display,
        str(listing.bid_count),
        f"[{status_style}]{listing.status.value}[/{status_style}]",
        listing.listing_type.value,
        listing.website.name if listing.website else "?",
      )

    console.print(table)


# ------------------------------------------------------------------
# discover
# ------------------------------------------------------------------

@cli.command("discover")
@click.option(
  "--no-search", is_flag=True, default=False,
  help="Skip saved searches; only fetch details for already-discovered listings.",
)
@click.option("--website", "-w", multiple=True, help="Scraper name(s). Can be specified multiple times.")
@click.option(
  "--verbose", "-v", is_flag=True, default=False,
  help="Enable debug logging to the console.",
)
@click.pass_context
def run_discover(context, no_search, website, verbose):
  """Run the discovery and fetch loop.

  Continuously runs saved searches to find new listings, then fetches
  full details (images, bids, attributes) for each.  Designed to run
  in parallel with the 'watch' command in a separate terminal.

  Both commands share the same database safely using WAL mode.

  Examples:

  \b
    auction_tracker discover
    auction_tracker discover -w ebay -w catawiki
    auction_tracker discover --no-search
  """
  from auction_tracker.scrapers.registry import ScraperRegistry
  from auction_tracker.smart_monitor import DiscoveryLoop
  config = context.obj["config"]
  all_scrapers = ScraperRegistry.list_registered()
  if not all_scrapers:
    console.print(
      "[yellow]No scrapers registered. Implement a scraper first.[/yellow]"
    )
    return

  scrapers = []
  if website:
    scrapers = list(website)
    unknown = set(scrapers) - set(all_scrapers)
    if unknown:
      console.print(f"[red]Unknown scraper(s): {', '.join(unknown)}[/red]")
      console.print(f"Available: {', '.join(all_scrapers)}")
      return
  else:
    scrapers = all_scrapers

  console.print(
    f"[bold]Starting discovery loop with {len(scrapers)} scraper(s): "
    f"{', '.join(scrapers)}[/bold]"
  )
  if verbose:
    console.print("[dim]Debug logging enabled (--verbose).[/dim]")

  loop = DiscoveryLoop(
    config, run_searches=not no_search, scrapers=scrapers, verbose=verbose,
  )
  loop.run()


# ------------------------------------------------------------------
# watch
# ------------------------------------------------------------------

@cli.command("watch")
@click.option("--website", "-w", multiple=True, help="Scraper name(s). Can be specified multiple times.")
@click.option(
  "--verbose", "-v", is_flag=True, default=False,
  help="Enable debug logging to the console.",
)
@click.pass_context
def run_watch(context, website, verbose):
  """Monitor active listings with timing-aware scheduling.

  Tracks all fully-fetched active listings and adapts the polling
  frequency based on how close each auction is to ending.  Runs
  independently of the 'discover' command so that long fetches never
  delay time-critical monitoring updates (every 20 seconds for
  imminent auctions).

  New listings fetched by 'discover' are automatically picked up
  within 30 seconds.  Both commands share the same database safely
  using WAL mode.

  Can be safely interrupted at any time with Ctrl+C; the database
  always remains consistent and the next run resumes where it left
  off.

  Examples:

  \b
    auction_tracker watch
    auction_tracker watch -w ebay -w yahoo_japan
  """
  from auction_tracker.scrapers.registry import ScraperRegistry
  from auction_tracker.smart_monitor import WatchLoop
  config = context.obj["config"]
  all_scrapers = ScraperRegistry.list_registered()
  if not all_scrapers:
    console.print(
      "[yellow]No scrapers registered. Implement a scraper first.[/yellow]"
    )
    return

  scrapers = []
  if website:
    scrapers = list(website)
    unknown = set(scrapers) - set(all_scrapers)
    if unknown:
      console.print(f"[red]Unknown scraper(s): {', '.join(unknown)}[/red]")
      console.print(f"Available: {', '.join(all_scrapers)}")
      return
  else:
    scrapers = all_scrapers

  console.print(
    f"[bold]Starting watch loop with {len(scrapers)} scraper(s): "
    f"{', '.join(scrapers)}[/bold]"
  )
  if verbose:
    console.print("[dim]Debug logging enabled (--verbose).[/dim]")

  loop = WatchLoop(config, scrapers=scrapers, verbose=verbose)
  loop.run()


# ------------------------------------------------------------------
# fix-database
# ------------------------------------------------------------------

@cli.command("fix-database")
@click.option(
  "--dry-run", is_flag=True, default=False,
  help="Show what would be fixed without making changes.",
)
@click.pass_context
def fix_database(context, dry_run):
  """Detect and fix incoherent listing statuses in the database.

  Scans all listings for status inconsistencies and fixes them using
  existing data only — no re-fetching or re-classification.

  Checks performed:

  \b
  1. UNKNOWN listings that were fully fetched and classified.
  2. Buy Now listings incorrectly marked as UNSOLD.
  3. Auction listings still marked ACTIVE long after ending.
  4. Leftover image files for classifier-rejected listings.
  5. Fully fetched listings that were never classified.
  6. SOLD/UNSOLD listings whose end time is still in the future.
  """
  from datetime import datetime, timedelta, timezone
  from pathlib import Path

  from sqlalchemy import func, or_, select

  from auction_tracker.database.models import (
    Listing,
    ListingAttribute,
    ListingImage,
    Website,
  )

  config = context.obj["config"]
  now = datetime.now(timezone.utc)
  # Grace period before considering an ACTIVE auction as stale.
  stale_threshold = timedelta(hours=48)

  action_label = "[dim](dry-run)[/dim]" if dry_run else "[green]FIXED[/green]"

  # Counters for the summary.
  fix_counts = {
    "manual_cancelled": 0,
    "stale_upcoming_marked": 0,
    "unknown_to_cancelled": 0,
    "unknown_to_active": 0,
    "unknown_to_upcoming": 0,
    "unknown_to_sold": 0,
    "unknown_to_unsold": 0,
    "unsold_buynow_to_sold": 0,
    "stale_active_to_sold": 0,
    "stale_active_to_unsold": 0,
    "premature_sold_to_active": 0,
    "premature_unsold_to_active": 0,
    "images_cleaned": 0,
    "unclassified_marked": 0,
    "liveauctioneers_bad_endtime": 0,
    "ebay_buynow_unsold": 0,
    "drouot_mark_refetch": 0,
    "classified_below_threshold_cancelled": 0,
  }

  def _make_aware(dt):
    """Ensure a datetime is timezone-aware (assume UTC if naive)."""
    if dt is None:
      return None
    if dt.tzinfo is None:
      return dt.replace(tzinfo=timezone.utc)
    return dt

  # Read manual cancel file (listing IDs queued from the web UI).
  manual_cancel_path = config.database.resolved_path.parent / "manual_cancel_listings.txt"
  manual_cancel_ids = []
  if manual_cancel_path.exists():
    with open(manual_cancel_path, "r", encoding="utf-8") as file_handle:
      for line in file_handle:
        line = line.strip()
        if not line:
          continue
        try:
          manual_cancel_ids.append(int(line))
        except ValueError:
          continue

  with session_scope() as session:
    # ----------------------------------------------------------------
    # Fix 0: Manual cancellations (queued from web "Mark as not a writing instrument")
    # ----------------------------------------------------------------
    if manual_cancel_ids:
      console.print(
        "\n[bold cyan]Check 0:[/bold cyan] Manual cancellations from web UI",
      )
      for listing_id in manual_cancel_ids:
        listing = session.execute(
          select(Listing).where(Listing.id == listing_id),
        ).scalars().first()
        if listing is None:
          console.print(f"  #{listing_id}: [yellow]Not found, skipped.[/yellow]")
          continue
        if listing.status == ListingStatus.CANCELLED:
          console.print(f"  #{listing_id}: [dim]Already cancelled, skipped.[/dim]")
          continue
        console.print(
          f"  #{listing_id}: {listing.status.value} → CANCELLED (manual) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.CANCELLED
          existing = session.execute(
            select(ListingAttribute)
            .where(ListingAttribute.listing_id == listing_id)
            .where(ListingAttribute.attribute_name == "manually_cancelled"),
          ).scalars().first()
          if not existing:
            session.add(
              ListingAttribute(
                listing_id=listing_id,
                attribute_name="manually_cancelled",
                attribute_value="true",
              ),
            )
        fix_counts["manual_cancelled"] += 1

    # ----------------------------------------------------------------
    # Fix 1: UNKNOWN + fully fetched → infer correct status
    # ----------------------------------------------------------------
    # These listings were fetched (and possibly classified) but the
    # old code never updated their status from the initial UNKNOWN
    # set during search discovery.

    console.print("\n[bold cyan]Check 1:[/bold cyan] UNKNOWN listings that were fully fetched")

    unknown_fetched = session.execute(
      select(Listing)
      .where(Listing.status == ListingStatus.UNKNOWN)
      .where(Listing.is_fully_fetched.is_(True))
    ).scalars().all()

    for listing in unknown_fetched:
      # Check if the classifier rejected this listing.
      rejected_attr = session.execute(
        select(ListingAttribute)
        .where(ListingAttribute.listing_id == listing.id)
        .where(ListingAttribute.attribute_name == "rejected_by_classifier")
        .where(ListingAttribute.attribute_value == "true")
      ).scalar_one_or_none()

      if rejected_attr:
        console.print(
          f"  #{listing.id}: UNKNOWN → CANCELLED "
          f"(rejected by classifier) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.CANCELLED
        fix_counts["unknown_to_cancelled"] += 1
        continue

      # Infer status from timing and listing type.
      start = _make_aware(listing.start_time)
      end = _make_aware(listing.end_time)

      if start and start > now:
        console.print(
          f"  #{listing.id}: UNKNOWN → UPCOMING "
          f"(starts {start:%Y-%m-%d %H:%M}) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.UPCOMING
        fix_counts["unknown_to_upcoming"] += 1

      elif end and end < now:
        # The listing's end time has passed — determine final outcome.
        if listing.final_price is not None:
          console.print(
            f"  #{listing.id}: UNKNOWN → SOLD "
            f"(has final price) {action_label}",
          )
          if not dry_run:
            listing.status = ListingStatus.SOLD
          fix_counts["unknown_to_sold"] += 1
        elif (
          listing.listing_type in (ListingType.AUCTION, ListingType.HYBRID)
          and listing.bid_count > 0
        ):
          console.print(
            f"  #{listing.id}: UNKNOWN → SOLD "
            f"(auction with {listing.bid_count} bid(s)) {action_label}",
          )
          if not dry_run:
            listing.status = ListingStatus.SOLD
          fix_counts["unknown_to_sold"] += 1
        elif listing.listing_type in (ListingType.AUCTION, ListingType.HYBRID):
          console.print(
            f"  #{listing.id}: UNKNOWN → UNSOLD "
            f"(auction ended, no bids) {action_label}",
          )
          if not dry_run:
            listing.status = ListingStatus.UNSOLD
          fix_counts["unknown_to_unsold"] += 1
        else:
          # Buy Now with an end time in the past — most likely sold.
          console.print(
            f"  #{listing.id}: UNKNOWN → SOLD "
            f"(buy-now listing ended) {action_label}",
          )
          if not dry_run:
            listing.status = ListingStatus.SOLD
          fix_counts["unknown_to_sold"] += 1

      elif listing.website and "gazette" in listing.website.name.lower():
        # Gazette Drouot is a historical archive. Listings are never "ACTIVE" in the sense of
        # live bidding monitored by us. If end_time is missing, it's a parsing failure or old item.
        # Fallback to UNSOLD to stop monitoring.
        console.print(
          f"  #{listing.id}: UNKNOWN -> UNSOLD "
          f"(Gazette Drouot is historical) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.UNSOLD
        fix_counts["unknown_to_unsold"] += 1

      else:
        # No end time, or end time still in the future → active.
        console.print(
          f"  #{listing.id}: UNKNOWN → ACTIVE "
          f"(listing is live) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.ACTIVE
        fix_counts["unknown_to_active"] += 1

    if not unknown_fetched:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 2: UNSOLD + BUY_NOW → SOLD
    # ----------------------------------------------------------------
    # Buy Now items that went out of stock were incorrectly marked
    # as UNSOLD by the old eBay scraper.  A Buy Now item going
    # unavailable means it was purchased — that is SOLD.

    console.print(
      "\n[bold cyan]Check 2:[/bold cyan] "
      "Buy Now listings incorrectly marked UNSOLD",
    )

    unsold_buynow = session.execute(
      select(Listing)
      .where(Listing.status == ListingStatus.UNSOLD)
      .where(Listing.listing_type == ListingType.BUY_NOW)
    ).scalars().all()

    for listing in unsold_buynow:
      console.print(
        f"  #{listing.id}: UNSOLD → SOLD "
        f"(Buy Now items are purchased, not unsold) {action_label}",
      )
      if not dry_run:
        listing.status = ListingStatus.SOLD
        if listing.current_price and not listing.final_price:
          listing.final_price = listing.current_price
      fix_counts["unsold_buynow_to_sold"] += 1

    if not unsold_buynow:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 3: Stale ACTIVE auctions (ended > 48 h ago)
    # ----------------------------------------------------------------
    # Auction listings that are still ACTIVE but whose end time
    # is far in the past — the monitor missed the transition.

    console.print(
      "\n[bold cyan]Check 3:[/bold cyan] "
      "Stale ACTIVE auctions (ended over 48 h ago)",
    )

    stale_active = session.execute(
      select(Listing)
      .where(Listing.status == ListingStatus.ACTIVE)
      .where(Listing.listing_type.in_([
        ListingType.AUCTION, ListingType.HYBRID,
      ]))
      .where(Listing.end_time.isnot(None))
    ).scalars().all()

    stale_found = False
    for listing in stale_active:
      end = _make_aware(listing.end_time)
      if end is None or (now - end) < stale_threshold:
        continue

      stale_found = True
      if listing.final_price is not None or listing.bid_count > 0:
        console.print(
          f"  #{listing.id}: ACTIVE → SOLD "
          f"(ended {end:%Y-%m-%d}, "
          f"{'has final price' if listing.final_price else f'{listing.bid_count} bid(s)'}"
          f") {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.SOLD
          if not listing.final_price and listing.current_price:
            listing.final_price = listing.current_price
        fix_counts["stale_active_to_sold"] += 1
      else:
        console.print(
          f"  #{listing.id}: ACTIVE → UNSOLD "
          f"(ended {end:%Y-%m-%d}, no bids) {action_label}",
        )
        if not dry_run:
          listing.status = ListingStatus.UNSOLD
        fix_counts["stale_active_to_unsold"] += 1

    if not stale_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 4: Leftover images for classifier-rejected listings
    # ----------------------------------------------------------------
    # The old code failed to delete images because of a missing
    # ``pathlib.Path`` import.  Clean up any that remain.

    console.print(
      "\n[bold cyan]Check 4:[/bold cyan] "
      "Leftover images on classifier-rejected listings",
    )

    images_dir = config.images.resolved_directory

    cancelled_listings = session.execute(
      select(Listing)
      .where(Listing.status == ListingStatus.CANCELLED)
    ).scalars().all()

    images_found = False
    for listing in cancelled_listings:
      # Only clean images for classifier rejections.
      rejected_attr = session.execute(
        select(ListingAttribute)
        .where(ListingAttribute.listing_id == listing.id)
        .where(ListingAttribute.attribute_name == "rejected_by_classifier")
        .where(ListingAttribute.attribute_value == "true")
      ).scalar_one_or_none()

      if not rejected_attr:
        continue

      orphan_images = session.execute(
        select(ListingImage)
        .where(ListingImage.listing_id == listing.id)
        .where(ListingImage.local_path.isnot(None))
      ).scalars().all()

      for image in orphan_images:
        images_found = True
        full_path = images_dir / image.local_path
        console.print(
          f"  #{listing.id}: Removing {image.local_path} {action_label}",
        )
        if not dry_run:
          try:
            Path(full_path).unlink(missing_ok=True)
          except Exception:
            console.print(
              f"    [yellow]Warning: could not delete {full_path}[/yellow]",
            )
          image.local_path = None
        fix_counts["images_cleaned"] += 1

    if not images_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 5: Fully fetched listings never classified
    # ----------------------------------------------------------------
    # These listings were fetched before classification was enabled
    # or had an error during classification.  Mark them as unfetched
    # so Phase 2 (initial fetch) will pick them up and run
    # classification.

    console.print(
      "\n[bold cyan]Check 5:[/bold cyan] "
      "Fully fetched listings never classified",
    )

    # Find fetched non-cancelled listings without classifier attributes
    fetched_listings = session.execute(
      select(Listing)
      .where(Listing.is_fully_fetched.is_(True))
      .where(Listing.status != ListingStatus.CANCELLED)
    ).scalars().all()

    unclassified_found = False
    for listing in fetched_listings:
      # Check if it has any classifier attributes
      has_classifier = session.execute(
        select(func.count(ListingAttribute.id))
        .where(ListingAttribute.listing_id == listing.id)
        .where(ListingAttribute.attribute_name.like("classifier_%"))
      ).scalar() > 0

      if not has_classifier:
        unclassified_found = True
        # Sanitize title to avoid encoding errors on Windows console
        title = listing.title[:50] if listing.title else "?"
        # Replace problematic characters that can't encode to cp1252
        title = title.encode("ascii", errors="replace").decode("ascii")
        console.print(
          f"  #{listing.id}: Mark for re-classification ({title}) {action_label}",
        )
        if not dry_run:
          # Reset is_fully_fetched so Phase 2 will re-fetch and classify
          listing.is_fully_fetched = False
        fix_counts["unclassified_marked"] += 1

    if not unclassified_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Check 5b: Classified listings below writing-instrument threshold → CANCELLED
    # ----------------------------------------------------------------
    # Listings that were classified but have a writing-instrument score
    # below the configured threshold should be CANCELLED (rejected by
    # classifier). Fixes e.g. UNKNOWN listings that are lighters, watches, etc.
    threshold = config.classifier.writing_instrument_threshold
    console.print(
      "\n[bold cyan]Check 5b:[/bold cyan] "
      "Classified listings below writing-instrument threshold (%.2f) → CANCELLED",
      threshold,
    )
    score_attrs = session.execute(
      select(ListingAttribute)
      .where(ListingAttribute.attribute_name == "classifier_max_score"),
    ).scalars().all()
    below_threshold_found = False
    for attr in score_attrs:
      listing = session.get(Listing, attr.listing_id)
      if listing is None or listing.status == ListingStatus.CANCELLED:
        continue
      try:
        score = float(attr.attribute_value or "0")
      except (TypeError, ValueError):
        continue
      if score < threshold:
        below_threshold_found = True
        title = (listing.title[:50] if listing.title else "?")
        title = title.encode("ascii", errors="replace").decode("ascii")
        console.print(
          f"  #{listing.id}: {listing.status.value} → CANCELLED "
          f"(score %.4f < %.2f: %s) {action_label}",
          score, threshold, title,
        )
        if not dry_run:
          listing.status = ListingStatus.CANCELLED
          existing = session.execute(
            select(ListingAttribute)
            .where(ListingAttribute.listing_id == listing.id)
            .where(ListingAttribute.attribute_name == "rejected_by_classifier"),
          ).scalar_one_or_none()
          if existing is None:
            session.add(
              ListingAttribute(
                listing_id=listing.id,
                attribute_name="rejected_by_classifier",
                attribute_value="true",
              ),
            )
        fix_counts["classified_below_threshold_cancelled"] += 1
    if not below_threshold_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 6: SOLD/UNSOLD listings whose end time is still in the future
    # ----------------------------------------------------------------
    # A scraper bug (e.g. false-positive "ended" text in the seller
    # description) can mark active auctions as SOLD or UNSOLD even
    # though their end time hasn't passed yet.  Reset them to ACTIVE
    # and clear the final_price so the monitor can track them properly.

    console.print(
      "\n[bold cyan]Check 6:[/bold cyan] "
      "SOLD/UNSOLD listings whose end time is still in the future",
    )

    premature_ended = session.execute(
      select(Listing)
      .where(Listing.status.in_([ListingStatus.SOLD, ListingStatus.UNSOLD]))
      .where(Listing.end_time.isnot(None))
    ).scalars().all()

    # A small grace period avoids flipping listings that just ended
    # moments ago and may have slight clock drift.
    grace = timedelta(minutes=5)
    premature_found = False

    for listing in premature_ended:
      end = _make_aware(listing.end_time)
      if end is None or end <= now + grace:
        continue

      premature_found = True
      old_status = listing.status.value.upper()
      remaining = end - now
      remaining_label = (
        f"{remaining.days}d {remaining.seconds // 3600}h"
        if remaining.days > 0
        else f"{remaining.seconds // 3600}h {(remaining.seconds % 3600) // 60}m"
      )
      title = listing.title[:50] if listing.title else "?"
      title = title.encode("ascii", errors="replace").decode("ascii")

      console.print(
        f"  #{listing.id}: {old_status} → ACTIVE "
        f"(ends in {remaining_label}: {title}) {action_label}",
      )
      if not dry_run:
        listing.status = ListingStatus.ACTIVE
        listing.final_price = None
      if old_status == "SOLD":
        fix_counts["premature_sold_to_active"] += 1
      else:
        fix_counts["premature_unsold_to_active"] += 1

    if not premature_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 7: LiveAuctioneers listings with end_time == start_time
    # ----------------------------------------------------------------
    # The LiveAuctioneers scraper had a bug where it would fall back
    # to using start_time as end_time when lotEndTimeEstimatedTs was
    # missing. This caused listings to have incorrect end dates and
    # often get marked as UNSOLD when they're actually UPCOMING or
    # ACTIVE. Mark these for re-fetch.

    console.print(
      "\n[bold cyan]Check 7:[/bold cyan] "
      "LiveAuctioneers listings with incorrect end times (end_time == start_time)",
    )

    # Find LiveAuctioneers listings where end_time equals start_time
    # (excluding cases where both are genuinely None).
    from auction_tracker.database.models import Website as WebsiteModel

    liveauctioneers_website = session.execute(
      select(WebsiteModel)
      .where(func.lower(WebsiteModel.name) == "liveauctioneers")
    ).scalar_one_or_none()

    bad_endtime_found = False
    if liveauctioneers_website:
      suspicious_listings = session.execute(
        select(Listing)
        .where(Listing.website_id == liveauctioneers_website.id)
        .where(Listing.start_time.isnot(None))
        .where(Listing.end_time.isnot(None))
        .where(Listing.start_time == Listing.end_time)
        .where(Listing.is_fully_fetched == True)  # Only fix listings that haven't been marked yet
      ).scalars().all()

      for listing in suspicious_listings:
        bad_endtime_found = True
        title = listing.title[:50] if listing.title else "?"
        title = title.encode("ascii", errors="replace").decode("ascii")

        console.print(
          f"  #{listing.id}: Mark for re-fetch (end_time == start_time: "
          f"{listing.end_time:%Y-%m-%d}, {listing.status.value.upper()}, {title}) {action_label}",
        )
        if not dry_run:
          # Reset is_fully_fetched so the listing will be re-fetched
          # with the corrected scraper logic.
          listing.is_fully_fetched = False
          # If it's marked as UNSOLD due to the bad date, reset to UNKNOWN
          # so the re-fetch can determine the correct status.
          if listing.status == ListingStatus.UNSOLD:
            listing.status = ListingStatus.UNKNOWN
        fix_counts["liveauctioneers_bad_endtime"] += 1

    if not bad_endtime_found:
      console.print("  [dim]No issues found.[/dim]")

    # ----------------------------------------------------------------
    # Fix 8: eBay Buy Now listings incorrectly marked as UNSOLD
    # ----------------------------------------------------------------
    # The eBay scraper had a bug where Buy Now listings with
    # auctionPossible=true were classified as AUCTION instead of
    # BUY_NOW. This caused them to be marked as UNSOLD when their
    # end_time passed, even though they're still available for purchase.

    console.print(
      "\n[bold cyan]Check 8:[/bold cyan] "
      "eBay Buy Now listings incorrectly marked as UNSOLD",
    )

    # Find the eBay website.
    ebay_website = session.execute(
      select(WebsiteModel)
      .where(func.lower(WebsiteModel.name) == "ebay")
    ).scalar_one_or_none()

    buynow_unsold_found = False
    if ebay_website:
      # Find UNSOLD eBay listings that have a Buy Now price.
      # These are likely fixed-price listings that were incorrectly
      # marked as UNSOLD when they should be ACTIVE (still available).
      unsold_buynow = session.execute(
        select(Listing)
        .where(Listing.website_id == ebay_website.id)
        .where(Listing.status == ListingStatus.UNSOLD)
        .where(Listing.buy_now_price.isnot(None))
        .where(Listing.is_fully_fetched == True)  # Only fix listings that haven't been marked yet
      ).scalars().all()

      for listing in unsold_buynow:
        # Additional check: look for quantity_available attribute > 0.
        # If quantity is still available, it's definitely still for sale.
        qty_attr = session.execute(
          select(ListingAttribute)
          .where(ListingAttribute.listing_id == listing.id)
          .where(ListingAttribute.attribute_name == "quantity_available")
        ).scalar_one_or_none()

        # If we have quantity info and it's > 0, definitely fix it.
        # If we don't have quantity info, be conservative and mark for re-fetch.
        should_fix = True
        if qty_attr:
          try:
            qty = int(qty_attr.attribute_value)
            should_fix = qty > 0
          except (ValueError, TypeError):
            pass

        if should_fix:
          buynow_unsold_found = True
          title = listing.title[:50] if listing.title else "?"
          title = title.encode("ascii", errors="replace").decode("ascii")

          qty_info = f" (qty: {qty_attr.attribute_value})" if qty_attr else ""
          console.print(
            f"  #{listing.id}: UNSOLD → Mark for re-fetch "
            f"(Buy Now listing{qty_info}: {title}) {action_label}",
          )
          if not dry_run:
            # Mark for re-fetch so it can be properly classified as BUY_NOW
            # and get the correct status.
            listing.is_fully_fetched = False
          fix_counts["ebay_buynow_unsold"] += 1

    if not buynow_unsold_found:
      console.print("  [dim]No issues found.[/dim]")

    # Flush all changes.
    if not dry_run:
      session.flush()

  # Clear manual cancel file after successful commit (so we do not lose IDs on failure).
  if not dry_run and fix_counts.get("manual_cancelled", 0) > 0 and manual_cancel_path.exists():
    try:
      with open(manual_cancel_path, "w", encoding="utf-8") as file_handle:
        file_handle.write("")
    except OSError:
      console.print(
        "[yellow]Warning: could not clear manual_cancel_listings.txt[/yellow]",
      )

  # ----------------------------------------------------------------
  # ----------------------------------------------------------------
  # Check 9: UPCOMING/ACTIVE with end_time in the past (mark for re-fetch)
  # ----------------------------------------------------------------
  # These were fetched when the sale was still in the future. The
  # watch loop would re-fetch them after end_time, but if "watch" was
  # not running they were never updated. Mark for re-fetch so the
  # discovery loop (or next watch run) will fetch and get SOLD/UNSOLD.
  with session_scope() as session:
    stale_upcoming = session.execute(
      select(Listing)
      .where(Listing.status.in_([ListingStatus.UPCOMING, ListingStatus.ACTIVE]))
      .where(Listing.is_fully_fetched.is_(True))
      .where(Listing.end_time.isnot(None)),
    ).scalars().all()

    if stale_upcoming:
      console.print(
        "\n[bold cyan]Check 9:[/bold cyan] "
        "UPCOMING/ACTIVE listings whose end time has passed (mark for re-fetch)",
      )
    for listing in stale_upcoming:
      end = _make_aware(listing.end_time)
      if end is None or end >= now:
        continue
      title = (listing.title[:50] if listing.title else "?")
      title = title.encode("ascii", errors="replace").decode("ascii")
      console.print(
        f"  #{listing.id}: UPCOMING/ACTIVE with end in past "
        f"({title}) → mark for re-fetch {action_label}",
      )
      if not dry_run:
        listing.is_fully_fetched = False
      fix_counts["stale_upcoming_marked"] += 1

  # ----------------------------------------------------------------
  # Check 10: Drouot listings to re-fetch (fix mistaken UNSOLD / wrong status)
  # ----------------------------------------------------------------
  # Drouot listings that are UNSOLD or have end_time in the past may have
  # been mis-scraped. Mark for re-fetch so discover will fetch again.
  # Never touch CANCELLED (classifier-rejected or manually cancelled).
  with session_scope() as session:
    drouot_websites = session.execute(
      select(Website).where(
        or_(
          func.lower(Website.name).like("%drouot%"),
          func.lower(Website.base_url).like("%drouot%"),
        ),
      ),
    ).scalars().all()
    for drouot_website in drouot_websites:
      # Skip Gazette Drouot - it is historical only.
      if "gazette" in drouot_website.name.lower():
          continue

      drouot_listings = session.execute(
        select(Listing)
        .where(Listing.website_id == drouot_website.id)
        .where(Listing.status != ListingStatus.CANCELLED)
        # Idempotency: skip listings already marked for re-fetch (UNKNOWN)
        .where(Listing.status != ListingStatus.UNKNOWN)
        .where(
          or_(
            Listing.status == ListingStatus.UNSOLD,
            Listing.end_time.isnot(None),
          ),
        ),
      ).scalars().all()
      if drouot_listings:
        console.print(
          "\n[bold cyan]Check 10:[/bold cyan] "
          "Drouot listings to re-fetch (UNSOLD or past end)",
        )
      for listing in drouot_listings:
        if listing.status == ListingStatus.CANCELLED:
          continue
        end = _make_aware(listing.end_time) if listing.end_time else None
        if listing.status == ListingStatus.UNSOLD or (end is not None and end < now):
          title = (listing.title[:50] if listing.title else "?")
          title = title.encode("ascii", errors="replace").decode("ascii")
          console.print(
            f"  #{listing.id}: {listing.status.value} "
            f"({title}) -> mark for re-fetch + UNKNOWN {action_label}",
          )
          if not dry_run:
            listing.is_fully_fetched = False
            listing.status = ListingStatus.UNKNOWN
          fix_counts["drouot_mark_refetch"] += 1

  # ----------------------------------------------------------------
  # Summary
  # ----------------------------------------------------------------

  total_fixes = sum(fix_counts.values())
  if total_fixes == 0:
    console.print("\n[green]No inconsistencies found. Database is clean.[/green]")
  else:
    verb = "Would fix" if dry_run else "Fixed"
    console.print(f"\n[bold]{verb} {total_fixes} issue(s):[/bold]")
    friendly_labels = {
      "manual_cancelled": "CANCELLED (manual, from web UI)",
      "stale_upcoming_marked": "UPCOMING/ACTIVE past end → mark for re-fetch",
      "unknown_to_cancelled": "UNKNOWN → CANCELLED (classifier-rejected)",
      "unknown_to_active": "UNKNOWN → ACTIVE",
      "unknown_to_upcoming": "UNKNOWN → UPCOMING",
      "unknown_to_sold": "UNKNOWN → SOLD",
      "unknown_to_unsold": "UNKNOWN → UNSOLD",
      "unsold_buynow_to_sold": "UNSOLD → SOLD (Buy Now)",
      "stale_active_to_sold": "ACTIVE → SOLD (stale auction)",
      "stale_active_to_unsold": "ACTIVE → UNSOLD (stale auction)",
      "premature_sold_to_active": "SOLD → ACTIVE (end time still in the future)",
      "premature_unsold_to_active": "UNSOLD → ACTIVE (end time still in the future)",
      "images_cleaned": "Orphan images removed",
      "unclassified_marked": "Listings marked for re-classification",
      "liveauctioneers_bad_endtime": "LiveAuctioneers listings with incorrect end times marked for re-fetch",
      "ebay_buynow_unsold": "eBay Buy Now listings incorrectly marked as UNSOLD",
      "drouot_mark_refetch": "Drouot listings (UNSOLD or past end) marked for re-fetch",
      "classified_below_threshold_cancelled": "Classified below writing-instrument threshold → CANCELLED",
    }
    for key, count in fix_counts.items():
      if count > 0:
        label = friendly_labels.get(key, key)
        console.print(f"  {label}: [bold]{count}[/bold]")

    if dry_run:
      console.print(
        "\n[yellow]No changes were made. "
        "Run without --dry-run to apply fixes.[/yellow]",
      )


# ------------------------------------------------------------------
# history
# ------------------------------------------------------------------

@cli.command("history")
@click.argument("query", required=False, default=None)
@click.option(
  "--website", "-w", multiple=True,
  help="Historical scraper(s) to use. Defaults to all historical scrapers.",
)
@click.option("--limit", "-n", default=50, type=int, help="Max results per query (default: 50).")
@click.option(
  "--dry-run", is_flag=True, default=False,
  help="Search and display results without storing them.",
)
@click.pass_context
def run_history(context, query, website, limit, dry_run):
  """Search historical / post-auction data sources.

  Runs saved search queries on historical-only scrapers (e.g. Gazette
  Drouot) that are excluded from the normal discover loop.

  If QUERY is given, runs that single ad-hoc search instead of the
  saved queries from the database.

  Examples:

  \b
    auction_tracker history
    auction_tracker history "waterman carene" --limit 10
    auction_tracker history -w gazette_drouot --dry-run
  """
  import time
  from auction_tracker.database.repository import (
    add_listing_search_source,
    get_active_search_queries,
    get_or_create_listing,
    get_or_create_website,
  )
  from auction_tracker.monitor import Monitor
  from auction_tracker.scrapers.registry import ScraperRegistry

  config = context.obj["config"]

  # ------------------------------------------------------------------
  # Determine which scrapers to use
  # ------------------------------------------------------------------
  if website:
    scraper_names = list(website)
    for name in scraper_names:
      if ScraperRegistry.get(name) is None:
        available = ", ".join(ScraperRegistry.list_registered())
        console.print(f"[red]Unknown scraper '{name}'. Available: {available}[/red]")
        sys.exit(1)
  else:
    # Auto-discover: all scrapers flagged as historical-only.
    scraper_names = []
    for name in ScraperRegistry.list_registered():
      try:
        scraper_cls = ScraperRegistry.get(name)
        if scraper_cls is not None:
          # Instantiate temporarily to check capabilities.
          tmp = scraper_cls(config.scraping)
          if tmp.capabilities.exclude_from_discover:
            scraper_names.append(name)
      except Exception:
        pass

  if not scraper_names:
    console.print(
      "[yellow]No historical scrapers found. "
      "Register a scraper with exclude_from_discover=True.[/yellow]"
    )
    return

  # ------------------------------------------------------------------
  # Determine queries
  # ------------------------------------------------------------------
  if query:
    queries = [(query, None)]
  else:
    with session_scope() as session:
      db_queries = get_active_search_queries(session)
      queries = [(q.query_text, q.category) for q in db_queries]

  if not queries:
    console.print(
      "[yellow]No queries found. Pass a query argument or create "
      "saved searches with 'auction_tracker search ... --save NAME'.[/yellow]"
    )
    return

  console.print(
    f"[bold]Running {len(queries)} query(ies) on "
    f"{len(scraper_names)} historical scraper(s): "
    f"{', '.join(scraper_names)}[/bold]"
  )
  console.print(f"[dim]Limit: {limit} | Dry run: {dry_run}[/dim]")

  monitor = None if dry_run else Monitor(config)
  total_found = 0
  total_stored = 0
  total_errors = 0

  for scraper_name in scraper_names:
    scraper = ScraperRegistry.create(scraper_name, config.scraping)

    # Start browser if the scraper has one.
    if hasattr(scraper, "start_browser"):
      console.print(f"\n[dim]Starting browser for {scraper_name}…[/dim]")
      scraper.start_browser()

    try:
      for query_text, category in queries:
        console.print(f"\n[cyan]{'-' * 50}[/cyan]")
        console.print(f"[bold]{scraper_name}[/bold] < [cyan]{query_text}[/cyan]")

        # Search
        try:
          results = scraper.search_past(query_text, limit=limit)
        except Exception as exc:
          console.print(f"  [red]Search failed: {exc}[/red]")
          total_errors += 1
          continue

        total_found += len(results)
        console.print(f"  Found {len(results)} result(s).")

        if dry_run:
          for i, r in enumerate(results, 1):
            price_str = (
              f"{r.current_price} {r.currency}"
              if r.current_price else "-"
            )
            console.print(
              f"  [{i}] {r.external_id}  {r.title[:60]}  {price_str}"
            )
          continue

        # Store basic search results (fast, lightweight).
        with session_scope() as session:
          ws = get_or_create_website(
            session,
            name=scraper.website_name,
            base_url=scraper.website_base_url,
          )
          for r in results:
            listing, _ = get_or_create_listing(
              session,
              website_id=ws.id,
              external_id=r.external_id,
              defaults={
                "url": r.url,
                "title": r.title,
                "currency": r.currency,
                "current_price": r.current_price,
                "listing_type": r.listing_type,
                "status": ListingStatus.UNKNOWN,
                "end_time": r.end_time,
                "is_fully_fetched": False,
              },
            )
            add_listing_search_source(
              session,
              listing_id=listing.id,
              search_query_text=query_text,
            )

        # Fetch full details and persist.
        for i, result in enumerate(results, 1):
          # Pre-filter: skip detail page fetch/Cloudflare if title is irrelevant.
          if hasattr(scraper, "is_relevant_title"):
            if not scraper.is_relevant_title(result.title):
              console.print(
                f"  [dim][{i}/{len(results)}] Skipping {result.external_id} "
                f"(irrelevant: {result.title[:30]}…)[/dim]"
              )
              continue

          console.print(
            f"  [{i}/{len(results)}] Fetching {result.external_id}…",
            end=" ",
          )
          try:
            listing = scraper.fetch_listing(result.url)
            db_id = monitor._store_scraped_listing(scraper, listing)
            total_stored += 1

            with session_scope() as session:
              add_listing_search_source(
                session,
                listing_id=db_id,
                search_query_text=query_text,
              )

            price = listing.final_price or listing.current_price
            console.print(
              f"[green]OK[/green] {listing.status.value} "
              f"{price or '-'} {listing.currency}"
            )
          except Exception as exc:
            console.print(f"[red]ERR {exc}[/red]")
            total_errors += 1

        # Pause between queries.
        if (query_text, category) != queries[-1]:
          time.sleep(5)

    finally:
      if hasattr(scraper, "stop_browser"):
        scraper.stop_browser()

  # Summary
  console.print(f"\n[cyan]{'-' * 50}[/cyan]")
  summary = f"[bold]Done.[/bold] Found: {total_found}"
  if not dry_run:
    summary += f", Stored: {total_stored}"
  if total_errors:
    summary += f", [red]Errors: {total_errors}[/red]"
  console.print(summary)


# ------------------------------------------------------------------
# viewer
# ------------------------------------------------------------------

@cli.command("viewer")
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", "-p", default=5000, type=int, help="Port to listen on.")
@click.option("--debug/--no-debug", default=False, help="Enable Flask debug mode.")
@click.pass_context
def run_viewer(context, host, port, debug):
  """Launch the web-based database viewer."""
  from auction_tracker.web.app import create_app

  config = context.obj["config"]
  app = create_app(config_path=None)
  console.print(
    f"[bold green]Starting viewer at http://{host}:{port}/[/bold green]"
  )
  app.run(host=host, port=port, debug=debug)


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
  cli()
