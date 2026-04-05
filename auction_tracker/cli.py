"""CLI entry point using Click.

All commands share a context object that holds the configuration,
database engine, and repository. Async commands use asyncio.run().
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from auction_tracker.config import AppConfig, load_config
from auction_tracker.database.engine import DatabaseEngine
from auction_tracker.database.repository import Repository
from auction_tracker.logging_setup import setup_logging

logger = logging.getLogger(__name__)
console = Console()


class AppContext:
  """Shared state for all CLI commands."""

  def __init__(self, config: AppConfig) -> None:
    self.config = config
    self.database = DatabaseEngine(config.database.path)
    self.repository = Repository()


pass_context = click.make_pass_decorator(AppContext)


@click.group()
@click.option(
  "--config", "config_path",
  type=click.Path(exists=False, path_type=Path),
  default=None,
  help="Path to YAML configuration file.",
)
@click.option(
  "--verbose", is_flag=True, default=False,
  help="Enable debug logging.",
)
@click.pass_context
def main(ctx: click.Context, config_path: Path | None, verbose: bool) -> None:
  """AuctionTracker v2 — Fountain pen auction monitor."""
  config = load_config(config_path)
  if verbose:
    config.logging.level = "DEBUG"

  setup_logging(
    level=config.logging.level,
    log_file=config.logging.file,
    max_bytes=config.logging.max_bytes,
    backup_count=config.logging.backup_count,
  )

  app = AppContext(config)
  app.database.initialize()
  ctx.ensure_object(dict)
  ctx.obj = app


# -------------------------------------------------------------------
# Database commands
# -------------------------------------------------------------------


@main.command("init-db")
@pass_context
def init_database(app: AppContext) -> None:
  """Initialize the database (creates tables if needed)."""
  console.print("[green]Database initialized successfully.[/green]")
  console.print(f"  Path: {app.config.database.path}")


@main.command("seed-websites")
@pass_context
def seed_websites(app: AppContext) -> None:
  """Seed the database with the configured websites."""
  website_urls = {
    "ebay": "https://www.ebay.com",
    "catawiki": "https://www.catawiki.com",
    "leboncoin": "https://www.leboncoin.fr",
    "drouot": "https://www.drouot.com",
    "interencheres": "https://www.interencheres.com",
    "liveauctioneers": "https://www.liveauctioneers.com",
    "invaluable": "https://www.invaluable.com",
    "yahoo_japan": "https://auctions.yahoo.co.jp",
    "gazette_drouot": "https://www.gazette-drouot.com",
  }

  with app.database.session() as session:
    for name, url in website_urls.items():
      app.repository.get_or_create_website(
        session, name=name, base_url=url,
        default_currency=("JPY" if name == "yahoo_japan" else "EUR"),
      )
    session.commit()

  console.print(f"[green]Seeded {len(website_urls)} websites.[/green]")


# -------------------------------------------------------------------
# Information commands
# -------------------------------------------------------------------


@main.command("websites")
@pass_context
def list_websites(app: AppContext) -> None:
  """List all configured websites and their status."""
  table = Table(title="Websites")
  table.add_column("Name", style="cyan")
  table.add_column("Transport", style="yellow")
  table.add_column("Strategy", style="green")
  table.add_column("Enabled", style="bold")
  table.add_column("Parser", style="magenta")

  from auction_tracker.parsing.base import ParserRegistry

  for name, website_config in sorted(app.config.websites.items()):
    has_parser = ParserRegistry.has(name)
    table.add_row(
      name,
      website_config.transport.value,
      website_config.monitoring_strategy.value,
      "yes" if website_config.enabled else "no",
      "yes" if has_parser else "[red]no[/red]",
    )

  console.print(table)


@main.command("parsers")
@pass_context
def list_parsers(app: AppContext) -> None:
  """List all registered parsers and their capabilities."""
  from auction_tracker.parsing.base import ParserRegistry

  table = Table(title="Registered Parsers")
  table.add_column("Website", style="cyan")
  table.add_column("Search", style="green")
  table.add_column("Listing", style="green")
  table.add_column("Bids", style="yellow")
  table.add_column("Seller", style="yellow")

  for name in ParserRegistry.list_registered():
    parser = ParserRegistry.get(name)
    capabilities = parser.capabilities
    table.add_row(
      name,
      _yes_no(capabilities.can_search),
      _yes_no(capabilities.can_parse_listing),
      _yes_no(capabilities.has_bid_history),
      _yes_no(capabilities.has_seller_info),
    )

  console.print(table)


@main.command("listings")
@click.option("--status", type=str, default=None, help="Filter by status.")
@click.option("--website", type=str, default=None, help="Filter by website.")
@click.option("--limit", type=int, default=50, help="Max results.")
@pass_context
def list_listings(app: AppContext, status: str | None, website: str | None, limit: int) -> None:
  """List tracked listings."""
  from sqlalchemy import select

  from auction_tracker.database.models import Listing, ListingStatus, Website

  with app.database.session() as session:
    statement = select(Listing).limit(limit)
    if website:
      statement = statement.join(Website).where(Website.name == website)
    if status:
      try:
        status_enum = ListingStatus(status)
        statement = statement.where(Listing.status == status_enum)
      except ValueError:
        console.print(f"[red]Unknown status: {status}[/red]")
        return
    statement = statement.order_by(Listing.updated_at.desc())

    listings = session.scalars(statement).all()

    table = Table(title=f"Listings ({len(listings)})")
    table.add_column("ID", style="dim")
    table.add_column("Website", style="cyan")
    table.add_column("Title", max_width=50)
    table.add_column("Price", style="green", justify="right")
    table.add_column("Status", style="yellow")
    table.add_column("End Time")

    for listing in listings:
      price = listing.final_price or listing.current_price
      price_str = f"{price} {listing.currency}" if price else "-"
      end_str = listing.end_time.strftime("%Y-%m-%d %H:%M") if listing.end_time else "-"
      table.add_row(
        str(listing.id),
        listing.website.name,
        listing.title[:50],
        price_str,
        listing.status.value,
        end_str,
      )

    console.print(table)


# -------------------------------------------------------------------
# Operational commands
# -------------------------------------------------------------------


@main.command("discover")
@click.option("--website", type=str, default=None, help="Only discover for this website.")
@click.option("--fetch/--no-fetch", default=True, help="Fetch full details for new listings.")
@pass_context
def discover(app: AppContext, website: str | None, fetch: bool) -> None:
  """Run saved searches to discover new listings."""
  asyncio.run(_discover_async(app, website, fetch))


async def _discover_async(app: AppContext, website: str | None, fetch: bool) -> None:
  from auction_tracker.orchestrator.discovery import DiscoveryLoop
  from auction_tracker.transport.router import TransportRouter

  async with TransportRouter(app.config) as router:
    loop = DiscoveryLoop(app.config, router, app.repository)
    with app.database.session() as session:
      stats, _new_listings = await loop.run_all(session, website_filter=website)
      console.print(
        f"[green]Discovery complete:[/green] "
        f"{stats.searches_run} searches, "
        f"{stats.results_found} results, "
        f"{stats.new_listings} new listings, "
        f"{stats.errors} errors"
      )

      if fetch and stats.new_listings > 0:
        fetch_stats = await loop.fetch_unfetched(session, website_filter=website)
        console.print(
          f"[green]Fetch complete:[/green] "
          f"{fetch_stats.listings_fetched} fetched, "
          f"{fetch_stats.listings_classified} classified, "
          f"{fetch_stats.listings_rejected} rejected"
        )


@main.command("watch")
@click.option("--website", type=str, default=None, help="Only watch this website.")
@click.option("--once", is_flag=True, default=False, help="Run a single pass instead of looping.")
@pass_context
def watch(app: AppContext, website: str | None, once: bool) -> None:
  """Monitor active listings for price changes and status updates."""
  asyncio.run(_watch_async(app, website, once))


async def _watch_async(app: AppContext, website: str | None, once: bool) -> None:
  from auction_tracker.orchestrator.watcher import Watcher
  from auction_tracker.transport.router import TransportRouter

  async with TransportRouter(app.config) as router:
    watcher = Watcher(app.config, app.database, router, app.repository)
    with app.database.session() as session:
      count = watcher.load_active_listings(session)
      console.print(f"[green]Loaded {count} active listings into watch queue.[/green]")

    if once:
      stats = await watcher.run_once()
      console.print(
        f"[green]Watch pass complete:[/green] "
        f"{stats.checks_performed} checked, "
        f"{stats.listings_updated} updated, "
        f"{stats.listings_completed} completed, "
        f"{stats.errors} errors"
      )
    else:
      console.print("[green]Starting continuous watch loop (Ctrl+C to stop)...[/green]")
      stop_event = asyncio.Event()
      try:
        await watcher.run_forever(stop_event)
      except KeyboardInterrupt:
        stop_event.set()
        console.print("\n[yellow]Watch loop stopped.[/yellow]")


@main.command("queue")
@pass_context
def show_queue(app: AppContext) -> None:
  """Show the current watch queue status."""
  from auction_tracker.orchestrator.watcher import Watcher
  from auction_tracker.transport.router import TransportRouter

  watcher = Watcher(app.config, app.database, TransportRouter(app.config), app.repository)
  with app.database.session() as session:
    watcher.load_active_listings(session)

  entries = watcher.get_queue_status()
  table = Table(title=f"Watch Queue ({len(entries)} listings)")
  table.add_column("ID", style="dim")
  table.add_column("Website", style="cyan")
  table.add_column("External ID", max_width=20)
  table.add_column("Strategy", style="green")
  table.add_column("Phase", style="yellow")
  table.add_column("Next In", justify="right")
  table.add_column("Failures", style="red", justify="right")

  for entry in entries:
    next_in = entry["next_check_in"]
    if next_in > 86400:
      time_str = f"{next_in / 86400:.1f}d"
    elif next_in > 3600:
      time_str = f"{next_in / 3600:.1f}h"
    elif next_in > 60:
      time_str = f"{next_in / 60:.0f}m"
    else:
      time_str = f"{next_in:.0f}s"

    table.add_row(
      str(entry["listing_id"]),
      entry["website"],
      entry["external_id"],
      entry["strategy"],
      entry["phase"],
      time_str,
      str(entry["consecutive_failures"]),
    )

  console.print(table)


@main.command("fetch")
@click.argument("url")
@click.option("--website", type=str, required=True, help="Website name (e.g. ebay, catawiki).")
@pass_context
def fetch_listing(app: AppContext, url: str, website: str) -> None:
  """Fetch and ingest a single listing by URL."""
  asyncio.run(_fetch_async(app, url, website))


async def _fetch_async(app: AppContext, url: str, website: str) -> None:
  from auction_tracker.orchestrator.ingest import Ingest
  from auction_tracker.orchestrator.utils import fetch_and_parse_listing
  from auction_tracker.parsing.base import ParserBlocked, ParserRegistry
  from auction_tracker.transport.router import TransportRouter

  if not ParserRegistry.has(website):
    console.print(f"[red]No parser registered for '{website}'[/red]")
    return

  parser = ParserRegistry.get(website)

  async with TransportRouter(app.config) as router:
    try:
      _result, scraped = await fetch_and_parse_listing(router, parser, website, url)
    except ParserBlocked as blocked:
      console.print(
        f"[red]All domains returned a blocked/challenge page for {blocked.url}[/red]"
      )
      return

    with app.database.session() as session:
      website_obj = app.repository.get_website_by_name(session, website)
      if website_obj is None:
        console.print(f"[red]Website '{website}' not in database. Run seed-websites first.[/red]")
        return

      ingest = Ingest(app.repository)
      _listing, is_new = ingest.ingest_listing(session, website_obj.id, scraped)
      session.commit()

      action = "Created" if is_new else "Updated"
      console.print(f"[green]{action} listing:[/green] {scraped.title}")
      console.print(f"  Price: {scraped.current_price or '?'} {scraped.currency}")
      console.print(f"  Status: {scraped.status or 'unknown'}")


@main.command("search")
@click.argument("query")
@click.option("--website", type=str, multiple=True, help="Restrict to specific website(s).")
@click.option("--save", is_flag=True, default=False, help="Save the search query globally.")
@click.option("--fetch/--no-fetch", default=False, help="Fetch full details for results.")
@pass_context
def search(
  app: AppContext,
  query: str,
  website: tuple[str, ...],
  save: bool,
  fetch: bool,
) -> None:
  """Search for listings across all enabled websites."""
  asyncio.run(_search_async(app, query, website, save, fetch))


async def _search_async(
  app: AppContext,
  query: str,
  websites: tuple[str, ...],
  save: bool,
  fetch: bool,
) -> None:
  from auction_tracker.orchestrator.ingest import Ingest
  from auction_tracker.parsing.base import ParserRegistry
  from auction_tracker.transport.router import TransportRouter

  target_websites = list(websites) if websites else ParserRegistry.list_registered()

  async with TransportRouter(app.config) as router:
    ingest = Ingest(app.repository)

    for website_name in target_websites:
      if not ParserRegistry.has(website_name):
        continue

      parser = ParserRegistry.get(website_name)
      if not parser.capabilities.can_search:
        continue

      website_config = app.config.website(website_name)
      if not website_config.enabled:
        continue

      console.print(f"\n[cyan]Searching {website_name}...[/cyan]")

      try:
        search_url = parser.build_search_url(query)
        result = await router.fetch(website_name, search_url)
        search_results = parser.parse_search_results(result.html, url=search_url)

        console.print(f"  Found {len(search_results)} results")

        with app.database.session() as session:
          website_obj = app.repository.get_website_by_name(session, website_name)
          if website_obj is None:
            continue

          for scraped_result in search_results:
            _listing, is_new = ingest.ingest_search_result(
              session, website_obj.id, scraped_result,
            )
            marker = " [NEW]" if is_new else ""
            price_str = f"{scraped_result.current_price} {scraped_result.currency}" if scraped_result.current_price else "?"
            console.print(f"    {scraped_result.title[:60]} — {price_str}{marker}")

          session.commit()

      except Exception as error:
        console.print(f"  [red]Error: {error}[/red]")
        logger.error("Search error on %s: %s", website_name, error, exc_info=True)

  if save:
    with app.database.session() as session:
      app.repository.upsert_search_query(session, name=query, query_text=query)
      session.commit()
    console.print(f"\n[green]Saved search:[/green] '{query}' (runs on all websites)")


# -------------------------------------------------------------------
# Saved searches management
# -------------------------------------------------------------------


@main.command("searches")
@pass_context
def list_searches(app: AppContext) -> None:
  """List all saved search queries."""
  with app.database.session() as session:
    searches = app.repository.get_active_searches(session)
    table = Table(title=f"Saved Searches ({len(searches)})")
    table.add_column("ID", style="dim")
    table.add_column("Name", style="cyan")
    table.add_column("Query", style="green")
    table.add_column("Last Run")
    table.add_column("Results", justify="right")

    for search_query in searches:
      last_run = search_query.last_run_at.strftime("%Y-%m-%d %H:%M") if search_query.last_run_at else "never"
      table.add_row(
        str(search_query.id),
        search_query.name,
        search_query.query_text,
        last_run,
        str(search_query.result_count or 0),
      )

    console.print(table)


@main.command("add-search")
@click.argument("query")
@click.option("--name", type=str, default=None, help="Name for the search (defaults to query text).")
@pass_context
def add_search(app: AppContext, query: str, name: str | None) -> None:
  """Add a saved search query (runs on all enabled websites)."""
  if name is None:
    name = query

  with app.database.session() as session:
    app.repository.upsert_search_query(session, name=name, query_text=query)
    session.commit()
  console.print(f"[green]Added search:[/green] '{name}'")


# -------------------------------------------------------------------
# Full pipeline command
# -------------------------------------------------------------------


@main.command("run")
@click.option("--website", type=str, default=None, help="Only process this website.")
@click.option("--no-classify", is_flag=True, default=False, help="Skip image classification.")
@click.option("--once", is_flag=True, default=False, help="Single pass instead of continuous loop.")
@click.option(
  "--discover-interval",
  type=int,
  default=30,
  show_default=True,
  help="Minutes between re-discovery runs in continuous mode.",
)
@click.option(
  "--max-fetch-per-cycle",
  type=int,
  default=50,
  show_default=True,
  help="Max listings to fully fetch per discovery cycle (rest deferred to next cycle).",
)
@pass_context
def run_pipeline(
  app: AppContext,
  website: str | None,
  no_classify: bool,
  once: bool,
  discover_interval: int,
  max_fetch_per_cycle: int,
) -> None:
  """Run the full pipeline: discover, fetch, classify, and watch.

  In continuous mode (default) the pipeline re-runs saved searches
  every --discover-interval minutes so that new listings are found
  throughout the day, while the watch loop monitors existing listings
  concurrently.
  """
  asyncio.run(
    _run_pipeline_async(
      app, website, not no_classify, once, discover_interval, max_fetch_per_cycle,
    )
  )


async def _run_pipeline_async(
  app: AppContext,
  website: str | None,
  classify: bool,
  once: bool,
  discover_interval: int,
  max_fetch_per_cycle: int,
) -> None:
  from auction_tracker.orchestrator.discovery import DiscoveryLoop
  from auction_tracker.orchestrator.metrics import LiveStatus, MetricsCollector
  from auction_tracker.orchestrator.watcher import Watcher
  from auction_tracker.transport.router import TransportRouter

  metrics = MetricsCollector(app.database)
  metrics.pipeline_started()

  status_path = app.config.database.path.parent / "pipeline_status.json"
  live = LiveStatus(status_path)
  live.start()

  # Discovery and watcher use separate transport routers so their
  # per-domain rate limiters are fully independent. Without this, a
  # discovery fetch burst to eBay would delay the watcher's urgent
  # eBay checks by the same rate-limit window.
  async with (
    TransportRouter(app.config) as discovery_router,
    TransportRouter(app.config) as watcher_router,
  ):
    discovery = DiscoveryLoop(
      app.config, discovery_router, app.repository,
      metrics=metrics, live=live,
    )
    watcher = Watcher(
      app.config, app.database, watcher_router, app.repository,
      metrics=metrics, live=live,
    )

    # --- Helpers shared by once and continuous modes ---

    async def run_searches() -> None:
      """Run all saved searches and ingest result stubs into the DB."""
      console.print("\n[bold cyan]Discovery: searching for new listings...[/bold cyan]")
      with app.database.session() as session:
        stats, _ = await discovery.run_all(session, website_filter=website)
        console.print(
          f"  Searches: {stats.searches_run} run, "
          f"{stats.results_found} found, "
          f"{stats.new_listings} new"
        )

    async def fetch_one_batch() -> int:
      """Fetch one batch of unfetched listings; return the count fetched."""
      with app.database.session() as session:
        stats = await discovery.fetch_unfetched(
          session,
          website_filter=website,
          classify=classify,
          max_per_cycle=max_fetch_per_cycle,
        )
      if stats.listings_fetched:
        console.print(
          f"  Fetched: {stats.listings_fetched}, "
          f"classified: {stats.listings_classified}, "
          f"rejected: {stats.listings_rejected}"
        )
      # Reload watcher queue so freshly fetched listings enter monitoring.
      with app.database.session() as session:
        count = watcher.load_active_listings(session)
        if stats.listings_fetched:
          console.print(f"  Watch queue: {count} active listings")
      return stats.listings_fetched

    # --- once mode: sequential single pass ---
    if once:
      await run_searches()
      await fetch_one_batch()
      watch_stats = await watcher.run_once()
      console.print(
        f"\n[bold cyan]Watch:[/bold cyan] "
        f"{watch_stats.checks_performed} checked, "
        f"{watch_stats.listings_updated} updated, "
        f"{watch_stats.listings_completed} completed"
      )
      return

    # --- Continuous mode: three independent concurrent loops ---
    #
    # search_loop  — runs saved searches every --discover-interval minutes.
    # fetch_loop   — continuously drains the unfetched queue in batches;
    #                immediately loops again if there was more work so the
    #                backlog cannot accumulate indefinitely.
    # watcher      — monitors active listings on its own router/rate-limiter.
    stop_event = asyncio.Event()

    async def search_loop() -> None:
      while not stop_event.is_set():
        try:
          await run_searches()
        except Exception as exc:
          logger.error("Search loop error: %s", exc, exc_info=True)
        live.search_sleeping(discover_interval * 60)
        with contextlib.suppress(TimeoutError):
          await asyncio.wait_for(stop_event.wait(), timeout=discover_interval * 60)

    async def fetch_loop() -> None:
      while not stop_event.is_set():
        try:
          fetched = await fetch_one_batch()
        except Exception as exc:
          logger.error("Fetch loop error: %s", exc, exc_info=True)
          fetched = 0
        if fetched == 0:
          live.fetch_sleeping()
          with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=60.0)

    console.print(
      f"\n[bold cyan]Continuous mode: "
      f"searching every {discover_interval} min, fetching continuously, "
      f"watching listings (Ctrl+C to stop)...[/bold cyan]"
    )

    search_task = asyncio.create_task(search_loop())
    fetch_task = asyncio.create_task(fetch_loop())
    try:
      await watcher.run_forever(stop_event)
    except KeyboardInterrupt:
      pass
    finally:
      stop_event.set()
      with contextlib.suppress(Exception):
        await asyncio.gather(search_task, fetch_task, return_exceptions=True)
      live.stop()
      metrics.pipeline_stopped()
      console.print("\n[yellow]Pipeline stopped.[/yellow]")


# -------------------------------------------------------------------
# Web frontend
# -------------------------------------------------------------------


@main.command("web")
@click.option("--host", type=str, default="127.0.0.1", help="Bind address.")
@click.option("--port", type=int, default=5001, help="Port to listen on.")
@click.option("--debug", is_flag=True, default=False, help="Enable Flask debug mode.")
@pass_context
def run_web(app: AppContext, host: str, port: int, debug: bool) -> None:
  """Start the web frontend for browsing the database."""
  from auction_tracker.web.app import create_app

  flask_app = create_app(config=app.config)
  console.print(f"[green]Starting web UI at http://{host}:{port}[/green]")
  flask_app.run(host=host, port=port, debug=debug)


# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------


def _yes_no(value: bool) -> str:
  return "[green]yes[/green]" if value else "[dim]no[/dim]"


if __name__ == "__main__":
  main()
