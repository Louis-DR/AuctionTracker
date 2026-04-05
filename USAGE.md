# AuctionTracker v2 — CLI Reference

## Installation and setup

```bash
# Create virtual environment and install
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Optional: browser support (LeBonCoin, Gazette Drouot)
pip install -e ".[browser]"
playwright install chromium

# Optional: image classification (requires ~400MB model download on first use)
pip install torch open-clip-torch Pillow
```

## Global options

Every command accepts these options **before** the subcommand name:

```
auction-tracker [OPTIONS] COMMAND [ARGS]...
```

| Option          | Default       | Description                                                                     |
| --------------- | ------------- | ------------------------------------------------------------------------------- |
| `--config PATH` | `config.yaml` | Path to YAML configuration file. Created automatically from defaults if absent. |
| `--verbose`     | off           | Enable DEBUG-level logging to the console.                                      |

---

## First-time setup

Run these two commands once before anything else:

```bash
# 1. Create the database and all tables
auction-tracker init-db

# 2. Populate the websites table (eBay, Catawiki, etc.)
auction-tracker seed-websites
```

---

## Commands

### `init-db`

Initialize the database. Creates the SQLite file and all tables if they do not exist. Safe to run multiple times — it is idempotent.

```bash
auction-tracker init-db
```

No options.

---

### `seed-websites`

Populate the `websites` table with the 9 supported websites and their base URLs. Safe to run multiple times.

```bash
auction-tracker seed-websites
```

Websites seeded: `ebay`, `catawiki`, `leboncoin`, `drouot`, `interencheres`, `liveauctioneers`, `invaluable`, `yahoo_japan`, `gazette_drouot`.

No options.

---

### `websites`

Display a table of all configured websites with their transport, monitoring strategy, enabled status, and whether a parser is registered.

```bash
auction-tracker websites
```

No options. The **Parser** column shows `no` in red for websites that are configured but not yet implemented.

---

### `parsers`

Display a table of all registered parsers and their declared capabilities (search, listing detail, bid history, seller info).

```bash
auction-tracker parsers
```

No options. Currently only `ebay` is registered.

---

### `searches`

List all saved search queries with their last run time and result count.

```bash
auction-tracker searches
```

No options.

---

### `add-search`

Create a saved search query that will be run automatically by `discover` and `run`.

```bash
auction-tracker add-search QUERY [OPTIONS]
```

| Argument / Option | Required       | Description                                                                                                     |
| ----------------- | -------------- | --------------------------------------------------------------------------------------------------------------- |
| `QUERY`           | Yes            | The search text (e.g. `"montblanc 149"`). Quote it if it contains spaces.                                       |
| `--name TEXT`     | No             | Display name for the search. Defaults to the query text.                                                        |
| `--website TEXT`  | No, repeatable | Restrict to one or more websites. Repeat the flag for multiple websites. Omit to create a cross-website search. |

**Examples:**

```bash
# One website
auction-tracker add-search "fountain pen" --website ebay

# Multiple websites
auction-tracker add-search "montblanc" --website ebay --website catawiki

# Custom name
auction-tracker add-search "stylo plume" --name "Stylo plume eBay" --website ebay

# No website restriction (all parsers will run it)
auction-tracker add-search "pelikan m800"
```

> **Note:** The website must already exist in the database (run `seed-websites` first).

---

### `search`

Run an ad-hoc search, display the results, and optionally ingest them into the database. Unlike `add-search`, this executes the search immediately instead of saving it for later.

```bash
auction-tracker search QUERY [OPTIONS]
```

| Argument / Option        | Required       | Description                                                                          |
| ------------------------ | -------------- | ------------------------------------------------------------------------------------ |
| `QUERY`                  | Yes            | The search text.                                                                     |
| `--website TEXT`         | No, repeatable | Only search on these websites. Defaults to all registered parsers.                   |
| `--save`                 | No             | Save the query as a saved search for future `discover` runs.                         |
| `--fetch` / `--no-fetch` | No             | After finding results, fetch each listing's full detail page. Default: `--no-fetch`. |

**Examples:**

```bash
# Quick look at what's available, not saved
auction-tracker search "vintage fountain pen" --website ebay

# Search and also persist results to the database
auction-tracker search "montblanc 149" --website ebay --save

# Search, save the query, and immediately fetch full details
auction-tracker search "pelikan" --website ebay --save --fetch
```

---

### `fetch`

Fetch and ingest a single listing from its URL. Useful for adding a specific listing you found manually, or for testing the parser on a real page.

```bash
auction-tracker fetch URL --website WEBSITE
```

| Argument / Option | Required | Description                                   |
| ----------------- | -------- | --------------------------------------------- |
| `URL`             | Yes      | Full URL of the listing page.                 |
| `--website TEXT`  | Yes      | Website identifier (e.g. `ebay`, `catawiki`). |

**Example:**

```bash
auction-tracker fetch "https://www.ebay.com/itm/1234567890" --website ebay
```

The command prints the listing title, current price, and status. The listing is stored in the database and will be picked up by `watch` on the next run.

---

### `discover`

Run all active saved searches, then optionally fetch full details for newly found listings and run image classification to filter out non-pen items.

```bash
auction-tracker discover [OPTIONS]
```

| Option                   | Default   | Description                                                                        |
| ------------------------ | --------- | ---------------------------------------------------------------------------------- |
| `--website TEXT`         | all       | Only run searches for this website.                                                |
| `--fetch` / `--no-fetch` | `--fetch` | After discovering new listings, fetch their full detail pages and classify images. |

**Examples:**

```bash
# Run all searches, fetch and classify new results (default)
auction-tracker discover

# Only discover on eBay
auction-tracker discover --website ebay

# Run searches but skip fetching details (faster, leaves them for later)
auction-tracker discover --no-fetch
```

**What it does:**
1. Reads all active `SearchQuery` rows from the database.
2. For each query, fetches the search results page and ingests new listings (marked `is_fully_fetched=False`).
3. If `--fetch` (default): fetches the detail page for each new listing, downloads up to 3 images, and runs the CLIP classifier. Listings scoring below the threshold (default 50%) are marked `CANCELLED`.

---

### `watch`

Monitor all active (non-terminal) listings by polling their pages at the scheduled time. Uses the priority-queue scheduler, so listings near their end time are checked more frequently.

```bash
auction-tracker watch [OPTIONS]
```

| Option           | Default | Description                                                                                |
| ---------------- | ------- | ------------------------------------------------------------------------------------------ |
| `--website TEXT` | all     | Only watch listings from this website.                                                     |
| `--once`         | off     | Run a single pass through due listings, then exit. Default: run continuously until Ctrl+C. |

**Examples:**

```bash
# Start the continuous monitoring loop
auction-tracker watch

# One pass (useful in cron or for debugging)
auction-tracker watch --once

# Only watch eBay listings
auction-tracker watch --website ebay
```

**Polling schedule for eBay (snapshot strategy):**

| Phase       | Condition                     | Poll interval    |
| ----------- | ----------------------------- | ---------------- |
| Routine     | > 1h until end                | Every 6 hours    |
| Approaching | 5 min–1h until end            | Every 10 minutes |
| Imminent    | < 5 min until end             | Every 60 seconds |
| Ending      | Past end time, result unknown | Every 2 minutes  |

After 10 minutes in Ending with no terminal status, the listing is automatically marked `UNSOLD`.

---

### `queue`

Show a snapshot of the current watch queue: which listings are loaded, their monitoring phase, and how long until the next check.

```bash
auction-tracker queue
```

No options. Reads from the database — does not require `watch` to be running.

**Columns:**

| Column      | Description                                                          |
| ----------- | -------------------------------------------------------------------- |
| ID          | Internal database ID                                                 |
| Website     | e.g. `ebay`                                                          |
| External ID | Website's listing ID                                                 |
| Strategy    | `snapshot`, `full`, or `post_auction`                                |
| Phase       | `routine`, `approaching`, `imminent`, `ending`, `waiting`, or `done` |
| Next In     | Time until next scheduled check (e.g. `4h`, `23m`, `45s`)            |
| Failures    | Consecutive fetch failures (triggers cooldown at 5)                  |

---

### `listings`

Display tracked listings from the database with filtering options.

```bash
auction-tracker listings [OPTIONS]
```

| Option           | Default | Description                                                                                   |
| ---------------- | ------- | --------------------------------------------------------------------------------------------- |
| `--status TEXT`  | all     | Filter by status: `upcoming`, `active`, `sold`, `unsold`, `cancelled`, `relisted`, `unknown`. |
| `--website TEXT` | all     | Only show listings from this website.                                                         |
| `--limit INT`    | 50      | Maximum number of results to display.                                                         |

**Examples:**

```bash
# Show the 50 most recently updated listings
auction-tracker listings

# Only active auctions on eBay
auction-tracker listings --website ebay --status active

# All sold listings (most recent first)
auction-tracker listings --status sold --limit 200

# Listings that were rejected by the classifier
auction-tracker listings --status cancelled
```

---

### `run`

Run the full pipeline in sequence: discover new listings, fetch details and classify, then monitor active listings. This is the main command for regular operation.

```bash
auction-tracker run [OPTIONS]
```

| Option           | Default | Description                                                                |
| ---------------- | ------- | -------------------------------------------------------------------------- |
| `--website TEXT` | all     | Only process this website.                                                 |
| `--no-classify`  | off     | Skip image download and CLIP classification.                               |
| `--once`         | off     | After discovery and fetch, do a single monitoring pass instead of looping. |

**Examples:**

```bash
# Standard nightly run: discover, classify, then monitor continuously
auction-tracker run

# First-time test: one full pass without the continuous loop
auction-tracker run --once

# Skip classification (faster, no torch required)
auction-tracker run --no-classify

# Only process eBay
auction-tracker run --website ebay --once
```

**What it does in order:**
1. **Step 1 — Discover:** Runs all saved searches, ingests new listings.
2. **Step 2 — Fetch & Classify:** Fetches full detail pages, downloads images, runs CLIP. Rejects non-pen listings.
3. **Step 3 — Monitor:** Loads all active listings into the priority queue and either runs one pass (`--once`) or loops continuously.

---

## Configuration file

Place a `config.yaml` in the working directory (or pass `--config PATH`). All settings are optional — defaults are used for anything not specified. Duration values accept `s`, `m`, `h`, `d` suffixes.

```yaml
database:
  path: data/auction_tracker.db

logging:
  level: INFO               # DEBUG, INFO, WARNING, ERROR
  file: data/auction_tracker.log
  max_bytes: 10485760       # 10MB — rotated automatically
  backup_count: 5

transport:
  default_request_delay: 2s # Minimum delay between requests to the same site
  default_timeout: 30s      # HTTP / browser navigation timeout
  max_retries: 3
  retry_backoff_factor: 2.0
  browser_page_limit: 3     # Max concurrent browser tabs
  impersonation: chrome     # TLS fingerprint for curl_cffi

classifier:
  enabled: true
  use_gpu: false            # Set true if a CUDA GPU is available
  threshold: 0.50           # Min CLIP score to keep a listing (0–1)
  images_directory: data/images
  max_images_per_listing: 3 # How many images to download per listing

scheduler:
  discovery_interval: 10m   # How often discover runs in the pipeline loop
  daily_refresh_interval: 1d
  consecutive_failure_threshold: 5
  failure_cooldown: 5m

  # eBay / fixed-end-time sites (snapshot strategy)
  snapshot:
    routine_interval: 6h
    approaching_threshold: 1h
    approaching_interval: 10m
    imminent_threshold: 5m
    imminent_interval: 60s
    ending_poll_interval: 2m
    ending_max_wait: 10m

  # Catawiki / extending auctions (full strategy)
  full:
    approaching_threshold: 1h
    approaching_interval: 5m
    imminent_threshold: 5m
    imminent_interval: 20s
    ending_poll_interval: 15s
    ending_max_wait: 10m

  # Drouot / results-after-sale (post_auction strategy)
  post_auction:
    delay_after_end: 15m
    recheck_interval: 1h
    max_wait: 3d
    max_recheck_count: 10

websites:
  ebay:
    enabled: true
    transport: http
    request_delay: 3s
    monitoring_strategy: snapshot
  leboncoin:
    transport: browser       # LeBonCoin requires a full browser (DataDome)
    monitoring_strategy: snapshot
  catawiki:
    transport: http
    fallback_transport: browser
    monitoring_strategy: full
```

---

### `web`

Start the web frontend for browsing the database. Provides a dashboard, listing browser with filters and price charts, individual listing detail pages with image galleries and bid history, seller and bidder views, and saved search statistics.

```bash
auction-tracker web [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--host TEXT` | `127.0.0.1` | Bind address. Use `0.0.0.0` to allow external access. |
| `--port INT` | `5000` | Port to listen on. |
| `--debug` | off | Enable Flask debug mode (auto-reload on code changes). |

**Examples:**

```bash
# Start on default port
auction-tracker web

# Custom port, accessible from other machines
auction-tracker web --host 0.0.0.0 --port 8080

# Development mode with auto-reload
auction-tracker web --debug
```

**Pages available:**

| URL | Description |
|---|---|
| `/` | Dashboard with aggregate statistics and recent listings |
| `/listings` | Filterable, sortable listing browser with price history scatter plot and histogram |
| `/listings/<id>` | Listing detail: image gallery, price info, bid history chart, price snapshots |
| `/sellers` | Seller index with listing counts |
| `/sellers/<id>` | Individual seller with their listings |
| `/bidders` | Bidder index with win counts and total spent |
| `/bidders/<username>` | Individual bidder: spending timeline chart, won items, bid history |
| `/searches` | Saved search overview with verification/acceptance rate statistics |

---

## Typical workflows

### First run from scratch

```bash
auction-tracker init-db
auction-tracker seed-websites
auction-tracker add-search "fountain pen" --website ebay
auction-tracker add-search "montblanc" --website ebay
auction-tracker add-search "pelikan souveran" --website ebay
auction-tracker run --once    # Test one full cycle
auction-tracker listings      # Verify listings were ingested
```

### Daily monitoring (cron)

```bash
# In crontab: run every 15 minutes
*/15 * * * * cd /path/to/project && .venv/bin/auction-tracker run --once --no-classify
```

Or run as a persistent process:

```bash
auction-tracker run           # Loops until Ctrl+C
```

### Adding a specific listing manually

```bash
auction-tracker fetch "https://www.ebay.com/itm/1234567890" --website ebay
auction-tracker listings --status active
```

### Checking on the monitoring queue

```bash
auction-tracker queue         # See all listings and their next check time
auction-tracker watch --once  # Force a monitoring pass right now
```

### Inspecting results

```bash
# What sold today
auction-tracker listings --status sold --limit 20

# What was rejected by the classifier
auction-tracker listings --status cancelled

# Active auctions (being monitored)
auction-tracker listings --status active --website ebay
```

---

## Exit codes

| Code | Meaning                               |
| ---- | ------------------------------------- |
| 0    | Success                               |
| 1    | Unhandled exception (details in log)  |
| 2    | Invalid arguments (Click usage error) |

Errors during fetch/parse/ingest are logged and counted but do not cause a non-zero exit — the tool is designed to continue past individual failures.
