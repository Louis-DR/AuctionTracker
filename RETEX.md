# AuctionTracker v1 — Retrospective

## 1. What the Tool Does

AuctionTracker monitors 9 auction/marketplace websites (eBay, Catawiki, LeBonCoin, Drouot, Interenchères, LiveAuctioneers, Invaluable, Yahoo Japan, Gazette Drouot) and:

1. **Discovers** listings via saved search queries
2. **Fetches** listing details (price, images, seller, bids, status)
3. **Monitors** active auctions with timing-adaptive polling
4. **Stores** everything in SQLite via SQLAlchemy
5. **Displays** data through a Flask web dashboard and CLI

---

## 2. What Worked Well

### Architecture & Design
- **Scraper registry pattern** — `@ScraperRegistry.auto_register("catawiki")` makes adding scrapers clean
- **Capabilities system** — `ScraperCapabilities` declares what each scraper supports (search, bids, seller, etc.) so the monitor adapts automatically
- **Monitoring strategies** — Three strategies (`full`, `snapshot`, `post_auction`) match real auction mechanics:
  - `full` for Catawiki (bids extend auctions)
  - `snapshot` for eBay/Yahoo (fixed end time)
  - `post_auction` for Drouot/Invaluable (just check results after)
- **Data model** — Well-designed normalized schema: Website → Listing → BidEvent/ListingImage/PriceSnapshot with proper constraints and indices
- **CLI** — Click-based CLI is well-organized: `search`, `fetch`, `discover`, `watch`, `fix-database`, `history`
- **Rich progress bars** — Per-website progress with error counts gives great visibility during long runs
- **Configuration** — YAML config with per-scraper overrides (delays, browser mode) is flexible
- **curl_cffi** — Browser TLS fingerprint impersonation works great for most sites without actual browser overhead
- **Website failure tracking** — Timeout mode after N consecutive failures prevents hammering broken sites

### Individual Scrapers
- **eBay** — Robust, handles auctions, Buy It Now, bid history, seller info
- **Catawiki** — Good extension detection for Catawiki's "extending close" mechanics
- **Yahoo Japan** — Handles yen currency, proxy URLs, Japanese locale
- **Gazette Drouot** — Historical auction results data
- **LeBonCoin** — Good `__NEXT_DATA__` JSON extraction (when it works)

---

## 3. What Didn't Go Well

### Browser Integration (The Critical Failure)
The #1 issue was the **Camoufox/Playwright browser layer**, which was fragile and ultimately broke the entire tool:

1. **Threading model mismatch** — Playwright's Sync API is fundamentally incompatible with Python's `ThreadPoolExecutor`. Playwright pins its event loop to the thread that creates it. The executor thread inherits asyncio state from the parent. Combining these two systems required increasingly complex hacks (asyncio loop cleanup, one-shot guard bypass, `_BrowserThreadStale` exception, `_browser_ever_created` flags) that interacted badly.

2. **Indefinite hangs** — Individual Playwright operations (`page.content()`, `locator.is_visible()`, `mouse.move()`) could hang forever with no timeout. One stuck call blocked the entire browser thread, causing 21-minute queue backlogs for all subsequent tasks.

3. **No isolation** — All 9 scrapers shared the same process, each with its own Camoufox/Firefox instance. 8 simultaneous Firefox processes competing for CPU/memory on one machine.

4. **Recovery was impossible** — When a browser died mid-operation, the executor thread was permanently poisoned (stale asyncio loop). Recovery logic required killing the browser, discarding the executor, creating a new thread, and re-initializing — but each fix introduced new edge cases.

5. **Original `_ensure_browser` was 60 lines** of workarounds — That's a red flag. Legitimate browser initialization should be 5-10 lines.

### Code Quality & Maintenance
- **No tests for scrapers** — Only 3 test files, none testing actual scraping logic. Every change was tested by running the full tool and watching logs.
- **Giant files** — `smart_monitor.py` (2190 lines), `__main__.py` (1481 lines), `ebay.py` (47KB), `drouot.py` (45KB). Hard to reason about, hard to change safely.
- **20+ ad-hoc debug scripts** — Signs of debugging by printf, no reproducible test harness.
- **No version control was used** — The project sits in Dropbox with no git. No way to revert safely, no diff history, no branches for experiments.
- **Tight coupling** — The smart monitor directly creates scrapers, manages browser threads, handles progress display, AND writes to the database — all in one class.

### Operational Issues
- **All scrapers browser-enabled** — Browser mode was enabled for sites that don't need it (eBay, Yahoo Japan, Drouot work fine with curl_cffi), wasting resources and increasing failure surface.
- **No graceful degradation** — When browser fails, there's no fallback to curl_cffi for sites that support both.
- **Log file grows unbounded** — 1.4M+ lines, making debugging harder each run.
- **Single-process architecture** — Discover and watch run as separate commands but share scraper instances and browser threads, creating contention.

---

## 4. Recommended Libraries & Tools for v2

### HTTP / Scraping
| Purpose | Recommended | Why |
|---------|------------|-----|
| HTTP client | **`curl_cffi`** | Keep it — TLS fingerprint impersonation is excellent, works for 80% of sites |
| Browser automation | **`playwright`** (directly) | Drop Camoufox. Use Playwright's async API with `asyncio`, not Sync API with threading. Simpler, better documented, fewer hacks |
| HTML parsing | **`selectolax`** or **`beautifulsoup4`** | selectolax is 10-30x faster for large pages; BS4 is fine for smaller pages |
| Anti-bot bypass | **`playwright-stealth`** or **`rebrowser-playwright`** | Lightweight stealth patches instead of a full wrapper like Camoufox |

### Data & Storage
| Purpose | Recommended | Why |
|---------|------------|-----|
| Database | **SQLite + SQLAlchemy 2.0** | Keep it — works great for this use case |
| Migrations | **Alembic** | Keep it — already a dependency |
| Validation | **Pydantic v2** | Already a dependency; use more aggressively for scraper output validation |

### Architecture & Quality
| Purpose | Recommended | Why |
|---------|------------|-----|
| Async runtime | **`asyncio`** | Eliminate threading for browser work entirely |
| Task queue | **`asyncio.Queue`** or **`celery`** (if multi-machine) | Decouple scraping from monitoring |
| Testing | **`pytest`** + **`pytest-asyncio`** | Mandatory from day 1 |
| Test fixtures | **`pytest-recording`** or **`vcrpy`** | Record HTTP responses, replay in tests |
| Version control | **`git`** | Non-negotiable |
| CI | **GitHub Actions** | Run tests on every push |
| Linting | **`ruff`** | Fast, covers flake8 + isort + pyupgrade |
| Type checking | **`mypy`** or **`pyright`** | Catches many bugs at edit time |

---

## 5. Recommended Architecture for v2

### Core Principle: Separate Concerns

```
┌───────────────────────────────────────────────────────┐
│                     CLI / Web UI                       │
│            (Click + Flask, display only)               │
└──────────────────────┬────────────────────────────────┘
                       │
┌──────────────────────▼────────────────────────────────┐
│                  Orchestrator                          │
│         (schedules work, manages lifecycle)            │
│    DiscoveryLoop    WatchLoop    HistoryLoop           │
└──────────────────────┬────────────────────────────────┘
                       │
┌──────────────────────▼────────────────────────────────┐
│                  Scraper Layer                         │
│         (pure functions, stateless)                    │
│    Each scraper: parse_search(html) → list[Result]    │
│                  parse_listing(html) → Listing         │
└──────────────────────┬────────────────────────────────┘
                       │
┌──────────────────────▼────────────────────────────────┐
│               Transport Layer                          │
│     (HTTP client / browser, totally independent)       │
│    CurlTransport    BrowserTransport                   │
│    get_page(url) → html: str                           │
└──────────────────────┬────────────────────────────────┘
                       │
┌──────────────────────▼────────────────────────────────┐
│                  Data Layer                            │
│       (SQLAlchemy models + repository)                 │
└───────────────────────────────────────────────────────┘
```

### Key Design Decisions

#### 1. Split scraping from transport
Scrapers should be **pure parsing functions** that take HTML and return structured data. They never touch HTTP or browsers directly.

```python
# BEFORE (v1): scraper does everything
class LeBonCoinScraper(BaseScraper):
    def fetch_listing(self, url):
        html = self._get_html_via_browser(url)  # transport
        data = _extract_next_data(html)          # parsing
        return self._parse_ad(data)              # mapping

# AFTER (v2): scraper is pure parsing
class LeBonCoinParser:
    def parse_listing(self, html: str) -> ScrapedListing:
        data = self._extract_next_data(html)
        return self._parse_ad(data)

    def parse_search(self, html: str) -> list[SearchResult]:
        data = self._extract_next_data(html)
        return [self._parse_result(ad) for ad in data["ads"]]

# Transport is separate
class HttpTransport:
    async def get(self, url: str) -> str: ...

class BrowserTransport:
    async def get(self, url: str) -> str: ...
```

This makes scrapers **trivially testable** — pass in saved HTML, verify output.

#### 2. Async-first browser transport
```python
class BrowserTransport:
    """One browser instance, async interface."""

    async def start(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=False)
        self._context = await self._browser.new_context()

    async def get(self, url: str, wait: str = "domcontentloaded") -> str:
        page = await self._context.new_page()
        try:
            await page.goto(url, wait_until=wait, timeout=30000)
            return await page.content()
        finally:
            await page.close()

    async def stop(self):
        await self._browser.close()
        await self._playwright.stop()
```

No threading, no executor, no asyncio hacks. Just native async Playwright.

#### 3. Per-website transport configuration
```yaml
websites:
  leboncoin:
    transport: browser    # needs anti-bot bypass
    parser: leboncoin
  ebay:
    transport: curl_cffi  # works fine without browser
    parser: ebay
    fallback_transport: browser  # optional fallback
```

#### 4. Bounded, isolated browser sessions
If browser mode is needed for N sites, run at most 2-3 browser tabs (not 8 Firefox instances). Use a semaphore:
```python
_browser_semaphore = asyncio.Semaphore(3)

async def browser_get(url):
    async with _browser_semaphore:
        page = await context.new_page()
        ...
```

---

## 6. Development Steps (In Order)

### Phase 0: Foundation (before writing any scraping code)
1. **Initialize git** — `git init`, proper `.gitignore`, first commit with current code as "v1-archive"
2. **Set up `pytest`** — `tests/` directory, `pytest.ini`, CI config
3. **Set up `ruff`** — linting from day 1
4. **Define the data model** — Port the SQLAlchemy models (they're good, keep them mostly as-is)
5. **Write the config loader** — Simpler: use Pydantic `BaseSettings` instead of manual YAML parsing

### Phase 1: Transport Layer (the hardest part — do it first)
1. **Build `HttpTransport`** — Wrapper around `curl_cffi` with rate limiting, retries, impersonation
2. **Build `BrowserTransport`** — Playwright async, single browser, page pooling, 30s default timeout
3. **Write transport tests** — Test rate limiting, timeout behavior, error handling
4. **Build `TransportRouter`** — Given a website name, picks the right transport (curl vs browser)
5. **Integration test** — Fetch one real page via each transport, verify HTML returned

> [!IMPORTANT]
> **Gate**: Do NOT proceed to Phase 2 until `BrowserTransport` can reliably fetch 20 pages in a row from LeBonCoin without hanging or crashing.

### Phase 2: Parsers (one website at a time)
For each website, in this order (easiest → hardest):
1. **eBay** — Well-structured HTML, most data in meta tags
2. **Invaluable** — Simple JSON API behind the scenes
3. **Drouot** — Standard HTML parsing
4. **Interenchères** — HTML + some JSON
5. **Yahoo Japan** — HTML parsing with Japanese locale
6. **Catawiki** — JSON API + HTML
7. **LiveAuctioneers** — Protected API
8. **LeBonCoin** — `__NEXT_DATA__` JSON extraction (needs browser for DataDome)
9. **Gazette Drouot** — Historical data, browser-only

For each parser:
1. **Save example HTML** — Capture 3-5 real pages (search + listing) to `tests/fixtures/{website}/`
2. **Write the parser** — Pure function: `html → structured data`
3. **Write parser tests** — Feed saved HTML, assert correct output
4. **Wire to transport** — Connect parser to the transport layer
5. **End-to-end test** — One real fetch + parse

> [!IMPORTANT]
> **Gate**: Each parser must have tests passing before moving to the next.

### Phase 3: Orchestrator
1. **Port `DiscoveryLoop`** — Saved searches → new listings → fetch details
2. **Port `WatchLoop`** — Monitoring strategies (full/snapshot/post_auction)
3. **Port progress display** — Rich progress bars
4. **Port CLI** — Click commands
5. **Write orchestrator tests** — Mock the transport layer, verify scheduling logic

### Phase 4: Web Dashboard
1. **Port Flask app** — Keep the templates, update backend
2. **Add basic API** — JSON endpoints for the dashboard

### Phase 5: Migration
1. **Database migration script** — v1 → v2 schema changes (if any)
2. **Import existing data** — Read v1 SQLite, write to v2
3. **Parallel run** — Run v1 and v2 side by side for a few days, compare outputs

---

## 7. How to Prevent Breakage

### Testing Discipline

1. **Golden files for parsers** — Save real HTML in `tests/fixtures/`, parse it, compare output to expected JSON. If a website changes HTML structure, the test fails immediately.

```
tests/
  fixtures/
    leboncoin/
      search_montblanc.html       # saved HTML
      search_montblanc.json       # expected parse output
      listing_3129582399.html
      listing_3129582399.json
    ebay/
      search_fountain_pen.html
      ...
```

2. **Integration tests with real HTTP** — Mark them `@pytest.mark.integration`, don't run in CI, but run manually before deploying changes:
```python
@pytest.mark.integration
async def test_leboncoin_live_search():
    transport = HttpTransport()
    html = await transport.get("https://www.leboncoin.fr/recherche?text=test")
    results = LeBonCoinParser().parse_search(html)
    assert len(results) > 0
```

3. **Transport tests with timeout assertions** — Verify the browser can't hang:
```python
async def test_browser_timeout():
    transport = BrowserTransport(timeout=5)
    with pytest.raises(TimeoutError):
        await transport.get("https://httpbin.org/delay/10")
```

### Operational Safety

4. **Graceful degradation** — If browser transport fails, fall back to curl_cffi:
```python
async def get_with_fallback(url, website):
    try:
        return await browser_transport.get(url)
    except Exception:
        logger.warning("Browser failed for %s, falling back to curl", url)
        return await http_transport.get(url)
```

5. **Log rotation** — `RotatingFileHandler` or `TimedRotatingFileHandler` to keep logs manageable.

6. **Health checks** — A simple `/health` endpoint or CLI command that tests each scraper with one fetch.

7. **Git workflow** — Feature branches, PR reviews (even if self-reviewed), semantic commits. Never edit production code without a way to revert.

---

## 8. Summary

| Aspect | v1 Status | v2 Recommendation |
|--------|----------|-------------------|
| Scraper parsing logic | ✅ Good | Keep, just split from transport |
| Data model | ✅ Good | Keep as-is |
| CLI & progress display | ✅ Good | Port with minor cleanup |
| Monitoring strategies | ✅ Good | Port with better separation |
| Browser integration | ❌ Broken | Rewrite: async Playwright, no threading |
| Error recovery | ❌ Fragile | New: fallback transport, circuit breaker |
| Testing | ❌ Nearly none | New: golden files, integration tests, CI |
| Version control | ❌ None | New: git from day 1 |
| File sizes | ⚠️ Giant files | Split into smaller modules |
| Debug tooling | ⚠️ Ad-hoc scripts | New: `debug_scraper.py` as first-class tool |

The core scraping logic and data model are solid — **the problem was entirely in the browser integration layer and the lack of testing**. A v2 that separates transport from parsing, uses async Playwright directly, and has golden-file parser tests from the start should be dramatically more stable.
