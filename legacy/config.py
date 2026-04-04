"""Configuration loading and validation."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import yaml


logger = logging.getLogger(__name__)

# The project root is one level above the auction_tracker package.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ------------------------------------------------------------------
# Time duration parsing
# ------------------------------------------------------------------

def parse_duration(value: Union[str, int, float]) -> float:
  """Parse a duration value into seconds.

  Accepts:
    - Plain numbers (int/float) → interpreted as seconds
    - Strings with units: "1d", "1.5h", "30m", "45s"
    - Strings without units: "3600" → interpreted as seconds

  Examples:
    - parse_duration(3600) → 3600.0
    - parse_duration("1h") → 3600.0
    - parse_duration("1.5d") → 129600.0
    - parse_duration("30m") → 1800.0
    - parse_duration("3600") → 3600.0
  """
  if isinstance(value, (int, float)):
    return float(value)

  if not isinstance(value, str):
    raise ValueError(f"Duration must be a number or string, got {type(value)}")

  value = value.strip()

  # Try to match a number followed by an optional unit.
  match = re.match(r'^([\d.]+)\s*([dhms])?$', value, re.IGNORECASE)
  if not match:
    raise ValueError(
      f"Invalid duration format: '{value}'. "
      f"Expected a number optionally followed by d/h/m/s (e.g. '1.5h', '30m')."
    )

  number_str, unit = match.groups()
  number = float(number_str)

  if unit is None:
    # No unit → default to seconds.
    return number

  unit_lower = unit.lower()
  if unit_lower == 'd':
    return number * 86400
  elif unit_lower == 'h':
    return number * 3600
  elif unit_lower == 'm':
    return number * 60
  elif unit_lower == 's':
    return number
  else:
    raise ValueError(f"Unknown time unit: {unit}")


@dataclass
class DatabaseConfig:
  path: str = "data/auction_tracker.db"

  @property
  def resolved_path(self) -> Path:
    """Return the database path resolved against the project root."""
    raw = Path(self.path)
    if raw.is_absolute():
      return raw
    return PROJECT_ROOT / raw


@dataclass
class ImagesConfig:
  directory: str = "data/images"
  max_dimension: Optional[int] = 1600
  timeout: float = 30.0  # seconds

  @property
  def resolved_directory(self) -> Path:
    """Return the images directory resolved against the project root."""
    raw = Path(self.directory)
    if raw.is_absolute():
      return raw
    return PROJECT_ROOT / raw


@dataclass
class ScrapingConfig:
  request_delay: float = 2.0  # seconds
  max_retries: int = 3
  timeout: float = 30.0  # seconds
  user_agent: str = "AuctionTracker/0.1 (price research tool)"
  # Per-scraper request delays (in seconds). If a scraper name is not
  # present, the default request_delay is used. Scraper names should be
  # lowercase (e.g., "leboncoin", "ebay", "catawiki").
  per_scraper_delays: Optional[dict[str, float]] = None
  # Optional dictionary of cookies to include in all requests.
  # Useful for authenticated scraping (e.g. Gazette Drouot).
  cookies: Optional[dict[str, str]] = None
  # Whether to use curl_cffi to impersonate a browser (e.g. Chrome)
  # to bypass TLS fingerprinting protections (Datadome/Cloudflare).
  use_impersonation: bool = False

  # Path to a persistent browser profile (User Data Dir) for Camoufox.
  # If set, compatible scrapers will use a headful browser with this
  # profile instead of requests/curl_cffi.
  browser_profile: Optional[str] = None

  # Browser timing parameters (all in seconds unless noted).
  # page.goto() timeout (seconds).
  browser_nav_timeout: float = 90.0
  # networkidle wait timeout (seconds).
  browser_idle_timeout: float = 15.0
  # Random delay before navigation [min, max] seconds.
  browser_pre_nav_delay: list[float] = field(default_factory=lambda: [0.5, 1.5])
  # Random delay after page load [min, max] seconds.
  browser_post_nav_delay: list[float] = field(default_factory=lambda: [0.5, 1.5])
  # Enable/disable human behavior simulation (mouse moves, scrolls).
  browser_human_behavior: bool = True
  # ThreadPoolExecutor future.result() timeout (seconds).
  browser_thread_timeout: float = 120.0

  # Delay after page navigation — used by Gazette Drouot (seconds).
  browser_post_goto_delay: float = 3.0
  # Delay after clicking an interactive element (e.g. Favorite) (seconds).
  browser_post_click_delay: float = 2.0

  # Per-scraper browser mode. If a scraper name maps to True, that
  # scraper will use a headful Camoufox browser instead of curl_cffi.
  # Example: {"leboncoin": true, "catawiki": true}
  per_scraper_browser: Optional[dict[str, bool]] = None

  def get_delay_for_scraper(self, scraper_name: str) -> float:
    """Return the request delay for a specific scraper.

    If the scraper has a custom delay configured, return that.
    Otherwise, return the default request_delay.
    """
    if self.per_scraper_delays is None:
      return self.request_delay
    scraper_key = scraper_name.lower()
    return self.per_scraper_delays.get(scraper_key, self.request_delay)


@dataclass
class MonitoringConfig:
  poll_interval: float = 300.0  # seconds
  snapshot_interval: float = 600.0  # seconds


@dataclass
class SmartMonitoringConfig:
  # How often to do a routine full refresh of each tracked listing.
  daily_refresh_interval: float = 86400.0  # seconds

  # How often to reload the database for newly added listings and to
  # run saved searches for discovery.
  discovery_interval: float = 600.0  # seconds

  # Per-website timeout for each phase of the monitoring cycle.
  # If a website's work in a given phase exceeds this duration, its
  # remaining tasks are deferred to the next cycle.  This prevents a
  # single slow website from blocking the entire cycle.
  phase_timeout: float = 600.0  # seconds (10 minutes)

  # Website failure tracking: if a website fails the last N consecutive
  # fetch/monitor operations, it will be temporarily disabled to avoid
  # wasting resources on a down site or when detected as a bot.
  failure_threshold: int = 100  # Number of consecutive failures before disabling
  disable_duration: float = 10800.0  # seconds (3 hours) - how long to disable a website

  # ---------- "full" strategy (Catawiki) ----------
  # Phase thresholds relative to the auction end time.
  # When the remaining time drops below a threshold, the monitor
  # switches to the corresponding (faster) polling interval.
  full_approaching_threshold: float = 3600.0  # seconds
  full_approaching_interval: float = 300.0  # seconds
  full_imminent_threshold: float = 300.0  # seconds
  full_imminent_interval: float = 20.0  # seconds

  # After the expected end time has passed, the auction may still be
  # extended (Catawiki adds a few minutes on late bids). The monitor
  # keeps polling at this interval until the listing status turns to
  # sold/unsold or the max-wait budget is exhausted.
  full_ending_poll_interval: float = 15.0  # seconds
  full_ending_max_wait: float = 900.0  # seconds

  # ---------- "snapshot" strategy (eBay, Yahoo Japan) ----------
  # How often to take price snapshots during the auction while the
  # listing is far from ending.
  snapshot_interval: float = 21600.0  # seconds (6 hours)
  snapshot_approaching_threshold: float = 3600.0  # seconds
  snapshot_approaching_interval: float = 300.0  # seconds
  snapshot_imminent_threshold: float = 600.0  # seconds
  snapshot_imminent_interval: float = 60.0  # seconds
  snapshot_ending_poll_interval: float = 30.0  # seconds
  # After the fixed end time, how long to keep checking for final
  # status before giving up (eBay/Yahoo have no extensions, so this
  # is short).
  snapshot_ending_max_wait: float = 600.0  # seconds (10 minutes)

  # ---------- "post_auction" strategy (Drouot, Invaluable, …) ----------
  # Delay after the expected end time before the first post-auction
  # check (auction houses may take time to publish results).
  post_auction_delay: float = 900.0  # seconds (15 minutes)
  # How often to re-check after the auction ended if the result is
  # not yet available.
  post_auction_recheck: float = 3600.0  # seconds (1 hour)
  # Max time to keep retrying before giving up.
  post_auction_max_wait: float = 172800.0  # seconds (48 hours)


@dataclass
class LoggingConfig:
  level: str = "INFO"
  file: Optional[str] = "data/auction_tracker.log"

  @property
  def resolved_file(self) -> Optional[Path]:
    """Return the log file path resolved against the project root."""
    if self.file is None:
      return None
    raw = Path(self.file)
    if raw.is_absolute():
      return raw
    return PROJECT_ROOT / raw


@dataclass
class ClassifierConfig:
  """Configuration for CLIP image classification filtering."""
  enabled: bool = True
  images_to_classify: int = 3
  writing_instrument_threshold: float = 0.50
  use_gpu: bool = False



@dataclass
class HistoricalConfig:
  """Configuration for historical data scraping."""
  enabled: bool = False
  interval: float = 604800.0  # seconds (1 week)
  queries: list[str] = field(default_factory=list)


@dataclass
class AppConfig:
  database: DatabaseConfig = field(default_factory=DatabaseConfig)
  images: ImagesConfig = field(default_factory=ImagesConfig)
  scraping: ScrapingConfig = field(default_factory=ScrapingConfig)
  monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
  smart_monitoring: SmartMonitoringConfig = field(default_factory=SmartMonitoringConfig)
  historical: HistoricalConfig = field(default_factory=HistoricalConfig)
  logging: LoggingConfig = field(default_factory=LoggingConfig)
  classifier: ClassifierConfig = field(default_factory=ClassifierConfig)


def _build_section(section_class, raw_dict: Optional[dict]):
  """Instantiate a config section dataclass from a raw dictionary.

  Unknown keys are silently ignored so that forward-compatible config
  files do not cause errors.

  Duration fields (those with type annotation ``float`` representing
  time) are automatically parsed with ``parse_duration()``.
  """
  if raw_dict is None:
    return section_class()

  known_fields = {f.name: f for f in section_class.__dataclass_fields__.values()}
  filtered = {}

  for key, value in raw_dict.items():
    if key not in known_fields:
      continue

    field_info = known_fields[key]
    field_type = field_info.type

    # Special handling for per_scraper_delays dictionary: parse duration
    # values for each scraper.
    if key == "per_scraper_delays" and isinstance(value, dict):
      parsed_delays = {}
      for scraper_name, delay_value in value.items():
        try:
          parsed_delays[scraper_name.lower()] = parse_duration(delay_value)
        except ValueError as error:
          logger.warning(
            "Invalid duration for scraper '%s' in per_scraper_delays: %s. Skipping.",
            scraper_name, error,
          )
      filtered[key] = parsed_delays if parsed_delays else None
      continue

    # If the field is annotated as float and its name suggests it's a
    # duration (contains "interval", "threshold", "delay", "wait",
    # "timeout", "poll"), parse it as a duration.
    # Note: field_type is a string due to `from __future__ import annotations`.
    is_duration_field = (
      field_type in (float, 'float')
      and any(
        keyword in key
        for keyword in ("interval", "threshold", "delay", "wait", "timeout", "poll", "recheck", "duration")
      )
    )

    if is_duration_field:
      try:
        filtered[key] = parse_duration(value)
      except ValueError as error:
        logger.warning(
          "Invalid duration for config field '%s': %s. Using default.",
          key, error,
        )
        # Skip this field — use the dataclass default.
        continue
    else:
      filtered[key] = value

  return section_class(**filtered)


def load_config(config_path: Optional[str] = None) -> AppConfig:
  """Load configuration from a YAML file.

  If *config_path* is ``None`` the function looks for ``config.yaml``
  in the project root.  If the file does not exist, default values are
  returned.
  """
  if config_path is None:
    config_path = os.environ.get("AUCTION_TRACKER_CONFIG")
  if config_path is None:
    config_path = str(PROJECT_ROOT / "config.yaml")

  path = Path(config_path)
  if not path.exists():
    logger.info("No config file found at %s – using defaults.", path)
    return AppConfig()

  logger.info("Loading configuration from %s", path)
  with open(path, "r", encoding="utf-8") as config_file:
    raw = yaml.safe_load(config_file) or {}

  return AppConfig(
    database=_build_section(DatabaseConfig, raw.get("database")),
    images=_build_section(ImagesConfig, raw.get("images")),
    scraping=_build_section(ScrapingConfig, raw.get("scraping")),
    monitoring=_build_section(MonitoringConfig, raw.get("monitoring")),
    smart_monitoring=_build_section(SmartMonitoringConfig, raw.get("smart_monitoring")),
    historical=_build_section(HistoricalConfig, raw.get("historical")),
    logging=_build_section(LoggingConfig, raw.get("logging")),
    classifier=_build_section(ClassifierConfig, raw.get("classifier")),
  )


def setup_logging(config: LoggingConfig) -> None:
  """Configure the root logger based on the logging configuration."""
  handlers: list[logging.Handler] = [logging.StreamHandler()]

  log_file = config.resolved_file
  if log_file is not None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(str(log_file), encoding="utf-8"))

  logging.basicConfig(
    level=getattr(logging, config.level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=handlers,
  )


def suppress_console_logging() -> None:
  """Remove console handlers from the root logger.

  Keeps file handlers intact so logs are still written to disk.  This
  is useful when Rich progress bars are active and console output would
  interleave messily with the bars.
  """
  root = logging.getLogger()
  # Remove all StreamHandler instances (console output).
  for handler in root.handlers[:]:
    if isinstance(handler, logging.StreamHandler) and not isinstance(
      handler, logging.FileHandler,
    ):
      root.removeHandler(handler)
