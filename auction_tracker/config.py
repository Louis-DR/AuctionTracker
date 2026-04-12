"""Application configuration using Pydantic settings.

All durations are stored as floats (seconds). The YAML config file
supports human-readable duration strings like "30s", "5m", "1h", "1d"
which are parsed on load.
"""

from __future__ import annotations

import enum
import re
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

_DURATION_PATTERN = re.compile(
  r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[smhd]?)\s*$",
  re.IGNORECASE,
)
_DURATION_MULTIPLIERS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_duration(value: str | float | int) -> float:
  """Parse a duration string into seconds.

  Accepts bare numbers (treated as seconds) or numbers followed by
  a unit suffix: s (seconds), m (minutes), h (hours), d (days).
  """
  if isinstance(value, (int, float)):
    return float(value)
  match = _DURATION_PATTERN.match(str(value))
  if not match:
    raise ValueError(f"Invalid duration string: {value!r}")
  number = float(match.group("value"))
  unit = match.group("unit").lower() or "s"
  return number * _DURATION_MULTIPLIERS[unit]


class TransportKind(enum.StrEnum):
  """Which transport to use for a website."""
  HTTP = "http"
  BROWSER = "browser"
  CAMOUFOX = "camoufox"


class DatabaseConfig(BaseModel):
  """Database connection settings."""
  path: Path = Field(default=Path("data/auction_tracker.db"))


class LoggingConfig(BaseModel):
  """Logging configuration."""
  level: str = "INFO"
  file: Path | None = Field(default=Path("data/auction_tracker.log"))
  max_bytes: int = Field(default=10 * 1024 * 1024, description="Max log file size before rotation")
  backup_count: int = Field(default=5, description="Number of rotated log files to keep")


class TransportConfig(BaseModel):
  """Global transport layer settings."""
  default_request_delay: float = 2.0
  default_timeout: float = 30.0
  max_retries: int = 3
  retry_backoff_factor: float = 2.0
  browser_page_limit: int = 3
  impersonation: str = "chrome"
  # Run the browser in non-headless mode. Non-headless browsers have a
  # fundamentally different fingerprint from headless ones and bypass
  # sophisticated anti-bot systems (e.g. DataDome) that headless mode
  # cannot evade even with stealth patches. The trade-off is that a
  # visible browser window briefly appears during fetches.
  browser_headless: bool = False

  @field_validator("default_request_delay", "default_timeout", mode="before")
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


class MonitoringStrategy(enum.StrEnum):
  """How a website's active listings should be monitored."""
  FULL = "full"
  SNAPSHOT = "snapshot"
  POST_AUCTION = "post_auction"


class WebsiteConfig(BaseModel):
  """Per-website configuration."""
  enabled: bool = True
  transport: TransportKind = TransportKind.HTTP
  fallback_transport: TransportKind | None = None
  request_delay: float = 2.0
  monitoring_strategy: MonitoringStrategy = MonitoringStrategy.SNAPSHOT
  historical_only: bool = False
  exclude_from_discovery: bool = False
  # For multi-domain sites (e.g. eBay), the regional domain to use when
  # building search URLs and as the preferred fetch domain.
  preferred_domain: str | None = None
  # When True, the HTTP transport makes a GET to the domain homepage
  # before the first real request to establish session cookies. This
  # avoids bot-detection "sorry" pages that eBay serves to cookieless
  # sessions. Equivalent to the browser transport's domain warm-up.
  http_warm_up: bool = False

  @field_validator("request_delay", mode="before")
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


class FullStrategyConfig(BaseModel):
  """Timing parameters for the FULL monitoring strategy (e.g. Catawiki).

  This strategy polls aggressively near auction end and supports
  auction extension detection.
  """
  approaching_threshold: float = 3600.0
  approaching_interval: float = 300.0
  imminent_threshold: float = 300.0
  imminent_interval: float = 20.0
  ending_poll_interval: float = 15.0
  ending_max_wait: float = 600.0

  @field_validator("*", mode="before")
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


class SnapshotStrategyConfig(BaseModel):
  """Timing parameters for the SNAPSHOT monitoring strategy (e.g. eBay).

  Periodic polling with phases that tighten as the auction end
  approaches. No extension detection.
  """
  routine_interval: float = 21600.0
  approaching_threshold: float = 3600.0
  approaching_interval: float = 600.0
  imminent_threshold: float = 300.0
  imminent_interval: float = 60.0
  ending_poll_interval: float = 120.0
  ending_max_wait: float = 600.0

  @field_validator("*", mode="before")
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


class PostAuctionStrategyConfig(BaseModel):
  """Timing parameters for POST_AUCTION monitoring (e.g. Drouot).

  Wait until after the auction ends, then check for results.
  """
  delay_after_end: float = 900.0
  recheck_interval: float = 3600.0
  max_wait: float = 259200.0
  max_recheck_count: int = 10

  @field_validator(
    "delay_after_end", "recheck_interval", "max_wait",
    mode="before",
  )
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


class SchedulerConfig(BaseModel):
  """Orchestrator scheduling settings."""
  discovery_interval: float = 600.0
  daily_refresh_interval: float = 86400.0
  phase_timeout: float = 600.0
  consecutive_failure_threshold: int = 5
  failure_cooldown: float = 300.0
  late_watch_threshold: float = 300.0

  full: FullStrategyConfig = Field(default_factory=FullStrategyConfig)
  snapshot: SnapshotStrategyConfig = Field(default_factory=SnapshotStrategyConfig)
  post_auction: PostAuctionStrategyConfig = Field(default_factory=PostAuctionStrategyConfig)

  @field_validator(
    "discovery_interval", "daily_refresh_interval", "phase_timeout",
    "failure_cooldown", "late_watch_threshold",
    mode="before",
  )
  @classmethod
  def coerce_duration(cls, value: str | float | int) -> float:
    return parse_duration(value)


# Default per-website configurations matching the known sites.
_DEFAULT_WEBSITES: dict[str, WebsiteConfig] = {
  "ebay": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
    preferred_domain="ebay.fr",
    http_warm_up=True,
  ),
  "catawiki": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.FULL,
    # Visit the Catawiki homepage first to establish session cookies,
    # mirroring the eBay warm-up. If blocked, the browser fallback kicks in.
    http_warm_up=True,
  ),
  "leboncoin": WebsiteConfig(
    # DataDome blocks curl_cffi and vanilla Playwright Chromium.
    # Camoufox (Firefox fork with C++ fingerprint masking, humanize,
    # persistent profiles, WebRTC blocking, and COOP disabling) is
    # the primary transport. If Camoufox is blocked, the browser
    # fallback uses rebrowser-playwright (CDP leak patches) which
    # can sometimes succeed where Camoufox fails.
    transport=TransportKind.CAMOUFOX,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=4.0,
  ),
  "drouot": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
  ),
  "interencheres": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
    enabled=False,
  ),
  "liveauctioneers": WebsiteConfig(
    # React SPA behind Cloudflare — browser fallback also returns 502
    # so HTTP-only is used; failing listings are skipped by the fetch
    # loop's consecutive-error backoff.
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
    enabled=False,
  ),
  "invaluable": WebsiteConfig(
    # CDN/WAF blocks both curl_cffi (503) and Playwright (502).
    # Browser fallback is pointless; the fetch loop's consecutive-error
    # backoff prevents hammering unreachable listings.
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
    enabled=False,
  ),
  "yahoo_japan": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    enabled=False,
  ),
  "gazette_drouot": WebsiteConfig(
    transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
    historical_only=True,
    exclude_from_discovery=True,
  ),
  "vinted": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
    preferred_domain="vinted.fr",
    http_warm_up=True,
  ),
  "todocoleccion": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
  ),
  "kleinanzeigen": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
  ),
  "ricardo": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
  ),
  "subito": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
  ),
  "marktplaats": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
    request_delay=3.0,
  ),
}


class ClassifierConfig(BaseModel):
  """Image classifier settings."""
  enabled: bool = True
  use_gpu: bool = False
  threshold: float = 0.50
  images_directory: Path = Field(default=Path("data/images"))
  max_images_per_listing: int = 3


class DisplayConfig(BaseModel):
  """Web UI display defaults for the price scatter chart."""
  chart_time_start: str | None = Field(
    default=None,
    description="Default start date (ISO, e.g. '2024-06-01'). "
    "None uses the earliest data point.",
  )
  chart_price_min: float | None = Field(
    default=None,
    description="Default minimum price filter. None uses data minimum.",
  )
  chart_price_max: float | None = Field(
    default=None,
    description="Default maximum price filter. None uses data maximum.",
  )


class AppConfig(BaseModel):
  """Root application configuration."""
  database: DatabaseConfig = Field(default_factory=DatabaseConfig)
  logging: LoggingConfig = Field(default_factory=LoggingConfig)
  transport: TransportConfig = Field(default_factory=TransportConfig)
  scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
  classifier: ClassifierConfig = Field(default_factory=ClassifierConfig)
  display: DisplayConfig = Field(default_factory=DisplayConfig)
  websites: dict[str, WebsiteConfig] = Field(default_factory=lambda: dict(_DEFAULT_WEBSITES))
  # IANA timezone name used to display all datetimes in the web UI.
  display_timezone: str = "Europe/Paris"
  # ISO 4217 currency code used to display all prices in the web UI.
  # The database always stores EUR-converted prices; if this is set to
  # a different currency, the web layer converts from EUR at display time.
  display_currency: str = "EUR"

  def website(self, name: str) -> WebsiteConfig:
    """Get config for a website, falling back to defaults."""
    return self.websites.get(name, WebsiteConfig())


def load_config(path: Path | None = None) -> AppConfig:
  """Load configuration from a YAML file.

  If no path is given, looks for ``config.yaml`` in the current
  directory. If the file does not exist, returns default settings.

  Website entries in the YAML are merged on top of the code defaults so
  that sites not mentioned in the file still appear with their built-in
  configuration.
  """
  import yaml

  if path is None:
    path = Path("config.yaml")
  if not path.exists():
    return AppConfig()
  with open(path) as handle:
    raw = yaml.safe_load(handle) or {}

  # Deep-merge website overrides: start from code defaults and apply
  # any YAML-level keys on top, so that newly-added sites are always
  # visible even when config.yaml doesn't mention them.
  yaml_websites: dict = raw.get("websites") or {}
  merged: dict[str, dict] = {}
  for name, default_cfg in _DEFAULT_WEBSITES.items():
    base = default_cfg.model_dump()
    if name in yaml_websites and yaml_websites[name]:
      base.update(yaml_websites[name])
    merged[name] = base
  # Include any site that appears only in the YAML (custom additions).
  for name, yaml_cfg in yaml_websites.items():
    if name not in merged:
      merged[name] = yaml_cfg or {}
  raw["websites"] = merged

  return AppConfig.model_validate(raw)
