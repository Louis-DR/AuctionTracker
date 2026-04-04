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

  full: FullStrategyConfig = Field(default_factory=FullStrategyConfig)
  snapshot: SnapshotStrategyConfig = Field(default_factory=SnapshotStrategyConfig)
  post_auction: PostAuctionStrategyConfig = Field(default_factory=PostAuctionStrategyConfig)

  @field_validator(
    "discovery_interval", "daily_refresh_interval", "phase_timeout",
    "failure_cooldown",
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
  ),
  "catawiki": WebsiteConfig(
    transport=TransportKind.HTTP,
    fallback_transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.FULL,
  ),
  "leboncoin": WebsiteConfig(
    transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
  ),
  "drouot": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
  ),
  "interencheres": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
  ),
  "liveauctioneers": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
  ),
  "invaluable": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
  ),
  "yahoo_japan": WebsiteConfig(
    transport=TransportKind.HTTP,
    monitoring_strategy=MonitoringStrategy.SNAPSHOT,
  ),
  "gazette_drouot": WebsiteConfig(
    transport=TransportKind.BROWSER,
    monitoring_strategy=MonitoringStrategy.POST_AUCTION,
    historical_only=True,
    exclude_from_discovery=True,
  ),
}


class AppConfig(BaseModel):
  """Root application configuration."""
  database: DatabaseConfig = Field(default_factory=DatabaseConfig)
  logging: LoggingConfig = Field(default_factory=LoggingConfig)
  transport: TransportConfig = Field(default_factory=TransportConfig)
  scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
  websites: dict[str, WebsiteConfig] = Field(default_factory=lambda: dict(_DEFAULT_WEBSITES))

  def website(self, name: str) -> WebsiteConfig:
    """Get config for a website, falling back to defaults."""
    return self.websites.get(name, WebsiteConfig())


def load_config(path: Path | None = None) -> AppConfig:
  """Load configuration from a YAML file.

  If no path is given, looks for ``config.yaml`` in the current
  directory. If the file does not exist, returns default settings.
  """
  import yaml

  if path is None:
    path = Path("config.yaml")
  if not path.exists():
    return AppConfig()
  with open(path) as handle:
    raw = yaml.safe_load(handle) or {}
  return AppConfig.model_validate(raw)
