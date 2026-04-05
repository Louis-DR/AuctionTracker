"""Tests for configuration loading and duration parsing."""

from __future__ import annotations

import pytest

from auction_tracker.config import (
  AppConfig,
  MonitoringStrategy,
  TransportKind,
  parse_duration,
)


class TestParseDuration:

  def test_bare_number(self):
    assert parse_duration(60) == 60.0

  def test_seconds_suffix(self):
    assert parse_duration("30s") == 30.0

  def test_minutes_suffix(self):
    assert parse_duration("5m") == 300.0

  def test_hours_suffix(self):
    assert parse_duration("2h") == 7200.0

  def test_days_suffix(self):
    assert parse_duration("1d") == 86400.0

  def test_fractional(self):
    assert parse_duration("1.5h") == 5400.0

  def test_string_number_no_suffix(self):
    assert parse_duration("120") == 120.0

  def test_invalid_raises(self):
    with pytest.raises(ValueError):
      parse_duration("abc")


class TestAppConfig:

  def test_defaults(self):
    config = AppConfig()
    assert config.database.path.name == "auction_tracker.db"
    assert "ebay" in config.websites
    assert config.websites["ebay"].transport == TransportKind.HTTP

  def test_website_lookup(self):
    config = AppConfig()
    ebay = config.website("ebay")
    assert ebay.monitoring_strategy == MonitoringStrategy.SNAPSHOT

  def test_unknown_website_returns_default(self):
    config = AppConfig()
    unknown = config.website("nonexistent")
    assert unknown.enabled is True
    assert unknown.transport == TransportKind.HTTP

  def test_leboncoin_uses_http_with_browser_fallback(self):
    config = AppConfig()
    leboncoin = config.website("leboncoin")
    assert leboncoin.transport == TransportKind.HTTP
    assert leboncoin.fallback_transport == TransportKind.BROWSER
