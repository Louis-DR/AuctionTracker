"""Currency conversion with historical rates from the Frankfurter API.

The Frankfurter API (https://api.frankfurter.app) is a free, open-source
service based on European Central Bank reference rates.  It supports
historical rates by date and requires no API key.

Exchange rates are cached locally in a JSON file so that repeated
conversions for the same date do not require network access.  When the
API is unreachable, hardcoded fallback rates are used instead.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

logger = logging.getLogger(__name__)

_API_BASE_URL = "https://api.frankfurter.app"

# Hardcoded fallback rates (currency -> units per 1 EUR) used when the
# API is unreachable.  These are approximate 2026 values.
_FALLBACK_RATES_VS_EUR: dict[str, float] = {
  "EUR": 1.0,
  "USD": 1.08,
  "GBP": 0.86,
  "CHF": 0.97,
  "JPY": 163.0,
  "CAD": 1.47,
  "AUD": 1.67,
  "SEK": 11.3,
  "DKK": 7.46,
  "NOK": 11.5,
  "PLN": 4.33,
  "CZK": 25.2,
  "HUF": 395.0,
  "TRY": 36.0,
  "BRL": 5.40,
  "MXN": 18.5,
  "KRW": 1450.0,
  "INR": 92.0,
  "CNY": 7.80,
  "HKD": 8.45,
  "SGD": 1.46,
  "THB": 38.5,
  "NZD": 1.82,
  "ZAR": 20.5,
}


class CurrencyConverter:
  """Convert amounts to EUR using historical exchange rates.

  Rates are fetched from the Frankfurter API on the first request
  for a given date, then cached in memory and on disk.  If the API
  is unreachable, hardcoded fallback rates are used.
  """

  def __init__(self, cache_path: Path | None = None) -> None:
    # In-memory cache: { "2026-04-10": { "USD": 1.08, "GBP": 0.86, ... } }
    self._cache: dict[str, dict[str, float]] = {}
    self._cache_path = cache_path
    self._cache_dirty = False

    if self._cache_path and self._cache_path.exists():
      try:
        with open(self._cache_path, encoding="utf-8") as handle:
          self._cache = json.load(handle)
        logger.debug(
          "Loaded exchange rate cache: %d date(s) from %s.",
          len(self._cache), self._cache_path,
        )
      except (json.JSONDecodeError, OSError) as error:
        logger.warning(
          "Could not load exchange rate cache from %s: %s",
          self._cache_path, error,
        )

  # -----------------------------------------------------------------
  # Public API
  # -----------------------------------------------------------------

  def to_eur(
    self,
    amount: Decimal | float | int,
    source_currency: str,
    at_date: date | datetime | None = None,
  ) -> tuple[Decimal | None, float | None]:
    """Convert *amount* in *source_currency* to EUR.

    Returns ``(eur_amount, rate)`` where *rate* is the number of
    *source_currency* units per 1 EUR.  Returns ``(None, None)`` if
    the currency is unknown and no rate could be found.
    """
    source_currency = source_currency.upper().strip()
    if source_currency == "EUR":
      rounded = Decimal(str(amount)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP,
      )
      return rounded, 1.0

    if amount is None:
      return None, None

    rate = self._get_rate(source_currency, at_date)
    if rate is None:
      return None, None

    eur_amount = (Decimal(str(amount)) / Decimal(str(rate))).quantize(
      Decimal("0.01"), rounding=ROUND_HALF_UP,
    )
    return eur_amount, rate

  def save_cache(self) -> None:
    """Persist the in-memory rate cache to disk."""
    if not self._cache_dirty or not self._cache_path:
      return
    try:
      self._cache_path.parent.mkdir(parents=True, exist_ok=True)
      with open(self._cache_path, "w", encoding="utf-8") as handle:
        json.dump(self._cache, handle, indent=2, sort_keys=True)
      self._cache_dirty = False
      logger.debug(
        "Saved exchange rate cache: %d date(s) to %s.",
        len(self._cache), self._cache_path,
      )
    except OSError as error:
      logger.warning(
        "Could not save exchange rate cache to %s: %s",
        self._cache_path, error,
      )

  # -----------------------------------------------------------------
  # Rate fetching
  # -----------------------------------------------------------------

  def _get_rate(
    self,
    currency: str,
    at_date: date | datetime | None = None,
  ) -> float | None:
    """Return the rate of *currency* vs EUR for the given date.

    Tries (in order):
    1. In-memory cache
    2. Frankfurter API
    3. Hardcoded fallback rates
    """
    date_key = self._date_key(at_date)

    if date_key in self._cache:
      rate = self._cache[date_key].get(currency)
      if rate is not None:
        return rate

    rates = self._fetch_rates_for_date(date_key)
    if rates:
      self._cache[date_key] = rates
      self._cache_dirty = True
      self.save_cache()
      rate = rates.get(currency)
      if rate is not None:
        return rate

    fallback_rate = _FALLBACK_RATES_VS_EUR.get(currency)
    if fallback_rate is not None:
      logger.debug(
        "Using fallback rate for %s: %s per EUR.",
        currency, fallback_rate,
      )
      return fallback_rate

    logger.warning(
      "No exchange rate found for %s (date=%s). "
      "Conversion will be skipped.",
      currency, date_key,
    )
    return None

  def _fetch_rates_for_date(
    self,
    date_key: str,
  ) -> dict[str, float] | None:
    """Fetch all EUR-based rates for a given date from the API."""
    today_key = date.today().isoformat()
    if date_key >= today_key:
      endpoint = f"{_API_BASE_URL}/latest?base=EUR"
    else:
      endpoint = f"{_API_BASE_URL}/{date_key}?base=EUR"

    try:
      http_request = urllib.request.Request(
        endpoint,
        headers={"Accept": "application/json", "User-Agent": "AuctionTracker/2.0"},
      )
      with urllib.request.urlopen(http_request, timeout=10) as response:
        data = json.loads(response.read().decode("utf-8"))
      rates: dict[str, float] = data.get("rates", {})
      rates["EUR"] = 1.0
      logger.info(
        "Fetched %d exchange rates for %s from Frankfurter API.",
        len(rates), date_key,
      )
      return rates
    except Exception as error:
      logger.warning(
        "Could not fetch exchange rates from Frankfurter API for %s: %s",
        date_key, error,
      )
      return None

  @staticmethod
  def _date_key(at_date: date | datetime | None = None) -> str:
    """Convert a date/datetime to an ISO date string key."""
    if at_date is None:
      return date.today().isoformat()
    if isinstance(at_date, datetime):
      return at_date.date().isoformat()
    return at_date.isoformat()
