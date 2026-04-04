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
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

# Frankfurter API base URL.
_API_BASE_URL = "https://api.frankfurter.app"

# Hardcoded fallback rates (currency → rate vs 1 EUR) so the tool
# works out of the box without network access.  These are approximate
# and are only used when the API is unreachable.
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

  def __init__(
    self,
    cache_path: Optional[Path] = None,
  ) -> None:
    # In-memory cache: { "2026-02-08": { "USD": 1.08, "GBP": 0.86, … } }
    self._cache: dict[str, dict[str, float]] = {}
    self._cache_path = cache_path
    self._cache_dirty = False

    # Load the on-disk cache if available.
    if self._cache_path and self._cache_path.exists():
      try:
        with open(self._cache_path, "r", encoding="utf-8") as cache_file:
          self._cache = json.load(cache_file)
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
    amount: Union[Decimal, float, int],
    source_currency: str,
    at_date: Optional[Union[date, datetime]] = None,
  ) -> Optional[Decimal]:
    """Convert *amount* in *source_currency* to EUR at the given date.

    Parameters
    ----------
    amount:
      The amount to convert.
    source_currency:
      ISO 4217 currency code (e.g. "USD", "JPY").
    at_date:
      The date for which to look up the exchange rate.  Defaults to
      today.  Only the date part is used (not the time).

    Returns
    -------
    The equivalent amount in EUR, rounded to 2 decimal places, or
    ``None`` if the currency is unknown and no rate could be found.
    """
    source_currency = source_currency.upper().strip()
    if source_currency == "EUR":
      return Decimal(str(amount)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP,
      )

    if amount is None:
      return None

    rate = self._get_rate(source_currency, at_date)
    if rate is None:
      return None

    amount_decimal = Decimal(str(amount))
    rate_decimal = Decimal(str(rate))
    eur_amount = amount_decimal / rate_decimal
    return eur_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

  def convert(
    self,
    amount: Decimal,
    source_currency: str,
    target_currency: str,
    at_date: Optional[Union[date, datetime]] = None,
  ) -> Optional[Decimal]:
    """Convert *amount* from *source_currency* to *target_currency*.

    Uses EUR as the pivot currency.  Returns ``None`` if either
    currency is unknown.
    """
    source_currency = source_currency.upper().strip()
    target_currency = target_currency.upper().strip()

    if source_currency == target_currency:
      return amount

    # Convert source → EUR → target.
    eur_amount = self.to_eur(amount, source_currency, at_date)
    if eur_amount is None:
      return None

    if target_currency == "EUR":
      return eur_amount

    target_rate = self._get_rate(target_currency, at_date)
    if target_rate is None:
      return None

    result = eur_amount * Decimal(str(target_rate))
    return result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

  def save_cache(self) -> None:
    """Persist the in-memory rate cache to disk."""
    if not self._cache_dirty or not self._cache_path:
      return
    try:
      self._cache_path.parent.mkdir(parents=True, exist_ok=True)
      with open(self._cache_path, "w", encoding="utf-8") as cache_file:
        json.dump(self._cache, cache_file, indent=2, sort_keys=True)
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
    at_date: Optional[Union[date, datetime]] = None,
  ) -> Optional[float]:
    """Return the rate of *currency* vs EUR for the given date.

    Tries (in order):
    1. In-memory cache
    2. Frankfurter API
    3. Hardcoded fallback rates
    """
    date_key = self._date_key(at_date)

    # Check in-memory cache.
    if date_key in self._cache:
      rate = self._cache[date_key].get(currency)
      if rate is not None:
        return rate

    # Try to fetch from the API.
    rates = self._fetch_rates_for_date(date_key)
    if rates:
      self._cache[date_key] = rates
      self._cache_dirty = True
      rate = rates.get(currency)
      if rate is not None:
        return rate

    # Fallback to hardcoded rates.
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

  def _fetch_rates_for_date(self, date_key: str) -> Optional[dict[str, float]]:
    """Fetch all EUR-based rates for a given date from the Frankfurter API.

    Returns a dict mapping currency code → rate vs EUR, or ``None``
    on failure.
    """
    # Use "latest" for today/future dates, historical for past dates.
    today_key = date.today().isoformat()
    if date_key >= today_key:
      endpoint = f"{_API_BASE_URL}/latest?base=EUR"
    else:
      endpoint = f"{_API_BASE_URL}/{date_key}?base=EUR"

    try:
      import requests
      response = requests.get(endpoint, timeout=10)
      if response.status_code != 200:
        logger.warning(
          "Frankfurter API returned HTTP %d for date %s.",
          response.status_code, date_key,
        )
        return None

      data = response.json()
      rates = data.get("rates", {})
      # Add EUR itself for completeness.
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
  def _date_key(at_date: Optional[Union[date, datetime]] = None) -> str:
    """Convert a date/datetime to an ISO date string key."""
    if at_date is None:
      return date.today().isoformat()
    if isinstance(at_date, datetime):
      return at_date.date().isoformat()
    return at_date.isoformat()
