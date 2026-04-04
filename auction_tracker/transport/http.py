"""HTTP transport using curl_cffi for TLS fingerprint impersonation.

Handles rate limiting, retries with exponential backoff, and
impersonation profiles. This is the default transport for most
websites that don't require a full browser.
"""

from __future__ import annotations

import asyncio
import logging
import time

from curl_cffi.requests import AsyncSession

from auction_tracker.transport.base import (
  FetchResult,
  Transport,
  TransportBlocked,
  TransportError,
  TransportTimeout,
)

logger = logging.getLogger(__name__)

# HTTP status codes that indicate bot detection or blocking.
_BLOCKED_STATUS_CODES = frozenset({403, 429, 503})


class HttpTransport(Transport):
  """Async HTTP transport backed by curl_cffi.

  Features:
  - TLS fingerprint impersonation (e.g. chrome, safari)
  - Per-domain rate limiting via asyncio.Lock + delay
  - Automatic retries with exponential backoff
  - Detection of blocked responses
  """

  def __init__(
    self,
    impersonation: str = "chrome",
    request_delay: float = 2.0,
    timeout: float = 30.0,
    max_retries: int = 3,
    retry_backoff_factor: float = 2.0,
  ) -> None:
    self._impersonation = impersonation
    self._request_delay = request_delay
    self._timeout = timeout
    self._max_retries = max_retries
    self._retry_backoff_factor = retry_backoff_factor
    self._session: AsyncSession | None = None
    self._rate_limit_lock = asyncio.Lock()
    self._last_request_time: float = 0.0

  @property
  def name(self) -> str:
    return "http"

  async def start(self) -> None:
    self._session = AsyncSession(impersonate=self._impersonation)

  async def stop(self) -> None:
    if self._session is not None:
      await self._session.close()
      self._session = None

  async def _enforce_rate_limit(self) -> None:
    """Wait until enough time has passed since the last request."""
    async with self._rate_limit_lock:
      now = time.monotonic()
      elapsed = now - self._last_request_time
      if elapsed < self._request_delay:
        await asyncio.sleep(self._request_delay - elapsed)
      self._last_request_time = time.monotonic()

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._session is None:
      await self.start()

    last_error: Exception | None = None

    for attempt in range(1, self._max_retries + 1):
      await self._enforce_rate_limit()
      start_time = time.monotonic()

      try:
        response = await self._session.get(
          url,
          timeout=self._timeout,
          allow_redirects=True,
        )
        elapsed = time.monotonic() - start_time

        if response.status_code in _BLOCKED_STATUS_CODES:
          raise TransportBlocked(
            f"Blocked by {url} (HTTP {response.status_code})",
            url=url,
            status_code=response.status_code,
          )

        if response.status_code >= 400:
          raise TransportError(
            f"HTTP {response.status_code} for {url}",
            url=url,
            status_code=response.status_code,
          )

        html = response.text
        logger.debug(
          "Fetched %s (%d bytes, %.1fs, attempt %d)",
          url, len(html), elapsed, attempt,
        )
        return FetchResult(
          html=html,
          url=url,
          status_code=response.status_code,
          redirected_url=str(response.url) if str(response.url) != url else None,
          elapsed_seconds=elapsed,
          transport_name=self.name,
        )

      except TransportBlocked:
        raise
      except TransportError as error:
        last_error = error
        if attempt < self._max_retries:
          wait = self._retry_backoff_factor ** (attempt - 1)
          logger.warning(
            "Attempt %d/%d failed for %s: %s (retrying in %.1fs)",
            attempt, self._max_retries, url, error, wait,
          )
          await asyncio.sleep(wait)
        continue
      except TimeoutError:
        elapsed = time.monotonic() - start_time
        last_error = TransportTimeout(
          f"Timeout after {elapsed:.1f}s for {url}",
          url=url,
        )
        if attempt < self._max_retries:
          wait = self._retry_backoff_factor ** (attempt - 1)
          logger.warning(
            "Attempt %d/%d timed out for %s (retrying in %.1fs)",
            attempt, self._max_retries, url, wait,
          )
          await asyncio.sleep(wait)
        continue
      except Exception as error:
        last_error = TransportError(f"Unexpected error for {url}: {error}", url=url)
        if attempt < self._max_retries:
          wait = self._retry_backoff_factor ** (attempt - 1)
          await asyncio.sleep(wait)
        continue

    raise last_error or TransportError(f"All {self._max_retries} attempts failed for {url}", url=url)
