"""HTTP transport using curl_cffi for TLS fingerprint impersonation.

Handles rate limiting, retries with exponential backoff, and
impersonation profiles. This is the default transport for most
websites that don't require a full browser.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from urllib.parse import urlparse

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
  - Per-domain rate limiting: requests to different domains do not
    block each other — only requests to the *same* domain are spaced
    out by ``request_delay`` seconds.
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
    # Per-domain state: each hostname gets its own lock and timestamp so
    # that requests to different sites never block each other.
    self._domain_locks: dict[str, asyncio.Lock] = {}
    self._domain_last_request: dict[str, float] = {}
    self._warmed_up_domains: set[str] = set()

  @property
  def name(self) -> str:
    return "http"

  async def start(self) -> None:
    self._session = AsyncSession(impersonate=self._impersonation)

  async def stop(self) -> None:
    if self._session is not None:
      await self._session.close()
      self._session = None

  def _domain_lock(self, domain: str) -> asyncio.Lock:
    """Return (creating if necessary) the per-domain asyncio Lock."""
    if domain not in self._domain_locks:
      self._domain_locks[domain] = asyncio.Lock()
      self._domain_last_request[domain] = 0.0
    return self._domain_locks[domain]

  async def _warm_up_domain(self, url: str) -> None:
    """Visit the domain homepage once to establish session cookies.

    Sites like eBay serve a bot-detection "sorry" page to cookieless
    sessions. A single homepage GET sets the necessary cookies so that
    subsequent requests look like a continuing browser session.
    """
    parsed = urlparse(url)
    domain = parsed.netloc
    if domain in self._warmed_up_domains:
      return
    homepage = f"{parsed.scheme}://{domain}/"
    with contextlib.suppress(Exception):
      await self._enforce_rate_limit(domain)
      await self._session.get(homepage, timeout=self._timeout, allow_redirects=True)
      logger.debug("HTTP warm-up complete for %s", domain)
    # Mark as warmed regardless of outcome so we don't retry infinitely.
    self._warmed_up_domains.add(domain)

  async def _enforce_rate_limit(self, domain: str) -> None:
    """Wait until enough time has passed since the last request to domain."""
    lock = self._domain_lock(domain)
    async with lock:
      now = time.monotonic()
      elapsed = now - self._domain_last_request[domain]
      if elapsed < self._request_delay:
        await asyncio.sleep(self._request_delay - elapsed)
      self._domain_last_request[domain] = time.monotonic()

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if self._session is None:
      await self.start()

    domain = urlparse(url).netloc
    if kwargs.pop("warm_up", False):
      await self._warm_up_domain(url)

    last_error: Exception | None = None

    for attempt in range(1, self._max_retries + 1):
      await self._enforce_rate_limit(domain)
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
