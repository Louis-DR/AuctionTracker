"""Abstract transport interface.

Every transport implementation provides a single async method:
``fetch(url) -> str`` that returns raw HTML. The transport knows
nothing about what the HTML contains.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass


class TransportError(Exception):
  """Base exception for transport-level failures."""

  def __init__(self, message: str, url: str = "", status_code: int | None = None) -> None:
    super().__init__(message)
    self.url = url
    self.status_code = status_code


class TransportTimeout(TransportError):
  """A request exceeded its timeout."""


class TransportBlocked(TransportError):
  """The website actively blocked the request (captcha, 403, etc.)."""


@dataclass
class FetchResult:
  """Result of a transport fetch operation.

  Wraps the raw HTML together with metadata that the caller may
  need for diagnostics or retry decisions.
  """
  html: str
  url: str
  status_code: int = 200
  redirected_url: str | None = None
  elapsed_seconds: float = 0.0
  transport_name: str = ""


class Transport(abc.ABC):
  """Abstract base for all transports."""

  @property
  @abc.abstractmethod
  def name(self) -> str:
    """Human-readable name for logging (e.g. 'http', 'browser')."""

  @abc.abstractmethod
  async def fetch(self, url: str, **kwargs) -> FetchResult:
    """Fetch the given URL and return its HTML content.

    Raises TransportError (or a subclass) on failure.
    """

  async def start(self) -> None:  # noqa: B027
    """Optional startup hook (e.g. launch browser)."""

  async def stop(self) -> None:  # noqa: B027
    """Optional shutdown hook (e.g. close browser)."""

  async def __aenter__(self):
    await self.start()
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.stop()
    return False
