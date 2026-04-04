"""Transport layer: fetches raw HTML from websites.

The transport layer is completely decoupled from parsing. It knows
nothing about auction data — it only delivers HTML (or JSON) given
a URL.
"""

from auction_tracker.transport.base import Transport, TransportError, TransportTimeout
from auction_tracker.transport.http import HttpTransport
from auction_tracker.transport.router import TransportRouter

__all__ = [
  "HttpTransport",
  "Transport",
  "TransportError",
  "TransportRouter",
  "TransportTimeout",
]
