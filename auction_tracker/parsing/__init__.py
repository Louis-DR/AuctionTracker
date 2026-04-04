"""Parsing layer: extracts structured data from raw HTML.

Parsers are pure functions that take HTML strings and return Pydantic
models.  They never perform HTTP requests or touch the browser.
"""

from auction_tracker.parsing.base import Parser, ParserRegistry

__all__ = [
  "Parser",
  "ParserRegistry",
]
