"""Tests for the transport layer."""

from __future__ import annotations

import pytest

from auction_tracker.transport.base import FetchResult, Transport, TransportError


class MockTransport(Transport):
  """A transport that returns canned responses for testing."""

  def __init__(self, responses: dict[str, str] | None = None) -> None:
    self._responses = responses or {}

  @property
  def name(self) -> str:
    return "mock"

  async def fetch(self, url: str, **kwargs) -> FetchResult:
    if url in self._responses:
      return FetchResult(
        html=self._responses[url],
        url=url,
        transport_name=self.name,
      )
    raise TransportError(f"No mock response for {url}", url=url)


class TestMockTransport:
  """Tests using MockTransport to verify the transport interface."""

  @pytest.mark.asyncio
  async def test_fetch_returns_html(self):
    transport = MockTransport({"https://example.com": "<html>test</html>"})
    result = await transport.fetch("https://example.com")
    assert result.html == "<html>test</html>"
    assert result.transport_name == "mock"

  @pytest.mark.asyncio
  async def test_fetch_unknown_url_raises(self):
    transport = MockTransport()
    with pytest.raises(TransportError):
      await transport.fetch("https://unknown.com")

  @pytest.mark.asyncio
  async def test_context_manager(self):
    transport = MockTransport({"https://example.com": "<html>ok</html>"})
    async with transport as t:
      result = await t.fetch("https://example.com")
      assert result.html == "<html>ok</html>"
