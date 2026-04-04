"""Image downloading and optional resizing for listing images."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from PIL import Image

from auction_tracker.config import ImagesConfig

logger = logging.getLogger(__name__)

# Domains that require curl_cffi for TLS fingerprint impersonation.
_CFFI_REQUIRED_DOMAINS = frozenset({
  "image.invaluable.com",
})


class ImageDownloader:
  """Downloads images from remote URLs and stores them locally.

  Images are saved in a directory structure organised by listing ID::

      <images_dir>/<listing_id>/<position>_<hash>.<ext>

  This avoids filename collisions and makes it easy to find all images
  for a specific listing on disk.
  """

  def __init__(self, config: ImagesConfig, user_agent: str = "") -> None:
    self.images_directory = config.resolved_directory
    self.max_dimension = config.max_dimension
    self.timeout = config.timeout
    self.user_agent = user_agent
    self._cffi_session: Optional[object] = None

  def _get_cffi_session(self):
    """Lazily create a curl_cffi session for anti-bot-protected CDNs."""
    if self._cffi_session is None:
      from curl_cffi import requests as cffi_requests
      self._cffi_session = cffi_requests.Session(impersonate="chrome")
    return self._cffi_session

  def _needs_cffi(self, url: str) -> bool:
    """Check if the URL requires curl_cffi for download."""
    from urllib.parse import urlparse
    domain = urlparse(url).hostname or ""
    return domain in _CFFI_REQUIRED_DOMAINS

  def download(
    self,
    source_url: str,
    listing_id: int,
    position: int = 0,
  ) -> Optional[dict]:
    """Download a single image and return metadata about the saved file.

    Returns a dictionary with keys ``local_path``, ``width``,
    ``height``, ``file_size_bytes``, and ``downloaded_at``, or
    ``None`` if the download fails.
    """
    try:
      listing_directory = self.images_directory / str(listing_id)
      listing_directory.mkdir(parents=True, exist_ok=True)

      if self._needs_cffi(source_url):
        session = self._get_cffi_session()
        response = session.get(
          source_url,
          timeout=self.timeout,
        )
      else:
        headers = {}
        if self.user_agent:
          headers["User-Agent"] = self.user_agent

        if "drouot.com" in source_url or "mk-media.s3" in source_url or "cdn.drouot" in source_url:
             headers["Referer"] = "https://drouot.com/"

        response = requests.get(
          source_url,
          headers=headers,
          timeout=self.timeout,
          stream=True,
        )
      response.raise_for_status()

      # Determine file extension from content type or URL.
      extension = _guess_extension(
        response.headers.get("Content-Type", ""),
        source_url,
      )

      # Build a stable filename from a hash of the source URL.
      url_hash = hashlib.sha256(source_url.encode()).hexdigest()[:12]
      filename = f"{position:03d}_{url_hash}{extension}"
      file_path = listing_directory / filename

      # Stream to disk.
      raw_bytes = response.content
      file_path.write_bytes(raw_bytes)
      file_size_bytes = len(raw_bytes)

      # Open with Pillow to read dimensions and optionally resize.
      width, height = self._process_image(file_path)

      # Re-read size after possible resize.
      if self.max_dimension is not None:
        file_size_bytes = file_path.stat().st_size

      local_path = str(file_path.relative_to(self.images_directory))
      logger.debug("Downloaded image: %s -> %s", source_url, local_path)

      return {
        "local_path": local_path,
        "width": width,
        "height": height,
        "file_size_bytes": file_size_bytes,
        "downloaded_at": datetime.utcnow(),
      }

    except Exception:
      logger.exception("Failed to download image: %s", source_url)
      return None

  def _process_image(self, file_path: Path) -> tuple[int, int]:
    """Open the image, optionally downscale it, and return (width, height)."""
    # Convert Path to string for PIL compatibility (especially on Windows).
    file_path_str = str(file_path)

    with Image.open(file_path_str) as image:
      width, height = image.size

      if self.max_dimension is not None:
        max_side = max(width, height)
        if max_side > self.max_dimension:
          ratio = self.max_dimension / max_side
          new_width = int(width * ratio)
          new_height = int(height * ratio)
          image = image.resize((new_width, new_height), Image.LANCZOS)
          # Use string path for save to avoid Windows path issues.
          image.save(file_path_str, quality=90, optimize=True)
          width, height = new_width, new_height

    return width, height


def _guess_extension(content_type: str, url: str) -> str:
  """Guess the file extension from the Content-Type header or URL."""
  content_type_map = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/tiff": ".tiff",
    "image/bmp": ".bmp",
  }
  for mime, extension in content_type_map.items():
    if mime in content_type.lower():
      return extension

  # Fallback: try the URL path.
  url_lower = url.lower().split("?")[0]
  for extension in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".tiff", ".bmp"):
    if url_lower.endswith(extension):
      return extension if extension != ".jpeg" else ".jpg"

  return ".jpg"
