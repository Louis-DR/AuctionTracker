"""Image downloading and classification for listing filtering.

Downloads listing images to local storage and optionally runs the
CLIP classifier to determine if the listing shows a writing instrument.
"""

from __future__ import annotations

import logging
from pathlib import Path

from auction_tracker.config import ClassifierConfig

logger = logging.getLogger(__name__)


async def download_image(url: str, destination: Path, timeout: float = 30.0) -> bool:
  """Download a single image from a URL.

  Uses curl_cffi for consistency with the HTTP transport. Returns
  True on success, False on failure (logged but not raised).
  """
  try:
    from curl_cffi.requests import AsyncSession

    destination.parent.mkdir(parents=True, exist_ok=True)
    async with AsyncSession() as session:
      response = await session.get(url, timeout=timeout)
      if response.status_code == 200:
        destination.write_bytes(response.content)
        return True
      logger.warning("Image download HTTP %d for %s", response.status_code, url)
      return False
  except Exception:
    logger.debug("Failed to download image: %s", url, exc_info=True)
    return False


async def download_listing_images(
  image_urls: list[str],
  listing_id: int,
  config: ClassifierConfig,
) -> list[Path]:
  """Download up to N images for a listing.

  Returns paths to successfully downloaded images.
  """
  images_dir = config.images_directory / str(listing_id)
  max_images = min(len(image_urls), config.max_images_per_listing)
  downloaded: list[Path] = []

  for index, url in enumerate(image_urls[:max_images]):
    extension = _guess_extension(url)
    destination = images_dir / f"{index}{extension}"

    if destination.exists():
      downloaded.append(destination)
      continue

    success = await download_image(url, destination)
    if success:
      downloaded.append(destination)

  return downloaded


def classify_listing(
  image_paths: list[Path],
  config: ClassifierConfig,
) -> tuple[bool, float, list[tuple[str, float]]]:
  """Run the classifier on downloaded images.

  Returns (is_relevant, max_score, top_classes). If the classifier
  is disabled or unavailable, returns (True, 0.0, []).
  """
  if not config.enabled:
    return True, 0.0, []

  if not image_paths:
    return True, 0.0, []

  try:
    from auction_tracker.classifier import get_classifier

    classifier = get_classifier(
      enabled=config.enabled,
      use_gpu=config.use_gpu,
    )
    if classifier is None:
      return True, 0.0, []

    path_strings = [str(path) for path in image_paths]
    return classifier.classify_listing_images(path_strings, threshold=config.threshold)

  except ImportError:
    logger.info("Classifier dependencies not available, skipping classification")
    return True, 0.0, []
  except Exception:
    logger.exception("Classification failed, assuming relevant")
    return True, 0.0, []


def _guess_extension(url: str) -> str:
  lower_url = url.lower().split("?")[0]
  if lower_url.endswith(".png"):
    return ".png"
  if lower_url.endswith(".webp"):
    return ".webp"
  if lower_url.endswith(".gif"):
    return ".gif"
  return ".jpg"
