"""Image downloading and classification for listing filtering.

Downloads listing images to local storage and optionally runs the
CLIP classifier to determine if the listing shows a writing instrument.
"""

from __future__ import annotations

import logging
import shutil
from decimal import Decimal
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
  *,
  max_count: int | None = None,
) -> list[Path]:
  """Download images for a listing.

  Downloads up to ``max_count`` images when provided, otherwise falls
  back to ``config.max_images_per_listing``.  Returns paths to
  successfully downloaded images.
  """
  images_dir = config.images_directory / str(listing_id)
  effective_max = max_count if max_count is not None else config.max_images_per_listing
  count = min(len(image_urls), effective_max)
  downloaded: list[Path] = []

  for index, url in enumerate(image_urls[:count]):
    extension = _guess_extension(url)
    destination = images_dir / f"{index}{extension}"

    if destination.exists():
      downloaded.append(destination)
      continue

    success = await download_image(url, destination)
    if success:
      downloaded.append(destination)

  return downloaded


def delete_listing_images(listing_id: int, config: ClassifierConfig) -> None:
  """Delete all downloaded images for a listing.

  Called when the classifier rejects a listing so that disk space is
  freed immediately rather than waiting for a manual cleanup.
  """
  images_dir = config.images_directory / str(listing_id)
  if images_dir.exists():
    shutil.rmtree(images_dir, ignore_errors=True)
    logger.info("Deleted images for rejected listing %d", listing_id)


def prune_listing_images_to_first(listing_id: int, config: ClassifierConfig) -> None:
  """Delete all images except the first for a listing.

  Called for low-value terminal listings to reclaim storage while
  keeping a single reference image.
  """
  images_dir = config.images_directory / str(listing_id)
  if not images_dir.exists():
    return

  # Sort by name so index 0 (the cover image) is always first.
  all_images = sorted(images_dir.iterdir())
  if len(all_images) <= 1:
    return

  deleted = 0
  for image in all_images[1:]:
    try:
      image.unlink()
      deleted += 1
    except OSError:
      logger.debug("Could not delete image %s", image)

  if deleted:
    logger.info(
      "Pruned %d extra image(s) for low-value listing %d (kept first only)",
      deleted, listing_id,
    )


def effective_price_eur(
  final_price_eur: Decimal | None,
  current_price_eur: Decimal | None,
) -> float | None:
  """Return the best available EUR price as a plain float.

  Prefers ``final_price_eur``; falls back to ``current_price_eur``
  for classifieds (buy-now items) where no separate final price is
  recorded.
  """
  price = final_price_eur if final_price_eur is not None else current_price_eur
  return float(price) if price is not None else None


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
