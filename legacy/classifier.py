"""CLIP-based image classification for filtering listings.

This module provides a CLIP/SigLIP classifier to filter out non-writing-instrument
listings from search results, reducing false positives like Montblanc watches,
Platinum jewelry, Sailor marine items, etc.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import torch
import open_clip
from PIL import Image

if TYPE_CHECKING:
  from auction_tracker.config import ClassifierConfig

logger = logging.getLogger(__name__)

# Global singleton for the classifier instance (lazy-loaded).
_classifier_instance: Optional["WritingInstrumentClassifier"] = None

# Labels for common false positives and writing instruments.
# The "writing instrument" meta-class aggregates all pen-related labels.
WRITING_INSTRUMENT_LABELS = [
  "a fountain pen",
  "a ballpoint pen",
  "a rollerball pen",
  "a mechanical pencil",
  "a dip pen",
  "a pen nib",
  "a demonstrator pen",
  "a transparent pen",
]

# All classification labels for zero-shot classification.
CLASSIFICATION_LABELS = [
  # Writing instruments (will be aggregated into meta-class)
  *WRITING_INSTRUMENT_LABELS,

  # Ink and accessories (related but not writing instruments)
  "a bottle of ink",
  "an ink cartridge",
  "an ink converter",

  # Common false positives
  "a lighter",
  "a wrist watch",
  "a bottle of perfume",
  "a ring",
  "a pair of earrings",
  "a necklace",
  "a bracelet",
  "a pair of glasses",
  "a wallet",
  "a bag or purse",
  "a pair of shoes",
  "a pair of gloves",
  "a belt",
  "a piece of clothing",

  # Collectibles
  "a book",
  "a notebook",
  "a painting",
  "a photograph",
  "a sculpture",
  "a coin",
  "a stamp",
  "cutlery",
  "a musical instrument",
  "a decorated box",

  # Other items
  "a piece of furniture",
  "a vehicle",
  "a rifle",
  "a knife",
  "a tool",
  "a camera",
  "machinery",

  # Low-quality / irrelevant
  "a picture of a house",
  "a picture of home decoration",
  "an empty packaging box",
  "a text document or invoice",
  "a scanned document with text",
  "a blurry low quality image",
  "an empty picture of nothing",
]


class WritingInstrumentClassifier:
  """CLIP-based classifier for detecting writing instruments in images.

  Uses a pre-trained CLIP model (ViT-B-16) to perform zero-shot
  classification against a set of reference labels. The classifier
  computes a "writing instrument" meta-class by summing the probabilities
  of all pen-related labels.

  The model is loaded lazily on first use to avoid slow startup when
  classification is disabled.
  """

  def __init__(self, use_gpu: bool = False) -> None:
    """Initialize the classifier.

    Args:
      use_gpu: Whether to use GPU acceleration if available.
    """
    self._use_gpu = use_gpu
    self._model = None
    self._preprocess = None
    self._tokenizer = None
    self._text_features = None
    self._device: str = "cpu"

  def _ensure_model_loaded(self) -> None:
    """Lazily load the CLIP model and cache text embeddings."""
    if self._model is not None:
      return

    # Select device based on availability.
    if self._use_gpu and torch.cuda.is_available():
      self._device = "cuda"
    elif self._use_gpu and torch.backends.mps.is_available():
      self._device = "mps"
    else:
      self._device = "cpu"

    # Load ViT-B-16 CLIP model (good balance of speed and accuracy).
    model_name = "ViT-B-16"
    pretrained_source = "datacomp_xl_s13b_b90k"

    logger.info(
      "Loading CLIP model %s on %s...",
      model_name, self._device,
    )
    self._model, _, self._preprocess = open_clip.create_model_and_transforms(
      model_name,
      pretrained=pretrained_source,
      device=self._device,
    )
    self._tokenizer = open_clip.get_tokenizer(model_name)

    # Pre-encode text labels for fast inference.
    self._cache_text_embeddings()
    logger.info("CLIP model loaded successfully.")

  def _cache_text_embeddings(self) -> None:
    """Pre-compute and cache text embeddings for all labels."""
    text_tokens = self._tokenizer(CLASSIFICATION_LABELS).to(self._device)

    with torch.no_grad():
      text_features = self._model.encode_text(text_tokens)
      # Normalize for cosine similarity.
      self._text_features = text_features / text_features.norm(
        dim=-1, keepdim=True
      )

  def classify_image(self, image_path: str) -> dict[str, float]:
    """Classify an image and return probabilities for all classes.

    Args:
      image_path: Path to the image file.

    Returns:
      Dictionary mapping class labels to their probabilities.
      Includes a special "writing instrument" meta-class that sums
      all pen-related probabilities.
    """
    self._ensure_model_loaded()

    try:
      # Load and preprocess image.
      image = Image.open(image_path).convert("RGB")
      image_tensor = self._preprocess(image).unsqueeze(0).to(self._device)

      with torch.no_grad():
        # Encode image.
        image_features = self._model.encode_image(image_tensor)
        image_features = image_features / image_features.norm(
          dim=-1, keepdim=True
        )

        # Calculate similarity logits.
        logits = (
          image_features @ self._text_features.T
        ) * self._model.logit_scale.exp()

        if self._model.logit_bias is not None:
          logits += self._model.logit_bias

        # Convert to probabilities via softmax.
        probs = torch.softmax(logits, dim=-1).cpu().numpy()[0]

      # Build scores dictionary.
      scores = {
        label: float(prob)
        for label, prob in zip(CLASSIFICATION_LABELS, probs)
      }

      # Create meta-class by summing writing instrument probabilities.
      scores["writing instrument"] = sum(
        scores.get(label, 0.0) for label in WRITING_INSTRUMENT_LABELS
      )

      return scores

    except Exception as error:
      logger.exception("Failed to classify image: %s", image_path)
      return {}

  def is_writing_instrument(
    self,
    image_path: str,
    threshold: float = 0.50,
  ) -> bool:
    """Check if an image contains a writing instrument.

    Args:
      image_path: Path to the image file.
      threshold: Minimum probability for "writing instrument" class.

    Returns:
      True if the writing instrument probability is at or above threshold.
    """
    scores = self.classify_image(image_path)
    if not scores:
      return False
    return scores.get("writing instrument", 0.0) >= threshold

  def classify_listing_images(
    self,
    image_paths: list[str],
    threshold: float = 0.50,
  ) -> tuple[bool, float, list[tuple[str, float]]]:
    """Classify multiple images for a listing.

    Checks if ANY of the provided images contains a writing instrument
    above the threshold. This handles cases where the first image might
    be packaging but subsequent images show the actual pen.

    Args:
      image_paths: List of paths to image files.
      threshold: Minimum probability for "writing instrument" class.

    Returns:
      Tuple of (is_relevant, max_score, top_classes) where:
      - is_relevant: True if any image passes the threshold
      - max_score: Highest writing instrument score across all images
      - top_classes: Top 3 classes by max probability across all images
    """
    if not image_paths:
      # No images to classify – assume relevant to avoid false rejections.
      logger.debug("No images to classify, assuming relevant.")
      return True, 0.0, []

    max_score = 0.0
    # Track max score for each label across all images.
    aggregated_scores: dict[str, float] = {}

    for image_path in image_paths:
      try:
        scores = self.classify_image(image_path)
        if not scores:
          continue

        # Update max score for writing instrument.
        score = scores.get("writing instrument", 0.0)
        max_score = max(max_score, score)

        # Aggregate max scores across all labels.
        for label, prob in scores.items():
          if label in aggregated_scores:
            aggregated_scores[label] = max(aggregated_scores[label], prob)
          else:
            aggregated_scores[label] = prob

        logger.debug(
          "  Image %s: writing_instrument=%.1f%%",
          Path(image_path).name,
          score * 100,
        )

        # Early exit if we find a matching image (but continue
        # aggregating if we haven't classified all yet for top classes).
        # Actually, let's classify all images for better top class data.

      except Exception:
        logger.exception("Failed to classify image: %s", image_path)
        continue

    # Get top 3 classes (excluding the meta "writing instrument" class
    # since it's redundant with component classes).
    sorted_classes = sorted(
      ((k, v) for k, v in aggregated_scores.items() if k != "writing instrument"),
      key=lambda x: x[1],
      reverse=True,
    )
    top_classes = sorted_classes[:3]

    is_relevant = max_score >= threshold
    return is_relevant, max_score, top_classes


def get_classifier(config: "ClassifierConfig") -> Optional[WritingInstrumentClassifier]:
  """Get the global classifier instance, creating it if needed.

  The classifier is a singleton to avoid loading the model multiple times.
  Returns None if classification is disabled in the config.

  Args:
    config: Classifier configuration.

  Returns:
    WritingInstrumentClassifier instance, or None if disabled.
  """
  global _classifier_instance

  if not config.enabled:
    return None

  if _classifier_instance is None:
    _classifier_instance = WritingInstrumentClassifier(use_gpu=config.use_gpu)

  return _classifier_instance
