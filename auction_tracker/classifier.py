"""CLIP-based image classification for filtering non-pen listings.

Uses zero-shot classification to detect whether images show writing
instruments (fountain pens, ballpoints, etc.) vs. common false
positives (watches, jewelry, perfume, etc.).

The classifier is optional — it requires torch and open_clip.
When unavailable, all listings pass through unfiltered.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

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

CLASSIFICATION_LABELS = [
  *WRITING_INSTRUMENT_LABELS,

  # Ink and accessories.
  "a bottle of ink",
  "an ink cartridge",
  "an ink converter",

  # Common false positives.
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

  # Collectibles.
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

  # Other items.
  "a piece of furniture",
  "a vehicle",
  "a rifle",
  "a knife",
  "a tool",
  "a camera",
  "machinery",

  # Low-quality / irrelevant.
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

  Lazy-loads the model on first use. Thread-safe for single-threaded
  async usage.
  """

  def __init__(self, use_gpu: bool = False) -> None:
    self._use_gpu = use_gpu
    self._model = None
    self._preprocess = None
    self._tokenizer = None
    self._text_features = None
    self._device: str = "cpu"

  def _ensure_model_loaded(self) -> None:
    if self._model is not None:
      return

    try:
      import open_clip
      import torch
    except ImportError as error:
      raise ImportError(
        "Image classification requires torch and open_clip. "
        "Install with: pip install torch open-clip-torch Pillow"
      ) from error

    if self._use_gpu and torch.cuda.is_available():
      self._device = "cuda"
    elif self._use_gpu and torch.backends.mps.is_available():
      self._device = "mps"
    else:
      self._device = "cpu"

    model_name = "ViT-B-16"
    pretrained_source = "datacomp_xl_s13b_b90k"

    logger.info("Loading CLIP model %s on %s...", model_name, self._device)
    self._model, _, self._preprocess = open_clip.create_model_and_transforms(
      model_name, pretrained=pretrained_source, device=self._device,
    )
    self._tokenizer = open_clip.get_tokenizer(model_name)

    text_tokens = self._tokenizer(CLASSIFICATION_LABELS).to(self._device)
    with torch.no_grad():
      text_features = self._model.encode_text(text_tokens)
      self._text_features = text_features / text_features.norm(dim=-1, keepdim=True)

    logger.info("CLIP model loaded successfully")

  def classify_image(self, image_path: str) -> dict[str, float]:
    """Classify an image, returning label probabilities.

    Includes a special "writing_instrument" meta-key that sums
    all pen-related probabilities.
    """
    self._ensure_model_loaded()

    import torch
    from PIL import Image

    try:
      image = Image.open(image_path).convert("RGB")
      image_tensor = self._preprocess(image).unsqueeze(0).to(self._device)

      with torch.no_grad():
        image_features = self._model.encode_image(image_tensor)
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        logits = (image_features @ self._text_features.T) * self._model.logit_scale.exp()
        if self._model.logit_bias is not None:
          logits += self._model.logit_bias
        probs = torch.softmax(logits, dim=-1).cpu().numpy()[0]

      scores = {
        label: float(prob)
        for label, prob in zip(CLASSIFICATION_LABELS, probs, strict=False)
      }
      scores["writing_instrument"] = sum(
        scores.get(label, 0.0) for label in WRITING_INSTRUMENT_LABELS
      )
      return scores

    except Exception:
      logger.exception("Failed to classify image: %s", image_path)
      return {}

  def is_writing_instrument(self, image_path: str, threshold: float = 0.50) -> bool:
    scores = self.classify_image(image_path)
    if not scores:
      return False
    return scores.get("writing_instrument", 0.0) >= threshold

  def classify_listing_images(
    self,
    image_paths: list[str],
    threshold: float = 0.50,
  ) -> tuple[bool, float, list[tuple[str, float]]]:
    """Classify multiple images for a listing.

    Returns (is_relevant, max_score, top_classes). If ANY image
    passes the threshold, the listing is considered relevant.
    If no images are provided, returns (True, 0.0, []) to avoid
    false rejections.
    """
    if not image_paths:
      return True, 0.0, []

    max_score = 0.0
    aggregated_scores: dict[str, float] = {}

    for image_path in image_paths:
      try:
        scores = self.classify_image(image_path)
        if not scores:
          continue
        score = scores.get("writing_instrument", 0.0)
        max_score = max(max_score, score)
        for label, prob in scores.items():
          aggregated_scores[label] = max(aggregated_scores.get(label, 0.0), prob)
      except Exception:
        logger.exception("Failed to classify image: %s", image_path)
        continue

    sorted_classes = sorted(
      ((key, value) for key, value in aggregated_scores.items() if key != "writing_instrument"),
      key=lambda pair: pair[1],
      reverse=True,
    )
    top_classes = sorted_classes[:3]
    is_relevant = max_score >= threshold
    return is_relevant, max_score, top_classes


# Module-level singleton (lazy-loaded).
_classifier_instance: WritingInstrumentClassifier | None = None


def get_classifier(enabled: bool = True, use_gpu: bool = False) -> WritingInstrumentClassifier | None:
  """Get the global classifier singleton, or None if disabled."""
  global _classifier_instance

  if not enabled:
    return None

  if _classifier_instance is None:
    _classifier_instance = WritingInstrumentClassifier(use_gpu=use_gpu)

  return _classifier_instance
