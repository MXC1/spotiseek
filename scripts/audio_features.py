"""Local audio-feature analysis using pretrained Essentia models.

Computes approachability / happiness / energy scores directly from a downloaded
audio file, as a local replacement for Spotify's now-restricted `/audio-features`
endpoint. Uses a shared discogs-effnet embedding plus three small classifier
heads (approachability, mood_happy, danceability as an energy proxy) — the
standard two-stage
inference pattern documented by the Essentia project.

`essentia.standard` is only imported inside `compute_audio_features()`, not at
module import time, so this module (and anything that imports it) stays
importable on hosts/images that don't have the heavy `essentia-tensorflow`
dependency installed (e.g. the dashboard container, the host test venv).

Public API:
- compute_audio_features(): Analyze one audio file, returns (approachability, happiness, energy)
"""

import json
import os

from scripts.logs_utils import write_log

MODEL_DIR = os.getenv("AUDIO_FEATURE_MODEL_DIR", "/app/models/essentia")

_EMBEDDING_MODEL_NAME = "discogs-effnet-bs64-1"
_EMBEDDING_OUTPUT_NODE = "PartitionedCall:1"  # "embeddings" output_purpose, per model metadata

# (head model name, positive class label to look up in its classes list)
# Note: the "energy" slot uses the danceability classifier as a proxy — Essentia
# has no discogs-effnet-native energy/arousal model (the true arousal model uses
# a different embedding entirely), and danceability is a reasonable stand-in for
# a DJ-relevant sense of energy.
_CLASSIFIER_HEADS = (
    ("approachability_2c-discogs-effnet-1", "approachable"),
    ("mood_happy-discogs-effnet-1", "happy"),
    ("danceability-discogs-effnet-1", "danceable"),
)

# Lazily populated on first use; keyed by model name.
_embedding_extractor = None
_classifier_cache: dict[str, tuple] = {}


def _prob_to_percent(prob: float) -> int:
    """Convert a 0.0-1.0 model probability into a clamped 0-100 int percentage."""
    return max(0, min(100, round(prob * 100)))


def _load_metadata(model_name: str) -> dict:
    metadata_path = os.path.join(MODEL_DIR, f"{model_name}.json")
    with open(metadata_path, encoding="utf-8") as f:
        return json.load(f)


def _get_embedding_extractor():
    global _embedding_extractor  # noqa: PLW0603
    if _embedding_extractor is None:
        from essentia.standard import TensorflowPredictEffnetDiscogs  # noqa: PLC0415

        graph_path = os.path.join(MODEL_DIR, f"{_EMBEDDING_MODEL_NAME}.pb")
        _embedding_extractor = TensorflowPredictEffnetDiscogs(
            graphFilename=graph_path, output=_EMBEDDING_OUTPUT_NODE,
        )
    return _embedding_extractor


def _get_classifier_head(model_name: str, positive_class: str):
    """Return (algorithm_instance, positive_class_index) for a classifier head, loading it once."""
    if model_name not in _classifier_cache:
        from essentia.standard import TensorflowPredict2D  # noqa: PLC0415

        metadata = _load_metadata(model_name)
        classes = metadata["classes"]
        positive_index = classes.index(positive_class)

        schema_outputs = metadata["schema"]["outputs"]
        predictions_output = next(o["name"] for o in schema_outputs if o["output_purpose"] == "predictions")
        input_node = metadata["schema"]["inputs"][0]["name"]

        graph_path = os.path.join(MODEL_DIR, f"{model_name}.pb")
        algorithm = TensorflowPredict2D(
            graphFilename=graph_path, input=input_node, output=predictions_output,
        )
        _classifier_cache[model_name] = (algorithm, positive_index)

    return _classifier_cache[model_name]


def compute_audio_features(local_file_path: str) -> tuple[int, int, int] | None:
    """Analyze an audio file and return (approachability, happiness, energy) scores.

    Each score is a 0-100 int derived from the corresponding Essentia classifier
    head's positive-class probability, averaged across all analysis patches in
    the track.

    Args:
        local_file_path: Absolute path to the downloaded/imported audio file.

    Returns:
        (approachability, happiness, energy) tuple, or None if analysis failed
        (missing/corrupt file, model load error, etc.) — caller should skip the
        track and try again on a later run.

    """
    try:
        from essentia.standard import MonoLoader  # noqa: PLC0415

        # discogs-effnet family models expect 16kHz mono, per each model's own metadata.
        audio = MonoLoader(filename=local_file_path, sampleRate=16000, resampleQuality=4)()
        embeddings = _get_embedding_extractor()(audio)

        scores = []
        for model_name, positive_class in _CLASSIFIER_HEADS:
            algorithm, positive_index = _get_classifier_head(model_name, positive_class)
            predictions = algorithm(embeddings)
            # predictions is (num_patches, num_classes); average the positive-class
            # probability across patches to get one track-level score.
            positive_probs = [patch[positive_index] for patch in predictions]
            avg_prob = sum(positive_probs) / len(positive_probs)
            scores.append(_prob_to_percent(avg_prob))

        approachability, happiness, energy = scores
        return (approachability, happiness, energy)

    except Exception as e:
        write_log.warn(
            "AUDIO_FEATURES_ANALYZE_FAIL",
            "Failed to compute local audio features for track.",
            {"local_file_path": local_file_path, "error": str(e)},
        )
        return None
