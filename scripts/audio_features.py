"""Local audio-feature analysis using pretrained Essentia models.

Computes approachability / happiness / energy scores directly from a downloaded
audio file, as a local replacement for Spotify's now-restricted `/audio-features`
endpoint. Runs two separate embedding pipelines:

- discogs-effnet, feeding the approachability and mood_happy classifier heads
- MusiCNN, feeding the DEAM arousal-valence regression head (energy = arousal)

Essentia has no discogs-effnet-native energy/arousal model, so the genuine
arousal model (DEAM dataset) needs its own embedding rather than reusing the
discogs-effnet one shared by the other two metrics.

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

# embedding_key -> (embedding model name, essentia.standard algorithm class name, output node)
_EMBEDDING_CONFIGS = {
    "discogs_effnet": (
        "discogs-effnet-bs64-1", "TensorflowPredictEffnetDiscogs", "PartitionedCall:1",
    ),
    "musicnn": (
        "msd-musicnn-1", "TensorflowPredictMusiCNN", "model/dense/BiasAdd",
    ),
}

# (embedding_key, head model name, positive class label, (value_min, value_max))
# The discogs-effnet heads are softmax classifiers, so their positive-class output
# is a 0.0-1.0 probability. The DEAM arousal-valence model is a *regression* head
# trained against a [1, 9] target scale (per Essentia's own model card) - treating
# its raw output as a 0-1 probability would silently clamp every real score to 100.
_CLASSIFIER_HEADS = (
    ("discogs_effnet", "approachability_2c-discogs-effnet-1", "approachable", (0.0, 1.0)),
    ("discogs_effnet", "mood_happy-discogs-effnet-1", "happy", (0.0, 1.0)),
    ("musicnn", "deam-msd-musicnn-2", "arousal", (1.0, 9.0)),
)

# Lazily populated on first use; keyed by embedding_key / model name.
_embedding_extractors: dict[str, object] = {}
_classifier_cache: dict[str, tuple] = {}


def _value_to_percent(value: float, value_min: float = 0.0, value_max: float = 1.0) -> int:
    """Normalize a raw model output to a clamped 0-100 int percentage given its value range."""
    normalized = (value - value_min) / (value_max - value_min)
    return max(0, min(100, round(normalized * 100)))


def _load_metadata(model_name: str) -> dict:
    metadata_path = os.path.join(MODEL_DIR, f"{model_name}.json")
    with open(metadata_path, encoding="utf-8") as f:
        return json.load(f)


def _get_embedding_extractor(embedding_key: str):
    """Return the (lazily loaded) embedding extractor algorithm for embedding_key."""
    if embedding_key not in _embedding_extractors:
        import essentia.standard as es  # noqa: PLC0415

        model_name, algorithm_name, output_node = _EMBEDDING_CONFIGS[embedding_key]
        algorithm_cls = getattr(es, algorithm_name)
        graph_path = os.path.join(MODEL_DIR, f"{model_name}.pb")
        _embedding_extractors[embedding_key] = algorithm_cls(graphFilename=graph_path, output=output_node)
    return _embedding_extractors[embedding_key]


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
    head's positive-class value, averaged across all analysis patches in the
    track. Energy comes from the DEAM arousal-valence regression model's
    "arousal" output, on a separate MusiCNN embedding.

    Args:
        local_file_path: Absolute path to the downloaded/imported audio file.

    Returns:
        (approachability, happiness, energy) tuple, or None if analysis failed
        (missing/corrupt file, model load error, etc.) — caller should skip the
        track and try again on a later run.

    """
    try:
        from essentia.standard import MonoLoader  # noqa: PLC0415

        # Both embedding families expect 16kHz mono, per each model's own metadata.
        audio = MonoLoader(filename=local_file_path, sampleRate=16000, resampleQuality=4)()

        embeddings_by_key: dict[str, object] = {}
        scores = []
        for embedding_key, model_name, positive_class, (value_min, value_max) in _CLASSIFIER_HEADS:
            if embedding_key not in embeddings_by_key:
                embeddings_by_key[embedding_key] = _get_embedding_extractor(embedding_key)(audio)
            embeddings = embeddings_by_key[embedding_key]

            algorithm, positive_index = _get_classifier_head(model_name, positive_class)
            predictions = algorithm(embeddings)
            # predictions is (num_patches, num_classes); average the positive-class
            # value across patches to get one track-level score.
            positive_values = [patch[positive_index] for patch in predictions]
            avg_value = sum(positive_values) / len(positive_values)
            scores.append(_value_to_percent(avg_value, value_min, value_max))

        approachability, happiness, energy = scores
        return (approachability, happiness, energy)

    except Exception as e:
        write_log.warn(
            "AUDIO_FEATURES_ANALYZE_FAIL",
            "Failed to compute local audio features for track.",
            {"local_file_path": local_file_path, "error": str(e)},
        )
        return None
