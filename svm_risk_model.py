"""Scikit-learn SVM adapter for the streaming-grid feature schema."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

from streaming_hazard_grid import FEATURE_SCHEMA, FEATURE_SCHEMA_VERSION


class ModelSchemaError(ValueError):
    """Raised when an estimator's declared feature schema is incompatible."""


class MissingModelFeatures(ValueError):
    """Raised if model inference is requested with unknown feature values."""


@dataclass(slots=True)
class SVMRiskModel:
    """Adapt an sklearn probability-enabled SVM to the grid's named feature map.

    `feature_order` must come from model training metadata. The estimator must
    include its fitted preprocessing (for example StandardScaler in a sklearn
    Pipeline) and expose `predict_proba` plus `classes_`.
    """

    estimator: Any
    feature_order: tuple[str, ...] = FEATURE_SCHEMA
    positive_label: Any = 1
    provenance: str = "unspecified"

    def __post_init__(self) -> None:
        if tuple(self.feature_order) != FEATURE_SCHEMA:
            raise ModelSchemaError(
                f"expected {FEATURE_SCHEMA_VERSION} feature order; got {tuple(self.feature_order)!r}"
            )
        model_feature_count = getattr(self.estimator, "n_features_in_", None)
        if model_feature_count is not None and model_feature_count != len(self.feature_order):
            raise ModelSchemaError(
                f"estimator expects {model_feature_count} features, grid provides {len(self.feature_order)}"
            )
        model_feature_names = getattr(self.estimator, "feature_names_in_", None)
        if model_feature_names is not None and tuple(model_feature_names) != tuple(self.feature_order):
            raise ModelSchemaError("estimator feature_names_in_ does not match the declared grid feature order")
        if not callable(getattr(self.estimator, "predict_proba", None)):
            raise ModelSchemaError("SVM estimator must expose predict_proba; do not infer probability from a class label")
        if not hasattr(self.estimator, "classes_"):
            raise ModelSchemaError("SVM estimator must expose fitted classes_")
        if self.positive_label not in list(self.estimator.classes_):
            raise ModelSchemaError(f"positive label {self.positive_label!r} is absent from fitted classes_")

    def __call__(self, features: Mapping[str, float | None]) -> float:
        missing = [name for name in self.feature_order if features.get(name) is None]
        if missing:
            raise MissingModelFeatures(f"unknown model features: {', '.join(missing)}")
        unknown = [name for name in self.feature_order if name not in features]
        if unknown:
            raise MissingModelFeatures(f"missing model features: {', '.join(unknown)}")
        row = [[float(features[name]) for name in self.feature_order]]
        probabilities = self.estimator.predict_proba(row)
        classes = list(self.estimator.classes_)
        score = float(probabilities[0][classes.index(self.positive_label)])
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            raise ValueError("SVM returned an invalid positive-class probability")
        return score

