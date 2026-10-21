"""Clearly labelled synthetic-only SVM demo; not a field-trained hazard model."""

from __future__ import annotations

import random
from dataclasses import dataclass

from streaming_hazard_grid import FEATURE_SCHEMA, StreamingHazardGrid
from svm_risk_model import SVMRiskModel


SYNTHETIC_DEMO_ONLY = True
SYNTHETIC_LABEL_RULE = (
    "positive if synthetic channel-0 center count >= 36000 and neighboring "
    "mean >= 30000, or synthetic channel-1 center count >= 50000"
)


@dataclass(frozen=True, slots=True)
class SyntheticDemoModel:
    risk_model: SVMRiskModel
    feature_order: tuple[str, ...]
    provenance: str = "synthetic demonstration data only"

    def __call__(self, features):
        return self.risk_model(features)


def build_synthetic_demo_model(*, seed: int = 20261021, sample_count: int = 2400) -> SyntheticDemoModel:
    """Train a deterministic scaled RBF SVC on synthetic rule-labelled rows.

    The labels only demonstrate the data path. They are not industrial-fire
    labels and must not be used to assert model quality or operational safety.
    """
    if sample_count < 100:
        raise ValueError("sample_count must be at least 100")
    try:
        from sklearn.pipeline import Pipeline
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.preprocessing import StandardScaler
        from sklearn.svm import SVC
    except ImportError as exc:
        raise RuntimeError("install project requirements to build the synthetic SVM demo") from exc

    rng = random.Random(seed)
    rows: list[list[float]] = []
    labels: list[int] = []
    for _ in range(sample_count):
        row = _synthetic_feature_row(rng)
        values = dict(zip(FEATURE_SCHEMA, row))
        positive = (
            values["current_sensor_0"] >= 36_000 and values["neighbor_sensor_mean_0"] >= 30_000
        ) or values["current_sensor_1"] >= 50_000
        rows.append(row)
        labels.append(int(positive))

    pipeline = Pipeline([
        ("scale", StandardScaler()),
        ("svc", CalibratedClassifierCV(
            estimator=SVC(kernel="rbf", random_state=seed, class_weight="balanced"),
            method="sigmoid",
            cv=3,
        )),
    ])
    pipeline.fit(rows, labels)
    model = SVMRiskModel(
        pipeline,
        feature_order=FEATURE_SCHEMA,
        positive_label=1,
        provenance="synthetic demonstration rule labels; no real measurements",
    )
    return SyntheticDemoModel(model, FEATURE_SCHEMA)


def build_synthetic_demo_grid(config, *, seed: int = 20261021) -> StreamingHazardGrid:
    """Create a streaming grid explicitly wired to the synthetic demo SVM."""
    return StreamingHazardGrid(config, risk_evaluator=build_synthetic_demo_model(seed=seed))


def _synthetic_feature_row(rng: random.Random) -> list[float]:
    row: dict[str, float] = {}
    for i in range(6):
        row[f"current_sensor_{i}"] = float(rng.randrange(0, 65_536))
    for i in range(6):
        row[f"current_confidence_{i}"] = rng.uniform(0.15, 1.0)
    for i in range(6):
        row[f"neighbor_sensor_mean_{i}"] = float(rng.randrange(0, 65_536))
    for i in range(6):
        row[f"neighbor_confidence_mean_{i}"] = rng.uniform(0.05, 1.0)
    row["neighbor_coverage"] = float(rng.randrange(0, 9))
    row["weather_temperature_c"] = rng.uniform(-10.0, 55.0)
    row["weather_wind_speed_mps"] = rng.uniform(0.0, 25.0)
    row["weather_wind_direction_degrees"] = rng.uniform(0.0, 360.0)
    row["weather_fresh"] = 1.0
    return [row[name] for name in FEATURE_SCHEMA]

