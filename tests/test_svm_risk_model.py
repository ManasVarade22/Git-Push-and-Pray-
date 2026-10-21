import unittest

from streaming_hazard_grid import FEATURE_SCHEMA, GridConfig, StreamingHazardGrid, WeatherObservation
from svm_risk_model import MissingModelFeatures, ModelSchemaError, SVMRiskModel
from synthetic_demo_svm import SYNTHETIC_DEMO_ONLY, build_synthetic_demo_model
from telemetry_protocol import TelemetryFrame


class FakeProbabilityEstimator:
    classes_ = [0, 1]

    def __init__(self):
        self.rows = []

    def predict_proba(self, rows):
        self.rows.extend(rows)
        return [[0.25, 0.75]]


def complete_features():
    return {name: float(index + 1) for index, name in enumerate(FEATURE_SCHEMA)}


class SVMRiskModelTests(unittest.TestCase):
    def test_feature_order_is_explicit_and_probability_uses_positive_label(self):
        estimator = FakeProbabilityEstimator()
        model = SVMRiskModel(estimator, FEATURE_SCHEMA)
        features = complete_features()
        self.assertEqual(model(features), 0.75)
        self.assertEqual(estimator.rows, [[features[name] for name in FEATURE_SCHEMA]])

    def test_wrong_feature_order_is_rejected(self):
        with self.assertRaises(ModelSchemaError):
            SVMRiskModel(FakeProbabilityEstimator(), tuple(reversed(FEATURE_SCHEMA)))

    def test_missing_inputs_are_not_coerced_to_zero(self):
        model = SVMRiskModel(FakeProbabilityEstimator())
        features = complete_features()
        features["weather_wind_speed_mps"] = None
        with self.assertRaises(MissingModelFeatures):
            model(features)

    def test_grid_evaluates_once_for_each_accepted_complete_frame(self):
        estimator = FakeProbabilityEstimator()
        model = SVMRiskModel(estimator)
        grid = StreamingHazardGrid(GridConfig(0, 0), risk_evaluator=model)
        for drone_id, (longitude, latitude) in enumerate(
            [(-45, -45), (0, -45), (90, -45), (-45, 0), (90, 0), (-45, 90), (0, 90), (90, 90)],
            start=2,
        ):
            grid.update(TelemetryFrame(drone_id, 1, 200, latitude, longitude, (20,) * 6), now=200)
        grid.update_weather(WeatherObservation(200, 22.0, 4.0, 180.0))
        center = TelemetryFrame(1, 1, 200, 0, 0, (10,) * 6)
        first = grid.update(center, now=200)
        duplicate = grid.update(center, now=201)
        self.assertEqual(first.risk_status, "evaluated")
        self.assertEqual(first.risk_score, 0.75)
        self.assertFalse(duplicate.accepted)
        self.assertEqual(len(estimator.rows), 1)

    def test_grid_skips_inference_for_missing_features(self):
        estimator = FakeProbabilityEstimator()
        grid = StreamingHazardGrid(GridConfig(0, 0), risk_evaluator=SVMRiskModel(estimator))
        result = grid.update(TelemetryFrame(1, 1, 200, 0, 0, (0,) * 6), now=200)
        self.assertEqual(result.risk_status, "incomplete_features")
        self.assertIsNone(result.risk_score)
        self.assertEqual(estimator.rows, [])

    def test_synthetic_demo_is_labelled_and_uses_scaled_sklearn_svc(self):
        self.assertTrue(SYNTHETIC_DEMO_ONLY)
        demo = build_synthetic_demo_model(seed=1234, sample_count=400)
        self.assertEqual(demo.feature_order, FEATURE_SCHEMA)
        self.assertIn("scale", demo.risk_model.estimator.named_steps)
        self.assertIn("svc", demo.risk_model.estimator.named_steps)
        score = demo(complete_features())
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)


if __name__ == "__main__":
    unittest.main()

