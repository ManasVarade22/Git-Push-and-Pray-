import math
from concurrent.futures import ThreadPoolExecutor
import unittest

from streaming_hazard_grid import (
    FEATURE_SCHEMA,
    GridConfig,
    StreamingHazardGrid,
    WeatherObservation,
)
from telemetry_protocol import TelemetryFrame


def frame(sequence, timestamp, latitude=0, longitude=0, readings=(10, 20, 30, 40, 50, 60)):
    return TelemetryFrame(1, sequence, timestamp, latitude, longitude, readings)


class StreamingHazardGridTests(unittest.TestCase):
    def setUp(self):
        self.config = GridConfig(0, 0, confidence_decay_seconds=10, stale_timeout_seconds=5,
                                 weather_max_age_seconds=5)
        self.grid = StreamingHazardGrid(self.config)

    def test_spatial_assignment_uses_ten_meter_metric_cells(self):
        self.assertEqual(self.grid.assign_cell(0, 0), (0, 0))
        self.assertEqual(self.grid.assign_cell(0, 90), (1, 0))
        self.assertEqual(self.grid.assign_cell(90, 0), (0, 1))

    def test_neighborhood_is_eight_adjacent_cells(self):
        neighbors = self.grid.neighboring_cells((2, -3))
        self.assertEqual(len(neighbors), 8)
        self.assertNotIn((2, -3), neighbors)
        self.assertIn((1, -4), neighbors)
        self.assertIn((3, -2), neighbors)

    def test_exponential_confidence_decay(self):
        result = self.grid.update(frame(1, 100), now=100)
        self.assertTrue(result.accepted)
        confidence = result.state.cells[(0, 0)].sensor_confidences[0]
        self.assertEqual(confidence, 1.0)
        later = self.grid.snapshot(now=110).cells[(0, 0)].sensor_confidences[0]
        self.assertAlmostEqual(later, math.exp(-1), places=7)

    def test_missing_neighbor_sensor_and_weather_stay_unknown(self):
        called = []
        grid = StreamingHazardGrid(self.config, risk_evaluator=lambda values: called.append(values) or 0.1)
        result = grid.update(frame(1, 100, readings=(0, 2, 3, 4, 5, 6)), valid_sensor_indices={0}, now=100)
        self.assertIsNone(result.features["current_sensor_1"])
        self.assertIsNone(result.features["neighbor_sensor_mean_0"])
        self.assertEqual(result.features["neighbor_coverage"], 0.0)
        self.assertIsNone(result.features["weather_temperature_c"])
        self.assertEqual(result.risk_status, "incomplete_features")
        self.assertIsNone(result.risk_score)
        self.assertEqual(called, [])

    def test_stale_weather_is_explicit_and_not_used_for_inference(self):
        calls = []
        grid = StreamingHazardGrid(self.config, risk_evaluator=lambda features: calls.append(features) or 0.5)
        grid.update_weather(WeatherObservation(90, 25.0, 3.0, 180.0))
        result = grid.update(frame(1, 100), now=100)
        self.assertIsNone(result.features["weather_temperature_c"])
        self.assertIsNone(result.features["weather_wind_speed_mps"])
        self.assertEqual(result.features["weather_fresh"], 0.0)
        self.assertEqual(result.risk_status, "incomplete_features")
        self.assertEqual(calls, [])

    def test_delayed_sensor_observation_does_not_overwrite_newer_reading(self):
        self.grid.update(frame(1, 200, readings=(99, 20, 30, 40, 50, 60)), now=200)
        delayed = self.grid.update(frame(2, 190, readings=(1, 2, 3, 4, 5, 6)), now=201)
        self.assertTrue(delayed.accepted)
        cell = delayed.state.cells[(0, 0)]
        self.assertEqual(cell.readings[0], 99)
        self.assertEqual(cell.sensor_timestamps[0], 200)
        self.assertEqual(delayed.state.latest_sequence_by_drone[1], 2)

    def test_cell_fresh_to_stale_transition_uses_received_time(self):
        self.grid.update(frame(1, 100), now=100)
        self.assertEqual(self.grid.snapshot(now=105).cells[(0, 0)].freshness, "fresh")
        self.assertEqual(self.grid.snapshot(now=105.01).cells[(0, 0)].freshness, "stale")

    def test_neighbor_fusion_and_complete_feature_schema(self):
        self.grid.update(frame(1, 100, latitude=0, longitude=90, readings=(20,) * 6), now=100)
        weather = WeatherObservation(100, 22.0, 4.0, 270.0)
        self.grid.update_weather(weather)
        result = self.grid.update(frame(2, 100, readings=(10,) * 6), now=100)
        self.assertEqual(set(result.features), set(FEATURE_SCHEMA))
        self.assertEqual(result.features["neighbor_sensor_mean_0"], 20.0)
        self.assertEqual(result.features["neighbor_coverage"], 1.0)
        self.assertEqual(result.features["weather_temperature_c"], 22.0)
        self.assertEqual(result.risk_status, "no_model")
        self.assertIsNone(result.risk_score)

    def test_old_sequence_is_rejected_without_mutating_grid(self):
        self.grid.update(frame(2, 100), now=100)
        result = self.grid.update(frame(1, 101, latitude=90), now=101)
        self.assertFalse(result.accepted)
        self.assertEqual(result.rejection_reason, "sequence_not_newer")
        self.assertNotIn((0, 1), result.state.cells)
        self.assertEqual(result.state.latest_sequence_by_drone[1], 2)

    def test_complete_features_are_passed_to_injected_risk_evaluator(self):
        seen = []
        grid = StreamingHazardGrid(self.config, risk_evaluator=lambda features: seen.append(dict(features)) or 0.7)
        # One synthetic observation is placed in each of the eight adjacent cells.
        neighbor_offsets = [(-45, -45), (0, -45), (90, -45), (-45, 0), (90, 0),
                            (-45, 90), (0, 90), (90, 90)]
        for drone_id, (lon, lat) in enumerate(neighbor_offsets, start=2):
            neighbor = TelemetryFrame(drone_id, 1, 200, lat, lon, (20,) * 6)
            grid.update(neighbor, now=200)
        grid.update_weather(WeatherObservation(200, 23.0, 5.0, 90.0))
        center = TelemetryFrame(1, 1, 200, 0, 0, (10,) * 6)
        result = grid.update(center, now=200)
        self.assertEqual(result.risk_status, "evaluated")
        self.assertEqual(result.risk_score, 0.7)
        self.assertEqual(result.features["neighbor_coverage"], 8.0)
        self.assertEqual(len(seen), 1)
        self.assertEqual(set(seen[0]), set(FEATURE_SCHEMA))

    def test_concurrent_updates_keep_consistent_per_drone_state(self):
        grid = StreamingHazardGrid(self.config)
        frames = [TelemetryFrame(i, 1, 100, 0, i * 90, (i,) * 6) for i in range(1, 13)]
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda item: grid.update(item, now=100), frames))
        self.assertTrue(all(result.accepted for result in results))
        snapshot = grid.snapshot(now=100)
        self.assertEqual(len(snapshot.latest_sequence_by_drone), 12)
        self.assertEqual(set(snapshot.latest_sequence_by_drone.values()), {1})


if __name__ == "__main__":
    unittest.main()

