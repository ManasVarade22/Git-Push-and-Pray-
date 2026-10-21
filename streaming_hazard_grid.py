"""Thread-safe local metric grid for validated drone telemetry frames."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Mapping

from telemetry_protocol import TelemetryFrame, is_newer_sequence


EARTH_RADIUS_METERS = 6_371_008.8
SENSOR_COUNT = 6
FEATURE_SCHEMA_VERSION = "stream-grid-v1"

FEATURE_SCHEMA = (
    *(f"current_sensor_{i}" for i in range(SENSOR_COUNT)),
    *(f"current_confidence_{i}" for i in range(SENSOR_COUNT)),
    *(f"neighbor_sensor_mean_{i}" for i in range(SENSOR_COUNT)),
    *(f"neighbor_confidence_mean_{i}" for i in range(SENSOR_COUNT)),
    "neighbor_coverage",
    "weather_temperature_c",
    "weather_wind_speed_mps",
    "weather_wind_direction_degrees",
    "weather_fresh",
)

CellId = tuple[int, int]
FeatureMap = Mapping[str, float | None]
RiskEvaluator = Callable[[FeatureMap], float]


@dataclass(frozen=True, slots=True)
class GridConfig:
    """Local equirectangular metric grid anchored at a configured site origin."""

    origin_latitude_microdegrees: int
    origin_longitude_microdegrees: int
    cell_size_meters: float = 10.0
    confidence_decay_seconds: float = 60.0
    stale_timeout_seconds: float = 30.0
    weather_max_age_seconds: float = 120.0

    def __post_init__(self) -> None:
        if not -90_000_000 <= self.origin_latitude_microdegrees <= 90_000_000:
            raise ValueError("origin latitude must be within [-90, 90] degrees")
        if not -180_000_000 <= self.origin_longitude_microdegrees <= 180_000_000:
            raise ValueError("origin longitude must be within [-180, 180] degrees")
        for name in ("cell_size_meters", "confidence_decay_seconds", "stale_timeout_seconds", "weather_max_age_seconds"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")


@dataclass(frozen=True, slots=True)
class WeatherObservation:
    timestamp: float
    temperature_c: float | None
    wind_speed_mps: float | None
    wind_direction_degrees: float | None

    def __post_init__(self) -> None:
        if not math.isfinite(self.timestamp):
            raise ValueError("weather timestamp must be finite")
        for name in ("temperature_c", "wind_speed_mps", "wind_direction_degrees"):
            value = getattr(self, name)
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} must be finite or None")


@dataclass(slots=True)
class _Cell:
    readings: dict[int, int] = field(default_factory=dict)
    sensor_timestamps: dict[int, float] = field(default_factory=dict)
    last_received_at: float = 0.0
    risk_score: float | None = None
    risk_status: str = "not_evaluated"


@dataclass(frozen=True, slots=True)
class CellSnapshot:
    cell_id: CellId
    center_latitude_microdegrees: float
    center_longitude_microdegrees: float
    readings: dict[int, int]
    sensor_timestamps: dict[int, float]
    sensor_confidences: dict[int, float]
    last_received_at: float
    confidence: float | None
    freshness: str
    risk_score: float | None
    risk_status: str


@dataclass(frozen=True, slots=True)
class GridSnapshot:
    version: int
    cells: dict[CellId, CellSnapshot]
    latest_sequence_by_drone: dict[int, int]


@dataclass(frozen=True, slots=True)
class GridUpdate:
    accepted: bool
    rejection_reason: str | None
    cell_id: CellId | None
    features: dict[str, float | None] | None
    risk_score: float | None
    risk_status: str
    state: GridSnapshot


class StreamingHazardGrid:
    """In-memory current-state grid; no database I/O is performed here."""

    def __init__(self, config: GridConfig, risk_evaluator: RiskEvaluator | None = None):
        self.config = config
        self.risk_evaluator = risk_evaluator
        self._lock = threading.RLock()
        self._cells: dict[CellId, _Cell] = {}
        self._latest_sequence_by_drone: dict[int, int] = {}
        self._weather: WeatherObservation | None = None
        self._version = 0

    def assign_cell(self, latitude_microdegrees: int, longitude_microdegrees: int) -> CellId:
        """Map coordinates into a floor-indexed local equirectangular grid.

        Cell width is `cell_size_meters`; its east-west scale is adjusted by
        cos(origin latitude). This is appropriate for a compact industrial site,
        not continental distances or polar locations.
        """
        if not -90_000_000 <= latitude_microdegrees <= 90_000_000:
            raise ValueError("latitude outside valid range")
        if not -180_000_000 <= longitude_microdegrees <= 180_000_000:
            raise ValueError("longitude outside valid range")
        lat0 = math.radians(self.config.origin_latitude_microdegrees / 1_000_000)
        dlat = math.radians((latitude_microdegrees - self.config.origin_latitude_microdegrees) / 1_000_000)
        dlon = math.radians((longitude_microdegrees - self.config.origin_longitude_microdegrees) / 1_000_000)
        north_m = EARTH_RADIUS_METERS * dlat
        east_m = EARTH_RADIUS_METERS * math.cos(lat0) * dlon
        size = self.config.cell_size_meters
        return math.floor(east_m / size), math.floor(north_m / size)

    @staticmethod
    def neighboring_cells(cell_id: CellId) -> tuple[CellId, ...]:
        x, y = cell_id
        return tuple((x + dx, y + dy) for dy in (-1, 0, 1) for dx in (-1, 0, 1) if dx != 0 or dy != 0)

    def update_weather(self, weather: WeatherObservation) -> bool:
        """Keep the newest weather sample; return False for a delayed sample."""
        with self._lock:
            if self._weather is not None and weather.timestamp < self._weather.timestamp:
                return False
            self._weather = weather
            self._version += 1
            return True

    def update(
        self,
        frame: TelemetryFrame,
        *,
        valid_sensor_indices: set[int] | None = None,
        weather: WeatherObservation | None = None,
        now: float | None = None,
    ) -> GridUpdate:
        """Accept a newer frame, update its cell, evaluate available risk, and publish state.

        `frame` must already have passed `TelemetryFrame.from_bytes` validation.
        The protocol has no missing-sensor marker, so by default all six raw
        counts—including zero—are treated as measured. Callers with external
        quality flags may provide `valid_sensor_indices`.
        """
        now = time.time() if now is None else now
        if not math.isfinite(now):
            raise ValueError("now must be finite")
        valid_indices = set(range(SENSOR_COUNT)) if valid_sensor_indices is None else set(valid_sensor_indices)
        if any(not isinstance(i, int) or isinstance(i, bool) or i not in range(SENSOR_COUNT) for i in valid_indices):
            raise ValueError("valid_sensor_indices must contain only indexes 0 through 5")

        with self._lock:
            if weather is not None:
                if self._weather is None or weather.timestamp >= self._weather.timestamp:
                    self._weather = weather
            previous_sequence = self._latest_sequence_by_drone.get(frame.drone_id)
            if previous_sequence is not None and not is_newer_sequence(frame.sequence, previous_sequence):
                return GridUpdate(False, "sequence_not_newer", None, None, None, "not_evaluated", self._snapshot_locked(now))

            self._latest_sequence_by_drone[frame.drone_id] = frame.sequence
            cell_id = self.assign_cell(frame.latitude_microdegrees, frame.longitude_microdegrees)
            cell = self._cells.setdefault(cell_id, _Cell())
            cell.last_received_at = now
            observation_time = float(frame.timestamp)
            for index in valid_indices:
                # A newer sequence can still carry a delayed sensor observation.
                old_timestamp = cell.sensor_timestamps.get(index)
                if old_timestamp is not None and observation_time < old_timestamp:
                    continue
                cell.readings[index] = frame.sensor_readings[index]
                cell.sensor_timestamps[index] = observation_time

            features = self._build_features_locked(cell_id, now)
            if self.risk_evaluator is None:
                cell.risk_score = None
                cell.risk_status = "no_model"
            elif any(features[name] is None for name in FEATURE_SCHEMA):
                cell.risk_score = None
                cell.risk_status = "incomplete_features"
            else:
                try:
                    score = float(self.risk_evaluator(features))
                    if not math.isfinite(score) or not 0.0 <= score <= 1.0:
                        raise ValueError("risk evaluator must return a finite score between 0 and 1")
                except Exception:
                    cell.risk_score = None
                    cell.risk_status = "evaluation_error"
                else:
                    cell.risk_score = score
                    cell.risk_status = "evaluated"
            self._version += 1
            state = self._snapshot_locked(now)
            return GridUpdate(True, None, cell_id, features, cell.risk_score, cell.risk_status, state)

    def build_features(self, cell_id: CellId, *, now: float | None = None) -> dict[str, float | None]:
        now = time.time() if now is None else now
        with self._lock:
            return self._build_features_locked(cell_id, now)

    def snapshot(self, *, now: float | None = None) -> GridSnapshot:
        """Return an independent current-state copy; no persistence completion is awaited."""
        now = time.time() if now is None else now
        with self._lock:
            return self._snapshot_locked(now)

    def _build_features_locked(self, cell_id: CellId, now: float) -> dict[str, float | None]:
        current = self._cells.get(cell_id)
        features: dict[str, float | None] = {}
        for index in range(SENSOR_COUNT):
            if current is None or index not in current.readings:
                features[f"current_sensor_{index}"] = None
                features[f"current_confidence_{index}"] = None
            else:
                features[f"current_sensor_{index}"] = float(current.readings[index])
                features[f"current_confidence_{index}"] = self._confidence(current.sensor_timestamps[index], now)

        neighbors = [self._cells[cell] for cell in self.neighboring_cells(cell_id) if cell in self._cells]
        features["neighbor_coverage"] = float(sum(bool(cell.readings) for cell in neighbors))
        for index in range(SENSOR_COUNT):
            weighted_sum = 0.0
            confidence_sum = 0.0
            observed = 0
            for neighbor in neighbors:
                if index not in neighbor.readings:
                    continue
                confidence = self._confidence(neighbor.sensor_timestamps[index], now)
                value = float(neighbor.readings[index])
                observed += 1
                weighted_sum += value * confidence
                confidence_sum += confidence
            features[f"neighbor_sensor_mean_{index}"] = (
                weighted_sum / confidence_sum if confidence_sum > 0 else None
            )
            features[f"neighbor_confidence_mean_{index}"] = confidence_sum / observed if observed else None

        weather = self._weather
        if weather is None:
            weather_values: tuple[float | None, ...] = (None, None, None, None)
        elif max(0.0, now - weather.timestamp) > self.config.weather_max_age_seconds:
            weather_values = (None, None, None, 0.0)
        else:
            weather_values = (
                weather.temperature_c,
                weather.wind_speed_mps,
                weather.wind_direction_degrees,
                1.0,
            )
        for name, value in zip(
            ("weather_temperature_c", "weather_wind_speed_mps", "weather_wind_direction_degrees", "weather_fresh"),
            weather_values,
        ):
            features[name] = value
        return features

    def _confidence(self, timestamp: float, now: float) -> float:
        age = max(0.0, now - timestamp)
        return math.exp(-age / self.config.confidence_decay_seconds)

    def _snapshot_locked(self, now: float) -> GridSnapshot:
        cells: dict[CellId, CellSnapshot] = {}
        for cell_id, cell in self._cells.items():
            confidences = {index: self._confidence(ts, now) for index, ts in cell.sensor_timestamps.items()}
            confidence = sum(confidences.values()) / len(confidences) if confidences else None
            freshness = "fresh" if max(0.0, now - cell.last_received_at) <= self.config.stale_timeout_seconds else "stale"
            lat, lon = self._cell_center(cell_id)
            cells[cell_id] = CellSnapshot(
                cell_id=cell_id,
                center_latitude_microdegrees=lat,
                center_longitude_microdegrees=lon,
                readings=dict(cell.readings),
                sensor_timestamps=dict(cell.sensor_timestamps),
                sensor_confidences=confidences,
                last_received_at=cell.last_received_at,
                confidence=confidence,
                freshness=freshness,
                risk_score=cell.risk_score,
                risk_status=cell.risk_status,
            )
        return GridSnapshot(self._version, cells, dict(self._latest_sequence_by_drone))

    def _cell_center(self, cell_id: CellId) -> tuple[float, float]:
        x, y = cell_id
        size = self.config.cell_size_meters
        lat0 = math.radians(self.config.origin_latitude_microdegrees / 1_000_000)
        north_m = (y + 0.5) * size
        east_m = (x + 0.5) * size
        lat = self.config.origin_latitude_microdegrees / 1_000_000 + math.degrees(north_m / EARTH_RADIUS_METERS)
        lon = self.config.origin_longitude_microdegrees / 1_000_000 + math.degrees(east_m / (EARTH_RADIUS_METERS * math.cos(lat0)))
        return lat * 1_000_000, lon * 1_000_000

