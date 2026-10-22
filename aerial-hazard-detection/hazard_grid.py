
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
import joblib


CELL_SIZE_METRES = 10.0
STALE_AFTER_SECONDS = 15.0
CONFIDENCE_DECAY_SECONDS = 30.0

SENSOR_NAMES = ("MQ-3", "MQ-4", "MQ-7", "MQ-8", "MQ-9", "MQ-135")


@dataclass
class HazardCell:
    cell_id: str
    x: int
    y: int
    latitude: float
    longitude: float
    sensors: tuple = None
    drone_id: int = None
    sequence: int = None
    timestamp: float = None
    received_at: float = None
    confidence: float = 0.0
    risk_score: float = 0.0
    risk_level: str = "UNKNOWN"
    alert_state: str = "NORMAL"
    high_count: int = 0
    below_threshold_since: float = None
    acknowledged: bool = False
    history: list = field(default_factory=list)

    def update(self, frame, now=None):
        now = time.time() if now is None else now
        self.latitude = frame.latitude
        self.longitude = frame.longitude
        self.sensors = tuple(frame.sensors)
        self.drone_id = frame.drone_id
        self.sequence = frame.sequence
        self.timestamp = frame.timestamp
        self.received_at = now
        self.confidence = 1.0
        self.history.append((now, self.sensors))
        self.history = self.history[-100:]

    def refresh_confidence(self, now=None):
        now = time.time() if now is None else now

        if self.received_at is None:
            self.confidence = 0.0
            return self.confidence

        age = max(0.0, now - self.received_at)
        self.confidence = math.exp(-age / CONFIDENCE_DECAY_SECONDS)
        return self.confidence

    def is_stale(self, now=None):
        now = time.time() if now is None else now
        return (
            self.received_at is None
            or now - self.received_at > STALE_AFTER_SECONDS
        )

    def as_dict(self, now=None):
        self.refresh_confidence(now)
        return {
            "cell_id": self.cell_id,
            "x": self.x,
            "y": self.y,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "sensors": dict(zip(SENSOR_NAMES, self.sensors or ())),
            "drone_id": self.drone_id,
            "sequence": self.sequence,
            "last_received": self.received_at,
            "confidence": round(self.confidence, 3),
            "stale": self.is_stale(now),
            "risk_score": self.risk_score,
            "risk_level": self.risk_level,
            "alert_state": self.alert_state,
            "acknowledged": self.acknowledged,
        }


class HazardGrid:
    def __init__(self, origin_lat=18.5204, origin_lon=73.8567):
        self.origin_lat = origin_lat
        self.origin_lon = origin_lon
        self.cells = {}
        model_path = Path(__file__).resolve().parent / "models" / "svm_model.joblib"
        self.model = joblib.load(model_path)

    def coordinates_to_cell(self, latitude, longitude):
        metres_per_degree_lat = 111_320.0
        metres_per_degree_lon = (
            111_320.0 * math.cos(math.radians(self.origin_lat))
        )

        x_metres = (longitude - self.origin_lon) * metres_per_degree_lon
        y_metres = (latitude - self.origin_lat) * metres_per_degree_lat

        x = math.floor(x_metres / CELL_SIZE_METRES)
        y = math.floor(y_metres / CELL_SIZE_METRES)

        return x, y

    def cell_to_coordinates(self, x, y):
        metres_per_degree_lat = 111_320.0
        metres_per_degree_lon = (
            111_320.0 * math.cos(math.radians(self.origin_lat))
        )

        latitude = self.origin_lat + (
            (y + 0.5) * CELL_SIZE_METRES / metres_per_degree_lat
        )
        longitude = self.origin_lon + (
            (x + 0.5) * CELL_SIZE_METRES / metres_per_degree_lon
        )
        return latitude, longitude

    def update(self, frame, now=None):
        x, y = self.coordinates_to_cell(frame.latitude, frame.longitude)
        cell_id = f"{x}:{y}"

        if cell_id not in self.cells:
            latitude, longitude = self.cell_to_coordinates(x, y)
            self.cells[cell_id] = HazardCell(
                cell_id=cell_id,
                x=x,
                y=y,
                latitude=latitude,
                longitude=longitude,
            )

        self.cells[cell_id].update(frame, now)
        return self.cells[cell_id]

    def get_neighbours(self, cell):
        neighbours = []

        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue

                neighbour_id = f"{cell.x + dx}:{cell.y + dy}"
                neighbours.append(self.cells.get(neighbour_id))

        return neighbours

    def fused_features(self, cell, temperature=20.0, wind_speed=0.0):
        features = []
        confidences = []

        for neighbour in [cell] + self.get_neighbours(cell):
            if neighbour is None or neighbour.sensors is None:
                features.extend([0.0] * 6)
                confidences.append(0.0)
            else:
                confidence = neighbour.refresh_confidence()
                features.extend(
                    value * confidence for value in neighbour.sensors
                )
                confidences.append(confidence)

        features.extend([temperature, wind_speed])
        return features, confidences


    
    def predict_cell(self, cell, temperature=20.0, wind_speed=0.0):
        features, confidences = self.fused_features(
            cell,
            temperature=temperature,
            wind_speed=wind_speed
        )

        prediction = int(self.model.predict([features])[0])

        if hasattr(self.model, "predict_proba"):
            probabilities = self.model.predict_proba([features])[0]
            classes = list(self.model.classes_)

            if 1 in classes:
                risk_score = float(probabilities[classes.index(1)]) * 100
            else:
                risk_score = 0.0
        else:
            risk_score = 100.0 if prediction == 1 else 0.0

        cell.risk_score = round(risk_score, 2)
        cell.risk_level = "HIGH" if prediction == 1 else "NORMAL"

        return {
            "cell_id": cell.cell_id,
            "risk_level": cell.risk_level,
            "risk_score": cell.risk_score,
            "confidence": cell.confidence,
            "neighbour_confidences": confidences
        }   


    def snapshot(self, now=None):
        return [
            cell.as_dict(now)
            for cell in self.cells.values()
        ]
