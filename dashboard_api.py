"""FastAPI snapshot/WebSocket API for the in-memory hazard grid.

Contract:
  GET /api/v1/map/snapshot -> {type: snapshot, data: ...}
  WS  /ws/map             -> initial and changed/current snapshot messages
  GET /api/v1/health       -> API, UDP, telemetry DB writer and alert-store health
  GET /api/v1/alerts       -> active per-cell alert states
  POST /api/v1/alerts/{id}/acknowledge {"acknowledged_by": "..."}

Map reads are served from StreamingHazardGrid and alert state in memory. They
never query PostgreSQL. The acknowledgement route is the intentional exception:
it waits for the PostgreSQL transaction before returning success.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping

from alert_engine import PerCellAlertStateMachine, PostgresAlertRepository
from persistence import AsyncTelemetryWriter
from streaming_hazard_grid import CellId, StreamingHazardGrid

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse
    from pydantic import BaseModel, Field
except ImportError as exc:  # helpful error if optional web dependencies are missing
    raise RuntimeError("install fastapi and uvicorn from requirements.txt to serve the dashboard") from exc


@dataclass(slots=True)
class DashboardRuntime:
    grid: StreamingHazardGrid
    alerts: PerCellAlertStateMachine
    telemetry_writer: AsyncTelemetryWriter | None = None
    packet_stats_provider: Callable[[], object] | None = None
    telemetry_source_label: str = "UNVERIFIED SOURCE"
    weather_source_label: str = "UNVERIFIED SOURCE"
    update_interval_seconds: float = 1.0
    last_ack_database_connected: bool | None = None
    _ack_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.update_interval_seconds) or self.update_interval_seconds <= 0:
            raise ValueError("update_interval_seconds must be positive and finite")

    @property
    def model_source_label(self) -> str:
        evaluator = self.grid.risk_evaluator
        if evaluator is None:
            return "NO MODEL LOADED"
        provenance = getattr(evaluator, "provenance", None)
        if provenance:
            return str(provenance)
        return "MODEL SOURCE UNVERIFIED"


class AcknowledgementRequest(BaseModel):
    acknowledged_by: str = Field(min_length=1, max_length=120)


def create_app(runtime: DashboardRuntime) -> FastAPI:
    """Build an API app around the active grid, alert machine and writer."""
    app = FastAPI(title="Aerial Hazard Dashboard API", version="1.0")
    web_root = Path(__file__).with_name("web")
    app.mount("/assets", StaticFiles(directory=str(web_root)), name="assets")

    @app.get("/")
    async def index():
        return FileResponse(web_root / "index.html")

    @app.get("/api/v1/map/snapshot")
    async def map_snapshot():
        return build_snapshot(runtime)

    @app.get("/api/v1/alerts")
    async def active_alerts():
        snapshot = build_snapshot(runtime)
        return {"alerts": snapshot["data"]["alerts"]}

    @app.get("/api/v1/health")
    async def health():
        return build_health(runtime)

    @app.post("/api/v1/alerts/{alert_id}/acknowledge")
    async def acknowledge(alert_id: str, request: AcknowledgementRequest):
        if not isinstance(runtime.alerts.repository, PostgresAlertRepository):
            raise HTTPException(status_code=503, detail="Durable acknowledgement storage is not configured")
        operator = request.acknowledged_by.strip()
        if not operator:
            raise HTTPException(status_code=422, detail="acknowledged_by cannot be blank")
        try:
            confirmed = await asyncio.to_thread(
                runtime.alerts.acknowledge, alert_id, operator
            )
        except Exception as exc:
            with runtime._ack_lock:
                runtime.last_ack_database_connected = False
            raise HTTPException(status_code=503, detail=f"Acknowledgement commit failed ({type(exc).__name__})") from exc
        with runtime._ack_lock:
            runtime.last_ack_database_connected = True
        if not confirmed:
            raise HTTPException(status_code=404, detail="No active alert with that identifier")
        # The alert machine returns only after PostgresAlertRepository's
        # transaction context has committed successfully.
        return {"alert_id": alert_id, "acknowledged": True,
                "hazard_active": True, "acknowledged_by": operator}

    @app.websocket("/ws/map")
    async def map_updates(websocket: WebSocket):
        await websocket.accept()
        try:
            # A complete memory snapshot on connect lets reconnecting clients
            # replace any stale local map before receiving fresh updates.
            await websocket.send_json(build_snapshot(runtime))
            while True:
                await asyncio.sleep(runtime.update_interval_seconds)
                await websocket.send_json(build_snapshot(runtime))
        except WebSocketDisconnect:
            return

    return app


def build_snapshot(runtime: DashboardRuntime, *, now: float | None = None) -> dict:
    """Create the documented WebSocket and REST snapshot from memory only."""
    now = time.time() if now is None else now
    grid = runtime.grid.snapshot(now=now)
    weather = runtime.grid.weather_observation()
    weather_age = None if weather is None else max(0.0, now - weather.timestamp)
    if weather is None:
        weather_state = "unavailable"
    elif weather_age > runtime.grid.config.weather_max_age_seconds:
        weather_state = "stale"
    else:
        weather_state = "fresh"
    alerts = runtime.alerts.states_snapshot()

    cells = []
    for cell_id, cell in grid.cells.items():
        alert = alerts.get(cell_id)
        readings = []
        for index in range(6):
            timestamp = cell.sensor_timestamps.get(index)
            readings.append({
                "channel": index + 1,
                "quantized_value": cell.readings.get(index),
                "timestamp": _iso(timestamp),
                "confidence": cell.sensor_confidences.get(index),
            })
        cells.append({
            "id": _cell_key(cell_id),
            "cell_x": cell_id[0],
            "cell_y": cell_id[1],
            "center": {"latitude": cell.center_latitude_microdegrees / 1_000_000,
                       "longitude": cell.center_longitude_microdegrees / 1_000_000},
            "cell_size_meters": runtime.grid.config.cell_size_meters,
            "risk": {"score": cell.risk_score, "status": cell.risk_status,
                     "source": runtime.model_source_label},
            "freshness": cell.freshness,
            "confidence": cell.confidence,
            "last_seen_at": _iso(cell.last_received_at),
            "sensor_readings": readings,
            "weather": {
                "temperature_c": None if weather is None else weather.temperature_c,
                "wind_speed_mps": None if weather is None else weather.wind_speed_mps,
                "wind_direction_degrees": None if weather is None else weather.wind_direction_degrees,
                "timestamp": None if weather is None else _iso(weather.timestamp),
                "feature_timestamps": {
                    "temperature": None if weather is None else _iso(weather.timestamp),
                    "wind_speed": None if weather is None else _iso(weather.timestamp),
                    "wind_direction": None if weather is None else _iso(weather.timestamp),
                },
                "age_seconds": weather_age,
                "status": weather_state,
                "source": runtime.weather_source_label,
            },
            "alert": _alert_payload(alert, cell_id),
        })
    active_alerts = [_alert_payload(state, cell_id) for cell_id, state in alerts.items()
                     if state.hazard_active]
    return {"type": "snapshot", "data": {
        "schema_version": 1,
        "generated_at": _iso(now),
        "grid_version": grid.version,
        "cell_size_meters": runtime.grid.config.cell_size_meters,
        "risk_thresholds": {"entry": runtime.alerts.config.entry_threshold,
                             "clear": runtime.alerts.config.clear_threshold},
        "sources": {
            "telemetry": runtime.telemetry_source_label,
            "model": runtime.model_source_label,
            "weather": runtime.weather_source_label,
        },
        "weather_status": weather_state,
        "cells": cells,
        "alerts": active_alerts,
        "health": build_health(runtime),
    }}


def build_health(runtime: DashboardRuntime) -> dict:
    writer_metrics = None if runtime.telemetry_writer is None else runtime.telemetry_writer.metrics()
    packet_stats = None
    if runtime.packet_stats_provider is not None:
        try:
            packet_stats = _plain(runtime.packet_stats_provider())
        except Exception as exc:
            packet_stats = {"available": False, "error": type(exc).__name__}
    with runtime._ack_lock:
        ack_db = runtime.last_ack_database_connected
    return {
        "api": "healthy",
        "udp_packet_stats": packet_stats,
        "telemetry_database": _plain(writer_metrics),
        "acknowledgement_store": {
            "durable": isinstance(runtime.alerts.repository, PostgresAlertRepository),
            "database_connected_last_result": ack_db,
        },
    }


def _alert_payload(state, cell_id: CellId | None = None) -> dict | None:
    if state is None:
        return None
    return {
        "active": state.hazard_active,
        "cell_id": None if cell_id is None else _cell_key(cell_id),
        "id": state.alert_id,
        "raised_at": _iso(state.raised_at),
        "acknowledged": state.acknowledged_at is not None,
        "acknowledged_at": _iso(state.acknowledged_at),
        "acknowledged_by": state.acknowledged_by,
        "resolved_at": _iso(state.resolved_at),
        "escalation_level": state.escalation_level,
        "last_notified_at": _iso(state.last_notified_at),
        "risk_score": state.last_risk_score,
        "quality_status": state.quality_status,
    }


def _cell_key(cell_id: CellId) -> str:
    return f"{cell_id[0]}:{cell_id[1]}"


def _iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _plain(value):
    if value is None:
        return None
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value

