"""Runnable UDP + FastAPI dashboard composition for field or demo deployments."""

from __future__ import annotations

import argparse
import math
import os
import socket
import threading
import time
from dataclasses import dataclass

from alert_engine import AlertConfig, InMemoryAlertRepository, PerCellAlertStateMachine, PostgresAlertRepository
from dashboard_api import DashboardRuntime, create_app
from persistence import AsyncTelemetryWriter, PostgresTelemetryRepository, TelemetryPersistenceCoordinator
from streaming_hazard_grid import GridConfig, StreamingHazardGrid, WeatherObservation
from telemetry_protocol import TelemetryFrame
from telemetry_simulator import ReorderBuffer


@dataclass
class PacketHealth:
    lock: threading.Lock
    values: dict

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.values)

    def increment(self, name: str, amount: int = 1) -> None:
        with self.lock:
            self.values[name] += amount

    def set(self, name: str, value) -> None:
        with self.lock:
            self.values[name] = value


def serve(args) -> None:
    origin_lat = int(round(args.origin_latitude * 1_000_000))
    origin_lon = int(round(args.origin_longitude * 1_000_000))
    grid_config = GridConfig(origin_lat, origin_lon, cell_size_meters=args.cell_size,
                             stale_timeout_seconds=args.cell_stale_seconds)

    evaluator = None
    if args.demo_model:
        from synthetic_demo_svm import build_synthetic_demo_model
        evaluator = build_synthetic_demo_model(seed=args.seed)
    grid = StreamingHazardGrid(grid_config, risk_evaluator=evaluator)

    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        alert_repository = PostgresAlertRepository(dsn)
        writer = AsyncTelemetryWriter(PostgresTelemetryRepository(dsn), capacity=args.queue_capacity)
        writer.start()
    else:
        alert_repository = InMemoryAlertRepository()
        writer = None
    alerts = PerCellAlertStateMachine(AlertConfig(args.entry_threshold, args.clear_threshold), alert_repository)
    coordinator = None if writer is None else TelemetryPersistenceCoordinator(grid, writer)

    if args.demo_weather:
        grid.update_weather(WeatherObservation(time.time(), 24.0, 3.0, 205.0))
        weather_label = "DEMONSTRATION WEATHER · SYNTHETIC"
    else:
        weather_label = os.environ.get("WEATHER_SOURCE_LABEL", "UNVERIFIED WEATHER SOURCE")

    if args.simulate:
        telemetry_label = "SIMULATED TELEMETRY · NOT FIELD MEASUREMENTS"
    else:
        telemetry_label = os.environ.get("TELEMETRY_SOURCE_LABEL", "UNVERIFIED TELEMETRY SOURCE")

    packet_health = PacketHealth(threading.Lock(), {
        "received": 0, "accepted": 0, "rejected": 0, "dropped": None,
        "sent": None, "duplicated": 0, "late": 0, "history_queue_full": 0,
        "listener_status": "starting",
    })
    runtime = DashboardRuntime(
        grid=grid,
        alerts=alerts,
        telemetry_writer=writer,
        packet_stats_provider=packet_health.snapshot,
        telemetry_source_label=telemetry_label,
        weather_source_label=weather_label,
    )
    stop = threading.Event()
    receiver = threading.Thread(
        target=_udp_receiver,
        args=(args.udp_host, args.udp_port, grid, alerts, coordinator, packet_health, stop),
        name="dashboard-udp-receiver", daemon=True,
    )
    receiver.start()

    if args.simulate:
        sender = threading.Thread(
            target=_demo_sender,
            args=(args.udp_host, args.udp_port, origin_lat, origin_lon, grid_config.cell_size_meters,
                  args.demo_weather, args.seed, grid, stop),
            name="dashboard-demo-udp-sender", daemon=True,
        )
        sender.start()

    import uvicorn
    try:
        uvicorn.run(create_app(runtime), host=args.http_host, port=args.http_port, log_level="info")
    finally:
        stop.set()
        receiver.join(timeout=2)
        if writer is not None:
            drained = writer.stop(timeout=10)
            if not drained:
                print("WARNING: telemetry persistence did not drain before shutdown timeout")


def _udp_receiver(host, port, grid, alerts, coordinator, packet_health, stop) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((host, port))
    except OSError as exc:
        packet_health.set("listener_status", f"failed · {type(exc).__name__}")
        sock.close()
        return
    packet_health.set("listener_status", "connected")
    sock.settimeout(0.25)
    reorder = ReorderBuffer(window_seconds=2.0)
    try:
        while not stop.is_set():
            try:
                payload, _peer = sock.recvfrom(64)
            except socket.timeout:
                for frame in reorder.advance():
                    _apply_frame(frame, grid, alerts, coordinator, packet_health)
                alerts.tick()
                continue
            packet_health.increment("received")
            try:
                frame = TelemetryFrame.from_bytes(payload)
            except ValueError:
                packet_health.increment("rejected")
                continue
            status, ready = reorder.push(frame)
            if status in ("duplicate", "late"):
                packet_health.increment("rejected")
                if status == "late":
                    packet_health.increment("late")
                else:
                    packet_health.increment("duplicated")
            for ready_frame in ready:
                _apply_frame(ready_frame, grid, alerts, coordinator, packet_health)
            alerts.tick()
    finally:
        sock.close()
        packet_health.set("listener_status", "stopped")


def _apply_frame(frame, grid, alerts, coordinator, packet_health) -> None:
    now = time.time()
    if coordinator is None:
        update = grid.update(frame, now=now)
        status = "queued"
    else:
        update, status = coordinator.accept(frame, now=now)
    if not update.accepted:
        packet_health.increment("rejected")
        return
    packet_health.increment("accepted")
    if status == "history_queue_full":
        packet_health.increment("history_queue_full")
    if update.risk_status == "evaluated":
        alerts.process_prediction(update.cell_id, update.risk_score, now=now)
    else:
        alerts.process_prediction(update.cell_id, None, valid=False, now=now)


def _demo_sender(host, port, origin_lat_micro, origin_lon_micro, cell_size, demo_weather, seed, grid, stop) -> None:
    """Send clearly synthetic frames over binary UDP to exercise the dashboard."""
    import random
    rng = random.Random(seed)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sequence = 0
    latitude_step = int(round(math.degrees(cell_size / 6_371_008.8) * 1_000_000))
    latitude_degrees = origin_lat_micro / 1_000_000
    longitude_step = int(round(latitude_step / max(0.01, math.cos(math.radians(latitude_degrees)))))
    offsets = [(0, 0), (-1, -1), (0, -1), (1, -1), (-1, 0), (1, 0), (-1, 1), (0, 1), (1, 1)]
    try:
        while not stop.is_set():
            if demo_weather:
                # This is generated demonstration weather, explicitly labelled upstream.
                grid_weather = WeatherObservation(time.time(), 24.0, 3.0, 205.0)
                grid.update_weather(grid_weather)
            for x, y in offsets:
                if stop.is_set():
                    break
                hot_channel = 42_000 if x == 0 and y == 0 else 38_000
                readings = (hot_channel, 8_000, rng.randrange(100, 500), 250, 120, 90)
                frame = TelemetryFrame(
                    drone_id=1,
                    sequence=sequence & 0xFFFF,
                    timestamp=int(time.time()),
                    latitude_microdegrees=origin_lat_micro + y * latitude_step,
                    longitude_microdegrees=origin_lon_micro + x * longitude_step,
                    sensor_readings=readings,
                )
                sock.sendto(frame.to_bytes(), (host, port))
                sequence += 1
                time.sleep(0.01)
            time.sleep(1.0)
    finally:
        sock.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--http-host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--udp-host", default="127.0.0.1")
    parser.add_argument("--udp-port", type=int, default=5005)
    parser.add_argument("--origin-latitude", type=float, default=0.0)
    parser.add_argument("--origin-longitude", type=float, default=0.0)
    parser.add_argument("--cell-size", type=float, default=10.0)
    parser.add_argument("--cell-stale-seconds", type=float, default=30.0)
    parser.add_argument("--entry-threshold", type=float, default=0.8)
    parser.add_argument("--clear-threshold", type=float, default=0.3)
    parser.add_argument("--queue-capacity", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20261021)
    parser.add_argument("--demo-model", action="store_true", help="use synthetic-only demonstration SVM")
    parser.add_argument("--demo-weather", action="store_true", help="use generated demonstration weather")
    parser.add_argument("--simulate", action="store_true", help="send clearly labelled synthetic binary UDP frames")
    args = parser.parse_args()
    serve(args)


if __name__ == "__main__":
    main()

