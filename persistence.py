"""Bounded asynchronous PostgreSQL persistence for accepted telemetry frames."""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

from telemetry_protocol import TelemetryFrame

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HistoricalReading:
    drone_id: int
    sequence: int
    timestamp: int
    latitude_microdegrees: int
    longitude_microdegrees: int
    sensor_readings: tuple[int, ...]

    @classmethod
    def from_frame(cls, frame: TelemetryFrame) -> "HistoricalReading":
        return cls(frame.drone_id, frame.sequence, frame.timestamp,
                   frame.latitude_microdegrees, frame.longitude_microdegrees,
                   tuple(frame.sensor_readings))


class HistoricalRepository(Protocol):
    def initialize(self) -> None: ...
    def insert_idempotent(self, reading: HistoricalReading) -> None: ...


class QueueFullError(RuntimeError):
    """Submission was rejected; caller must record/retry the non-persisted frame."""


@dataclass(frozen=True, slots=True)
class PersistenceMetrics:
    queue_depth: int
    queue_capacity: int
    enqueued: int
    written: int
    write_failures: int
    queue_overflows: int
    database_connected: bool | None
    last_error: str | None


class PostgresTelemetryRepository:
    """Idempotent PostgreSQL writer with reproducible, versioned schema setup."""

    def __init__(self, conninfo: str):
        if not conninfo:
            raise ValueError("PostgreSQL connection information is required")
        self.conninfo = conninfo

    @classmethod
    def from_environment(cls, variable: str = "DATABASE_URL") -> "PostgresTelemetryRepository":
        import os
        conninfo = os.environ.get(variable)
        if not conninfo:
            raise RuntimeError(f"set {variable} to a PostgreSQL connection string")
        return cls(conninfo)

    def initialize(self) -> None:
        migration = Path(__file__).with_name("migrations") / "003_historical_telemetry.sql"
        sql = migration.read_text(encoding="utf-8")
        psycopg = _get_psycopg()
        with psycopg.connect(self.conninfo) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)

    def insert_idempotent(self, reading: HistoricalReading) -> None:
        psycopg = _get_psycopg()
        with psycopg.connect(self.conninfo) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO historical_telemetry
                       (drone_id, sequence, observed_at, latitude_microdegrees,
                        longitude_microdegrees, sensor_readings)
                       VALUES (%s, %s, to_timestamp(%s), %s, %s, %s)
                       ON CONFLICT (drone_id, sequence, observed_at) DO NOTHING""",
                    (reading.drone_id, reading.sequence, reading.timestamp,
                     reading.latitude_microdegrees, reading.longitude_microdegrees,
                     list(reading.sensor_readings)),
                )
        # Exiting the connection context commits before this method returns.


class AsyncTelemetryWriter:
    """Bounded queue and one retrying database worker.

    enqueue is non-blocking. When full, it raises QueueFullError and increments
    a visible overflow counter. The caller must surface this result and retain
    responsibility for the rejected historical record. DB outages are retried
    with capped exponential backoff; queued records are not discarded.
    """

    def __init__(self, repository: HistoricalRepository, *, capacity: int = 1000,
                 retry_initial_seconds: float = 0.1, retry_max_seconds: float = 5.0,
                 sleep: Callable[[float], None] = time.sleep):
        if capacity <= 0 or retry_initial_seconds <= 0 or retry_max_seconds < retry_initial_seconds:
            raise ValueError("invalid queue capacity or retry interval")
        self.repository = repository
        self._queue: queue.Queue[HistoricalReading] = queue.Queue(maxsize=capacity)
        self._retry_initial = retry_initial_seconds
        self._retry_max = retry_max_seconds
        self._sleep = sleep
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._enqueued = self._written = self._failures = self._overflows = 0
        self._connected: bool | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="telemetry-db-writer", daemon=True)
            self._thread.start()

    def enqueue(self, frame: TelemetryFrame) -> None:
        if self._thread is None or not self._thread.is_alive():
            raise RuntimeError("start the persistence worker before enqueueing")
        reading = HistoricalReading.from_frame(frame)
        try:
            self._queue.put_nowait(reading)
        except queue.Full as exc:
            with self._lock:
                self._overflows += 1
            logger.error("historical telemetry queue full; frame %s/%s rejected", frame.drone_id, frame.sequence)
            raise QueueFullError(f"history queue full for frame {frame.drone_id}/{frame.sequence}") from exc
        with self._lock:
            self._enqueued += 1

    def metrics(self) -> PersistenceMetrics:
        with self._lock:
            return PersistenceMetrics(self._queue.qsize(), self._queue.maxsize,
                                      self._enqueued, self._written, self._failures,
                                      self._overflows, self._connected, self._last_error)

    def stop(self, timeout: float | None = None) -> bool:
        """Request drain and stop; False means records remain and caller must not claim clean shutdown."""
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
            if thread.is_alive():
                return False
        self._thread = None
        return self._queue.empty()

    def _run(self) -> None:
        delay = self._retry_initial
        initialized = False
        while not initialized:
            try:
                initialize = getattr(self.repository, "initialize", None)
                if initialize is not None:
                    initialize()
                initialized = True
                with self._lock:
                    self._connected = True
                    self._last_error = None
            except Exception as exc:
                with self._lock:
                    self._failures += 1
                    self._connected = False
                    self._last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("database initialization failed; retrying: %s", exc)
                if self._stop.is_set() and self._queue.empty():
                    return
                self._sleep(delay)
                delay = min(self._retry_max, delay * 2)

        while not self._stop.is_set() or not self._queue.empty():
            try:
                reading = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            delay = self._retry_initial
            while True:
                try:
                    self.repository.insert_idempotent(reading)
                except Exception as exc:
                    with self._lock:
                        self._failures += 1
                        self._connected = False
                        self._last_error = f"{type(exc).__name__}: {exc}"
                    logger.warning("historical write failed; retrying queued frame: %s", exc)
                    self._sleep(delay)
                    delay = min(self._retry_max, delay * 2)
                    continue
                else:
                    with self._lock:
                        self._written += 1
                        self._connected = True
                        self._last_error = None
                    break
            self._queue.task_done()


class TelemetryPersistenceCoordinator:
    """Submit to memory grid first; history enqueue never waits for a DB commit."""

    def __init__(self, grid, writer: AsyncTelemetryWriter):
        self.grid = grid
        self.writer = writer

    def accept(self, frame: TelemetryFrame, *, now: float | None = None, **grid_options):
        update = self.grid.update(frame, now=now, **grid_options)
        if update.accepted:
            try:
                self.writer.enqueue(frame)
            except QueueFullError:
                # Explicitly annotate that the live update succeeded but its
                # history record was rejected; caller can alert/backpressure.
                return update, "history_queue_full"
            return update, "queued"
        return update, "not_accepted"


def _get_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("install psycopg[binary] for PostgreSQL persistence") from exc
    return psycopg

