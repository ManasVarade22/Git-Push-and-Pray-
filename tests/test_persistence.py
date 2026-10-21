import os
import threading
import time
import unittest
from uuid import uuid4

from alert_engine import AlertConfig, PerCellAlertStateMachine, PostgresAlertRepository
from persistence import (
    AsyncTelemetryWriter,
    HistoricalReading,
    PersistenceMetrics,
    PostgresTelemetryRepository,
    QueueFullError,
    TelemetryPersistenceCoordinator,
)
from streaming_hazard_grid import GridConfig, StreamingHazardGrid
from telemetry_protocol import TelemetryFrame


def frame(sequence, drone_id=1):
    return TelemetryFrame(drone_id, sequence, 1_700_000_000 + sequence,
                          0, sequence, (10, 20, 30, 40, 50, 60))


class RecordingRepository:
    def __init__(self, block=None, fail_count=0):
        self.rows = []
        self.block = block
        self.fail_count = fail_count
        self.calls = 0

    def initialize(self):
        pass

    def insert_idempotent(self, reading):
        self.calls += 1
        if self.block is not None:
            self.block.wait()
        if self.fail_count:
            self.fail_count -= 1
            raise OSError("temporary database outage")
        self.rows.append(reading)


class PersistenceUnitTests(unittest.TestCase):
    def test_live_grid_update_finishes_while_database_worker_is_blocked(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockedRepository(RecordingRepository):
            def insert_idempotent(self, reading):
                entered.set()
                super().insert_idempotent(reading)

        repository = BlockedRepository(block=release)
        writer = AsyncTelemetryWriter(repository, capacity=2)
        writer.start()
        grid = StreamingHazardGrid(GridConfig(0, 0))
        coordinator = TelemetryPersistenceCoordinator(grid, writer)
        try:
            update, status = coordinator.accept(frame(1), now=100)
            self.assertTrue(update.accepted)
            self.assertEqual(status, "queued")
            self.assertTrue(entered.wait(1))
            # Worker is blocked inside DB call, yet live state has already advanced.
            self.assertIn(update.cell_id, grid.snapshot(now=100).cells)
            self.assertEqual(writer.metrics().write_failures, 0)
        finally:
            release.set()
            self.assertTrue(writer.stop(timeout=2))

    def test_temporary_database_failure_retries_without_dropping_record(self):
        repository = RecordingRepository(fail_count=2)
        writer = AsyncTelemetryWriter(repository, retry_initial_seconds=0.001,
                                      retry_max_seconds=0.002)
        writer.start()
        try:
            writer.enqueue(frame(2))
            deadline = time.monotonic() + 2
            while writer.metrics().written != 1 and time.monotonic() < deadline:
                time.sleep(0.005)
            metrics = writer.metrics()
            self.assertEqual(metrics.written, 1)
            self.assertEqual(metrics.write_failures, 2)
            self.assertEqual(len(repository.rows), 1)
            self.assertTrue(metrics.database_connected)
        finally:
            self.assertTrue(writer.stop(timeout=2))

    def test_queue_overflow_is_explicit_and_counted(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockingRepo(RecordingRepository):
            def insert_idempotent(self, reading):
                entered.set()
                super().insert_idempotent(reading)

        writer = AsyncTelemetryWriter(BlockingRepo(block=release), capacity=1)
        writer.start()
        try:
            writer.enqueue(frame(3))
            self.assertTrue(entered.wait(1))
            writer.enqueue(frame(4))
            with self.assertRaises(QueueFullError):
                writer.enqueue(frame(5))
            metrics = writer.metrics()
            self.assertEqual(metrics.queue_overflows, 1)
            self.assertEqual(metrics.queue_depth, 1)
        finally:
            release.set()
            self.assertTrue(writer.stop(timeout=2))

    def test_retries_use_idempotent_repository_contract(self):
        reading = HistoricalReading.from_frame(frame(6))
        self.assertEqual((reading.drone_id, reading.sequence), (1, 6))
        self.assertEqual(len(reading.sensor_readings), 6)

    def test_acknowledgement_is_not_confirmed_when_repository_save_fails(self):
        class FailNextSave:
            def __init__(self):
                self.rows = {}
                self.fail = False

            def load_states(self):
                from dataclasses import replace
                return {key: replace(value) for key, value in self.rows.items()}

            def save_state(self, state):
                if self.fail:
                    self.fail = False
                    raise OSError("commit failed")
                from dataclasses import replace
                self.rows[state.cell_id] = replace(state)

        repository = FailNextSave()
        machine = PerCellAlertStateMachine(AlertConfig(0.8, 0.3), repository)
        cell = (7, 8)
        machine.process_prediction(cell, 0.9, now=10)
        machine.process_prediction(cell, 0.9, now=11)
        alert = machine.process_prediction(cell, 0.9, now=12)[0]
        repository.fail = True
        with self.assertRaisesRegex(OSError, "commit failed"):
            machine.acknowledge(alert.alert_id, "operator", now=13)
        self.assertIsNone(machine.state_for(cell).acknowledged_at)
        self.assertIsNone(repository.rows[cell].acknowledged_at)


@unittest.skipUnless(os.environ.get("TEST_POSTGRES_DSN"), "TEST_POSTGRES_DSN not configured")
class PostgresPersistenceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ["TEST_POSTGRES_DSN"]
        cls.repository = PostgresTelemetryRepository(cls.dsn)
        cls.repository.initialize()

    def test_normal_historical_write_and_duplicate_retry_are_idempotent(self):
        reading = HistoricalReading.from_frame(frame(int(uuid4().hex[:4], 16), drone_id=250))
        self.repository.insert_idempotent(reading)
        self.repository.insert_idempotent(reading)
        import psycopg
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM historical_telemetry WHERE drone_id=%s AND sequence=%s AND observed_at=to_timestamp(%s)",
                               (reading.drone_id, reading.sequence, reading.timestamp))
                self.assertEqual(cursor.fetchone()[0], 1)

    def test_temporary_failure_is_retried_and_eventually_committed(self):
        class FailOnce:
            def __init__(self, delegate):
                self.delegate = delegate
                self.remaining = 1

            def insert_idempotent(self, reading):
                if self.remaining:
                    self.remaining -= 1
                    raise OSError("injected temporary outage")
                self.delegate.insert_idempotent(reading)

        writer = AsyncTelemetryWriter(FailOnce(self.repository), retry_initial_seconds=0.001,
                                      retry_max_seconds=0.002)
        writer.start()
        try:
            writer.enqueue(frame(int(uuid4().hex[:4], 16), drone_id=249))
            deadline = time.monotonic() + 5
            while writer.metrics().written != 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(writer.metrics().write_failures, 1)
            self.assertEqual(writer.metrics().written, 1)
        finally:
            self.assertTrue(writer.stop(timeout=5))

    def test_acknowledgement_commit_and_restart_recovery(self):
        import psycopg
        cell = (int(uuid4().hex[:7], 16), int(uuid4().hex[:7], 16))
        config = AlertConfig(0.8, 0.3)
        first = PerCellAlertStateMachine(config, PostgresAlertRepository(self.dsn))
        for timestamp in (1_700_000_000.0, 1_700_000_001.0):
            first.process_prediction(cell, 0.9, now=timestamp)
        raised = first.process_prediction(cell, 0.9, now=1_700_000_002.0)[0]
        self.assertTrue(first.acknowledge(raised.alert_id, "p07-integration", now=1_700_000_003.0))
        recovered = PerCellAlertStateMachine(config, PostgresAlertRepository(self.dsn))
        state = recovered.state_for(cell)
        self.assertEqual(state.alert_id, raised.alert_id)
        self.assertEqual(state.acknowledged_by, "p07-integration")
        self.assertEqual(state.acknowledged_at, 1_700_000_003.0)
        self.assertTrue(state.hazard_active)

