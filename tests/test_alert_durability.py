"""PostgreSQL-backed alert durability and full dashboard restart tests.

Set TEST_POSTGRES_DSN to a dedicated disposable PostgreSQL test database.
Each test class creates and drops its own random schema; it never truncates or
modifies tables in the database's existing search path.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from uuid import uuid4
from pathlib import Path

from alert_engine import AlertConfig, PerCellAlertStateMachine, PostgresAlertRepository


@unittest.skipUnless(os.environ.get("TEST_POSTGRES_DSN"), "TEST_POSTGRES_DSN not configured")
class PostgresAlertDurabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import make_conninfo

        cls.psycopg = psycopg
        cls.base_dsn = os.environ["TEST_POSTGRES_DSN"]
        cls.schema = "p10_" + uuid4().hex
        with psycopg.connect(cls.base_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA "{cls.schema}"')
        cls.schema_dsn = make_conninfo(cls.base_dsn, options=f"-c search_path={cls.schema}")
        PostgresAlertRepository(cls.schema_dsn).initialize()
        cls.config = AlertConfig(
            entry_threshold=0.8,
            clear_threshold=0.3,
            prediction_stale_after_seconds=3600,
            repeat_interval_seconds=3600,
            escalate_after_seconds=7200,
            escalation_interval_seconds=7200,
        )

    @classmethod
    def tearDownClass(cls):
        with cls.psycopg.connect(cls.base_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA IF EXISTS "{cls.schema}" CASCADE')

    def _create_active_alert(self, cell):
        machine = PerCellAlertStateMachine(self.config, PostgresAlertRepository(self.schema_dsn))
        machine.process_prediction(cell, 0.95, now=1_800_000_000.0)
        machine.process_prediction(cell, 0.95, now=1_800_000_001.0)
        raised = machine.process_prediction(cell, 0.95, now=1_800_000_002.0)[0]
        self.assertTrue(machine.state_for(cell).hazard_active)
        return raised.alert_id

    def test_acknowledgement_is_visible_from_new_connection_after_commit(self):
        cell = (int(uuid4().hex[:7], 16), int(uuid4().hex[:7], 16))
        alert_id = self._create_active_alert(cell)
        machine = PerCellAlertStateMachine(self.config, PostgresAlertRepository(self.schema_dsn))
        self.assertTrue(machine.acknowledge(alert_id, "p10-integration", now=1_800_000_003.0))

        # This query uses an independent PostgreSQL connection after acknowledge()
        # returned, so the acknowledgement must have committed, not just mutated RAM.
        with self.psycopg.connect(self.schema_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT acknowledged_at, acknowledged_by FROM hazard_alerts WHERE alert_id=%s",
                    (alert_id,),
                )
                acknowledged_at, acknowledged_by = cursor.fetchone()
        self.assertIsNotNone(acknowledged_at)
        self.assertEqual(acknowledged_by, "p10-integration")

    def test_acknowledgement_and_active_hazard_survive_full_dashboard_process_restart(self):
        cell = (int(uuid4().hex[:7], 16), int(uuid4().hex[:7], 16))
        alert_id = self._create_active_alert(cell)
        first = self._start_dashboard()
        try:
            base_url, process = first
            before_ack = self._get_alert(base_url, alert_id)
            self.assertTrue(before_ack["active"])
            self.assertFalse(before_ack["acknowledged"])
            request = urllib.request.Request(
                base_url + f"/api/v1/alerts/{alert_id}/acknowledge",
                data=json.dumps({"acknowledged_by": "p10-dashboard-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                ack_response = json.loads(response.read())
            self.assertTrue(ack_response["acknowledged"])
            self.assertTrue(ack_response["hazard_active"])
            before_restart = self._get_alert(base_url, alert_id)
            self.assertTrue(before_restart["active"])
            self.assertTrue(before_restart["acknowledged"])
            self.assertEqual(before_restart["acknowledged_by"], "p10-dashboard-test")
            with self.psycopg.connect(self.schema_dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT acknowledged_at FROM hazard_alerts WHERE alert_id=%s", (alert_id,))
                    self.assertIsNotNone(cursor.fetchone()[0])
        finally:
            self._stop_dashboard(first)

        # A second independent dashboard_server process loads alert state from
        # PostgreSQL during startup. Its in-memory grid starts empty by design.
        second = self._start_dashboard()
        try:
            base_url, process = second
            after_restart = self._get_alert(base_url, alert_id)
            self.assertTrue(after_restart["active"])
            self.assertTrue(after_restart["acknowledged"])
            self.assertEqual(after_restart["acknowledged_by"], "p10-dashboard-test")
            self.assertIsNone(after_restart["cell_id"])
            snapshot = self._get_json(base_url + "/api/v1/map/snapshot")
            self.assertEqual(snapshot["data"]["cells"], [])
        finally:
            self._stop_dashboard(second)

    def test_acknowledgement_commit_failure_is_not_confirmed_or_cached(self):
        import psycopg

        cell = (int(uuid4().hex[:7], 16), int(uuid4().hex[:7], 16))
        machine = PerCellAlertStateMachine(self.config, PostgresAlertRepository(self.schema_dsn))
        machine.process_prediction(cell, 0.95, now=1_800_000_010.0)
        machine.process_prediction(cell, 0.95, now=1_800_000_011.0)
        raised = machine.process_prediction(cell, 0.95, now=1_800_000_012.0)[0]
        constraint = "p10_reject_ack_" + uuid4().hex[:12]
        with psycopg.connect(self.schema_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'ALTER TABLE hazard_alerts ADD CONSTRAINT "{constraint}" '
                    "CHECK (acknowledged_by IS DISTINCT FROM 'p10-force-rollback')"
                )
        try:
            with self.assertRaises(psycopg.errors.CheckViolation):
                machine.acknowledge(raised.alert_id, "p10-force-rollback", now=1_800_000_013.0)
            self.assertIsNone(machine.state_for(cell).acknowledged_at)
            with psycopg.connect(self.schema_dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT acknowledged_at FROM hazard_alerts WHERE alert_id=%s",
                        (raised.alert_id,),
                    )
                    self.assertIsNone(cursor.fetchone()[0])
        finally:
            with psycopg.connect(self.schema_dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f'ALTER TABLE hazard_alerts DROP CONSTRAINT "{constraint}"')

    def _start_dashboard(self):
        http_port = self._free_port()
        udp_port = self._free_port()
        environment = os.environ.copy()
        environment["DATABASE_URL"] = self.schema_dsn
        repo_root = Path(__file__).resolve().parents[1]
        process = subprocess.Popen(
            [sys.executable, "dashboard_server.py", "--http-host", "127.0.0.1",
             "--http-port", str(http_port), "--udp-host", "127.0.0.1", "--udp-port", str(udp_port)],
            cwd=repo_root,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        base_url = f"http://127.0.0.1:{http_port}"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.fail(f"dashboard process exited during startup with code {process.returncode}")
            try:
                self._get_json(base_url + "/api/v1/health", timeout=0.25)
                return base_url, process
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
                time.sleep(0.05)
        process.terminate()
        process.wait(timeout=3)
        self.fail("dashboard process did not become ready within 15 seconds")

    @staticmethod
    def _stop_dashboard(dashboard):
        _base_url, process = dashboard
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    @staticmethod
    def _free_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    @staticmethod
    def _get_json(url, timeout=2):
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read())

    def _get_alert(self, base_url, alert_id):
        alerts = self._get_json(base_url + "/api/v1/alerts")["alerts"]
        match = next((item for item in alerts if item["id"] == alert_id), None)
        self.assertIsNotNone(match, f"alert {alert_id} was not restored by the backend")
        return match


if __name__ == "__main__":
    unittest.main()

