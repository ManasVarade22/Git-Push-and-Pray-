import os
import unittest
from uuid import uuid4

from alert_engine import (
    CLEAR_DURATION_SECONDS,
    AlertConfig,
    InMemoryAlertRepository,
    PerCellAlertStateMachine,
    PostgresAlertRepository,
)


class FakeClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value

    def set(self, value):
        self.value = value


def config(**overrides):
    values = dict(
        entry_threshold=0.8,
        clear_threshold=0.3,
        prediction_stale_after_seconds=60.0,
        repeat_interval_seconds=60.0,
        escalate_after_seconds=120.0,
        escalation_interval_seconds=120.0,
    )
    values.update(overrides)
    return AlertConfig(**values)


class AlertEngineTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.repository = InMemoryAlertRepository()
        self.machine = PerCellAlertStateMachine(config(), self.repository, clock=self.clock)
        self.cell = (12, -4)

    def high(self, t):
        self.clock.set(t)
        return self.machine.process_prediction(self.cell, 0.9)

    def test_three_consecutive_high_predictions_raise_one_alert(self):
        self.assertEqual(self.high(0), [])
        self.assertEqual(self.high(1), [])
        events = self.high(2)
        self.assertEqual([event.event_type for event in events], ["RAISED"])
        state = self.machine.state_for(self.cell)
        self.assertTrue(state.hazard_active)
        self.assertIsNotNone(state.alert_id)
        self.assertEqual(state.consecutive_high_count, 0)

    def test_valid_below_entry_resets_high_counter(self):
        self.high(0)
        self.high(1)
        self.clock.set(2)
        self.assertEqual(self.machine.process_prediction(self.cell, 0.79), [])
        self.high(3)
        self.high(4)
        self.assertFalse(self.machine.state_for(self.cell).hazard_active)
        events = self.high(5)
        self.assertEqual([event.event_type for event in events], ["RAISED"])

    def test_stale_gap_breaks_consecutive_high_prediction_streak(self):
        self.high(0)
        self.high(1)
        self.assertEqual(self.high(100), [])
        self.assertFalse(self.machine.state_for(self.cell).hazard_active)
        self.assertEqual(self.machine.state_for(self.cell).consecutive_high_count, 1)
        self.assertEqual(self.high(101), [])
        self.assertEqual([event.event_type for event in self.high(102)], ["RAISED"])

    def _raise_alert(self):
        self.high(0)
        self.high(1)
        return self.high(2)[0]

    def test_clear_requires_five_continuous_minutes_below_clear_threshold(self):
        self._raise_alert()
        self.clock.set(3)
        self.machine.process_prediction(self.cell, 0.2)
        self.assertEqual(self.machine.state_for(self.cell).clear_started_at, 3)
        for t in range(53, 303, 50):
            self.clock.set(t)
            events = self.machine.process_prediction(self.cell, 0.2)
            if t < 303:
                self.assertNotIn("CLEARED", [event.event_type for event in events])
        self.clock.set(303)
        events = self.machine.tick()
        self.assertEqual([event.event_type for event in events], ["CLEARED"])
        state = self.machine.state_for(self.cell)
        self.assertFalse(state.hazard_active)
        self.assertEqual(state.resolved_at, 303)
        self.assertGreaterEqual(state.resolved_at - 3, CLEAR_DURATION_SECONDS)

    def test_risk_at_or_above_clear_threshold_resets_clear_timer(self):
        self._raise_alert()
        self.clock.set(3)
        self.machine.process_prediction(self.cell, 0.2)
        self.clock.set(20)
        self.machine.process_prediction(self.cell, 0.2)
        self.clock.set(30)
        self.machine.process_prediction(self.cell, 0.3)
        self.assertIsNone(self.machine.state_for(self.cell).clear_started_at)
        self.clock.set(31)
        self.machine.process_prediction(self.cell, 0.2)
        self.assertEqual(self.machine.state_for(self.cell).clear_started_at, 31)

    def test_stale_missing_or_invalid_readings_do_not_clear_hazard(self):
        self._raise_alert()
        self.clock.set(3)
        self.machine.process_prediction(self.cell, 0.2)
        self.clock.set(20)
        self.machine.process_prediction(self.cell, None, valid=False)
        state = self.machine.state_for(self.cell)
        self.assertIsNone(state.clear_started_at)
        self.assertTrue(state.hazard_active)
        self.clock.set(21)
        self.machine.process_prediction(self.cell, 0.0, stale=True)
        self.assertTrue(self.machine.state_for(self.cell).hazard_active)
        self.clock.set(22)
        self.machine.process_prediction(self.cell, float("nan"))
        self.assertEqual(self.machine.state_for(self.cell).quality_status, "invalid")
        self.assertTrue(self.machine.state_for(self.cell).hazard_active)

    def test_repeat_and_escalation_stop_after_acknowledgement(self):
        event = self._raise_alert()
        self.clock.set(62)
        repeated = self.machine.tick()
        self.assertEqual([item.event_type for item in repeated], ["REPEATED"])
        self.clock.set(122)
        escalated = self.machine.tick()
        self.assertEqual([item.event_type for item in escalated], ["ESCALATED"])
        self.assertEqual(escalated[0].escalation_level, 1)
        self.assertTrue(self.machine.acknowledge(event.alert_id, "operator-1", now=123))
        state = self.machine.state_for(self.cell)
        self.assertTrue(state.hazard_active)
        self.assertIsNotNone(state.acknowledged_at)
        self.clock.set(1000)
        self.assertEqual(self.machine.tick(), [])
        self.assertTrue(self.machine.state_for(self.cell).hazard_active)

    def test_acknowledgement_and_active_alert_reconstruct_from_in_memory_repository(self):
        event = self._raise_alert()
        self.assertTrue(self.machine.acknowledge(event.alert_id, "operator-2", now=3))
        recovered = PerCellAlertStateMachine(config(), self.repository, clock=self.clock)
        state = recovered.state_for(self.cell)
        self.assertTrue(state.hazard_active)
        self.assertEqual(state.alert_id, event.alert_id)
        self.assertEqual(state.acknowledged_by, "operator-2")
        self.assertEqual(state.acknowledged_at, 3)
        self.assertEqual(recovered.tick(now=1000), [])
        self.assertTrue(recovered.state_for(self.cell).hazard_active)

    def test_escalation_does_not_change_hazard_resolution_state(self):
        self._raise_alert()
        events = self.machine.tick(now=122)
        self.assertEqual(events[0].event_type, "ESCALATED")
        self.assertTrue(self.machine.state_for(self.cell).hazard_active)


@unittest.skipUnless(os.environ.get("TEST_POSTGRES_DSN"), "TEST_POSTGRES_DSN not configured")
class PostgresAlertRestartTests(unittest.TestCase):
    def test_acknowledgement_and_alert_id_survive_repository_restart(self):
        repo1 = PostgresAlertRepository(os.environ["TEST_POSTGRES_DSN"])
        repo1.initialize()
        machine1 = PerCellAlertStateMachine(config(), repo1)
        cell = (int(uuid4().hex[:7], 16), int(uuid4().hex[:7], 16))
        for now in (10.0, 11.0):
            machine1.process_prediction(cell, 0.9, now=now)
        event = machine1.process_prediction(cell, 0.9, now=12.0)[0]
        machine1.acknowledge(event.alert_id, "postgres-restart-test", now=13.0)

        repo2 = PostgresAlertRepository(os.environ["TEST_POSTGRES_DSN"])
        recovered = PerCellAlertStateMachine(config(), repo2)
        state = recovered.state_for(cell)
        self.assertTrue(state.hazard_active)
        self.assertEqual(state.alert_id, event.alert_id)
        self.assertEqual(state.acknowledged_by, "postgres-restart-test")
        self.assertEqual(state.acknowledged_at, 13.0)

