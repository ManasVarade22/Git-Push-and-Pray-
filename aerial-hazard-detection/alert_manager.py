
import os
import time
import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg


HIGH_THRESHOLD = 70.0
CLEAR_THRESHOLD = 40.0
HIGH_FRAMES_REQUIRED = 3
CLEAR_DURATION_SECONDS = 300
REPEAT_INTERVAL_SECONDS = 30

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "dbname=aerosafe user=saksham"
)


def to_datetime(timestamp):
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc)


def to_timestamp(value):
    if value is None:
        return None
    return value.timestamp()


@dataclass
class CellAlert:
    cell_id: str
    state: str = "NORMAL"
    consecutive_high: int = 0
    below_threshold_since: float = None
    alert_id: str = None
    acknowledged: bool = False
    acknowledged_by: str = None
    acknowledged_at: float = None
    raised_at: float = None
    last_notification_at: float = None
    escalation_level: int = 0
    risk_score: float = 0.0
    history: list = field(default_factory=list)


class AlertManager:
    def __init__(self):
        self.alerts = {}
        self.restore()

    def restore(self):
        with psycopg.connect(DATABASE_URL) as conn:
            rows = conn.execute("""
                SELECT cell_id, state, alert_id, risk_score,
                       consecutive_high, below_threshold_since,
                       acknowledged, acknowledged_by, acknowledged_at,
                       raised_at, last_notification_at, escalation_level
                FROM alerts
            """).fetchall()

        for row in rows:
            (
                cell_id, state, alert_id, risk_score,
                consecutive_high, below_since,
                acknowledged, acknowledged_by, acknowledged_at,
                raised_at, last_notification_at, escalation_level
            ) = row

            active = state in ("ACTIVE", "ESCALATED")

            self.alerts[cell_id] = CellAlert(
                cell_id=cell_id,
                state=state,
                alert_id=alert_id if active else None,
                risk_score=risk_score or 0.0,
                consecutive_high=consecutive_high,
                below_threshold_since=to_timestamp(below_since),
                acknowledged=acknowledged,
                acknowledged_by=acknowledged_by,
                acknowledged_at=to_timestamp(acknowledged_at),
                raised_at=to_timestamp(raised_at) if active else None,
                last_notification_at=to_timestamp(last_notification_at),
                escalation_level=escalation_level,
            )

        logging.info("Restored %s cell alerts", len(self.alerts))

    def _get_alert(self, cell_id):
        if cell_id not in self.alerts:
            self.alerts[cell_id] = CellAlert(cell_id=cell_id)
        return self.alerts[cell_id]

    def _persist(self, alert):
        stored_alert_id = alert.alert_id or f"normal:{alert.cell_id}"

        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute("""
                INSERT INTO alerts (
                    alert_id, cell_id, state, risk_score,
                    raised_at, acknowledged_at, acknowledged_by,
                    updated_at, consecutive_high,
                    below_threshold_since, last_notification_at,
                    escalation_level, acknowledged
                )
                VALUES (
                    %s, %s, %s, %s, COALESCE(%s, NOW()),
                    %s, %s, NOW(), %s, %s, %s, %s, %s
                )
                ON CONFLICT (cell_id) DO UPDATE SET
                    alert_id = EXCLUDED.alert_id,
                    state = EXCLUDED.state,
                    risk_score = EXCLUDED.risk_score,
                    raised_at = EXCLUDED.raised_at,
                    acknowledged_at = EXCLUDED.acknowledged_at,
                    acknowledged_by = EXCLUDED.acknowledged_by,
                    updated_at = NOW(),
                    consecutive_high = EXCLUDED.consecutive_high,
                    below_threshold_since = EXCLUDED.below_threshold_since,
                    last_notification_at = EXCLUDED.last_notification_at,
                    escalation_level = EXCLUDED.escalation_level,
                    acknowledged = EXCLUDED.acknowledged
            """, (
                stored_alert_id,
                alert.cell_id,
                alert.state,
                alert.risk_score,
                to_datetime(alert.raised_at),
                to_datetime(alert.acknowledged_at),
                alert.acknowledged_by,
                alert.consecutive_high,
                to_datetime(alert.below_threshold_since),
                to_datetime(alert.last_notification_at),
                alert.escalation_level,
                alert.acknowledged,
            ))

    def _save_event(self, event):
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute("""
                INSERT INTO alert_events (
                    event_id, alert_id, cell_id, event_type,
                    state, risk_score, acknowledged,
                    acknowledged_by, acknowledged_at,
                    escalation_level, event_timestamp
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (event_id) DO NOTHING
            """, (
                event["event_id"],
                event["alert_id"] or f'normal:{event["cell_id"]}',
                event["cell_id"],
                event["event"],
                event["state"],
                event["risk_score"],
                event["acknowledged"],
                event["acknowledged_by"],
                to_datetime(event["acknowledged_at"]),
                event["escalation_level"],
                to_datetime(event["timestamp"]),
            ))

    def _record_event(self, alert, event_type, risk_score, now):
        event = self._event(alert, event_type, risk_score, now)
        alert.history.append(event)
        self._save_event(event)
        return event

    def update(self, cell_id, risk_score, now=None):
        now = time.time() if now is None else now
        alert = self._get_alert(cell_id)
        alert.risk_score = float(risk_score)
        events = []

        if risk_score >= HIGH_THRESHOLD:
            alert.consecutive_high += 1
            alert.below_threshold_since = None

            if (
                alert.state == "NORMAL"
                and alert.consecutive_high >= HIGH_FRAMES_REQUIRED
            ):
                alert.state = "ACTIVE"
                alert.alert_id = str(uuid.uuid4())
                alert.acknowledged = False
                alert.acknowledged_by = None
                alert.acknowledged_at = None
                alert.raised_at = now
                alert.last_notification_at = now
                alert.escalation_level = 0
                events.append(
                    self._record_event(alert, "RAISED", risk_score, now)
                )

        elif risk_score < CLEAR_THRESHOLD:
            alert.consecutive_high = 0

            if alert.state in ("ACTIVE", "ESCALATED"):
                if alert.below_threshold_since is None:
                    alert.below_threshold_since = now
                elif now - alert.below_threshold_since >= CLEAR_DURATION_SECONDS:
                    events.append(
                        self._record_event(alert, "CLEARED", risk_score, now)
                    )
                    alert.state = "NORMAL"
                    alert.alert_id = None
                    alert.acknowledged = False
                    alert.acknowledged_by = None
                    alert.acknowledged_at = None
                    alert.raised_at = None
                    alert.escalation_level = 0
                    alert.last_notification_at = None
                    alert.below_threshold_since = None

        else:
            alert.consecutive_high = 0
            alert.below_threshold_since = None

        if alert.state in ("ACTIVE", "ESCALATED"):
            if (
                not alert.acknowledged
                and alert.last_notification_at is not None
                and now - alert.last_notification_at >= REPEAT_INTERVAL_SECONDS
            ):
                alert.escalation_level += 1
                alert.state = "ESCALATED"
                alert.last_notification_at = now
                events.append(
                    self._record_event(alert, "ESCALATED", risk_score, now)
                )

        self._persist(alert)
        return events

    def acknowledge(self, cell_id, acknowledged_by="operator", now=None):
        now = time.time() if now is None else now
        alert = self._get_alert(cell_id)

        if alert.state not in ("ACTIVE", "ESCALATED") or alert.acknowledged:
            return None

        alert.acknowledged = True
        alert.acknowledged_by = acknowledged_by
        alert.acknowledged_at = now

        event = self._record_event(alert, "ACKNOWLEDGED", None, now)
        self._persist(alert)
        return event

    def _event(self, alert, event_type, risk_score, now):
        return {
            "event_id": str(uuid.uuid4()),
            "alert_id": alert.alert_id,
            "cell_id": alert.cell_id,
            "event": event_type,
            "state": alert.state,
            "risk_score": risk_score,
            "acknowledged": alert.acknowledged,
            "acknowledged_by": alert.acknowledged_by,
            "acknowledged_at": alert.acknowledged_at,
            "escalation_level": alert.escalation_level,
            "timestamp": now,
        }

    def snapshot(self):
        return [
            {
                "cell_id": alert.cell_id,
                "state": alert.state,
                "alert_id": alert.alert_id,
                "consecutive_high": alert.consecutive_high,
                "acknowledged": alert.acknowledged,
                "acknowledged_by": alert.acknowledged_by,
                "acknowledged_at": alert.acknowledged_at,
                "escalation_level": alert.escalation_level,
                "raised_at": alert.raised_at,
            }
            for alert in self.alerts.values()
        ]
