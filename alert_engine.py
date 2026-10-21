"""Per-cell hazard alert state machine and PostgreSQL persistence adapter."""

from __future__ import annotations

import math
import os
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable, Protocol
from uuid import UUID, uuid4


HIGH_RISK_FRAMES_TO_RAISE = 3
CLEAR_DURATION_SECONDS = 5 * 60


@dataclass(frozen=True, slots=True)
class AlertConfig:
    entry_threshold: float
    clear_threshold: float
    prediction_stale_after_seconds: float = 30.0
    repeat_interval_seconds: float = 60.0
    escalate_after_seconds: float = 300.0
    escalation_interval_seconds: float = 300.0

    def __post_init__(self) -> None:
        for name in ("entry_threshold", "clear_threshold"):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be a finite risk value between 0 and 1")
        if self.clear_threshold >= self.entry_threshold:
            raise ValueError("clear_threshold must be below entry_threshold to provide hysteresis")
        for name in ("prediction_stale_after_seconds", "repeat_interval_seconds", "escalate_after_seconds", "escalation_interval_seconds"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")


@dataclass(slots=True)
class CellAlertState:
    cell_id: tuple[int, int]
    consecutive_high_count: int = 0
    hazard_active: bool = False
    alert_id: str | None = None
    raised_at: float | None = None
    acknowledged_at: float | None = None
    acknowledged_by: str | None = None
    resolved_at: float | None = None
    clear_started_at: float | None = None
    last_notified_at: float | None = None
    escalation_level: int = 0
    last_valid_prediction_at: float | None = None
    last_risk_score: float | None = None
    quality_status: str = "unknown"
    updated_at: float = 0.0


@dataclass(frozen=True, slots=True)
class AlertEvent:
    event_type: str
    cell_id: tuple[int, int]
    alert_id: str
    timestamp: float
    escalation_level: int
    risk_score: float | None


class AlertRepository(Protocol):
    def load_states(self) -> dict[tuple[int, int], CellAlertState]: ...
    def save_state(self, state: CellAlertState) -> None: ...


class InMemoryAlertRepository:
    """Test/development repository. It is not durable across process restarts."""

    def __init__(self):
        self._rows: dict[tuple[int, int], CellAlertState] = {}
        self._lock = threading.Lock()

    def load_states(self) -> dict[tuple[int, int], CellAlertState]:
        with self._lock:
            return {cell_id: replace(state) for cell_id, state in self._rows.items()}

    def save_state(self, state: CellAlertState) -> None:
        with self._lock:
            self._rows[state.cell_id] = replace(state)


class PostgresAlertRepository:
    """Persist active cell state and alert history with PostgreSQL.

    The connection string is supplied by the caller or read from DATABASE_URL;
    no credential is stored in source. This adapter requires psycopg 3.
    """

    def __init__(self, conninfo: str):
        if not conninfo:
            raise ValueError("PostgreSQL connection information is required")
        self.conninfo = conninfo
        self._initialized = False
        self._lock = threading.RLock()

    @classmethod
    def from_environment(cls, variable: str = "DATABASE_URL") -> "PostgresAlertRepository":
        conninfo = os.environ.get(variable)
        if not conninfo:
            raise RuntimeError(f"set {variable} to a PostgreSQL connection string")
        return cls(conninfo)

    def initialize(self) -> None:
        with self._lock:
            if self._initialized:
                return
            psycopg = _get_psycopg()
            with psycopg.connect(self.conninfo) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS hazard_alerts (
                            alert_id UUID PRIMARY KEY,
                            cell_x INTEGER NOT NULL,
                            cell_y INTEGER NOT NULL,
                            raised_at TIMESTAMPTZ NOT NULL,
                            acknowledged_at TIMESTAMPTZ,
                            acknowledged_by TEXT,
                            resolved_at TIMESTAMPTZ,
                            last_notified_at TIMESTAMPTZ,
                            escalation_level INTEGER NOT NULL DEFAULT 0,
                            last_risk_score DOUBLE PRECISION
                        )
                    """)
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS hazard_cell_alert_state (
                            cell_x INTEGER NOT NULL,
                            cell_y INTEGER NOT NULL,
                            consecutive_high_count INTEGER NOT NULL,
                            hazard_active BOOLEAN NOT NULL,
                            alert_id UUID,
                            clear_started_at TIMESTAMPTZ,
                            last_valid_prediction_at TIMESTAMPTZ,
                            last_risk_score DOUBLE PRECISION,
                            quality_status TEXT NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL,
                            PRIMARY KEY (cell_x, cell_y)
                        )
                    """)
            self._initialized = True

    def load_states(self) -> dict[tuple[int, int], CellAlertState]:
        self.initialize()
        psycopg = _get_psycopg()
        with psycopg.connect(self.conninfo) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT c.cell_x, c.cell_y, c.consecutive_high_count, c.hazard_active,
                           c.clear_started_at, c.last_valid_prediction_at, c.last_risk_score,
                           c.quality_status, c.updated_at, a.alert_id::text, a.raised_at,
                           a.acknowledged_at, a.acknowledged_by, a.resolved_at,
                           a.last_notified_at, a.escalation_level
                    FROM hazard_cell_alert_state AS c
                    LEFT JOIN hazard_alerts AS a ON a.alert_id = c.alert_id
                """)
                rows = cursor.fetchall()
        states: dict[tuple[int, int], CellAlertState] = {}
        for row in rows:
            cell_id = (row[0], row[1])
            states[cell_id] = CellAlertState(
                cell_id=cell_id,
                consecutive_high_count=row[2],
                hazard_active=row[3],
                clear_started_at=_epoch(row[4]),
                last_valid_prediction_at=_epoch(row[5]),
                last_risk_score=row[6],
                quality_status=row[7],
                updated_at=_epoch(row[8]) or 0.0,
                alert_id=row[9],
                raised_at=_epoch(row[10]),
                acknowledged_at=_epoch(row[11]),
                acknowledged_by=row[12],
                resolved_at=_epoch(row[13]),
                last_notified_at=_epoch(row[14]),
                escalation_level=row[15] or 0,
            )
        return states

    def save_state(self, state: CellAlertState) -> None:
        self.initialize()
        psycopg = _get_psycopg()
        x, y = state.cell_id
        with self._lock, psycopg.connect(self.conninfo) as connection:
            with connection.cursor() as cursor:
                if state.alert_id is not None and state.raised_at is not None:
                    cursor.execute("""
                        INSERT INTO hazard_alerts (
                            alert_id, cell_x, cell_y, raised_at, acknowledged_at,
                            acknowledged_by, resolved_at, last_notified_at,
                            escalation_level, last_risk_score
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (alert_id) DO UPDATE SET
                            acknowledged_at = EXCLUDED.acknowledged_at,
                            acknowledged_by = EXCLUDED.acknowledged_by,
                            resolved_at = EXCLUDED.resolved_at,
                            last_notified_at = EXCLUDED.last_notified_at,
                            escalation_level = EXCLUDED.escalation_level,
                            last_risk_score = EXCLUDED.last_risk_score
                    """, (
                        UUID(state.alert_id), x, y, _datetime(state.raised_at),
                        _datetime(state.acknowledged_at), state.acknowledged_by,
                        _datetime(state.resolved_at), _datetime(state.last_notified_at),
                        state.escalation_level, state.last_risk_score,
                    ))
                cursor.execute("""
                    INSERT INTO hazard_cell_alert_state (
                        cell_x, cell_y, consecutive_high_count, hazard_active, alert_id,
                        clear_started_at, last_valid_prediction_at, last_risk_score,
                        quality_status, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cell_x, cell_y) DO UPDATE SET
                        consecutive_high_count = EXCLUDED.consecutive_high_count,
                        hazard_active = EXCLUDED.hazard_active,
                        alert_id = EXCLUDED.alert_id,
                        clear_started_at = EXCLUDED.clear_started_at,
                        last_valid_prediction_at = EXCLUDED.last_valid_prediction_at,
                        last_risk_score = EXCLUDED.last_risk_score,
                        quality_status = EXCLUDED.quality_status,
                        updated_at = EXCLUDED.updated_at
                """, (
                    x, y, state.consecutive_high_count, state.hazard_active,
                    UUID(state.alert_id) if state.alert_id else None,
                    _datetime(state.clear_started_at), _datetime(state.last_valid_prediction_at),
                    state.last_risk_score, state.quality_status, _datetime(state.updated_at),
                ))


class PerCellAlertStateMachine:
    """Alert lifecycle. Acknowledgement suppresses repeats; it never clears hazard state."""

    def __init__(
        self,
        config: AlertConfig,
        repository: AlertRepository | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ):
        self.config = config
        self.repository = repository or InMemoryAlertRepository()
        self.clock = clock
        self._lock = threading.RLock()
        self._states = self.repository.load_states()

    def process_prediction(
        self,
        cell_id: tuple[int, int],
        risk_score: float | None,
        *,
        valid: bool = True,
        stale: bool = False,
        now: float | None = None,
    ) -> list[AlertEvent]:
        now = self.clock() if now is None else now
        if not math.isfinite(now):
            raise ValueError("now must be finite")
        with self._lock:
            state = self._states.setdefault(cell_id, CellAlertState(cell_id))
            if stale:
                quality = "stale"
            elif not valid or risk_score is None or isinstance(risk_score, bool) or not isinstance(risk_score, (int, float)) or not math.isfinite(risk_score) or not 0.0 <= risk_score <= 1.0:
                quality = "invalid"
            else:
                quality = "valid"

            state.updated_at = now
            state.quality_status = quality
            if quality != "valid":
                state.consecutive_high_count = 0
                if state.clear_started_at is not None:
                    state.clear_started_at = None
                    self._persist(state)
                return []

            score = float(risk_score)
            if (
                state.last_valid_prediction_at is not None
                and now - state.last_valid_prediction_at > self.config.prediction_stale_after_seconds
            ):
                # A stale gap breaks both a high-risk streak and any continuous
                # low-risk interval that could otherwise clear an active alert.
                state.consecutive_high_count = 0
                if state.hazard_active and state.clear_started_at is not None:
                    state.clear_started_at = None
                    self._persist(state)
            state.last_valid_prediction_at = now
            state.last_risk_score = score
            events: list[AlertEvent] = []

            if score >= self.config.entry_threshold:
                if not state.hazard_active:
                    state.consecutive_high_count += 1
                    if state.consecutive_high_count >= HIGH_RISK_FRAMES_TO_RAISE:
                        events.append(self._raise(state, score, now))
                else:
                    state.consecutive_high_count = 0
            else:
                state.consecutive_high_count = 0

            if state.hazard_active:
                if score < self.config.clear_threshold:
                    if state.clear_started_at is None:
                        state.clear_started_at = now
                        self._persist(state)
                    if now - state.clear_started_at >= CLEAR_DURATION_SECONDS:
                        events.append(self._clear(state, score, now))
                elif state.clear_started_at is not None:
                    state.clear_started_at = None
                    self._persist(state)
            return events

    def acknowledge(self, alert_id: str, acknowledged_by: str, *, now: float | None = None) -> bool:
        now = self.clock() if now is None else now
        if not acknowledged_by.strip():
            raise ValueError("acknowledged_by cannot be empty")
        with self._lock:
            state = next((item for item in self._states.values() if item.alert_id == alert_id and item.hazard_active), None)
            if state is None:
                return False
            if state.acknowledged_at is not None:
                return True
            # Persist a candidate first. If PostgreSQL is unavailable or the
            # transaction fails, the in-memory state must remain unacknowledged
            # so callers cannot mistake an uncommitted acknowledgement as done.
            candidate = replace(state)
            candidate.acknowledged_at = now
            candidate.acknowledged_by = acknowledged_by
            candidate.updated_at = now
            self._persist(candidate)
            self._states[state.cell_id] = candidate
            return True

    def tick(self, *, now: float | None = None) -> list[AlertEvent]:
        """Run stale checks and due repeat/escalation notifications."""
        now = self.clock() if now is None else now
        if not math.isfinite(now):
            raise ValueError("now must be finite")
        events: list[AlertEvent] = []
        with self._lock:
            for state in self._states.values():
                if state.hazard_active and state.last_valid_prediction_at is not None:
                    age = max(0.0, now - state.last_valid_prediction_at)
                    if age > self.config.prediction_stale_after_seconds:
                        changed = state.quality_status != "stale" or state.clear_started_at is not None
                        state.quality_status = "stale"
                        state.clear_started_at = None
                        if changed:
                            state.updated_at = now
                            self._persist(state)
                if (
                    state.hazard_active
                    and state.clear_started_at is not None
                    and state.last_risk_score is not None
                    and state.last_risk_score < self.config.clear_threshold
                    and state.last_valid_prediction_at is not None
                    and now - state.last_valid_prediction_at <= self.config.prediction_stale_after_seconds
                    and now - state.clear_started_at >= CLEAR_DURATION_SECONDS
                ):
                    events.append(self._clear(state, state.last_risk_score, now))
                    continue
                if not state.hazard_active or state.acknowledged_at is not None or state.last_notified_at is None:
                    continue
                if now - state.last_notified_at < self.config.repeat_interval_seconds:
                    continue
                elapsed = max(0.0, now - (state.raised_at or now))
                if elapsed >= self.config.escalate_after_seconds:
                    level = 1 + int((elapsed - self.config.escalate_after_seconds) // self.config.escalation_interval_seconds)
                else:
                    level = 0
                event_type = "ESCALATED" if level > state.escalation_level else "REPEATED"
                state.escalation_level = max(state.escalation_level, level)
                state.last_notified_at = now
                state.updated_at = now
                events.append(AlertEvent(event_type, state.cell_id, state.alert_id or "", now, state.escalation_level, state.last_risk_score))
                self._persist(state)
        return events

    def state_for(self, cell_id: tuple[int, int]) -> CellAlertState | None:
        with self._lock:
            state = self._states.get(cell_id)
            return replace(state) if state is not None else None

    def states_snapshot(self) -> dict[tuple[int, int], CellAlertState]:
        """Return detached alert-state copies for read-only dashboard views."""
        with self._lock:
            return {cell_id: replace(state) for cell_id, state in self._states.items()}

    def _raise(self, state: CellAlertState, score: float, now: float) -> AlertEvent:
        state.hazard_active = True
        state.alert_id = str(uuid4())
        state.raised_at = now
        state.acknowledged_at = None
        state.acknowledged_by = None
        state.resolved_at = None
        state.clear_started_at = None
        state.last_notified_at = now
        state.escalation_level = 0
        state.last_risk_score = score
        state.consecutive_high_count = 0
        state.updated_at = now
        self._persist(state)
        return AlertEvent("RAISED", state.cell_id, state.alert_id, now, 0, score)

    def _clear(self, state: CellAlertState, score: float, now: float) -> AlertEvent:
        state.hazard_active = False
        state.resolved_at = now
        state.clear_started_at = None
        state.consecutive_high_count = 0
        state.last_risk_score = score
        state.updated_at = now
        self._persist(state)
        return AlertEvent("CLEARED", state.cell_id, state.alert_id or "", now, state.escalation_level, score)

    def _persist(self, state: CellAlertState) -> None:
        self.repository.save_state(replace(state))


def _get_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("install psycopg[binary] to use PostgreSQL alert persistence") from exc
    return psycopg


def _datetime(timestamp: float | None) -> datetime | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def _epoch(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()

