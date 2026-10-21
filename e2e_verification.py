"""Reproducible, localhost-only end-to-end verification for lossy UDP telemetry."""

from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from alert_engine import AlertConfig, InMemoryAlertRepository, PerCellAlertStateMachine
from streaming_hazard_grid import GridConfig, StreamingHazardGrid, WeatherObservation
from telemetry_protocol import ChecksumError, FrameLengthError, TelemetryFrame
from telemetry_simulator import (
    DEFAULT_SEED,
    ImpairmentConfig,
    ImpairmentInjector,
    ReorderBuffer,
    generate_frames,
)


BASE_TIME = 1_700_000_000.0


class ManualClock:
    def __init__(self, value: float = BASE_TIME):
        self.value = value
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            return self.value

    def set(self, value: float) -> None:
        with self.lock:
            self.value = value


@dataclass(slots=True)
class ObservedCounts:
    sent: int = 0
    received: int = 0
    dropped: int = 0
    duplicated: int = 0
    corrupted: int = 0
    accepted: int = 0
    rejected: int = 0
    late: int = 0
    deduplicated: int = 0
    crc_rejected: int = 0
    grid_updates: int = 0
    risk_evaluations: int = 0
    alerts_raised: int = 0


class LocalhostTelemetryReceiver:
    """A real UDP receiver thread connected directly to grid and alert objects."""

    def __init__(self, grid: StreamingHazardGrid, alerts: PerCellAlertStateMachine,
                 clock: ManualClock, *, reorder_window_seconds: float = 2.0):
        self.grid = grid
        self.alerts = alerts
        self.clock = clock
        self.reorder = ReorderBuffer(window_seconds=reorder_window_seconds)
        self.counts = ObservedCounts()
        self.accepted_sequences: list[tuple[int, int]] = []
        self._seen_frames: set[tuple[int, int, int, bytes]] = set()
        self._state_lock = threading.RLock()
        self._condition = threading.Condition()
        self._received_total = 0
        self._stop = threading.Event()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))
        self.socket.settimeout(0.02)
        self.address = self.socket.getsockname()
        self._thread = threading.Thread(target=self._run, name="p09-local-udp-receiver", daemon=True)
        self._thread.start()

    def send_from_simulated_drone(self, send_socket: socket.socket, payload: bytes,
                                  *, arrival_time: float, timeout: float = 2.0) -> None:
        """Send one binary datagram and wait for receiver acknowledgement."""
        with self._condition:
            target = self._received_total + 1
        self.clock.set(arrival_time)
        send_socket.sendto(payload, self.address)
        self.counts.sent += 1
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._received_total < target:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("localhost UDP receiver did not acknowledge a datagram")
                self._condition.wait(remaining)

    def flush_reorder_window(self, *, now: float) -> None:
        """Advance the injected receiver clock; no two-second wall sleep is needed."""
        self.clock.set(now)
        with self._state_lock:
            self._process_ready(self.reorder.advance(now))

    def wait_until_idle(self, timeout: float = 2.0) -> None:
        self._thread.join(timeout=0)  # receiver remains live; send acknowledgements are synchronous

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        self.socket.close()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                payload, _peer = self.socket.recvfrom(64)
            except socket.timeout:
                continue
            except OSError:
                return
            with self._state_lock:
                self._handle(payload)
            with self._condition:
                self._received_total += 1
                self._condition.notify_all()

    def _handle(self, payload: bytes) -> None:
        now = self.clock()
        self.counts.received += 1
        # Expired pending packets are released before interpreting this datagram.
        self._process_ready(self.reorder.advance(now))
        try:
            frame = TelemetryFrame.from_bytes(payload)
        except (ChecksumError, FrameLengthError):
            self.counts.rejected += 1
            self.counts.crc_rejected += 1 if len(payload) == 29 else 0
            return
        signature = (frame.drone_id, frame.sequence, frame.timestamp, payload)
        if signature in self._seen_frames:
            self.counts.deduplicated += 1
            self.counts.rejected += 1
            return
        self._seen_frames.add(signature)
        status, ready = self.reorder.push(frame, now)
        if status == "late":
            self.counts.late += 1
            self.counts.rejected += 1
        elif status == "duplicate":
            self.counts.deduplicated += 1
            self.counts.rejected += 1
        self._process_ready(ready)

    def _process_ready(self, frames: list[TelemetryFrame]) -> None:
        for frame in frames:
            now = self.clock()
            update = self.grid.update(frame, now=now)
            if not update.accepted:
                self.counts.rejected += 1
                continue
            self.counts.accepted += 1
            self.counts.grid_updates += 1
            self.accepted_sequences.append((frame.drone_id, frame.sequence))
            if update.risk_status == "evaluated":
                self.counts.risk_evaluations += 1
                events = self.alerts.process_prediction(update.cell_id, update.risk_score, now=now)
                self.counts.alerts_raised += sum(event.event_type == "RAISED" for event in events)
            else:
                self.alerts.process_prediction(update.cell_id, None, valid=False, now=now)


def run_lossy_batch(*, source_count: int = 512, seed: int = DEFAULT_SEED,
                    drop_probability: float = 0.30, duplicate_probability: float = 0.10,
                    corruption_probability: float = 0.05, max_delay_seconds: float = 0.025,
                    source_interval_seconds: float = 0.002) -> dict:
    """Run seeded loss/duplication/delay/reordering/corruption through localhost UDP."""
    config = ImpairmentConfig(drop_probability=drop_probability, duplicate_probability=duplicate_probability,
                              corruption_probability=corruption_probability, max_delay_seconds=max_delay_seconds,
                              reorder_window_seconds=2.0, seed=seed)
    frames = generate_frames(source_count, starting_sequence=0)
    injector = ImpairmentInjector(config)
    schedule = injector.plan(frames)
    # Network arrival = source emission time + injected latency. This preserves
    # a paced drone stream while allowing jitter to reorder nearby frames.
    scheduled = sorted(
        schedule,
        key=lambda packet: int.from_bytes(packet.payload[1:3], "big") * source_interval_seconds
        + packet.delay_seconds,
    )
    base = BASE_TIME
    grid = StreamingHazardGrid(GridConfig(37_774_900, -122_419_400, stale_timeout_seconds=10))
    alerts = PerCellAlertStateMachine(AlertConfig(0.8, 0.3), InMemoryAlertRepository())
    clock = ManualClock(base)
    receiver = LocalhostTelemetryReceiver(grid, alerts, clock, reorder_window_seconds=2.0)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    wire_order: list[int] = []
    actual_start = time.monotonic()
    try:
        for datagram in scheduled:
            source_sequence = int.from_bytes(datagram.payload[1:3], "big")
            network_delay = source_sequence * source_interval_seconds + datagram.delay_seconds
            deadline = actual_start + network_delay
            delay = deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            if datagram.is_corrupted:
                receiver.counts.corrupted += 1
            try:
                parsed = TelemetryFrame.from_bytes(datagram.payload)
            except ValueError:
                pass
            else:
                wire_order.append(parsed.sequence)
            receiver.send_from_simulated_drone(
                sender, datagram.payload, arrival_time=base + network_delay
            )
        receiver.counts.dropped = injector.counts.dropped
        receiver.counts.duplicated = injector.counts.duplicated
        receiver.flush_reorder_window(now=base + source_count * source_interval_seconds
                                      + config.max_delay_seconds + config.reorder_window_seconds + 0.01)
        metrics = asdict(receiver.counts)
        # The receiver sees every scheduled datagram (drops were not emitted).
        assert metrics["received"] == metrics["sent"], "UDP sender/receiver datagram count mismatch"
        assert metrics["accepted"] == metrics["grid_updates"], "an accepted packet did not update grid exactly once"
        assert len(receiver.accepted_sequences) == len(set(receiver.accepted_sequences)), "duplicate state update detected"
        assert grid.snapshot(now=clock()).version == metrics["grid_updates"], "invalid/duplicate packet mutated grid"
        assert metrics["corrupted"] > 0 and metrics["crc_rejected"] == metrics["corrupted"], "deliberate CRC corruption was not rejected"
        assert metrics["deduplicated"] > 0, "seeded duplicate copies were not detected"
        assert metrics["accepted"] + metrics["rejected"] == metrics["sent"], "not every emitted datagram was classified"
        assert wire_order != sorted(wire_order), "seeded delays failed to reorder wire traffic"
        return {
            "scenario": "seeded_lossy_localhost_udp",
            "passed": True,
            "configuration": asdict(config),
            "source_frames": source_count,
            "source_interval_seconds": source_interval_seconds,
            "emitted_udp_datagrams": len(schedule),
            "counts": metrics,
            "observed_drop_fraction_per_transmitted_copy": (
                injector.counts.dropped / (len(schedule) + injector.counts.dropped)
                if len(schedule) + injector.counts.dropped else 0.0
            ),
            "observed_duplicate_attempt_fraction_per_source_frame": (
                injector.counts.duplicated / source_count if source_count else 0.0
            ),
            "wire_order_reordered": True,
            "grid_version": grid.snapshot(now=clock()).version,
        }
    finally:
        sender.close()
        receiver.close()


def run_protocol_grid_alert_scenario() -> dict:
    """Exercise wraparound, dedupe, CRC rejection, reorder expiry, stale and alert trigger."""
    clock = ManualClock(BASE_TIME)
    evaluator_calls = []
    def high_risk(_features):
        evaluator_calls.append(1)
        return 0.95

    grid = StreamingHazardGrid(GridConfig(0, 0, stale_timeout_seconds=5), risk_evaluator=high_risk)
    grid.update_weather(WeatherObservation(BASE_TIME, 21.0, 2.0, 90.0))
    alerts = PerCellAlertStateMachine(
        AlertConfig(0.8, 0.3, prediction_stale_after_seconds=60), InMemoryAlertRepository()
    )
    receiver = LocalhostTelemetryReceiver(grid, alerts, clock, reorder_window_seconds=2.0)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sequence_processing_order: list[int] = []
    original_apply = receiver._process_ready

    def capture_order(frames):
        original_apply(frames)
        sequence_processing_order.extend(frame.sequence for frame in frames if frame.drone_id == 1)
    receiver._process_ready = capture_order

    def transmit(frame: TelemetryFrame, offset: float = 0.0):
        receiver.send_from_simulated_drone(sender, frame.to_bytes(), arrival_time=clock() + offset)

    def frame(drone_id, sequence, latitude, longitude, timestamp=None):
        return TelemetryFrame(drone_id, sequence, int(clock() if timestamp is None else timestamp),
                              latitude, longitude, (42_000, 8_000, 300, 250, 120, 90))

    try:
        # Populate all eight adjacent cells using valid fixed-size UDP frames.
        neighbors = [(-45, -45), (0, -45), (90, -45), (-45, 0),
                     (90, 0), (-45, 90), (0, 90), (90, 90)]
        for index, (longitude, latitude) in enumerate(neighbors, start=2):
            transmit(frame(index, 1, latitude, longitude), 0.01 * index)

        calls_before_center = len(evaluator_calls)
        center_first = frame(1, 65534, 0, 0)
        transmit(center_first, 0.1)
        before_high = grid.snapshot(now=clock()).version
        transmit(frame(1, 0, 0, 0), 0.2)  # forward across wrap, buffered behind 65535
        version_while_buffered = grid.snapshot(now=clock()).version
        transmit(frame(1, 65535, 0, 0), 0.3)  # releases 65535 then 0 in serial order
        active = alerts.state_for((0, 0))
        alert_after_three = active is not None and active.hazard_active
        expected_order = [65534, 65535, 0]
        order_ok = sequence_processing_order[:3] == expected_order
        risk_count_after_trigger = len(evaluator_calls) - calls_before_center
        version_before_duplicate = grid.snapshot(now=clock()).version
        transmit(frame(1, 65535, 0, 0, timestamp=int(BASE_TIME + 0.3)))
        version_after_duplicate = grid.snapshot(now=clock()).version
        duplicate_detected = receiver.counts.deduplicated == 1

        version_before_corruption = grid.snapshot(now=clock()).version
        bad_payload = bytearray(frame(1, 1, 0, 0).to_bytes())
        bad_payload[15] ^= 0x01
        receiver.send_from_simulated_drone(sender, bytes(bad_payload), arrival_time=clock() + 0.4)
        version_after_corruption = grid.snapshot(now=clock()).version
        crc_rejected = receiver.counts.crc_rejected == 1

        # Sequence 2 is held while expected 1 is missing. Advance the controlled
        # receiver clock beyond the two-second window, then expire that gap.
        transmit(frame(1, 2, 0, 0), 0.5)
        version_with_pending_gap = grid.snapshot(now=clock()).version
        receiver.flush_reorder_window(now=clock() + 2.01)
        version_after_window_expiry = grid.snapshot(now=clock()).version
        transmit(frame(1, 1, 0, 0), 2.02)  # the missing packet is now late, not recovered
        late_rejected = receiver.counts.late == 1
        stale = grid.snapshot(now=clock() + 6).cells[(0, 0)].freshness == "stale"

        receiver.counts.duplicated = 1
        receiver.counts.corrupted = 1
        result = asdict(receiver.counts)
        invariants = {
            "sequence_wraparound_order": order_ok,
            "buffered_packet_did_not_mutate_grid_before_gap_filled": version_while_buffered == before_high,
            "three_consecutive_high_predictions_raised_alert": alert_after_three,
            "risk_evaluated_once_for_each_center_frame": risk_count_after_trigger == 3,
            "duplicate_did_not_mutate_grid": duplicate_detected and version_after_duplicate == version_before_duplicate,
            "bad_crc_did_not_mutate_grid": crc_rejected and version_after_corruption == version_before_corruption,
            "reorder_window_released_pending_packet": version_after_window_expiry == version_with_pending_gap + 1,
            "arrival_after_window_was_late": late_rejected,
            "stale_cell_transition": stale,
            "database_isolated": isinstance(alerts.repository, InMemoryAlertRepository),
        }
        failed = [name for name, okay in invariants.items() if not okay]
        if failed:
            raise AssertionError("scenario invariants failed: " + ", ".join(failed)
                                 + f"; center_evaluator_calls={risk_count_after_trigger}, order={sequence_processing_order}")
        return {
            "scenario": "wraparound_crc_reorder_grid_stale_alert",
            "passed": True,
            "configuration": {"reorder_window_seconds": 2.0, "stale_timeout_seconds": 5,
                               "entry_threshold": 0.8, "clear_threshold": 0.3,
                               "clock": "controllable logical time; no two-second wall sleep"},
            "counts": result,
            "invariants": invariants,
            "sequence_processing_order": sequence_processing_order,
            "risk_evaluator_calls_after_three_center_frames": risk_count_after_trigger,
            "alert_active": alerts.state_for((0, 0)).hazard_active,
            "grid_version": grid.snapshot(now=clock()).version,
        }
    finally:
        sender.close()
        receiver.close()


def run_report(*, source_count: int = 512, seed: int = DEFAULT_SEED,
               drop_probability: float = 0.30, duplicate_probability: float = 0.10,
               corruption_probability: float = 0.05, max_delay_seconds: float = 0.025,
               source_interval_seconds: float = 0.002) -> dict:
    batch = run_lossy_batch(source_count=source_count, seed=seed,
                            drop_probability=drop_probability,
                            duplicate_probability=duplicate_probability,
                            corruption_probability=corruption_probability,
                            max_delay_seconds=max_delay_seconds,
                            source_interval_seconds=source_interval_seconds)
    invariants = run_protocol_grid_alert_scenario()
    return {
        "report_schema_version": 1,
        "project": "Aerial Hazard Detection streaming telemetry",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "database_or_hardware_used": False,
        "reproducibility": {
            "commands": [
                "python -m unittest -v tests.test_e2e_lossy_udp",
                "python e2e_verification.py --json-report reports/p09_e2e_report.json --summary-report reports/p09_e2e_summary.md",
            ],
            "default_seed": DEFAULT_SEED,
        },
        "scenarios": [batch, invariants],
        "passed": all(scenario["passed"] for scenario in (batch, invariants)),
    }


def human_summary(report: dict) -> str:
    lines = ["# P09 End-to-end lossy UDP verification", "",
             f"Overall result: **{'PASS' if report['passed'] else 'FAIL'}**",
             "", "All traffic used localhost UDP with binary 29-byte telemetry frames. No production database or drone hardware was used.", ""]
    for scenario in report["scenarios"]:
        lines.extend([f"## {scenario['scenario']}", "", f"Result: **{'PASS' if scenario['passed'] else 'FAIL'}**"])
        if scenario["scenario"] == "seeded_lossy_localhost_udp":
            config = scenario["configuration"]
            lines.extend([
                f"Configured probabilities: drop {config['drop_probability']:.0%}, duplicate {config['duplicate_probability']:.0%}, CRC corruption {config['corruption_probability']:.0%}; seed {config['seed']}.",
                "These are configured probabilities; counts below are observed finite-sample outcomes.",
                f"Original source frames: {scenario['source_frames']}; emitted UDP datagrams: {scenario['emitted_udp_datagrams']}.",
                f"Observed drop ratio per transmitted copy: {scenario['observed_drop_fraction_per_transmitted_copy']:.1%}; duplicate-copy generation ratio per source frame: {scenario['observed_duplicate_attempt_fraction_per_source_frame']:.1%}; CRC-corruption ratio among emitted datagrams: {scenario['counts']['corrupted'] / scenario['counts']['sent']:.1%}.",
            ])
        lines.extend(["", "| Count | Observed |", "|---|---:|"])
        for key, value in scenario["counts"].items():
            lines.append(f"| {key} | {value} |")
        if "invariants" in scenario:
            lines.extend(["", "Invariant checks:"])
            lines.extend(f"- {'PASS' if passed else 'FAIL'} — {name.replace('_', ' ')}" for name, passed in scenario["invariants"].items())
            lines.append(f"- Sequence processing order: `{scenario['sequence_processing_order']}`")
        lines.append("")
    lines.extend(["## Reproduce", "", "```sh", *report["reproducibility"]["commands"], "```", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json-report", default="reports/p09_e2e_report.json")
    parser.add_argument("--summary-report", default="reports/p09_e2e_summary.md")
    parser.add_argument("--source-count", type=int, default=512)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--drop-probability", type=float, default=0.30)
    parser.add_argument("--duplicate-probability", type=float, default=0.10)
    parser.add_argument("--corruption-probability", type=float, default=0.05)
    parser.add_argument("--max-delay-seconds", type=float, default=0.025)
    parser.add_argument("--source-interval-seconds", type=float, default=0.002)
    args = parser.parse_args()
    report = run_report(source_count=args.source_count, seed=args.seed,
                        drop_probability=args.drop_probability,
                        duplicate_probability=args.duplicate_probability,
                        corruption_probability=args.corruption_probability,
                        max_delay_seconds=args.max_delay_seconds,
                        source_interval_seconds=args.source_interval_seconds)
    json_path = Path(args.json_report)
    summary_path = Path(args.summary_report)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(human_summary(report), encoding="utf-8")
    print(human_summary(report))
    print(f"Machine-readable report: {json_path}")
    print(f"Human-readable summary: {summary_path}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

