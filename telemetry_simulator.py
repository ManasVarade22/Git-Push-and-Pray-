"""Deterministic lossy binary UDP telemetry simulator and reorder buffer."""

from __future__ import annotations

import argparse
import heapq
import random
import socket
import time
from dataclasses import dataclass, field
from telemetry_protocol import TelemetryFrame, is_newer_sequence


DEFAULT_SEED = 20261021


@dataclass(frozen=True, slots=True)
class ImpairmentConfig:
    drop_probability: float = 0.30
    duplicate_probability: float = 0.10
    corruption_probability: float = 0.0
    max_delay_seconds: float = 0.25
    reorder_window_seconds: float = 2.0
    seed: int = DEFAULT_SEED

    def __post_init__(self) -> None:
        for name in ("drop_probability", "duplicate_probability", "corruption_probability"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.max_delay_seconds < 0:
            raise ValueError("max_delay_seconds cannot be negative")
        if self.reorder_window_seconds <= 0:
            raise ValueError("reorder_window_seconds must be positive")


@dataclass(slots=True)
class PacketCounts:
    sent: int = 0
    dropped: int = 0
    duplicated: int = 0
    corrupted: int = 0
    accepted: int = 0
    rejected: int = 0
    late: int = 0

    def summary(self, config: ImpairmentConfig | None = None) -> str:
        counts = ", ".join(f"{name}={getattr(self, name)}" for name in (
            "sent", "dropped", "duplicated", "corrupted", "accepted", "rejected", "late"
        ))
        if config is None:
            return counts
        return (
            f"configured(drop={config.drop_probability:.1%}, duplicate={config.duplicate_probability:.1%}, "
            f"corrupt={config.corruption_probability:.1%}, seed={config.seed}); observed({counts})"
        )


@dataclass(frozen=True, slots=True)
class ScheduledDatagram:
    payload: bytes
    delay_seconds: float
    is_duplicate: bool = False
    is_corrupted: bool = False


class ImpairmentInjector:
    """Applies seeded Bernoulli impairments and random latency to binary frames.

    Duplication happens before packet loss. Each generated copy then has an
    independent drop trial. Counts therefore use: drop rate / transmitted
    copies; duplicate rate / original frames; corruption rate / delivered
    copies. Random finite-batch outcomes are not forced to equal probabilities.
    """

    def __init__(self, config: ImpairmentConfig):
        self.config = config
        self.random = random.Random(config.seed)
        self.counts = PacketCounts()

    def plan(self, frames: list[TelemetryFrame]) -> list[ScheduledDatagram]:
        self.counts.sent += len(frames)
        scheduled: list[ScheduledDatagram] = []
        for frame in frames:
            encoded = frame.to_bytes()
            copies = [(encoded, False)]
            if self.random.random() < self.config.duplicate_probability:
                copies.append((encoded, True))
                self.counts.duplicated += 1
            for payload, duplicate in copies:
                if self.random.random() < self.config.drop_probability:
                    self.counts.dropped += 1
                    continue
                corrupted = self.random.random() < self.config.corruption_probability
                if corrupted:
                    payload = _flip_one_payload_bit(payload)
                    self.counts.corrupted += 1
                delay = self.random.uniform(0.0, self.config.max_delay_seconds)
                scheduled.append(ScheduledDatagram(payload, delay, duplicate, corrupted))
        return scheduled


@dataclass(slots=True)
class _DroneWindow:
    expected_sequence: int
    pending: dict[int, tuple[TelemetryFrame, float]] = field(default_factory=dict)


class ReorderBuffer:
    """Per-drone sequence reorder buffer with a fixed late-arrival window.

    The first valid packet initializes that drone's expected sequence and is
    delivered immediately. A forward packet is held while a gap is open. When
    the oldest held packet has waited for the window duration, missing sequence
    numbers are skipped and the held packet is released. A packet that then
    arrives behind the expected sequence is classified `late` and rejected;
    it is never used to fill a gap retroactively.
    """

    def __init__(self, window_seconds: float = 2.0):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self._drones: dict[int, _DroneWindow] = {}
        self.late_count = 0
        self.duplicate_count = 0

    def push(self, frame: TelemetryFrame, now: float | None = None) -> tuple[str, list[TelemetryFrame]]:
        now = time.monotonic() if now is None else now
        released = self.advance(now)
        state = self._drones.get(frame.drone_id)
        if state is None:
            self._drones[frame.drone_id] = _DroneWindow((frame.sequence + 1) & 0xFFFF)
            return "accepted", released + [frame]

        sequence = frame.sequence
        if sequence in state.pending:
            self.duplicate_count += 1
            return "duplicate", released
        if sequence == state.expected_sequence:
            state.expected_sequence = (sequence + 1) & 0xFFFF
            delivered = released + [frame]
            delivered.extend(self._drain_contiguous(state))
            return "accepted", delivered
        if not is_newer_sequence(sequence, state.expected_sequence):
            self.late_count += 1
            return "late", released
        state.pending[sequence] = (frame, now)
        return "buffered", released

    def advance(self, now: float | None = None) -> list[TelemetryFrame]:
        now = time.monotonic() if now is None else now
        released: list[TelemetryFrame] = []
        for state in self._drones.values():
            while state.pending:
                sequence, (frame, arrived_at) = min(
                    state.pending.items(), key=lambda item: (item[1][1], (item[0] - state.expected_sequence) & 0xFFFF)
                )
                if now - arrived_at < self.window_seconds:
                    break
                # The pending packet proves one or more sequence gaps exceeded the window.
                del state.pending[sequence]
                state.expected_sequence = (sequence + 1) & 0xFFFF
                released.append(frame)
                released.extend(self._drain_contiguous(state))
        return released

    @staticmethod
    def _drain_contiguous(state: _DroneWindow) -> list[TelemetryFrame]:
        output: list[TelemetryFrame] = []
        while state.expected_sequence in state.pending:
            frame, _arrived_at = state.pending.pop(state.expected_sequence)
            output.append(frame)
            state.expected_sequence = (state.expected_sequence + 1) & 0xFFFF
        return output


def generate_frames(count: int, drone_id: int = 1, starting_sequence: int = 0) -> list[TelemetryFrame]:
    """Generate deterministic synthetic protocol-valid frames (not sensor data)."""
    if count < 0:
        raise ValueError("count cannot be negative")
    return [
        TelemetryFrame(
            drone_id=drone_id,
            sequence=(starting_sequence + index) & 0xFFFF,
            timestamp=1_700_000_000 + index,
            latitude_microdegrees=37_774_900,
            longitude_microdegrees=-122_419_400,
            sensor_readings=tuple((index * 17 + channel * 101) & 0xFFFF for channel in range(6)),
        )
        for index in range(count)
    ]


def run_local_udp_simulation(
    frames: list[TelemetryFrame],
    config: ImpairmentConfig,
    bind_host: str = "127.0.0.1",
) -> PacketCounts:
    """Send impaired binary datagrams over local UDP and validate/reorder them."""
    injector = ImpairmentInjector(config)
    schedule = injector.plan(frames)
    recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    recv_socket.bind((bind_host, 0))
    recv_socket.settimeout(1.0)
    destination = recv_socket.getsockname()
    buffer = ReorderBuffer(config.reorder_window_seconds)
    start = time.monotonic()
    counts = injector.counts
    try:
        for item in sorted(schedule, key=lambda packet: packet.delay_seconds):
            remaining = start + item.delay_seconds - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            send_socket.sendto(item.payload, destination)
            try:
                payload, _address = recv_socket.recvfrom(2048)
            except socket.timeout as exc:
                raise RuntimeError("local UDP receiver did not receive a sent datagram") from exc
            try:
                frame = TelemetryFrame.from_bytes(payload)
            except ValueError:
                counts.rejected += 1
                continue
            status, delivered = buffer.push(frame)
            if status == "late":
                counts.late += 1
                counts.rejected += 1
            elif status == "duplicate":
                counts.rejected += 1
            counts.accepted += len(delivered)
        # Advance the logical receiver clock past the window without claiming
        # that any dropped datagram was recovered.
        counts.accepted += len(buffer.advance(now=time.monotonic() + config.reorder_window_seconds))
    finally:
        recv_socket.close()
        send_socket.close()
    return counts


def _flip_one_payload_bit(payload: bytes) -> bytes:
    mutable = bytearray(payload)
    mutable[15] ^= 0x01  # corrupt a sensor byte; leave the checksum unchanged
    return bytes(mutable)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10_000, help="synthetic source frames to generate")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--drop", type=float, default=0.30, help="per-copy drop probability")
    parser.add_argument("--duplicate", type=float, default=0.10, help="per-original duplicate probability")
    parser.add_argument("--corrupt", type=float, default=0.0, help="per-delivered-copy corruption probability")
    parser.add_argument("--max-delay-ms", type=float, default=250.0)
    parser.add_argument("--window-seconds", type=float, default=2.0)
    args = parser.parse_args()
    config = ImpairmentConfig(
        drop_probability=args.drop,
        duplicate_probability=args.duplicate,
        corruption_probability=args.corrupt,
        max_delay_seconds=args.max_delay_ms / 1000,
        reorder_window_seconds=args.window_seconds,
        seed=args.seed,
    )
    counts = run_local_udp_simulation(generate_frames(args.count), config)
    print(counts.summary(config))


if __name__ == "__main__":
    main()

