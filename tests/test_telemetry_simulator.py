import unittest

from telemetry_protocol import TelemetryFrame
from telemetry_simulator import (
    DEFAULT_SEED,
    ImpairmentConfig,
    ImpairmentInjector,
    ReorderBuffer,
    generate_frames,
    run_local_udp_simulation,
)


class TelemetrySimulatorTests(unittest.TestCase):
    def test_seeded_large_batch_summary(self):
        config = ImpairmentConfig(seed=DEFAULT_SEED, max_delay_seconds=0.0)
        injector = ImpairmentInjector(config)
        plan = injector.plan(generate_frames(10_000))
        counts = injector.counts
        self.assertEqual(counts.sent, 10_000)
        self.assertEqual(counts.dropped, 3_317)
        self.assertEqual(counts.duplicated, 1_004)
        self.assertEqual(len(plan), 7_687)
        # These are seeded finite-sample outcomes, not a promise that every batch
        # exactly matches its configured probabilities.
        print("SIMULATOR_BATCH " + counts.summary(config) + f", emitted_copies={len(plan)}")

    def test_reorder_and_wraparound(self):
        buffer = ReorderBuffer(window_seconds=2.0)
        frames = generate_frames(4, starting_sequence=65534)
        status, out = buffer.push(frames[0], now=0.0)
        self.assertEqual((status, [f.sequence for f in out]), ("accepted", [65534]))
        status, out = buffer.push(frames[2], now=0.1)
        self.assertEqual((status, out), ("buffered", []))
        status, out = buffer.push(frames[1], now=0.2)
        self.assertEqual((status, [f.sequence for f in out]), ("accepted", [65535, 0]))
        status, out = buffer.push(frames[3], now=0.3)
        self.assertEqual((status, [f.sequence for f in out]), ("accepted", [1]))

    def test_seeded_delays_reorder_packets(self):
        config = ImpairmentConfig(drop_probability=0.0, duplicate_probability=0.0,
                                  max_delay_seconds=1.0, seed=DEFAULT_SEED)
        schedule = ImpairmentInjector(config).plan(generate_frames(20))
        arrival_order = [
            TelemetryFrame.from_bytes(item.payload).sequence
            for item in sorted(schedule, key=lambda item: item.delay_seconds)
        ]
        self.assertNotEqual(arrival_order, list(range(20)))

    def test_arrival_after_window_is_rejected_as_late(self):
        buffer = ReorderBuffer(window_seconds=2.0)
        first, missing, later = generate_frames(3)
        self.assertEqual(buffer.push(first, now=0.0)[0], "accepted")
        self.assertEqual(buffer.push(later, now=0.1)[0], "buffered")
        self.assertEqual([f.sequence for f in buffer.advance(now=2.2)], [2])
        status, released = buffer.push(missing, now=2.3)
        self.assertEqual(status, "late")
        self.assertEqual(released, [])
        self.assertEqual(buffer.late_count, 1)

    def test_invalid_crc_is_rejected_in_udp_path(self):
        config = ImpairmentConfig(drop_probability=0.0, duplicate_probability=0.0, corruption_probability=1.0,
                                  max_delay_seconds=0.0, seed=7)
        counts = run_local_udp_simulation(generate_frames(3), config)
        self.assertEqual(counts.sent, 3)
        self.assertEqual(counts.corrupted, 3)
        self.assertEqual(counts.rejected, 3)
        self.assertEqual(counts.accepted, 0)

    def test_seeded_local_udp_summary(self):
        config = ImpairmentConfig(seed=DEFAULT_SEED, max_delay_seconds=0.0)
        counts = run_local_udp_simulation(generate_frames(250), config)
        self.assertEqual(
            (counts.sent, counts.dropped, counts.duplicated, counts.corrupted,
             counts.accepted, counts.rejected, counts.late),
            (250, 83, 22, 0, 176, 13, 0),
        )
        print("SIMULATOR_UDP " + counts.summary(config))


if __name__ == "__main__":
    unittest.main()

