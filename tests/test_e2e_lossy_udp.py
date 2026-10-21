import unittest

from e2e_verification import run_lossy_batch, run_protocol_grid_alert_scenario


class EndToEndLossyUDPTests(unittest.TestCase):
    def test_seeded_impairments_use_local_udp_and_update_unique_grid_state(self):
        report = run_lossy_batch(source_count=512)
        counts = report["counts"]
        self.assertTrue(report["passed"])
        self.assertEqual(counts["received"], report["emitted_udp_datagrams"])
        self.assertEqual(counts["accepted"], counts["grid_updates"])
        self.assertGreater(counts["dropped"], 0)
        self.assertGreater(counts["duplicated"], 0)
        self.assertGreater(counts["corrupted"], 0)
        self.assertGreater(counts["deduplicated"], 0)
        self.assertTrue(report["wire_order_reordered"])

    def test_wraparound_crc_reorder_staleness_and_three_frame_alert(self):
        report = run_protocol_grid_alert_scenario()
        self.assertTrue(report["passed"])
        self.assertTrue(all(report["invariants"].values()))
        self.assertEqual(report["sequence_processing_order"][:3], [65534, 65535, 0])
        self.assertTrue(report["alert_active"])


if __name__ == "__main__":
    unittest.main()

