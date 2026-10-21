import unittest

from telemetry_protocol import (
    ChecksumError,
    FrameLengthError,
    TelemetryFrame,
    crc16_ccitt_false,
    is_newer_sequence,
)


class TelemetryProtocolTests(unittest.TestCase):
    def setUp(self):
        self.frame = TelemetryFrame(7, 1234, 1_700_000_000, 37_774_900, -122_419_400, (0, 1, 42, 256, 32768, 65535))

    def test_round_trip_valid_frame(self):
        encoded = self.frame.to_bytes()
        self.assertEqual(len(encoded), 29)
        self.assertEqual(TelemetryFrame.from_bytes(encoded), self.frame)

    def test_crc_known_vector(self):
        self.assertEqual(crc16_ccitt_false(b"123456789"), 0x29B1)

    def test_corrupted_payload_is_rejected(self):
        encoded = bytearray(self.frame.to_bytes())
        encoded[15] ^= 0x01
        with self.assertRaises(ChecksumError):
            TelemetryFrame.from_bytes(encoded)

    def test_corrupted_checksum_is_rejected(self):
        encoded = bytearray(self.frame.to_bytes())
        encoded[-1] ^= 0x80
        with self.assertRaises(ChecksumError):
            TelemetryFrame.from_bytes(encoded)

    def test_truncated_frame_is_rejected(self):
        with self.assertRaises(FrameLengthError):
            TelemetryFrame.from_bytes(self.frame.to_bytes()[:-1])

    def test_extra_bytes_are_rejected(self):
        with self.assertRaises(FrameLengthError):
            TelemetryFrame.from_bytes(self.frame.to_bytes() + b"\x00")

    def test_boundary_values_round_trip(self):
        boundary = TelemetryFrame(255, 65535, 0xFFFFFFFF, -90_000_000, 180_000_000, (0, 65535, 0, 65535, 0, 65535))
        self.assertEqual(TelemetryFrame.from_bytes(boundary.to_bytes()), boundary)

    def test_sequence_wraparound(self):
        self.assertTrue(is_newer_sequence(0, 65535))
        self.assertTrue(is_newer_sequence(1, 65535))
        self.assertFalse(is_newer_sequence(65535, 0))
        self.assertFalse(is_newer_sequence(42, 42))
        self.assertFalse(is_newer_sequence(0x8000, 0))

    def test_invalid_geographic_boundary_is_rejected(self):
        with self.assertRaises(ValueError):
            TelemetryFrame(1, 0, 0, 90_000_001, 0, (0,) * 6)


if __name__ == "__main__":
    unittest.main()

