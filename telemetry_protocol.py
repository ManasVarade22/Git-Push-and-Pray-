"""Fixed-size binary UDP telemetry frame (29 bytes, network byte order)."""

from dataclasses import dataclass


FRAME_SIZE = 29
CHECKSUM_SIZE = 2

# Documented byte offsets; all multibyte values are big-endian.
DRONE_ID_OFFSET = 0
SEQUENCE_OFFSET = 1
TIMESTAMP_OFFSET = 3
LATITUDE_OFFSET = 7
LONGITUDE_OFFSET = 11
SENSORS_OFFSET = 15
SENSOR_COUNT = 6
SENSOR_WIDTH = 2
CHECKSUM_OFFSET = 27

MAX_SEQUENCE = 0xFFFF
SEQUENCE_HALF_RANGE = 0x8000
LATITUDE_LIMIT_MICRODEGREES = 90_000_000
LONGITUDE_LIMIT_MICRODEGREES = 180_000_000


class FrameError(ValueError):
    """Base class for malformed telemetry frames."""


class FrameLengthError(FrameError):
    """Raised when a frame is not exactly FRAME_SIZE bytes."""


class ChecksumError(FrameError):
    """Raised when the CRC does not match the frame contents."""


def crc16_ccitt_false(data: bytes) -> int:
    """CRC-16/CCITT-FALSE: poly=0x1021, init=0xFFFF, refin/out=false, xorout=0."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


@dataclass(frozen=True, slots=True)
class TelemetryFrame:
    drone_id: int
    sequence: int
    timestamp: int
    latitude_microdegrees: int
    longitude_microdegrees: int
    sensor_readings: tuple[int, int, int, int, int, int]

    def __post_init__(self) -> None:
        _check_unsigned("drone_id", self.drone_id, 8)
        _check_unsigned("sequence", self.sequence, 16)
        _check_unsigned("timestamp", self.timestamp, 32)
        _check_signed("latitude_microdegrees", self.latitude_microdegrees, 32)
        _check_signed("longitude_microdegrees", self.longitude_microdegrees, 32)
        if not -LATITUDE_LIMIT_MICRODEGREES <= self.latitude_microdegrees <= LATITUDE_LIMIT_MICRODEGREES:
            raise ValueError("latitude is outside [-90, 90] degrees")
        if not -LONGITUDE_LIMIT_MICRODEGREES <= self.longitude_microdegrees <= LONGITUDE_LIMIT_MICRODEGREES:
            raise ValueError("longitude is outside [-180, 180] degrees")
        if len(self.sensor_readings) != SENSOR_COUNT:
            raise ValueError(f"sensor_readings must contain exactly {SENSOR_COUNT} values")
        for value in self.sensor_readings:
            _check_unsigned("sensor reading", value, 16)

    def to_bytes(self) -> bytes:
        """Serialize a validated frame and append its CRC in big-endian order."""
        frame = bytearray(FRAME_SIZE)
        frame[DRONE_ID_OFFSET] = self.drone_id
        frame[SEQUENCE_OFFSET:SEQUENCE_OFFSET + 2] = self.sequence.to_bytes(2, "big")
        frame[TIMESTAMP_OFFSET:TIMESTAMP_OFFSET + 4] = self.timestamp.to_bytes(4, "big")
        frame[LATITUDE_OFFSET:LATITUDE_OFFSET + 4] = self.latitude_microdegrees.to_bytes(4, "big", signed=True)
        frame[LONGITUDE_OFFSET:LONGITUDE_OFFSET + 4] = self.longitude_microdegrees.to_bytes(4, "big", signed=True)
        for index, value in enumerate(self.sensor_readings):
            start = SENSORS_OFFSET + index * SENSOR_WIDTH
            frame[start:start + SENSOR_WIDTH] = value.to_bytes(SENSOR_WIDTH, "big")
        frame[CHECKSUM_OFFSET:FRAME_SIZE] = crc16_ccitt_false(frame[:CHECKSUM_OFFSET]).to_bytes(2, "big")
        return bytes(frame)

    @classmethod
    def from_bytes(cls, data: bytes) -> "TelemetryFrame":
        """Validate length and CRC before decoding any fields."""
        if len(data) != FRAME_SIZE:
            raise FrameLengthError(f"expected exactly {FRAME_SIZE} bytes, received {len(data)}")
        expected = crc16_ccitt_false(data[:CHECKSUM_OFFSET])
        received = int.from_bytes(data[CHECKSUM_OFFSET:FRAME_SIZE], "big")
        if received != expected:
            raise ChecksumError(f"CRC mismatch: expected 0x{expected:04X}, received 0x{received:04X}")
        readings = tuple(
            int.from_bytes(data[SENSORS_OFFSET + i * SENSOR_WIDTH:SENSORS_OFFSET + (i + 1) * SENSOR_WIDTH], "big")
            for i in range(SENSOR_COUNT)
        )
        return cls(
            drone_id=data[DRONE_ID_OFFSET],
            sequence=int.from_bytes(data[SEQUENCE_OFFSET:TIMESTAMP_OFFSET], "big"),
            timestamp=int.from_bytes(data[TIMESTAMP_OFFSET:LATITUDE_OFFSET], "big"),
            latitude_microdegrees=int.from_bytes(data[LATITUDE_OFFSET:LONGITUDE_OFFSET], "big", signed=True),
            longitude_microdegrees=int.from_bytes(data[LONGITUDE_OFFSET:SENSORS_OFFSET], "big", signed=True),
            sensor_readings=readings,  # type: ignore[arg-type]
        )


def is_newer_sequence(candidate: int, reference: int) -> bool:
    """Return whether candidate follows reference under 16-bit serial arithmetic.

    A forward distance in [1, 32767] is newer; equal, backward, and the
    ambiguous half-range distance (32768) are not. Thus 0 follows 65535.
    """
    _check_unsigned("candidate", candidate, 16)
    _check_unsigned("reference", reference, 16)
    distance = (candidate - reference) & MAX_SEQUENCE
    return 0 < distance < SEQUENCE_HALF_RANGE


def _check_unsigned(name: str, value: int, bits: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < (1 << bits):
        raise ValueError(f"{name} must be an unsigned {bits}-bit integer")


def _check_signed(name: str, value: int, bits: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
        raise ValueError(f"{name} must be a signed {bits}-bit integer")

