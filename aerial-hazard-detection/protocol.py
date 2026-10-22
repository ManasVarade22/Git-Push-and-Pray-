
import struct
from dataclasses import dataclass

FRAME_FORMAT = ">BHIii6HH"
FRAME_SIZE = struct.calcsize(FRAME_FORMAT)

@dataclass
class DroneFrame:
    drone_id: int
    sequence: int
    timestamp: int
    latitude: float
    longitude: float
    sensors: tuple

def crc16(data):
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc

def encode_frame(frame):
    if len(frame.sensors) != 6:
        raise ValueError("Exactly six sensor readings are required")

    if not 0 <= frame.drone_id <= 255:
        raise ValueError("Drone ID must fit in one byte")

    if not 0 <= frame.sequence <= 65535:
        raise ValueError("Sequence number must fit in 16 bits")

    if not 0 <= frame.timestamp <= 4294967295:
        raise ValueError("Timestamp must fit in 32 bits")

    latitude = round(frame.latitude * 10_000_000)
    longitude = round(frame.longitude * 10_000_000)

    if not -900_000_000 <= latitude <= 900_000_000:
        raise ValueError("Latitude out of range")
    if not -1_800_000_000 <= longitude <= 1_800_000_000:
        raise ValueError("Longitude out of range")

    sensors = tuple(int(v) for v in frame.sensors)
    if any(v < 0 or v > 65535 for v in sensors):
        raise ValueError("Sensor values must fit in unsigned 16 bits")

    body = struct.pack(
        ">BHIii6H",
        frame.drone_id,
        frame.sequence,
        frame.timestamp,
        latitude,
        longitude,
        *sensors
    )
    checksum = crc16(body)
    packet = body + struct.pack(">H", checksum)

    assert len(packet) == 29
    return packet

def decode_frame(packet):
    if len(packet) != FRAME_SIZE:
        raise ValueError(f"Invalid frame length: {len(packet)}")

    body = packet[:-2]
    received_crc = struct.unpack(">H", packet[-2:])[0]

    if crc16(body) != received_crc:
        raise ValueError("Invalid CRC16 checksum")

    values = struct.unpack(">BHIii6H", body)

    return DroneFrame(
        drone_id=values[0],
        sequence=values[1],
        timestamp=values[2],
        latitude=values[3] / 10_000_000,
        longitude=values[4] / 10_000_000,
        sensors=tuple(values[5:11])
    )
