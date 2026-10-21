# Binary UDP telemetry protocol

Each datagram is exactly 29 bytes. Multi-byte fields use big-endian (network)
byte order. There is no JSON or HTTP encoding in the telemetry transport.

| Offset | Length | Field | Encoding / range |
|---:|---:|---|---|
| 0 | 1 | Drone ID | unsigned 8-bit |
| 1 | 2 | Sequence | unsigned 16-bit, wraps modulo 65536 |
| 3 | 4 | Unix timestamp | unsigned seconds, 0 through 2^32-1 |
| 7 | 4 | Latitude | signed microdegrees, valid range -90,000,000..90,000,000 |
| 11 | 4 | Longitude | signed microdegrees, valid range -180,000,000..180,000,000 |
| 15 | 12 | Six sensor readings | six consecutive unsigned 16-bit quantized counts |
| 27 | 2 | CRC | CRC-16/CCITT-FALSE over bytes 0..26, big-endian |

The CRC parameters are polynomial `0x1021`, initial value `0xFFFF`, no input
or output reflection, and final XOR `0x0000` (the standard `123456789` test
vector produces `0x29B1`). A receiver must check exact length and CRC before
decoding or changing application state.

## Sensor quantization

The packet carries raw quantized counts from 0 to 65,535 for channels 1–6;
one count is the protocol's base increment. The frame does not identify sensor
types, engineering units, or calibration factors, and no calibration scales
were supplied with the project. Convert counts to engineering values only
using externally configured per-channel scale and offset, for example
`value = count * scale + offset`. This keeps gas units and calibration from
being silently invented. The six counts round-trip exactly through the Python
codec.

## Sequence ordering

For 16-bit sequence values, candidate `a` is newer than reference `b` when
`0 < ((a - b) mod 65536) < 32768`. Therefore sequence `0` follows `65535`.
Equal values, backward values, and the ambiguous half-range distance are not
considered newer. This helper only compares sequence values; windowing and
deduplication policy belongs to the receiver.

