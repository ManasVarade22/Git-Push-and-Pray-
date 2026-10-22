
import socket
import time
import struct
from alert_manager import AlertManager
from collections import defaultdict

from protocol import decode_frame
from hazard_grid import HazardGrid
from database_writer import DatabaseWriter

HOST = "127.0.0.1"
PORT = 9999
REORDER_WINDOW_SECONDS = 2.0
STALE_AFTER_SECONDS = 15.0


class UDPReceiver:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.grid = HazardGrid()
        self.db_writer = DatabaseWriter()
        self.alert_manager = AlertManager()
        self.pending = defaultdict(dict)
        self.seen = defaultdict(dict)
        self.expected = {}
        self.first_pending_at = {}
        self.last_received = {}
        self.stats = {
            "received": 0,
            "valid": 0,
            "invalid": 0,
            "duplicates": 0,
            "processed": 0,
            "missing": 0,
        }

    def handle_packet(self, packet, now=None):
        now = time.monotonic() if now is None else now
        self.stats["received"] += 1

        try:
            frame = decode_frame(packet)
            print(f"VALID FRAME: drone={frame.drone_id}, seq={frame.sequence}")
        except (ValueError, TypeError, struct.error):
            self.stats["invalid"] += 1
            print("Rejected invalid frame")
            return []

        self.stats["valid"] += 1
        drone = frame.drone_id
        seq = frame.sequence

        if seq in self.seen[drone]:
            self.stats["duplicates"] += 1
            print(f"Duplicate ignored: drone={drone}, seq={seq}")
            return []

        if drone not in self.expected:
            self.expected[drone] = seq

        expected = self.expected[drone]
        delta = (seq - expected) & 0xFFFF

        if delta >= 0x8000:
            self.stats["duplicates"] += 1
            print(f"Late frame ignored: drone={drone}, seq={seq}")
            return []

        self.seen[drone][seq] = now
        self.pending[drone][seq] = (now, frame)
        self.last_received[drone] = now

        if drone not in self.first_pending_at:
            self.first_pending_at[drone] = now

        return self.flush_ready(now)

    def flush_ready(self, now=None, force=False):
        now = time.monotonic() if now is None else now
        output = []

        for drone in list(self.pending):
            pending = self.pending[drone]

            while pending:
                expected = self.expected[drone]

                if expected in pending:
                    arrival, frame = pending.pop(expected)
                    output.append(frame)
                    self.expected[drone] = (expected + 1) & 0xFFFF
                    self.stats["processed"] += 1
                    continue

                oldest = min(item[0] for item in pending.values())
                if not force and now - oldest < REORDER_WINDOW_SECONDS:
                    break

                seq = min(
                    pending,
                    key=lambda candidate: (
                        candidate - expected
                    ) & 0xFFFF
                )
                arrival, frame = pending.pop(seq)
                delta = (seq - expected) & 0xFFFF
                self.stats["missing"] += delta
                self.expected[drone] = (seq + 1) & 0xFFFF
                output.append(frame)
                self.stats["processed"] += 1

            if not pending:
                self.first_pending_at.pop(drone, None)

        
        for frame in output:
            cell = self.grid.update(frame)

            prediction = self.grid.predict_cell(cell)
            events = self.alert_manager.update(
                cell.cell_id,
                prediction["risk_score"]
            )

            current_alerts = {
                item["cell_id"]: item
                for item in self.alert_manager.snapshot()
            }
            alert = current_alerts.get(cell.cell_id, {})

            cell.alert_state = alert.get("state", "NORMAL")
            cell.acknowledged = alert.get("acknowledged", False)

            self.db_writer.submit_reading(frame, cell)

            print(
                f"Grid updated: cell={cell.cell_id} "
                f"drone={frame.drone_id} seq={frame.sequence} "
                f"confidence={cell.confidence:.2f} "
                f"risk={prediction['risk_score']:.2f}% "
                f"level={prediction['risk_level']} "
                f"alert={cell.alert_state} "
                f"stale={cell.is_stale()} sensors={cell.sensors}"
            )

            for event in events:
                print(f"ALERT EVENT: {event}")


        return output

    def stale_drones(self, now=None):
        now = time.monotonic() if now is None else now
        return [
            drone for drone, stamp in self.last_received.items()
            if now - stamp > STALE_AFTER_SECONDS
        ]

    def serve_forever(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((self.host, self.port))
            sock.settimeout(0.25)
            print(f"UDP receiver listening on {self.host}:{self.port}")

            while True:
                try:
                    packet, address = sock.recvfrom(2048)
                    self.handle_packet(packet)
                except socket.timeout:
                    self.flush_ready()
                    for drone in self.stale_drones():
                        print(f"STALE drone: {drone}")


if __name__ == "__main__":
    UDPReceiver().serve_forever()
