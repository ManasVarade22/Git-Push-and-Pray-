
import os
import queue
import threading
import logging
import time

import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "dbname=aerosafe user=saksham"
)


class DatabaseWriter:
    def __init__(self, max_queue_size=5000):
        self.queue = queue.Queue(maxsize=max_queue_size)
        self.thread = threading.Thread(
            target=self._worker,
            daemon=True
        )
        self.thread.start()

    def submit_reading(self, frame, cell):
        record = (
        frame.drone_id,
        frame.sequence,
        cell.x,
        cell.y,
        frame.latitude,
        frame.longitude,
        list(frame.sensors),
        cell.risk_score,
        cell.risk_level,
        cell.confidence
)

        try:
            self.queue.put_nowait(record)
            return True
        except queue.Full:
            logging.warning("Database queue full; reading not stored")
            return False

    def _worker(self):
        while True:
            record = self.queue.get()

            try:
                with psycopg.connect(DATABASE_URL) as conn:
                    conn.execute(
                    """
                    INSERT INTO sensor_readings (
                    drone_id, sequence_number,
                     cell_x, cell_y, latitude, longitude,
                    sensor_values, risk_score, risk_level, confidence,
                     received_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (drone_id, sequence_number)
                        DO UPDATE SET
                         risk_score = EXCLUDED.risk_score,
                         risk_level = EXCLUDED.risk_level,
                         confidence = EXCLUDED.confidence,
                         received_at = NOW()
                    """,
                    record
                        )
            except Exception:
                logging.exception("Failed to save sensor reading")
                time.sleep(1)
            finally:
                self.queue.task_done()
