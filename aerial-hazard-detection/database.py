
import os
import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "dbname=aerosafe user=saksham"
)

def initialize_database():
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id BIGSERIAL PRIMARY KEY,
                drone_id INTEGER NOT NULL,
                sequence_number INTEGER NOT NULL,
                cell_x INTEGER NOT NULL,
                cell_y INTEGER NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                sensor_values INTEGER[] NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (drone_id, sequence_number)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_cell_time
            ON sensor_readings (cell_x, cell_y, received_at DESC)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id BIGSERIAL PRIMARY KEY,
                alert_id TEXT NOT NULL UNIQUE,
                cell_id TEXT NOT NULL,
                state TEXT NOT NULL,
                risk_score DOUBLE PRECISION NOT NULL,
                raised_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                acknowledged_at TIMESTAMPTZ,
                acknowledged_by TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    print("PostgreSQL tables initialized successfully.")

if __name__ == "__main__":
    initialize_database()
