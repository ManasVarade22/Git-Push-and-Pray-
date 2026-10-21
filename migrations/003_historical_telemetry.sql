-- Reproducible, idempotent migration for historical UDP telemetry frames.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS historical_telemetry (
    drone_id SMALLINT NOT NULL CHECK (drone_id BETWEEN 0 AND 255),
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 65535),
    observed_at TIMESTAMPTZ NOT NULL,
    latitude_microdegrees INTEGER NOT NULL,
    longitude_microdegrees INTEGER NOT NULL,
    sensor_readings SMALLINT[] NOT NULL CHECK (cardinality(sensor_readings) = 6),
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Include timestamp so a valid 16-bit sequence wrap creates a new row,
    -- while retransmission of that exact frame remains idempotent.
    PRIMARY KEY (drone_id, sequence, observed_at)
);

INSERT INTO schema_migrations(version) VALUES (3) ON CONFLICT (version) DO NOTHING;

