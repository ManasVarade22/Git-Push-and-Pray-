# P07 — Asynchronous PostgreSQL persistence

`TelemetryPersistenceCoordinator` applies a validated frame to the in-memory
grid first, then submits it to `AsyncTelemetryWriter` with a non-blocking queue
operation. Grid updates and risk predictions never wait for PostgreSQL. A
dedicated daemon worker initializes the schema and writes historical records.
The database connection and write counters are exposed by `metrics()`.

The queue has a configured finite capacity. Database errors keep the current
record in the worker and trigger capped exponential retries. While unavailable,
the queue can fill; `enqueue()` then raises `QueueFullError`, increments
`queue_overflows`, and the coordinator returns `history_queue_full` while the
live grid update remains accepted. The caller must surface this status and
retain responsibility for the rejected history record (for example, retry or
apply upstream backpressure). No queue-overflow loss is reported as successful
persistence. A shutdown returns `False` if the worker has not drained before
its timeout; do not treat that as a clean persisted shutdown.

Historical writes use `ON CONFLICT DO NOTHING` against `(drone_id, sequence,
observed_at)`, making retries idempotent while allowing a later 16-bit sequence
wrap to produce a distinct historical row. The versioned idempotent migration
is `migrations/003_historical_telemetry.sql`. Run it through
`PostgresTelemetryRepository.initialize()` or with psql. Alert state uses the
reproducible `CREATE TABLE IF NOT EXISTS` schema initialization in
`PostgresAlertRepository.initialize()`.

Acknowledgement remains synchronous and is confirmed only after the alert
repository's transaction exits successfully. It persists a candidate state
before publishing acknowledgement in memory; a failed commit raises and leaves
the in-memory alert unacknowledged. `PerCellAlertStateMachine` loads durable
alert state during construction. Acknowledgement never clears the hazard.

The tests verify queueing while a fake database call is blocked, retry behavior,
explicit overflow, and failed acknowledgement commit semantics. PostgreSQL
integration tests for historical writes, retry/idempotency, acknowledgement
durability, and restart recovery require `TEST_POSTGRES_DSN`. In-memory results
do not prove PostgreSQL durability.

