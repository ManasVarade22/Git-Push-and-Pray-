# P06 — Per-cell alert state machine

`PerCellAlertStateMachine` consumes risk predictions produced by the streaming
grid. Configure the entry and clear thresholds for the selected model; entry
must be higher than clear. Three consecutive valid predictions at or above
entry raise an alert. Any valid prediction below entry resets the candidate
counter. Missing, stale, invalid, and invalid-range predictions reset that
counter and cannot clear an active alert.

An active alert starts its clear timer only on a valid score strictly below the
clear threshold. A score at or above clear resets that timer. Missing/invalid
readings or a prediction gap beyond `prediction_stale_after_seconds` also reset
it. The hazard clears after 300 continuous seconds below clear while valid
predictions continue to arrive within the configured stale timeout. The timer
uses timestamps and `tick()`/incoming predictions; no sleeping is needed in
tests.

Each new alert receives a UUID and UTC timestamps. `acknowledge()` persists the
operator and timestamp but leaves `hazard_active` true. Acknowledgement only
suppresses repeats/escalations; only the clear rule resolves the hazard.
`tick()` returns repeat/escalation events for the caller to deliver through its
notification channel. Repeat, first escalation, and later escalation
intervals are configurable.

## PostgreSQL and recovery

`PostgresAlertRepository` stores current per-cell alert state in
`hazard_cell_alert_state` and alert identifiers, acknowledgement data, and
raised/resolved/notification timestamps in `hazard_alerts`. Set `DATABASE_URL`
or pass a connection string at runtime; no secret is embedded in the code.
Install `requirements.txt` to use psycopg 3. State is loaded when the state
machine starts and transition/acknowledgement changes are written to
PostgreSQL.

The regular restart test uses an in-memory repository to verify state-machine
recovery semantics. A separate PostgreSQL integration restart test runs only
when `TEST_POSTGRES_DSN` is configured. An in-memory test is not evidence of
durable PostgreSQL persistence; report database durability only after that
integration test succeeds against PostgreSQL.

This repository still has no original store-then-predict implementation. The
alert state machine is a separate consumer and does not add, replace, or claim
that legacy pipeline exists.

