# P10 Alert Durability and Restart Tests

Run from the repository root with `TEST_POSTGRES_DSN` set to a dedicated,
disposable PostgreSQL test database:

```sh
python -m unittest -v tests.test_alert_engine tests.test_alert_durability
```

Without `TEST_POSTGRES_DSN`, PostgreSQL integration tests are skipped. They do
not silently substitute `InMemoryAlertRepository` and call that durable.

## Test coverage

Clock-controlled unit tests in `tests/test_alert_engine.py`:

- `test_three_consecutive_high_predictions_raise_one_alert`
- `test_valid_below_entry_resets_high_counter`
- `test_clear_requires_five_continuous_minutes_below_clear_threshold`
- `test_risk_at_or_above_clear_threshold_resets_clear_timer`
- `test_repeat_and_escalation_stop_after_acknowledgement`
- `test_stale_missing_or_invalid_readings_do_not_clear_hazard`
- `test_escalation_does_not_change_hazard_resolution_state`
- `test_stale_gap_breaks_consecutive_high_prediction_streak`

PostgreSQL integration tests in `tests/test_alert_durability.py`:

- `test_acknowledgement_is_visible_from_new_connection_after_commit`
- `test_acknowledgement_commit_failure_is_not_confirmed_or_cached`
- `test_acknowledgement_and_active_hazard_survive_full_dashboard_process_restart`

The restart test seeds an active alert, starts a fresh `dashboard_server.py`,
acknowledges through the HTTP API, verifies the response and a separate database
connection, terminates that entire server process, then starts a second server
process. It checks that `/api/v1/alerts` restores the acknowledged active alert.
It also checks `/api/v1/map/snapshot` has no cells after restart, documenting
that the in-memory telemetry grid is not durable.

## PostgreSQL schema

`PostgresAlertRepository.initialize()` creates these tables in the current
search path:

- `hazard_alerts`: alert UUID, cell coordinates, raised/acknowledged/resolved
  timestamps, acknowledging operator, notification timestamp, escalation level,
  and last risk score.
- `hazard_cell_alert_state`: per-cell high-risk counter, active flag, alert UUID,
  below-clear timer, last valid prediction timestamp, last risk score, quality
  status, and update timestamp.

The integration test creates a random isolated schema, applies the same
repository initialization, and drops that schema afterward. It does not truncate
or alter tables in the configured database search path.

## Current execution evidence

The unit tests ran with a controllable clock. PostgreSQL integration tests were
not executed in the current environment because `TEST_POSTGRES_DSN` was not
configured. Therefore no database commit or backend process restart is reported
as passing here. Set the variable to the dedicated test database and run the
command above to execute those checks.

The persisted schema restores alert/acknowledgement state only. The grid's cell
readings, weather cache, and freshness state remain in memory and restart empty;
the restart test explicitly verifies the empty map snapshot.

