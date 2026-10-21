# P08 — Live Leaflet hazard map

## Confirmed backend contract

The P01–P07 repository had no FastAPI app, map API, or WebSocket endpoint. P08
adds `dashboard_api.py` around the existing in-memory `StreamingHazardGrid`,
`PerCellAlertStateMachine`, and optional `AsyncTelemetryWriter`. The map contract
is implemented before the Leaflet client consumes it:

- `GET /api/v1/map/snapshot` returns `{ "type": "snapshot", "data": ... }`.
- `WS /ws/map` sends a complete snapshot on connect and then refreshed memory
  snapshots at the configured interval. A reconnecting client fetches a fresh
  REST snapshot before opening its next WebSocket.
- `GET /api/v1/alerts` lists active alerts and escalation levels.
- `GET /api/v1/health` reports packet counters, telemetry-writer/database
  status, and whether the acknowledgement store is PostgreSQL-backed.
- `POST /api/v1/alerts/{alert_id}/acknowledge` accepts
  `{ "acknowledged_by": "operator name" }`. It returns success only after the
  durable PostgreSQL repository operation commits. It returns 503 if durable
  storage is not configured or the commit fails.

Map and health snapshots read current values from memory. They do not query
PostgreSQL. The acknowledgement route is the only map API operation that writes
to the database. The WebSocket message is a full snapshot with `schema_version`
1; each cell includes center coordinates, metric cell size, risk score/status,
freshness, confidence, last-seen time, six quantized channels and per-channel
timestamps, shared weather values and feature timestamps, source labels, and
alert state. Missing values are represented as `null` or an explicit status.

## Provenance and interpretation

The demo server labels its generated telemetry as simulated and its generated
weather as demonstration weather. The optional SVM is the separately labelled
synthetic-data demonstration model. Those generated inputs and outputs are not
field measurements or verified fire predictions. In non-demo mode, telemetry
and weather default to `UNVERIFIED SOURCE` unless the deployment supplies
`TELEMETRY_SOURCE_LABEL` and `WEATHER_SOURCE_LABEL`. Raw sensor channels remain
generic quantized counts because the repository does not define gas names or
physical unit conversions. Stale cells render gray with a dashed outline;
staleness never means the site is safe.

## Reproducible manual checklist

1. Install project dependencies with `python -m pip install -r requirements.txt`.
2. For durable acknowledgements, configure `DATABASE_URL` to a PostgreSQL test
   database before startup. Do not place credentials in source control. With no
   database configured the read-only map can run, while acknowledgement
   requests intentionally return 503.
3. From the repository root, run:

   ```powershell
   python dashboard_server.py --http-host 127.0.0.1 --http-port 8000 `
     --udp-host 127.0.0.1 --udp-port 5005 --origin-latitude 0 `
     --origin-longitude 0 --demo-model --demo-weather --simulate
   ```

4. Open `http://127.0.0.1:8000`. Confirm the source strip identifies simulated
   telemetry, synthetic demonstration model output, and demonstration weather.
   Confirm cells appear and the selected-cell panel shows six generic quantized
   channels, timestamps, confidence, model status/output, weather timestamps,
   and alert status.
5. Open `http://127.0.0.1:8000/api/v1/map/snapshot` and inspect the same memory
   snapshot schema. Confirm `/api/v1/health` includes live UDP counts and queue/
   database status without needing a map query to historical PostgreSQL tables.
6. With PostgreSQL configured, wait for a demo alert and acknowledge it. Confirm
   the response says `hazard_active: true`, the map marks it acknowledged, and
   the alert remains visible until the risk clear state machine resolves it.
   Restart the app and confirm the acknowledgement and alert identifier load
   from PostgreSQL.
7. Stop the simulator or run without `--simulate`, send a protocol-valid binary
   UDP frame, then wait longer than `--cell-stale-seconds`. Confirm the cell
   becomes gray and dashed while its last readings remain inspectable.
8. Use browser network controls to interrupt the WebSocket. Confirm the status
   changes to reconnecting, then the client fetches a fresh REST snapshot and
   restores live updates after the connection returns.

Leaflet and OpenStreetMap tiles are loaded from their public CDN/tile service;
the dashboard requires network access to render the basemap. The grid overlay,
cell inspection, alert state, and health panels use the backend contract above.

