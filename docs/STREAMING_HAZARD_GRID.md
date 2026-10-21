# Streaming spatial hazard grid

`StreamingHazardGrid` keeps current cell state in process memory and exposes a
versioned immutable-by-copy snapshot immediately after each accepted update.
The snapshot has no dependency on database writes. Persistence and UI transport
are intentionally outside this stage.

## Spatial indexing

The grid uses a local equirectangular metric projection anchored at the
configured site origin. The default cell size is 10 metres; east-west distance
is adjusted by the cosine of the origin latitude. Integer cells use floor
indexing, and a cell centre can be derived for display. This is an approximate
local grid for an industrial site, not a global projection. Configure the
origin near the site being monitored.

## Values, time, and confidence

Sensor IDs are the protocol's six channel indexes (0–5). Raw unsigned readings
are kept as counts. Zero is a valid count and is never interpreted as missing.
Because P02 has no missing-value marker, external quality flags can be passed
as `valid_sensor_indices`; absent indexes remain `None`. Each sensor reading
has its own observation timestamp and confidence `exp(-age / tau)`, with
configurable `confidence_decay_seconds`. An older timestamp never overwrites a
newer reading, even when the frame's sequence is newer. A valid received frame
refreshes cell freshness; cells become `stale` only after
`stale_timeout_seconds` without another accepted frame.

Weather is held as its latest timestamped observation. Values older than
`weather_max_age_seconds` are represented as `None` and `weather_fresh=0`; no
temperature or wind value is substituted. Missing weather has `None` for all
weather features, including freshness.

Neighbour features use the eight adjacent grid cells, excluding the target.
For each sensor, the feature is a confidence-weighted mean over cells where
that sensor has an observation; its companion feature is mean confidence.
Unknown sensor means/confidences remain `None`. `neighbor_coverage` reports how
many of the eight cells contain any reading, and is zero when none do.

## Risk model boundary

The repository contains no SVM artifact, model loader, or expected trained
feature schema. The presentation is not an executable model artifact. This
implementation therefore does not invent a trained model or risk score. It
defines the versioned `stream-grid-v1` feature names in
`streaming_hazard_grid.py` and accepts an optional risk evaluator. The
evaluator is called only when every feature is present; without one, risk is
`None` with status `no_model`, and incomplete data yields
`incomplete_features`. The declared feature schema must be matched to a real
artifact before attaching an SVM.

The repository also has no original store-then-predict implementation. This
change does not create or replace a legacy mode; one can be retained separately
when its source is available.

## Threading

Frame acceptance, sequence state, weather state, feature construction,
inference, and snapshots are protected by one reentrant lock. `update` returns
the new grid snapshot as soon as the in-memory update and optional inference
finish; database completion is not part of this component.

