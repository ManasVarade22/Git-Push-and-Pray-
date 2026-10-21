# Lossy UDP telemetry simulator

Run from the repository root with `python telemetry_simulator.py`. The default
creates 10,000 deterministic synthetic frames, then sends impaired 29-byte
binary datagrams over local UDP. It prints the configured probabilities and
the observed counts separately. The default seed is `20261021`; use `--seed`
to select another deterministic run.

## Impairment semantics

- A duplicate is added independently per original frame with probability 10%.
- Each original or duplicate copy is then independently dropped with
  probability 30%.
- Corruption, when enabled, flips a sensor payload bit without updating CRC;
  the receiver should reject it.
- Every copy that survives loss gets a seeded uniform delay between zero and
  `--max-delay-ms`. Sending by scheduled arrival time can reorder sequence
  numbers. The `--window-seconds` default is two seconds.
- `sent` counts generated source frames; `duplicated` counts added copies;
  `dropped` counts copies discarded by the loss stage; `corrupted` counts
  delivered copies modified; `accepted` counts frames released in order;
  `rejected` counts invalid, duplicate, or late arrivals; `late` is the late
  subset of rejected.

The configured percentages are Bernoulli probabilities, not quotas. A finite
seeded run will usually have observed rates near, but not exactly equal to,
the configured values. The simulator does not recreate dropped packets or
claim recovery. The generated sensor readings are synthetic protocol test
values, not field measurements.

## Reordering behavior

The first valid frame from each drone establishes its initial sequence. Later
forward frames are buffered while an earlier sequence is missing. After the
oldest buffered frame has waited two seconds, the gap is closed: missing
sequence values are skipped and available buffered frames are released. A
missing packet arriving after that point is counted as late and rejected; it
is not delivered retroactively. Sequence ordering uses 16-bit modular
comparison, so wrap from 65535 to 0 is forward progress.

The unit tests use seed `20261021` over 10,000 frames. For this specific run,
the loss stage drops 3,317 of 11,004 copies (30.14%); duplication adds 1,004
copies (10.04% of original frames), leaving 7,687 copies for delivery. Those
are observed finite-sample results, not guaranteed rates for another count or
seed.

A separate local UDP integration test uses 250 frames with zero delay range.
It observes 83 dropped copies, 22 added duplicates, 176 accepted frames, 13
rejected duplicate arrivals, and 0 late arrivals. Each received datagram is
read immediately after its local `sendto`, so this check avoids attributing
unconfigured host queue overflow to the impairment model.

