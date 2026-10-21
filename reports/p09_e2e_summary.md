# P09 End-to-end lossy UDP verification

Overall result: **PASS**

All traffic used localhost UDP with binary 29-byte telemetry frames. No production database or drone hardware was used.

## seeded_lossy_localhost_udp

Result: **PASS**
Configured probabilities: drop 30%, duplicate 10%, CRC corruption 5%; seed 20261021.
These are configured probabilities; counts below are observed finite-sample outcomes.
Original source frames: 512; emitted UDP datagrams: 378.
Observed drop ratio per transmitted copy: 32.3%; duplicate-copy generation ratio per source frame: 9.0%; CRC-corruption ratio among emitted datagrams: 5.6%.

| Count | Observed |
|---|---:|
| sent | 378 |
| received | 378 |
| dropped | 180 |
| duplicated | 46 |
| corrupted | 21 |
| accepted | 236 |
| rejected | 142 |
| late | 0 |
| deduplicated | 17 |
| crc_rejected | 21 |
| grid_updates | 236 |
| risk_evaluations | 0 |
| alerts_raised | 0 |

## wraparound_crc_reorder_grid_stale_alert

Result: **PASS**

| Count | Observed |
|---|---:|
| sent | 15 |
| received | 15 |
| dropped | 0 |
| duplicated | 1 |
| corrupted | 1 |
| accepted | 12 |
| rejected | 3 |
| late | 1 |
| deduplicated | 1 |
| crc_rejected | 1 |
| grid_updates | 12 |
| risk_evaluations | 11 |
| alerts_raised | 1 |

Invariant checks:
- PASS — sequence wraparound order
- PASS — buffered packet did not mutate grid before gap filled
- PASS — three consecutive high predictions raised alert
- PASS — risk evaluated once for each center frame
- PASS — duplicate did not mutate grid
- PASS — bad crc did not mutate grid
- PASS — reorder window released pending packet
- PASS — arrival after window was late
- PASS — stale cell transition
- PASS — database isolated
- Sequence processing order: `[65534, 65535, 0, 2]`

## Reproduce

```sh
python -m unittest -v tests.test_e2e_lossy_udp
python e2e_verification.py --json-report reports/p09_e2e_report.json --summary-report reports/p09_e2e_summary.md
```

