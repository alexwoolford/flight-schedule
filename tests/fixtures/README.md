# Test fixtures

## `bts_flights_2025_07_18.parquet`

**Real BTS data, not generated.** A single-day slice of the US DOT Bureau of
Transportation Statistics On-Time Performance feed for **2025-07-18**, extracted
verbatim from `data/bts_flight_data/bts_flights_2025_07.parquet` as produced by
`download_bts_flight_data.py`. Every row is a real BTS-reported flight, per rule 1
in `CLAUDE.md` (no synthetic data, ever).

| | |
|---|---|
| rows | 21,495 (21,376 after the loader's `cancelled == 0` filter) |
| columns | 110 — the **full** BTS schema, unmodified |
| size | 996 KB, zstd |

Committed on purpose: it is what lets the `integration-test` job in
`.github/workflows/ci.yml` load a real graph and run the routing assertions on
every push. It is small enough for git and the only alternative — downloading
~200 MB from BTS in CI — makes the gate depend on a third-party site being up.

### Why this particular day

It has to reproduce the two defects that `tests/test_graph_validation.py`
regression-tests, otherwise those assertions would pass vacuously in CI and gate
nothing. Measured on this day:

| property | count | gates |
|---|---|---|
| legs landing on a later local day than they departed | 915 | `test_connects_to_has_no_overnight_inbound_legs`, `test_deadline_query_needs_no_overnight_guard` |
| `CONNECTS_TO` edges joining a mainline to its wholly-owned regional (AA↔MQ/OH) | 110,642 | `test_connects_to_carrier_is_sellable` |
| distinct airports acting as a connecting hub | 341 | `test_connects_to_hubs_are_real` |
| airports reachable in the UTC-offset solve, in one component | 341 | `test_solves_every_airport_in_the_data` |
| airports whose offset spans the dateline (GUM, SPN) | 2 | `test_dateline_airports_are_normalised` |

Those test methods assert the counts are non-zero as well as asserting the defect
is absent, so replacing this fixture with a day lacking any of these properties
fails loudly rather than silently weakening the suite. The dateline pair matters
in particular: without GUM/SPN the offset normalisation is never exercised.

### Regenerating

The full month must be downloaded first (`python download_bts_flight_data.py
--year 2025 --month 7`). Reading the source schema with `pq.read_schema` and
passing it to `from_pandas` is what preserves all 110 columns and their exact
dtypes — without it, pandas re-infers narrower types and the fixture stops being
byte-compatible with the loader's expectations.

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

src = "data/bts_flight_data/bts_flights_2025_07.parquet"
schema = pq.read_schema(src)
df = pd.read_parquet(src)
day = df[df["flightdate"].astype(str).str.startswith("2025-07-18")].copy()
pq.write_table(
    pa.Table.from_pandas(day, schema=schema, preserve_index=False),
    "tests/fixtures/bts_flights_2025_07_18.parquet",
    compression="zstd",
)
```

### Loading it

```bash
python load_bts_data.py --single-file bts_flights_2025_07_18.parquet \
                        --data-path tests/fixtures
python load_bts_data.py --solve-offsets 2025-07-18      # must precede the next
python load_bts_data.py --build-connections 2025-07-18
```

`--solve-offsets` is not optional and must run first: it writes the
`scheduled_*_utc` properties that `--build-connections` computes layovers from, and
repairs the local arrival date. Skipping it makes `--build-connections` raise
rather than silently build nothing.

Produces 21,376 `Schedule`, 341 `Airport`, 14 `Carrier`, 5,325 `ROUTE` and
623,508 `CONNECTS_TO` in about 35 seconds. The loader is **not idempotent** for
`Schedule` (see `CLAUDE.md`), so load into an empty database. `--solve-offsets`
*is* idempotent — re-running it is a fixed point.
