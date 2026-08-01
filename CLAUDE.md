# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`AGENTS.md` is the project's own lessons-learned file and holds additional detail. Read it too — but note it is partly stale (see "Known documentation drift" below). Where the two disagree, trust the code.

## Non-negotiable project rules

These come from `AGENTS.md` and reflect past incidents. They are not stylistic preferences.

1. **No synthetic data, ever.** No `np.random`, `random.choice()` for data values, generated flight/schedule IDs, fabricated timestamps, or "sample" records. Two scripts were deleted from this repo for doing exactly that. Every flight record must correspond to a real BTS-reported flight. If you think you need data, ask instead of generating it. (`random` is legitimate only for *selecting* among already-real values — as in `neo4j_flight_load_test.py` picking airports and dates read out of the database.)
2. **Never delete, overwrite, or edit `.env`.** Read it, suggest changes, but don't touch it. `NEO4J_DATABASE` is intentionally set per-environment.
3. **Never delete `.gitignore`d files during cleanup.** `private_data/`, `logs/`, `data/*.parquet`, and caches hold irreplaceable local context. Use `git status --porcelain` and `git check-ignore <file>` to decide what "cleanup" means.
4. **Conda only.** Add dependencies to `environment.yml` (use its `pip:` section for pip-only packages). Do not create `requirements*.txt`; do not `pip install` outside the env.
5. **No hard-coded connection details.** All Neo4j access goes through `load_dotenv()` + `os.getenv()`. That includes hostnames and IPs, not just passwords.

## Environment

```bash
conda env create -f environment.yml     # first time
conda activate flight-schedule          # every session
cp .env.example .env                    # then edit with real credentials
```

Python is pinned to 3.12.8 by `environment.yml`. The `python-version: [3.9, 3.10, 3.11]` matrix in `.github/workflows/ci.yml` is effectively cosmetic — the conda step immediately replaces that interpreter with 3.12.8, so all three matrix legs run the same Python.

Required `.env` keys: `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`.

## Commands

### Tests

`pytest.ini` sets `testpaths = tests` and declares the `integration`, `slow`, and `unit` markers.

CI has **two** gates: the DB-free `test` job, and a `integration-test` job that
loads a real one-day fixture into a Neo4j service container and runs the routing
assertions against it.

```bash
# Gate 1 — what the `test` job runs (no database needed); must pass before committing
pytest tests/test_ci_unit.py tests/test_flight_search_unit.py \
       tests/test_download_bts_unit.py tests/test_load_bts_unit.py \
       tests/test_system_validation_unit.py -v --cov=. --cov-report=term-missing

# Gate 2 — what the `integration-test` job runs, reproducible locally against an
# EMPTY database (the loader is not idempotent for Schedule). Takes ~35s.
python load_bts_data.py --single-file bts_flights_2025_07_18.parquet \
                        --data-path tests/fixtures
python load_bts_data.py --build-connections 2025-07-18
python tests/ci_verify_loaded.py     # guards against a false green from all-skipped
pytest tests/test_graph_validation.py tests/test_connection_logic.py \
       tests/test_integration_heavy.py -v

# Single file / class / test
pytest tests/test_load_bts_unit.py -v
pytest tests/test_load_bts_unit.py::TestSparkConfiguration -v
pytest tests/test_load_bts_unit.py::TestSparkConfiguration::test_default_spark_config_structure -v

# Deselect by marker
pytest tests/ -m "not slow" -v
```

**Tests that require a loaded Neo4j database**: `test_connection_logic.py`, `test_graph_validation.py`, `test_integration_heavy.py`, `test_performance.py`, `test_performance_baseline.py`. The first three are in the `integration-test` gate and pass against the one-day fixture; the last two are not, because they hard-code `date('2024-03-01')` and legacy property names (see "Known documentation drift"). Everything else is pure-Python and DB-free. `test_flight_search_unit.py` is mixed — one test reaches for the DB but skips cleanly if it can't connect.

The `conftest.py` fixtures **skip** rather than fail when Neo4j is unreachable, which is right for a laptop and dangerous for a gate — an all-skipped run reports success. `tests/ci_verify_loaded.py` exists to close that hole and runs before the assertions in CI. For the same reason, the `CONNECTS_TO` regression tests assert both that the defect is absent *and* that the fixture could have exposed it (893 overnight legs, 110,642 cross-family edges, asserted non-zero); swapping in a fixture that lacks either property fails loudly instead of silently gating nothing. Pinning matters too: the service container is `neo4j:2026.05.0-community` because the loader uses the variable-scope `CALL (r) { ... } IN TRANSACTIONS` form that 5.x parsers reject.

`test_load_testing_framework.py` skips most of its body unless `flight_test_scenarios.json` exists (a generated, gitignored file — see below).

### Quality checks

```bash
black .                  # then: black --check --diff .
isort .                  # then: isort --check-only --diff .
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics   # must pass
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=88 --statistics
mypy --install-types --non-interactive .   # CI: continue-on-error
bandit -r . -x tests/ -ll                  # CI: continue-on-error
pre-commit run --all-files
```

Only black, isort, and the first flake8 invocation are hard CI failures. Line length is 88. `setup.cfg` is the flake8 config that actually takes effect — the `[tool.flake8]` block in `pyproject.toml` is inert (flake8 doesn't read TOML) and duplicates it; keep them in sync or ignore the TOML copy.

### Pipeline

```bash
python download_bts_flight_data.py                        # full year 2024 → data/bts_flight_data/*.parquet
python download_bts_flight_data.py --year 2024 --month 3  # one month (~586K records, fast iteration)
python download_bts_flight_data.py --summary              # record counts per file
python download_bts_flight_data.py --validate             # cross-file dtype consistency check

python load_bts_data.py --load-all-files --data-path data/bts_flight_data
python load_bts_data.py --single-file bts_flights_2024_03.parquet   # one month, for debugging
python load_bts_data.py --no-parallel-loader                       # bypass deadlock-avoidance grouping

./setup-and-run.sh    # interactive end-to-end: env → download → load → validate (~35-50 min)
```

### Load testing

```bash
python generate_flight_scenarios.py          # writes flight_test_scenarios.json — but NOTHING reads it
locust -f neo4j_flight_load_test.py          # web UI at :8089
locust -f neo4j_flight_load_test.py --headless --users 50 --spawn-rate 5 --run-time 300s
python quick_load_test_analysis.py locust_stats.csv
```

**The load test does not measure realistic traffic, and its published numbers are unsupportable.**

- `generate_flight_scenarios.py` is **dead code**. `neo4j_flight_load_test.py` never reads `flight_test_scenarios.json` — it imports only os/random/time/typing/dotenv/locust/neo4j. The documented "70% Popular / 20% Medium / 10% Niche" distribution does not exist; the reality is two tasks, `@task(70)` direct-count and `@task(30)` routing. `setup-and-run.sh:431` still runs the generator and logs success.
- `_load_airports()` at `:67-81` sorts **lexicographically** (`ORDER BY a.code`) then slices `[:100]`, so the sampling universe is ABE…ELM — overlapping the 100 busiest airports by only 28/100 and excluding 19 of the top 30 (ORD, LAX, JFK, LGA, SFO, EWR, MIA, SEA, PHX, LAS, MCO, SLC, MSP, IAH, FLL, PHL, SAN, TPA, MDW). LGA, the origin in the README's own showcase, cannot be sampled at all. Measured: **~95% of the 70%-weighted direct search returns zero rows.**
- The `if total_routes < 5` short-circuit at `:200` fires on only ~1.5% of sampled cells, so the expensive 6-hop query runs on ~98% of the 30% task while traversing far less than a real hub set would demand. The two errors point in opposite directions and cancel unpredictably.
- `:166/:249/:291` pass a per-airport-pair string as Locust's `name`, producing up to 29,700 single-sample stats entries, so reported percentiles are computed over 1-2 samples each. `quick_load_test_analysis.py:74-83` matches substrings present in none of them and labels every row "Other".
- `_load_dates()` at `:82-101` runs an unbounded `DISTINCT` over every Schedule node, once per simulated user.
- `README.md:323`'s "Connection Pooling: enabled" is false as written: no pool parameter is set anywhere, and `:40` constructs one driver **per simulated user**.

Do not quote the existing latency figures. `AGENTS.md:271-274` attributes 73-431ms to a "991 airports" dataset that cannot exist in US-domestic BTS (measured: 334 Jan, 331 Mar, 342 Jul). `README.md:229`'s "~140ms" is attached to a UNION ALL query present in no code path. `README.md:321`'s "200+ QPS" is arithmetically unreachable at ≤100 users given `wait_time = between(1, 3)` and ~1.3 requests per iteration — the ceiling is ~65 req/s.

## Architecture

Three top-level scripts form a linear pipeline; there is no package or `src/` directory, and tests import the scripts as modules from the repo root (`sys.path.insert` at the top of each test file).

```
download_bts_flight_data.py  →  data/bts_flight_data/*.parquet
load_bts_data.py (PySpark)   →  Neo4j graph
neo4j_flight_load_test.py    →  Locust queries against the graph
```

### Graph model

```
(Schedule)-[:DEPARTS_FROM]->(Airport)
(Schedule)-[:ARRIVES_AT]->(Airport)
(Schedule)-[:OPERATED_BY]->(Carrier)
```

`Airport` and `Carrier` carry only `code`. `Schedule` holds everything else.

**`Schedule` has no surrogate ID.** Its identity is the 5-part composite key `(flightdate, reporting_airline, flight_number_reporting_airline, origin, dest)`, enforced by `schedule_composite_unique`. This is a deliberate consequence of rule 1 — inventing a `schedule_id` would be fabricating data. It has a knock-on effect throughout `load_bts_data.py`: every relationship write passes that same 5-tuple as `relationship.source.node.keys` with `save.mode = match`, so `origin` and `dest` must remain as properties on `Schedule` even though they're also reachable via the relationships.

Property names are the BTS CSV column names lowercased with spaces → underscores (`reporting_airline`, `flight_number_reporting_airline`, `origin`, `dest`, `tail_number`, …). Temporal properties are native Neo4j types, not integers or strings: `flightdate` is a `Date`; `scheduled_departure_time`, `scheduled_arrival_time`, `actual_departure_time`, `actual_arrival_time` are `DateTime`s built in `load_bts_data.py` by concatenating `flightdate` with the time-of-day from BTS's `crsdeptime`/`crsarrtime`/`deptime`/`arrtime` columns.

### Durations are wrong: the timezone defect (read before writing any routing query)

**Do not reuse the `CASE`-based duration idiom found in `ROUTING_QUERY_REFERENCE.md:27-37`, `README.md:172-181`, and `neo4j_flight_load_test.py:229-237`. It is broken.** It is a workaround for a modeling error one layer down, and it makes a large subset of cases worse.

BTS `CRSDepTime` is local time at the **origin**; `CRSArrTime` is local time at the **destination**. `load_bts_data.py:862-873` composes both onto the same `flightdate` with no timezone, and `Airport` nodes carry no timezone to correct with. Measured against BTS `CRSElapsedTime` as ground truth over all 526,882 loaded Jan-2024 flights:

- the stored arrival-minus-departure duration is correct for only **49.56%** of flights
- mean absolute error is **151 minutes** on the wrong subset
- **14,552 flights (2.76%)** store an arrival *earlier* than their departure
- the error histogram is pure timezone offset (−60 min: 88,801 flights; +60: 88,068; −120: 27,502; +120: 24,756; ±180: 21,907)

The `CASE` idiom assumes `arrival < departure` means a midnight crossing. Usually it means a westbound timezone offset. It yields the correct duration for only **50.10%** of flights. Worst verified case: **9E 4853 ATL→HSV, 2024-01-08**, CRSDep 08:25, CRSArr 08:24, true elapsed **59 minutes** — the idiom returns **1439 minutes**, and the `WHERE flight_duration > 0 AND < 1440` guard does not catch it.

`tests/test_performance_baseline.py:497-570` passes on this data, so no existing test detects any of it.

**What is still sound:** hub layover arithmetic. Both timestamps at a connecting hub are local to the same airport, so same-day connection windows are correct. What breaks is every displayed leg duration, every journey total, and all cross-midnight sequencing.

**The fix, when someone does it:** load `crselapsedtime` (already typed at `download_bts_flight_data.py:86`, already in the parquet, documented as 100% populated at `BTS_SCHEMA_ANALYSIS.md:45`, and listed among the columns that doc recommends loading at `:89`) and store arrival as departure + block. Airport UTC offsets need no external data source: `(CRSArrTime − CRSDepTime − CRSElapsedTime) mod 1440` recovers each directed pair's offset delta, and BFS over the pair graph solves all 334 airports from the parquet alone (dateline GUM/SPN need a special case). Also emit `TIMESTAMP_NTZ` rather than `TimestampType` — per the connector's type docs, Spark `TIMESTAMP` is written as a UTC `ZonedDateTime`, so the current code additionally bakes in the loader machine's UTC offset and produces different graphs on a laptop than in the container.

Multi-hop queries admit `s2.flightdate IN [date($d), date($d) + duration('P1D')]` so a connection can spill into the next day. Layover bounds are inconsistent across the repo: 720 min in `neo4j_flight_load_test.py:279`, 1200 in `ROUTING_QUERY_REFERENCE.md:105`, 300 in `README.md:314` and `tests/test_connection_logic.py:64,124` — the tests enforce a bound the shipped query does not use. All queries are plain Cypher, no APOC, deliberately for Aura compatibility.

The 1-stop query also has **no carrier predicate**, so it splices any carrier to any other: measured 67–78% of returned itineraries are cross-carrier and unsellable, including Southwest, which interlines with nobody. Adding `AND s1.reporting_airline = s2.reporting_airline` takes that to zero.

Routing is done by **iterative deepening in Python**, not a single monolithic Cypher query: try direct flights, and only issue the 1-stop query if fewer than N results came back. That is why the routing logic lives in the load-test client rather than in a `.cypher` file.

### Schema management — and two defects in it

There are no `.cypher` schema files. `setup_database_schema()` in `load_bts_data.py:291-395` is the single source of truth for all 8 indexes and 3 constraints, and it runs as a pre-flight step before every load. Add or change indexes there. The index set is deliberately pruned from `readCount` analysis — unused indexes cost write throughput during bulk loading.

**Two constraints never actually exist.** Line 316 creates an index on `(:Airport {code})` and line 326 then requests a uniqueness constraint on the *identical* label+property; same collision for `Carrier` at lines 317/327. Neo4j rejects the constraint with an index-conflict error on every run, including against an empty database — `IF NOT EXISTS` does not suppress index conflicts. The bare `except Exception` at line 357 swallows it and prints the misleading "Constraint skipped: … (duplicates exist)", then lines 377-385 print "Database ready for loading!" and `return True` unconditionally. Nothing ever runs `SHOW CONSTRAINTS`, and `verify_optimal_loading_conditions()` counts only RANGE indexes against a `>= 3` threshold that the 8 index statements satisfy with all three constraints missing. The failure is structurally undetectable. Fix: delete lines 316-317 and assert via `SHOW CONSTRAINTS` instead of returning `True`.

**The loader is not idempotent.** All three node writes use `.mode("Append")` (Carrier `:922-923`, Airport `:946-947`, Schedule `:963-964`; the three relationship writes at `:471-472`, `:524-525`, `:577-578` do too), which the pinned connector 4.1.5 maps to `CREATE`, not `MERGE` — so `node.keys` is inert and does not dedupe. Because the Airport/Carrier constraints don't exist (above), a second run **silently duplicates** those dimension nodes, then dies on the Schedule write with `ConstraintValidationFailed` after a 15-30 minute wait, leaving the duplicates behind. `setup-and-run.sh:351-359` offers a "Reload data? (y/N)" path straight into this. Fix: `.mode("Overwrite")`, which the connector maps to `MERGE` on `node.keys` — the semantics the constraints exist to support.

Consequently, **the single-file debug command below is not safe to re-run** against a populated database without clearing it first.

**Fixed, but related, and worth knowing:** when `setup_database_schema()` returned `False`, `load_bts_data()` printed "aborting load" and did a bare `return` — so the process **exited 0 having loaded nothing**. Any caller gating on the loader (the `integration-test` CI job does) saw a successful load against an empty database. It now raises `RuntimeError`. The usual trigger is bad credentials, and note that `load_dotenv()` here is called *without* `override=True`, so an exported `NEO4J_PASSWORD` from another project beats `.env` and produces exactly this failure.

### Why the loaders are defensive

BTS's monthly CSVs are not dtype-stable across months, which produced `ClassCastException` and `TIMESTAMP(NANOS)` failures in Spark. Two mitigations, in this order:

- `download_bts_flight_data.py` declares `BTS_COLUMN_TYPES` (~110 columns) and a matching PyArrow schema, and writes every month's Parquet through it so all files are byte-compatible. Timestamps are floored to microseconds because Spark can't read nanosecond Parquet timestamps. `--validate` re-checks consistency after the fact.
- `load_bts_data.py` still wraps the Parquet read in a three-tier fallback: strict read → permissive read with `mergeSchema` → per-file reads that union whatever succeeds and log the rest. If you see "individual file processing" in the output, some month's schema drifted and is worth investigating rather than ignoring.

Relationship writes go through `neo4j_parallel_spark_loader`'s `group_and_batch_spark_dataframe`, which partitions by source/target so concurrent writers don't touch the same nodes. Without it, parallel relationship creation deadlocks in Neo4j. `--no-parallel-loader` disables this for debugging only.

Both loader scripts log to `logs/{script}_{timestamp}.log` (gitignored). `load_bts_data.py` deliberately routes progress through `log_and_print()` with a `cli_mode` flag so it can be quiet when imported; keep new output going through that helper rather than bare `print`.

## Generated files — regenerate, never commit

| File | Regenerate with |
|---|---|
| `data/bts_flight_data/*.parquet` | `python download_bts_flight_data.py` |
| `flight_test_scenarios.json` | `python generate_flight_scenarios.py` (needs a loaded DB) |
| `logs/*.log` | produced on every run |

## Unused data worth knowing about

`tail_number` is loaded at `load_bts_data.py:888`, is 100% populated on all 526,882 loaded Jan-2024 rows, and is referenced by **zero queries**. `departure_delay_minutes` / `arrival_delay_minutes` and the actual-time properties are likewise loaded and unused.

Sorting by `(tail_number, flightdate, crsdeptime)` yields 400,840 consecutive same-tail pairs, of which **388,563 (96.94%)** satisfy `next.origin == this.dest` — a self-validating aircraft rotation, where the ~3% that fail are themselves the diagnostic signal. A single added relationship, `(:Schedule)-[:NEXT_LEG {ground_minutes}]->(:Schedule)`, is loadable as a fourth write in `create_relationships_fast()` reusing the existing composite key and parallel-loader grouping.

The signal is strong: P(next leg ≥15 min late | inbound ≥15 late) is **72.7% vs a 12.4% baseline**, rising to **91.5% on 0-45 minute ground time**. BTS's own `LateAircraftDelay` is 39.0% of all attributed delay minutes in Jan 2024, the single largest cause. Also note `load_bts_data.py:850` discards 20,389 Jan-2024 cancellations, including a real 2,962-cancellation storm on 2024-01-15; out-of-position aircraft raise the next leg's cancellation probability **10.7x** (17.0% vs 1.58%).

If asked to build something new on this graph, delay propagation over `NEXT_LEG` is better supported by the loaded data than the flight-search story the README tells, and it does not depend on the broken duration arithmetic.

## Known documentation drift

- **`AGENTS.md` documents an older graph schema.** It refers to `schedule_id`, `date_of_operation`, `first_seen_time`, and `last_seen_time`. None of those exist in the current graph — the current names are the composite key plus `flightdate` / `scheduled_departure_time` / `scheduled_arrival_time`. Its "Critical Indexes" and "Sample Query" sections are stale for the same reason; `load_bts_data.py` is authoritative.
- **`tests/test_performance.py` and `tests/test_performance_baseline.py` cannot pass against current data** — the former asserts `>1M schedules` and the latter hard-codes `date('2024-03-01')` and uses the broken `CASE` duration idiom. Deliberately excluded from the `integration-test` gate; treat failures there as pre-existing. (`test_integration_heavy.py` was in this list but does pass, and is now in the gate.)
- **`AGENTS.md`'s pre-commit checklist lists 8 test files; CI runs 5.** The three extras (`test_data_transformations.py`, `test_business_rules.py`, `test_error_scenarios.py`) are DB-free and fast, so running them too is harmless — but `.github/workflows/ci.yml` defines the actual gate.
- **`REAL_DATA_SETUP.md` references `setup_real_data.py` and `pip install -r requirements.txt`.** Neither exists, and the latter violates rule 4. Use `setup-and-run.sh` and `environment.yml`.
- **A credential was committed and has been removed from this repo's history, but is still exposed on GitHub.** Fixed in `fbfac4a`: `.env.backup` is gone, `.env.example` now holds `changeme`, and `AGENTS.md` no longer quotes the value. The three commits that contained it (81edc69, a28b421, aaa4fc8) survive locally as unreachable objects but are **not** ancestors of `HEAD`, so pushing does not transmit them. **However** — this repo is public, and GitHub serves unreachable objects by SHA indefinitely, so the value remains fetchable from the hosted copy via those commit URLs. A history rewrite does not close that; only rotating the credential (or asking GitHub Support to purge the objects) does. The owner has been told and declined rotation, so treat the password as **disclosed**: never reuse it, and don't copy it or the RFC-1918 host into code.
- **`README.md:234-245`'s showcase results block does not come from the query above it.** `DL308 → UA1071 via ATL` (`:244`) is impossible in the data: DL308's 55 March-2024 legs are only DFW↔LGA, UA1071's 31 legs are only ORD→DFW, and UA operated zero ATL→DFW legs that month. Every quoted departure time is also shifted exactly +7h from real BTS (`:238` says DL308 departs 13:00; the real CRSDep is 06:00), consistent with a run on an MST machine — which is the `TimestampType`/UTC issue described above. The "27 routes" count at `:236` is also unsupported; the query as written returns 17 nonstops + 1,092 one-stops before `LIMIT`. Do not cite this block or reuse its numbers.
- **Several documented features match no code**: "efficiency scoring", "16-hour max journeys", "2-stop connections", "single unified query", "Pre-generated Scenarios" (`README.md:313-316,345-349`). `LOAD_TESTING_GUIDE.md:76-81`'s distribution table sums to 130%, and its `s.cancelled = 0` example (`:142`) always returns zero rows because `cancelled` is filtered at load but never persisted.
- **Diverted flights are loaded unfiltered.** `load_bts_data.py:850` filters `cancelled == 0` only; 1,512 diverted Jan-2024 flights load, and the 244 with `DivReachedDest=0` keep an `ARRIVES_AT` edge to an airport the aircraft never reached (e.g. AA491 DCA→MIA actually landed at CLT).
