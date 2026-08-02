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
# Gate 1 — what the `test` job runs (no database needed); must pass before committing.
# Files are listed explicitly, NOT as `pytest tests/`: conftest.py skips the
# DB-requiring files rather than failing, so a directory run would report green
# having asserted nothing.
pytest tests/test_ci_unit.py tests/test_flight_search_unit.py \
       tests/test_download_bts_unit.py tests/test_load_bts_unit.py \
       tests/test_system_validation_unit.py \
       tests/test_flight_search_service_unit.py \
       tests/test_business_rules.py tests/test_data_quality_checks.py \
       tests/test_data_transformations.py tests/test_environment_scenarios.py \
       tests/test_error_scenarios.py tests/test_performance_boundaries.py \
       tests/test_pipeline_integration.py \
       -v --cov=. --cov-report=term-missing

# Gate 1b — a SEPARATE process, and it must stay separate. Importing this file
# imports locust, which gevent-patches `threading` for the rest of the
# interpreter; FastAPI's TestClient drives the app through an anyio blocking-portal
# thread, and the two deadlock. Folded into the run above it HANGS rather than
# fails, so CI burns its timeout instead of reporting red. Gated by
# TestGeventIsolation.
pytest tests/test_load_testing_framework.py -v

# Gate 2 — what the `integration-test` job runs, reproducible locally against an
# EMPTY database (the loader is not idempotent for Schedule). Takes ~35s.
#
# CI sets TZ=America/Denver on this job deliberately — runners default to UTC,
# the one zone where a timezone bug in the loader is invisible. Prefix these with
# `TZ=America/Denver` to reproduce the gate exactly; the output must be identical
# either way, and that identity is the point.
python load_bts_data.py --single-file bts_flights_2025_07_18.parquet \
                        --data-path tests/fixtures
python load_bts_data.py --solve-offsets 2025-07-18      # must precede the next line
python load_bts_data.py --build-connections 2025-07-18
python tests/ci_verify_loaded.py 2025-07-18   # guards against a false green from all-skipped
pytest tests/test_graph_validation.py tests/test_connection_logic.py \
       tests/test_integration_heavy.py tests/test_documented_queries.py \
       tests/test_timezone_offsets.py \
       tests/test_flight_search_integration.py \
       tests/test_query_plan.py -v

# Single file / class / test
pytest tests/test_load_bts_unit.py -v
pytest tests/test_load_bts_unit.py::TestSparkConfiguration -v
pytest tests/test_load_bts_unit.py::TestSparkConfiguration::test_default_spark_config_structure -v

# Deselect by marker
pytest tests/ -m "not slow" -v
```

**Tests that require a loaded Neo4j database**: `test_connection_logic.py`, `test_graph_validation.py`, `test_integration_heavy.py`, `test_documented_queries.py`, `test_timezone_offsets.py`, `test_flight_search_integration.py`, `test_query_plan.py`. **All seven are in the `integration-test` gate** and pass against the one-day fixture. Everything else is pure-Python and DB-free and is in the `test` gate. `test_flight_search_unit.py` is mixed — one test reaches for the DB but skips cleanly if it can't connect. `test_query_plan.py` needs only a *reachable* database, not a loaded one: `EXPLAIN` compiles without executing.

No test file is ungated any more. The two that used to be — `test_performance.py` and `test_performance_baseline.py` — were **deleted**, not fixed: see "Deleted test files" below. Being *in* a gate was not sufficient either: `test_integration_heavy.py` was gated while 5 of its 6 tests matched zero rows behind `assert count >= 0`. Those are gone too, and the file now carries a guard against that pattern returning.

**The service layer is tested from both sides on purpose.** `test_flight_search_service_unit.py` (DB-free) asserts the *rendered Cypher text* and the HTTP status mapping; `test_flight_search_integration.py` asserts *results* against real flights. The split exists because the failures that matter here are silent — a dropped acyclicity guard or `localdatetime`→`datetime` slip returns plausible wrong answers rather than an error, so one side checks the query is written correctly and the other that it answers correctly. Mutation-verified: dropping the guard, swapping `localdatetime` for `datetime`, and computing `total_minutes` from local timestamps each fail 3, 8 and 5 tests respectively.

**A serving-sized `LIMIT` cannot see the acyclicity defect**, which is why `TestItineraryValidity`'s `deep_sample` fixture uses `limit=3000`. Measured with the guard removed on LGA→DFW `{0,3}`: 0 revisits in the top 1,000 results, first at rank 1,038, 531 in the top 5,000 — revisits are long detours, so ranking by total journey buries them. A `limit=50` test would have passed against the broken code.

The `conftest.py` fixtures **skip** rather than fail when Neo4j is unreachable, which is right for a laptop and dangerous for a gate — an all-skipped run reports success. `tests/ci_verify_loaded.py` exists to close that hole and runs before the assertions in CI. For the same reason, the `CONNECTS_TO` and timezone regression tests assert both that the defect is absent *and* that the fixture could have exposed it (915 legs landing a local day later, 110,642 cross-family edges, 2 dateline airports, all asserted non-zero); swapping in a fixture that lacks any of those properties fails loudly instead of silently gating nothing. Pinning matters too: the service container is `neo4j:2026.05.0-community` because the loader uses the variable-scope `CALL (r) { ... } IN TRANSACTIONS` form that 5.x parsers reject.

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
locust -f neo4j_flight_load_test.py          # web UI at :8089
locust -f neo4j_flight_load_test.py --headless --users 50 --spawn-rate 5 --run-time 300s
python quick_load_test_analysis.py locust_stats.csv
```

**Rewritten. It now drives `flight_search.search_itineraries()` and holds no Cypher of its own** (one airport-volume sampling query aside), so it cannot drift from what the service runs. Two tasks: `@task(70)` `nonstop lookup`, `@task(30)` `itinerary search {0,2}`. Both stat names are module constants — `NONSTOP_TASK` / `SEARCH_TASK` — and `tests/test_load_testing_framework.py::test_load_test_holds_no_cypher_of_its_own` fails if itinerary Cypher reappears.

Six defects fixed, each of which had made the numbers meaningless:

- Sampling is by **flight volume** (60 busiest origins on a searchable date). The old `ORDER BY a.code` + `[:100]` gave ABE…ELM, excluding 19 of the top 30 airports, so **~95% of the weighted task returned zero rows**.
- Dates come from `searchable_dates()` (`CONNECTS_TO` coverage), not an unbounded `DISTINCT` over every Schedule node run once per simulated user.
- **One process-wide pooled driver** via `flight_search.get_driver()`, not one per simulated user, so driver and TLS setup are outside the measurement.
- **Constant Locust stat names**, so percentiles aggregate; per-pair names produced up to 29,700 single-sample rows.
- The broken `CASE` duration idiom is **gone** (it was never what the service ran).
- Class-level setup is **double-checked under a lock**. Locust runs each user in its own greenlet, and the unlocked version ran the setup queries 5 times on a 10-user spawn because each greenlet yielded inside `session.run()` before any had assigned the result.

`quick_load_test_analysis.py` prints Locust's own `Name` column and its p95/p99 rather than matching substrings ("direct_flight", "multi_hop", "analytics") that appear in no name this repo emits — which is why it used to label every row "Other". It also no longer grades throughput: RPS is bounded by `--users` and the 1-3s think time, not by the graph, so grading it blamed Neo4j for the load generator's configuration. It now exits non-zero on a bad CSV instead of printing an error and returning success.

Still not a realistic traffic model: route selection is uniform over hub pairs, while real demand is heavily concentrated.

Do not quote the pre-rewrite latency figures. `AGENTS.md`'s "Performance Results" section used to attribute 73-431ms to a "991 airports" dataset that cannot exist in US-domestic BTS (measured: 334 Jan, 331 Mar, 342 Jul, 352 across the full 2025 load) alongside "4.8M+ schedules" and "14.4M+ relationships" against a measured 6,898,743 and 24,731,734. Those numbers are **deleted** and replaced with a measured table plus an explicit "do not reinstate" — so if you see them quoted anywhere, that citation is stale. The README's unsupportable claims ("200+ QPS", "~140ms", "Connection Pooling: enabled", the fabricated `DL308 → UA1071 via ATL` showcase block) were **removed in `f20f20c`** — if you see them cited as present anywhere, that citation is stale.

## Architecture

Three top-level scripts form a linear pipeline; there is no package or `src/` directory, and tests import the scripts as modules from the repo root (`sys.path.insert` at the top of each test file).

```
download_bts_flight_data.py  →  data/bts_flight_data/*.parquet
load_bts_data.py (PySpark)   →  Neo4j graph
flight_search.py             →  the one itinerary query (no Cypher lives elsewhere)
api.py (FastAPI)             →  HTTP over flight_search
neo4j_flight_load_test.py    →  Locust, driving flight_search
```

`flight_search.py` is the single place the routing Cypher exists. `api.py` adds only parameter parsing, error mapping (`SearchError`→400, driver/Neo4j failure→503) and a pooled driver opened in `lifespan`. Run it with `uvicorn api:app`; endpoints are `/itineraries`, `/dates`, `/health` (503 unless the graph has routing edges — a static `{"ok": true}` would hide an empty database).

### Graph model

```
(Schedule)-[:DEPARTS_FROM]->(Airport)
(Schedule)-[:ARRIVES_AT]->(Airport)
(Schedule)-[:OPERATED_BY]->(Carrier)
```

`Airport` and `Carrier` carry only `code`. `Schedule` holds everything else.

**`Schedule` has no surrogate ID.** Its identity is the 5-part composite key `(flightdate, reporting_airline, flight_number_reporting_airline, origin, dest)`, enforced by `schedule_composite_unique`. This is a deliberate consequence of rule 1 — inventing a `schedule_id` would be fabricating data. It has a knock-on effect throughout `load_bts_data.py`: every relationship write passes that same 5-tuple as `relationship.source.node.keys` with `save.mode = match`, so `origin` and `dest` must remain as properties on `Schedule` even though they're also reachable via the relationships.

Property names are the BTS CSV column names lowercased with spaces → underscores (`reporting_airline`, `flight_number_reporting_airline`, `origin`, `dest`, `tail_number`, …). Temporal properties are native Neo4j types, not integers or strings: `flightdate` is a `Date`; `scheduled_departure_time`, `scheduled_arrival_time`, `actual_departure_time`, `actual_arrival_time` are `DateTime`s built in `load_bts_data.py` by concatenating `flightdate` with the time-of-day from BTS's `crsdeptime`/`crsarrtime`/`deptime`/`arrtime` columns.

### Time zones: fixed, and how (read before writing any routing query)

**Every `Schedule` carries four timestamps. Two frames — never mix them.**

| property | type | frame |
|---|---|---|
| `scheduled_departure_time` | `LOCAL DATETIME` | local wall clock at the **origin** |
| `scheduled_arrival_time` | `LOCAL DATETIME` | local wall clock at the **destination** |
| `scheduled_departure_utc` / `scheduled_arrival_utc` | `LOCAL DATETIME` holding UTC | absolute instants |
| `scheduled_duration_minutes` | int | BTS `CRSElapsedTime`, timezone-independent |

Rules: **durations, sequencing, and cross-airport ordering use the UTC pair or `scheduled_duration_minutes`. "Arrives before 3pm where I land" uses `scheduled_arrival_time`.** Never subtract one local timestamp from the other — they are in different zones.

**Do not reintroduce the `CASE`-based duration idiom.** It no longer exists anywhere in the repo — deleted from `neo4j_flight_load_test.py` in the load-test rewrite, and its last home, `tests/test_performance_baseline.py`, has been deleted. It assumes `arrival < departure` means a midnight crossing; usually it means a westbound timezone offset. Correct for only ~50% of flights. Worst verified case: **9E 4853 ATL→HSV, 2024-01-08**, CRSDep 08:25, CRSArr 08:24, true elapsed **59 minutes** — the idiom returns **1439 minutes**, and a `> 0 AND < 1440` guard does not catch it.

**How the offsets are recovered — no timezone database, no external source.** For any flight, `(arrival_local − departure_local) − block` is exactly `offset(dest) − offset(origin)`. Each directed airport pair therefore reveals the *difference* between its endpoints' offsets; BFS over the pair graph propagates relative offsets, and one absolute anchor places them all on UTC. `solve_airport_offsets()` does this per date. `OFFSET_ANCHOR = ("PHX", -420)` — Phoenix because Arizona doesn't observe DST, so the constant needs no seasonal branch.

Measured 2025-07-18 (`tests/fixtures`): **341 of 341 airports** solved, **1** connected component, **0** conflicts, every offset a whole hour, all 22 real-world spot checks exact (JFK −240 … GUM +600). The solve **raises** on a conflict, a disconnected graph, a missing anchor, or a fractional offset — with real BTS data none occur, so any of them means a data problem.

Offsets are **DST-dependent and cannot be a static `Airport` property**: 18 of 317 airports differ between January and July, and mainland airports shift wholesale (ORD −6→−5). They are passed to Cypher as a parameter map instead.

**Results, 2025-07-18, 21,376 flights:**

| | before | after |
|---|---|---|
| `arrival − departure` matches BTS block time | 10,453 (48.9%) | **21,376 (100.00%)** |
| flights arriving before they depart (UTC) | 940 | **0** |
| local arrival round-trips from UTC + `offset(dest)` | — | **21,376 (100.00%)** |

**`--solve-offsets DATE...` must run after the Schedule load and *before* `--build-connections`**, which computes layovers from the UTC properties. Skipping it makes the connection build raise rather than silently produce nothing. It is idempotent; the Schedule load is not.

**It also repairs `scheduled_arrival_time`'s date.** The loader composes both timestamps onto the origin's `flightdate`, so a leg crossing local midnight was stamped a day early (915 of 21,376). The time-of-day was always correct — only the date was wrong. Rewriting it as `arrival_utc + offset(dest)` is what makes a deadline filter correct **with no guard at all**.

Two guards that were tried here and are both wrong, recorded so nobody reintroduces them: a **±180-minute block-time tolerance** (what this repo shipped) cannot span the widest offset gaps — HNL→DFW needs 240–360 min, which left 11,975 impossible `CONNECTS_TO` edges; and **`date(arrival_utc) = date(departure_utc)`** tests *UTC* midnight, wrongly excluding 3,135 ordinary evening flights while admitting 876 real red-eyes. The destination's offset is not recoverable inside a query, so only the loader can fix this.

Legitimately, a leg may arrive on an **earlier** local date than it departed: `UA200 GUM→HNL` departs 07:15 on 07-18 and lands 18:25 on **07-17** across the dateline; 20 more land at an earlier local time the same day (`DL2250 ATL→BHM`, 09:40→09:32, 52 minutes westbound one hour). Both are correct — never "repair" them.

Gated by `tests/test_timezone_offsets.py` (20 tests) and `TestDeadlineFilters` in `tests/test_graph_validation.py`. The falsifiable assertion is the **round-trip**, not `arrival_utc − departure_utc == block` — the latter is tautological, since `arrival_utc` is *defined* as `departure_utc + block`.

**The `TimestampType`/`TIMESTAMP_NTZ` concern is closed, and this entry replaces the "still not fixed" claim that used to sit here — that claim was wrong.** The Schedule write has always used `to_timestamp_ntz` for all four timestamps (`load_bts_data.py:1466-1489`), and the loader's output is byte-identical across timezones. Measured on `tests/fixtures/bts_flights_2025_07_18.parquet` by SHA-256 over all 21,376 rows × 4 timestamps after the real transform: `TZ=UTC`, `America/Denver`, `Asia/Tokyo`, `Pacific/Auckland` and `Asia/Kolkata` all produce `44ee57ce…`.

What *was* genuinely wrong is that this correctness was unpinned. Neither governing key was set anywhere in the repo, so the graph depended on a Spark 3.5 default nobody had chosen. Both are now explicit in `create_spark_session()`:

| `inferTimestampNTZ` | `session.timeZone` | result (`TZ=America/Denver`) |
|---|---|---|
| `true` | `UTC` | correct — `44ee57ce…` |
| `false` | `UTC` | correct — `44ee57ce…` |
| `true` | unset | correct — `44ee57ce…` |
| **`false`** | **unset** | **corrupt — `f2d10acd…`** |

**They are each independently sufficient**, which is the non-obvious part and the reason the gating is split. Losing one is undetectable end-to-end, so `test_timezone_semantics_are_pinned_not_inherited` asserts on the config dict directly (mutation-checked: flipping either value, or deleting either key, fails it). Losing both bakes the loader machine's offset in: `DL31` ATL moves `2025-07-18 20:30` → `2025-07-17 13:30`, and `flightdate` moves for **all 21,376 rows** — which matters far more than the display shift, since `flightdate` is one of the five composite-key fields, so the whole day relocates and `--solve-offsets 2025-07-18` then finds nothing. That +7h is the same signature as the fabricated README block removed in `f20f20c`.

The `integration-test` job therefore runs with **`TZ: America/Denver`**, not UTC. GitHub runners default to UTC, which is precisely the zone where correct and offset-baked code paths agree — a gate that only ever runs in UTC cannot see a timezone bug. `tests/ci_verify_loaded.py` already fails if `flightdate` lands off `FIXTURE_DATE`, so it becomes the backstop for the both-keys-gone case. Note also that the `spark.sql.*` keys passed via `.option()` on the fallback reader (`load_bts_data.py:1377-1393`) are **inert** — reader options are not session config — which is the argument for pinning at the session: the fallback inherits it.

**Itineraries revisit airports unless the query forbids it.** `CONNECTS_TO`'s backtrack guard is *pairwise* (`s2.dest <> s1.origin`) and does not compose over 3+ legs. Measured 2025-07-18 LGA→DFW at `{0,3}`: **2,115 of 11,488 paths (18.41%)** revisit an airport and **385 return to the origin** (`LGA→MIA→CLT→LGA`). Cypher's `ACYCLIC`/`TRAIL` path modes **do not fix this** — they dedupe path *nodes*, which here are `Schedule` nodes and always distinct; the repeating entity is an `Airport` reached off-path via `DEPARTS_FROM`/`ARRIVES_AT` (verified: identical 2,115 under all three modes). The guard must compare airport codes; it is in every routing query in the docs and gated by `TestItineraryShape`. Cost is within run-to-run noise (±5% over six routes).

**Latency: only *filtered* `{0,2}` clears a 200 ms budget unconditionally. Two things dominate the number — the departure-time filter and the route mix of the sample — so a figure quoted without both conditions is not reproducible.** 40 pairs, top-20 sorted, guard on, repeat-warm, limit=20; ranges are the spread over three runs, and the two samples differ only in how hub-heavy they are:

| depth | filter | p50 | p95 | over 200 ms |
|---|---|---|---|---|
| `{0,2}` | `08:00`, top 60 origins | 23–28 ms | 54–66 ms | **0 / 40** |
| `{0,2}` | `08:00`, top 20 origins | 41–44 ms | 61–67 ms | **0 / 40** |
| `{0,2}` | whole day, top 60 | 45–48 ms | 171–220 ms | 0–2 / 40 |
| `{0,2}` | whole day, top 20 | 118–125 ms | 194–253 ms | 1–3 / 40 |
| `{0,3}` | `08:00`, top 60 | 70–81 ms | 212–223 ms | 2–3 / 40 |
| `{0,3}` | `08:00`, top 20 | 140–154 ms | 232–243 ms | 12–14 / 40 |
| `{0,3}` | whole day, top 60 | 251–280 ms | 573–672 ms | 25–27 / 40 |
| `{0,3}` | whole day, top 20 | 492–506 ms | 635–657 ms | **39 / 40** |

Serve `{0,2}`, and prefer a departure-time filter. Two corrections to figures this file used to carry, both of which flattered the system:

- **Whole-day `{0,2}` was published as "p50 85 ms / p95 175 ms / 0 of 40 over".** It is not reliably under budget — p95 landed at 171, 192, 205 and 229 ms across four runs, with 0, 1, 2 and 3 pairs over. The claim was one sample's luck reported as a property. It sits *on* the budget; say so rather than quoting a zero.
- **Every figure is sample-dependent**, which no earlier version of this table admitted. Concentrating the same 40 pairs on the top 20 origins instead of the top 60 roughly triples whole-day `{0,2}` p50 (48→121 ms) and takes filtered `{0,3}` from 2 of 40 over budget to 13 — same query, same graph, same date. Denser hubs enumerate more paths. Quote the sample or the number means nothing.

The older "p50 36 ms / p95 56 ms" and "p95 218 ms" figures were the *filtered* case published without that condition; they understate `{0,3}` by ~2.7x.

**One silent trap remains in any deadline filter**, gated by `TestDeadlineFilters` in `tests/test_graph_validation.py`: `scheduled_arrival_time` is a `LOCAL DATETIME`, and comparing it to `datetime('...')` (zoned) yields **NULL**, not false, so `WHERE` drops every row and a route with 40 valid itineraries returns zero with no error. Use `localdatetime()`. The overnight trap that used to sit alongside it is fixed at load time (above).

Multi-hop queries admit `s2.flightdate IN [date($d), date($d) + duration('P1D')]` so a connection can spill into the next day. Layover bounds now have **one** authority: the `CONNECTS_TO` edge, built at [45, 300] minutes in `create_connects_to()`. Since `flight_search.py` traverses that edge and the load test calls `flight_search.py`, no client restates the bound — the old 720-vs-1200-vs-300 spread across the load test, `ROUTING_QUERY_REFERENCE.md` and the tests is gone on the query side. Doc examples that hand-roll the join still pass `$min_layover`/`$max_layover` explicitly. All queries are plain Cypher, no APOC, deliberately for Aura compatibility.

**The carrier predicate is no longer missing.** It used to be absent from the 1-stop query, splicing any carrier to any other — 67–78% of returned itineraries were cross-carrier and unsellable, including Southwest, which interlines with nobody. It is now enforced in two places: `create_connects_to()` compares `CARRIER_FAMILY`-mapped codes at build time, so no `CONNECTS_TO` edge crosses carriers, and the hand-rolled 1-stop example in `README.md` carries `AND s1.reporting_airline = s2.reporting_airline` explicitly. Don't cite this as an open defect.

**Three scope limits to state rather than discover.** They are not fixable by better queries and they belong in any demo:

- **`CONNECTS_TO` covers 7 dates of 365** — `2025-07-14 … 2025-07-20`, 4,028,572 edges. A full year of `Schedule` nodes is loaded. `GET /dates` reports the real coverage. A full year would be ~211M edges.

  **A connecting search on an unbuilt date now raises `CoverageError` (HTTP 409); it does not return zero rows.** The claim that used to sit here — "routing on any other date returns zero rows, correctly and silently" — was wrong on all three counts, and the truth was worse than the claim. The nonstop leg of the query (`MATCH p = (first)`) reads `Schedule` directly and never traverses `CONNECTS_TO`, so a `{0,2}` search on a date with flights but no edges returned **nonstops only**: well-formed, plausible, and silently incomplete. Measured on the dev graph, LGA→DFW on `2025-03-14` returned **18 itineraries, all 0-stop**, against 22 nonstops among 500 results on `2025-07-18` — and **358 of the 365 loaded dates** behaved that way. `search_itineraries()` now probes coverage when `max_stops > 0` and refuses; `max_stops=0` is unaffected, because a nonstop search is exactly as correct without edges as with them. Gated by `TestUnbuiltDateCoverage` (integration) and six service-unit tests.

  Two details worth keeping. `CoverageError` **subclasses `SearchError`**, so `api.py`'s `except CoverageError` must stay *above* `except SearchError` or it is silently swallowed and reported as 400 — mutation-checked, reordering fails 3 tests. And the probe is `RETURN true AS built ... LIMIT 1`, **not** `count(*) > 0`: an aggregation cannot short-circuit, so the count form drained all 623,508 edges of the date before there was anything to compare, costing 33.6 ms against **0.39 ms** (86×). At the count form's cost the per-search probe would have roughly doubled `{0,2}` p50; as written it is inside run-to-run noise (p50 35 ms with the guard, 34 ms before).
- **No price, seat availability, booking class, or per-airport minimum connection time.** A flat [45, 300] stands in for MCT. The system answers "is this flyable as scheduled", not "is this purchasable".
- **8.64% of edges (348,000) are `OO`→`OO` (283,368) or `YX`→`YX` (64,632)** and their marketing carrier is **not derivable from BTS at all** — On-Time Performance publishes only the operating carrier. SkyWest and Republic each fly for several mainlines, so an `OO` leg sold as Delta Connection connecting to an `OO` leg sold as United Express passes the carrier check and is still unsellable. That is why both are excluded from `CARRIER_FAMILY` — strict is the honest answer, not a complete one. Closing it needs a source with marketing carriers (OAG, ATPCO, a GDS), which is out of scope.

Routing lives in **`flight_search.py`**, as a **single quantified-path query** spanning 0..`max_stops` and ranked globally by total journey. `api.py` (FastAPI) puts HTTP in front of it and `neo4j_flight_load_test.py` drives it under load; neither holds a copy of the Cypher.

**Iterative deepening was tried and rejected on measurement**, so don't reintroduce it: direct-first-then-deepen is *slower at the tail* (p95 1,323 ms vs 63 ms with `depart_after=08:00`; 249 vs 204 ms unfiltered), because a route needing depth pays for the shallow queries *and* the deep one. It also forfeits global ranking — a 1-stop beating every nonstop is unreachable once nonstops fill `limit`. Gated by `test_one_query_is_issued_not_one_per_depth`.

The one path-level property `CONNECTS_TO` cannot express is "no airport twice", so that guard lives in the query (`_ACYCLIC_GUARD`) and must not be dropped.

### Schema management

There are no `.cypher` schema files. `setup_database_schema()` at `load_bts_data.py:349` is the single source of truth for all 6 indexes and 3 constraints, and it runs as a pre-flight step before every load. Add or change indexes there. The index set is deliberately pruned from `readCount` analysis — unused indexes cost write throughput during bulk loading.

**Do not add a plain index on `(:Airport {code})` or `(:Carrier {code})`.** The uniqueness constraints create their own backing indexes, and Neo4j rejects a constraint with `IndexAlreadyExists` when a plain index on the identical label+property exists — `IF NOT EXISTS` does **not** suppress that. This repo shipped exactly that collision for a long time: a bare `except Exception` swallowed the error and printed "Constraint skipped: … (duplicates exist)", then returned `True`, so **all three constraints were silently absent** and nothing detected it. Now a failed index or constraint returns `False` and aborts, and the function asserts against `SHOW CONSTRAINTS` rather than trusting that creation didn't raise.

**Node and relationship writes use `.mode("Overwrite")`**, which the connector maps to `MERGE` on `node.keys` — the semantics the constraints exist to support. They previously used `.mode("Append")`, which maps to `CREATE` and makes `node.keys` inert; combined with the missing constraints, a second run silently duplicated Airport/Carrier nodes and then died on the Schedule write with `ConstraintValidationFailed` after a 15-30 minute wait.

**The loader is still not idempotent for `Schedule`** in practice — load into an empty database. `--solve-offsets` and `--build-connections` *are* idempotent and safe to re-run.

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
| `logs/*.log` | produced on every run |

## Unused data worth knowing about

`tail_number` is loaded at `load_bts_data.py:1501`, is 100% populated on all 526,882 loaded Jan-2024 rows, and is referenced by **zero queries**. `departure_delay_minutes` / `arrival_delay_minutes` and the actual-time properties are likewise loaded and unused.

Sorting by `(tail_number, flightdate, crsdeptime)` yields 400,840 consecutive same-tail pairs, of which **388,563 (96.94%)** satisfy `next.origin == this.dest` — a self-validating aircraft rotation, where the ~3% that fail are themselves the diagnostic signal. A single added relationship, `(:Schedule)-[:NEXT_LEG {ground_minutes}]->(:Schedule)`, is loadable as a fourth write in `create_relationships_fast()` reusing the existing composite key and parallel-loader grouping.

The signal is strong: P(next leg ≥15 min late | inbound ≥15 late) is **72.7% vs a 12.4% baseline**, rising to **91.5% on 0-45 minute ground time**. BTS's own `LateAircraftDelay` is 39.0% of all attributed delay minutes in Jan 2024, the single largest cause. Also note the `cancelled == 0` filter at `load_bts_data.py:1441` discards 20,389 Jan-2024 cancellations, including a real 2,962-cancellation storm on 2024-01-15; out-of-position aircraft raise the next leg's cancellation probability **10.7x** (17.0% vs 1.58%).

If asked to build something new on this graph, delay propagation over `NEXT_LEG` is better supported by the loaded data than the flight-search story the README tells, and it does not depend on the broken duration arithmetic.

## Known documentation drift

- **`AGENTS.md`'s stale schema is fixed.** It used to document `schedule_id`, `date_of_operation`, `first_seen_time` and `last_seen_time`, none of which this graph has ever had, along with a "Critical Indexes" Cypher block naming `schedule_id_unique` and a "Sample Query" that could not return a row. All four sections now carry the composite key, the two-frame temporal table, a pointer to `setup_database_schema()` instead of a copyable index block, and a `flight_search.search_itineraries()` example. `load_bts_data.py` remains authoritative — if the two ever disagree again, trust the code.
- **Five vacuous tests removed from `tests/test_integration_heavy.py`**, which is in the `integration-test` gate, so this was a gate reporting green on nothing. `test_popular_routes`, `test_time_filtering`, `test_airport_coverage`, `test_traveler_scenarios` and `test_query_performance` all queried 2024 dates or European ICAO codes against a 2025 US-domestic graph and returned **0 rows each** (measured: JFK→LAX on `2024-03-01` → 0; June-2024 flights → 0; **0 of 8** of `EGLL`/`LFPG`/`EHAM`/`EDDF`/`EGPH`/`LFMN`/`EIDW`/`EGCC` exist as `Airport` nodes; the `date_of_operation` queries also raise `UnknownPropertyKeyWarning`). Every assertion was `>= 0`, which cannot fail for a count — they passed *because* they matched nothing. `test_connection_timing` was already repaired and stays, now with an anti-vacuity companion asserting ATL/DFW/ORD really are loaded (ORD 1,035, ATL 980, DFW 965 on the fixture day) and a DB-free guard that fails if any `assert … >= 0` returns to the file. Both mutation-checked. Their equivalent coverage lives in `test_graph_validation.py`, `test_flight_search_integration.py` and `test_query_plan.py`.
- **Deleted test files — `tests/test_performance.py` and `tests/test_performance_baseline.py` are gone.** Both were ungated, and neither was worth fixing. `test_performance.py` was **vacuous, not merely stale**: measured against the dev graph, 6,898,743 `Schedule` nodes are loaded and **0** carry the `date_of_operation` / `first_seen_time` properties it filtered on, and **0** `Airport` nodes match the European ICAO codes (`EGPH`, `LFMN`, `EGLL`, `LFPG`, `EDDF`, `EHAM`, `LIRF`) it queried in US-domestic BTS data. Every result assertion was `count >= 0`, so it passed on empty results by construction; its only live assertion was a wall-clock bound on a query matching nothing. `test_performance_baseline.py` had the same `assert count >= 0` pattern, hard-coded `date('2024-03-01')`, carried the broken `CASE` duration idiom, and compared a `LOCAL DATETIME` against `datetime()` — the NULL trap — so those rows were empty too. **Do not restore either from history.**

  `tests/test_query_plan.py` replaces the part that was worth gating. It asserts on `EXPLAIN` output rather than elapsed time, because the plan is deterministic and latency on a shared runner is not: an index seek versus a `NodeByLabelScan` is visible at any graph size, whereas a millisecond threshold on a one-day fixture cannot tell them apart. It also gates `Top` over `Sort` — `ORDER BY total_minutes LIMIT $limit` must keep a bounded heap, not buffer all 11,488 LGA→DFW paths. Scope worth knowing: the assertion is "an indexed entry point exists", not "this particular index is used" — dropping the `flightdate` seek still planned a `NodeUniqueIndexSeek` off `airport_code_unique`. Verified to fail on a query that genuinely cannot seek (`Schedule` filtered on the unindexed `tail_number` → `NodeByLabelScan`), and mutation-checked: removing `LIMIT $limit` turns `Top` into `Sort` and fails.
- **`AGENTS.md`'s pre-commit checklist now matches CI — verified programmatically, and the earlier claim here that it already did was wrong.** This entry used to say the divergence was closed because the three files the checklist named beyond CI's list had been added to the gate. That fixed one direction only: the checklist still listed **8 files against the gate's 14**, silently omitting `test_data_quality_checks.py`, `test_environment_scenarios.py`, `test_flight_search_service_unit.py`, `test_load_testing_framework.py`, `test_performance_boundaries.py` and `test_pipeline_integration.py` — so following it could pass locally and fail in CI. Both lists are now diffed by parsing the workflow YAML rather than eyeballed, and the checklist also carries the separate-process invocation for the gevent reason. `.github/workflows/ci.yml` remains the actual gate; if you need to know whether the two agree, parse it rather than reading either doc.
- **`QUICK_START.md` and `REAL_DATA_SETUP.md` are gone**, folded into `README.md`. Everything they held was already duplicated there — setup, the download flags, the dataset counts, the dtype-normalisation rationale — so they were a second place for the same facts to drift. The one thing only `QUICK_START.md` had, a connection-troubleshooting snippet, now sits under "Configure Connection" in the README. Both had already been corrected before deletion, so the note that used to live here about `REAL_DATA_SETUP.md` citing a nonexistent `setup_real_data.py` and `pip install -r requirements.txt` was itself stale.
- **Never commit a `.env` variant, and treat any secret that reaches a public commit as unrecoverable.** This is a hard rule, not a preference. Removing a secret from reachable history does **not** un-publish it: hosting providers keep unreachable objects addressable by commit SHA indefinitely, so a client-side rewrite closes nothing. The only real remedy is rotating the value at its source, which is why the cheap moment to stop this is before staging.

  Two guards enforce it, and both are load-bearing because they fail differently. `.gitignore` covers `.env.*` with a `!.env.example` negation — but `git add -f` overrides `.gitignore` entirely, which is the usual path a secret takes into a commit. The `no-env-files` `pre-commit` hook closes that: it rejects any staged `.env*` except `.env.example` and does not care about `-f`. Keep both; neither is redundant.

  When adding a credential-bearing key, it goes in `.env` locally and as a placeholder in `.env.example`. Never in code, never in docs, never in a comment or a commit message, and never in test fixtures — and that includes RFC-1918 hosts and internal URIs, not just passwords (see rule 5).
- **The README's fabricated showcase block is gone** (removed in `f20f20c`; verified absent — `DL308`, `UA1071` and "27 routes" appear nowhere in `README.md`). Recorded because the *reason* still matters: that block did not come from the query printed above it. `DL308 → UA1071 via ATL` was impossible in the data — DL308's 55 March-2024 legs were only DFW↔LGA, UA1071's 31 legs only ORD→DFW, and UA operated zero ATL→DFW legs that month. Every quoted departure time was shifted exactly +7h from real BTS (13:00 against a real CRSDep of 06:00), consistent with a run on an MST machine — the `TimestampType`/UTC failure mode described above, which is now pinned shut in `create_spark_session()` and gated by a `TZ: America/Denver` integration job. Never paste query output into docs without re-running it on the machine that will be quoted.
- **The phantom-feature and load-guide entries are resolved.** "efficiency scoring", "16-hour max journeys", "2-stop connections", "single unified query" and "Pre-generated Scenarios" are all **gone from `README.md`** — verified absent. `LOAD_TESTING_GUIDE.md`'s 130% distribution table is gone too, and the guide now names the real tasks (`nonstop lookup` at 70%, `itinerary search {0,2}` at 30%, both calling `flight_search.py`) rather than the deleted `direct_flight_search` / `comprehensive_routing_search`. Its stale "sampling is skewed" and "per-route stats names" warnings are rewritten as *fixed*, with a note that pre-fix numbers aren't comparable. The `s.cancelled = 0` caveat stays — that predicate really does match nothing, because `cancelled` is filtered at load and never persisted.
- **Diverted flights are loaded unfiltered.** `load_bts_data.py:1441` filters `cancelled == 0` only; 1,512 diverted Jan-2024 flights load, and the 244 with `DivReachedDest=0` keep an `ARRIVES_AT` edge to an airport the aircraft never reached (e.g. AA491 DCA→MIA actually landed at CLT).
