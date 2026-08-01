# Flight Routing Query Reference

## Overview

This document gives the recommended Cypher for multi-hop flight routing over this
graph, and explains the one modeling trap you need to know about before writing
your own.

## Read this first: durations

`Schedule` carries two local wall-clock timestamps:

- `scheduled_departure_time` — local time **at the origin**
- `scheduled_arrival_time` — local time **at the destination**

They are in **different timezones**. Subtracting one from the other therefore
does not give a flight duration. Measured across a full month of loaded data,
naive `arrival − departure` agrees with BTS's own reported block time for only
about **49%** of flights, and produces a *negative* result for roughly **2.7%**.

Use **`scheduled_duration_minutes`** instead. It is BTS's reported scheduled
block time (`CRSElapsedTime`), it is 100% populated, and it is both timezone- and
DST-independent.

### The `CASE`-based idiom is wrong — don't copy it

Earlier versions of this document recommended a `CASE` expression that treated
`arrival < departure` as a midnight crossing and added 1440 minutes. That
inference is invalid: on this data, `arrival < departure` usually means the flight
flew **westbound across a timezone**, not that it crossed midnight. The idiom
yields the correct duration for only about **50%** of flights.

Worst observed case: a short ATL→HSV hop scheduled 22:55 → 22:56 local (61 real
minutes, one timezone westbound). The `CASE` idiom returns **1439 minutes**, and a
`WHERE duration > 0 AND duration < 1440` guard does *not* filter it out.

**What is still sound:** layover arithmetic at a connecting hub. Both timestamps
at the hub are local to the same airport, so connection windows computed by
subtraction are correct.

## Explicit 1-stop join

Use this when you want exactly one stop and don't want to build any extra edges.
For variable-depth search (direct *or* 1-stop *or* 2-stop in one query), use the
quantified path pattern in the next section instead — it's at parity on 1-stop and
the only practical option beyond it.

```cypher
MATCH (:Airport {code: $origin})<-[:DEPARTS_FROM]-(s1:Schedule)-[:ARRIVES_AT]->(hub:Airport)
      <-[:DEPARTS_FROM]-(s2:Schedule)-[:ARRIVES_AT]->(:Airport {code: $dest})
WHERE s1.flightdate = date($date)
  // Allow the second leg to spill into the next day
  AND s2.flightdate IN [date($date), date($date) + duration('P1D')]
  AND hub.code <> $origin AND hub.code <> $dest
  // Same carrier throughout: a random splice of two airlines is not a sellable
  // itinerary. Drop this line to allow interline connections.
  AND s1.reporting_airline = s2.reporting_airline
WITH s1, s2, hub,
     // Both timestamps are local to the same hub, so this subtraction is valid.
     duration.inSeconds(s1.scheduled_arrival_time,
                        s2.scheduled_departure_time).seconds / 60 AS layover
WHERE layover >= $min_layover AND layover <= $max_layover
RETURN [s1.reporting_airline + toString(s1.flight_number_reporting_airline),
        s2.reporting_airline + toString(s2.flight_number_reporting_airline)] AS flights,
       hub.code AS via,
       s1.scheduled_departure_time AS departs,
       s2.scheduled_arrival_time AS arrives,
       layover,
       // Sum of real block times. Do NOT compute arrives - departs.
       s1.scheduled_duration_minutes + s2.scheduled_duration_minutes AS air_minutes
ORDER BY departs
LIMIT $limit
```

**`duration.inSeconds(...)`, not `.minutes`** — `duration.between(...).minutes` is
a component accessor that **excludes whole days**, so a 25½-hour span reports as
90 minutes. `inSeconds()` gives the true total.
## Quantified path patterns: variable-depth search in one query

QPPs (Neo4j 5.9+) express "direct, or 1 stop, or 2 stops…" as **one pattern with
one number to change** — no `UNION ALL` per hop count and no iterative deepening
in the client. On this graph they need one thing first: a direct
`Schedule`→`Schedule` edge.

### Build the connection edges

```bash
python load_bts_data.py --build-connections 2025-07-18
```

That writes `(:Schedule)-[:CONNECTS_TO {layover_minutes}]->(:Schedule)` for every
**bookable** connection on those dates — same marketing carrier, layover within
45–300 minutes, no immediate backtrack, and no inbound leg that actually lands the
next day. About **625,000 edges per day**, built in **~12 seconds**. It is
idempotent (`MERGE`), so re-running is safe.

The connection policy is baked into the edges. After changing
`--min-layover`/`--max-layover` or `--strict-carrier`, rebuild with
`--rebuild-connections` — `MERGE` alone cannot remove edges a looser rule already
wrote.

### The query

```cypher
MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {code: $origin})
WHERE first.flightdate = date($date)
MATCH p = (first)-[:CONNECTS_TO]->{0,2}(last:Schedule)
MATCH (last)-[:ARRIVES_AT]->(:Airport {code: $dest})
WITH p, nodes(p) AS legs, relationships(p) AS conns
RETURN size(legs) - 1 AS stops,
       [f IN legs | f.reporting_airline + toString(f.flight_number_reporting_airline)] AS flights,
       [f IN legs | f.origin + '-' + f.dest] AS route,
       // Sum of real block times. Do NOT compute arrives - departs.
       reduce(t = 0, f IN legs | t + f.scheduled_duration_minutes) AS air_minutes,
       reduce(t = 0, c IN conns | t + c.layover_minutes) AS layover_minutes
ORDER BY stops, air_minutes + layover_minutes
LIMIT $limit
```

`{0,2}` means **direct, 1-stop, or 2-stop in a single query**. `{0,3}` adds
3-stop. Nothing else changes — that is the whole point.

Because the connection rules live in the edge, there are no inter-repetition
predicates left to write, and no way to accidentally splice two carriers into an
unsellable itinerary.

### Measured

LGA→DFW, 2025-07-18, best-first, `LIMIT 5`, warm page cache:

| query | wall clock | itineraries |
|---|---|---|
| QPP `{1,1}` (1-stop) | **146 ms** | 138 |
| QPP `{0,1}` | **56 ms** | 160 |
| QPP `{0,2}` | **105 ms** | 2,122 |
| QPP `{0,3}` | **478 ms** | 11,488 |

At 1-stop this is at parity with the hand-written explicit join. Beyond 1-stop the
join has no equivalent — you would hand-write another `MATCH` block per hop count,
which is what the iterative-deepening client loop in
`neo4j_flight_load_test.py` exists to do.

Across 12 routes at `{0,2}`: **min 88 ms, median 131 ms, max 261 ms**. Time these
warm; a cold page cache roughly triples the first hit on each route.

Routes with no nonstop, where multi-hop is the *only* answer:

| route | wall clock | best itinerary |
|---|---|---|
| GUM→BOS | **4.7 ms** | GUM-HNL, HNL-DEN, DEN-BOS |
| FCA→SAV | **26.4 ms** | FCA-DEN, DEN-SAV |
| BOI→ALB | **154.5 ms** | BOI-MDW, MDW-ALB |

### Validated against real routings

The edges were checked against published airline route data (Wikipedia airport and
airline articles, which carry per-carrier nonstop destination tables with seasonal
annotations). Every leg in the sampled itineraries corresponds to real scheduled
service, and every route the query *works around* genuinely has no nonstop:

| itinerary leg | external check |
|---|---|
| `FCA-DEN` (UA2446) | UA / United Express serve DEN from FCA year-round |
| `FCA-DFW` (AA2939) | AA serves DFW from FCA **seasonally** — July is in season |
| `FCA-MSP` (DL1578) | DL serves MSP from FCA year-round |
| FCA→SAV via ATL ranks 3rd | FCA has **no** ATL service; ATL only ever appears as a 2nd hop |
| `BOI-MDW` (WN1382) | WN serves MDW from BOI seasonally — July in season |
| `BOI-ORD` (UA573) | United Express serves ORD from BOI year-round |
| BOI→ALB needs a stop | no BOI→ALB nonstop exists |
| `GUM-HNL` (UA200) | UA is the **only** GUM–HNL carrier; GUM is a UA hub |
| GUM→BOS needs 3 legs | GUM has **no** mainland-US nonstop at all |

Structurally, connections concentrate exactly where real US hubs are — ATL, DFW,
DEN, ORD, CLT lead, and the top 10 airports carry **71.6%** of all connections.
Per-carrier hub sets derived from the graph match the real networks with no
exceptions (DL at ATL/MSP/DTW/SLC, WN at DEN/LAS/MDW/BWI, HA at HNL/OGG/LIH/KOA,
G4 at PIE/SFB). `tests/test_graph_validation.py` asserts this.

**These guarantees gate every push.** The `integration-test` job in
`.github/workflows/ci.yml` loads a committed one-day BTS fixture
(`tests/fixtures/bts_flights_2025_07_18.parquet` — real records, see the README
there) into a throwaway Neo4j, builds `CONNECTS_TO` with the production loader,
and runs the assertions against the result. So the claims on this page are
checked on real data continuously, not just when someone remembers to load
locally.

Two things keep that gate honest:

- The conftest fixtures skip cleanly when Neo4j is unreachable, which would make
  an all-skipped run look green. `tests/ci_verify_loaded.py` runs first and fails
  if the graph does not hold what the fixture should have produced.
- The regression tests assert the defect is *absent* **and** that the fixture
  could have exposed it — 893 overnight legs and 110,642 cross-family edges are
  asserted non-zero. Otherwise a dataset without those properties would pass
  while gating nothing. Verified by injecting one bad edge of each kind into
  625,220 good ones: each trips its own assertion and nothing else.

Two defects that validation found, both now fixed in the builder:

**Inbound legs that land the next day.** 17,502 edges (3.29%) connected off a leg
whose stored arrival was same-day but which really arrives the following morning —
`AA1009 LAX-ORD dep 22:59 arr 05:18` spliced to a 06:55 ORD departure. This is the
timezone defect at the top of this document leaking into connection *dates*. The
detector needs no timezone table: if apparent duration is negative **and** adding
1440 minutes reconciles it with `scheduled_duration_minutes`, the leg is an
overnight. Those legs are now excluded.

**Regional affiliates are not interline.** BTS reports the *operating* carrier and
the feed has **no marketing-carrier column**, so Envoy (`MQ`) and PSA (`OH`) —
both wholly-owned American subsidiaries selling under AA flight numbers — never
connected to AA. A strict code comparison drops **112,501 sellable connections per
day**. `CARRIER_FAMILY` in `load_bts_data.py` maps wholly-owned regionals to their
parent before comparing; `--strict-carrier` restores the old behaviour.

SkyWest (`OO`, 2,533 flights/day) and Republic (`YX`, 1,030) are **deliberately
excluded** from that map: each flies for several mainlines and BTS carries nothing
to say which one a given flight was sold under. They stay strict rather than
guessed. True interline between unrelated carriers (AA↔AS) is also not derivable
here — and matters, since allowing arbitrary cross-carrier connections would add
1.1M pairs a day, most unsellable.

**Known and accepted:** 142 airports act as a connecting point for fewer than 100
connections each (TTN, WYS, MGW…), which no airline would sell. That is **0.49%**
of edges — too small to affect a ranked result, so it is measured rather than
filtered.

### Why the edge is necessary: don't traverse through `Airport`

Writing the same QPP *without* `CONNECTS_TO` — hopping
`Schedule → Airport → Schedule` — is **200-400x slower** and, in the naive form,
also wrong. `PROFILE`d on the same route and date:

| formulation | wall clock | result |
|---|---|---|
| QPP over `CONNECTS_TO` `{1,1}` | **185 ms** | 135 ✅ |
| QPP via `Airport`, predicates inside the quantifier | 69,322 ms | 135 ✅ |
| QPP via `Airport`, `all(i IN range(...))` post-filter | 41,878 ms | 157 ❌ |

`Airport` carries only `code` — it has **no date**. So when expansion reaches a
hub, the next hop must bind that hub's departures for the *entire loaded period*
before any date predicate can apply: **3,783,541** candidate `Schedule` nodes
bound to keep **11,695**, or **99.69% discarded**.

Moving the predicates inside the quantifier does not fix this, and that is the
non-obvious part: a predicate like
`s1.reporting_airline = s2.reporting_airline` needs `s2`'s properties, so `s2`
must be *materialised* before it can be tested. **Per-repetition predicates prune
after the supernode crossing, not during it** — and unlike a `MATCH`, a predicate
inside a quantifier cannot be served by an index. That is why the stateful
formulation is *slower* than the post-filter one: same traversal, plus 3.8M
predicate evaluations.

The explicit join avoids it because `s2.flightdate = date($date)` lets the planner
seek the ~11,695 relevant nodes via `schedule_date_departure` and *then* check the
hub — it never walks the 3.8M. `CONNECTS_TO` avoids it by removing the juncture
entirely.

The `{1,1}` row above is also the control that rules out "QPP is just slow":
restricted to one leg, so never crossing a hub, the `Airport` form runs in 304 ms.
The cost is the juncture, not the quantifier.

### Cost and scope

`CONNECTS_TO` is date-scoped on purpose:

| scope | edges |
|---|---|
| 1 day | ~625,000 |
| full year (extrapolated) | ~228,000,000 |

A full year is ~11x the rest of the graph, and a date-specific search never needs
it. Build the dates you intend to query. Same-day connections only, so this also
sidesteps the cross-midnight timezone problem described above.

## The `ROUTE` projection: topology, not itineraries

The loader also writes an aggregated route network:

```
(:Airport)-[:ROUTE {flights, carriers, first_date, last_date}]->(:Airport)
```

One edge per distinct directed route rather than one per flight — 352 airports,
~6,900 edges, out-degree avg ~20 / max 186 versus 19,599 for `DEPARTS_FROM`.

**Calibrate your expectations before using this.** It is a 352-node graph, so
everything here is fast for uninteresting reasons, and the answers tend toward the
degenerate: ATL reaches 347 of 352 airports within 2 legs and all 352 within 3;
the average across all origins is 228 within 2 legs. "What can I reach in N legs"
mostly answers "almost everything."

It also **overstates what is bookable**, because it has no time dimension:
`ROUTE` claims 66 connecting hubs for LGA→DFW where only **26** have an actual
same-day, same-carrier, 45–300-minute connection — a **2.5x** overstatement. Use
it as a cheap hub pre-filter or for topology questions, then verify against
`Schedule` or `CONNECTS_TO`. It is not an itinerary search.

**Bounded reachability** — how much of the network is within N legs of LGA:

```cypher
MATCH (:Airport {code: $origin})-[:ROUTE]->{1,3}(reachable:Airport)
RETURN count(DISTINCT reachable) AS airports;
```

Measured from LGA (**1-2 ms**, 7,985 dbHits at `{1,3}`): 78 airports nonstop, 326
within 2 legs, 348 within 3 — of 352 total. The US domestic network is a
small-world graph; almost all of it is three legs from anywhere.

**Thick routes only** — restrict expansion to routes with real service. The
predicate sits *inside* the quantifier, so it prunes per repetition, which is the
shape QPP handles well:

```cypher
MATCH (:Airport {code: $origin})
      (()-[r:ROUTE WHERE r.flights >= $min_flights]->()){1,3}
      (reachable:Airport)
RETURN count(DISTINCT reachable) AS airports;
```

At `$min_flights = 500` this drops the 3-leg reach from 348 to **280** in 53 ms.

**Fewest-legs path** — use `SHORTEST`, not `ORDER BY length(p)`:

```cypher
CYPHER 25
MATCH p = SHORTEST 1 (:Airport {code: $origin})-[:ROUTE]->+(:Airport {code: $dest})
RETURN length(p) AS legs, [n IN nodes(p) | n.code] AS route;
```

**1 ms**, and it needs no upper bound. The obvious-looking alternative —
`-[:ROUTE]->{1,4}` with `ORDER BY legs LIMIT 1` — enumerates every path up to
length 4 before sorting: **1,425 ms** and 13.6M dbHits for the same one-row
answer. Widening the bound is what costs, not the answer's depth; `{1,2}` returns
in 1 ms. `SHORTEST` stops at the first hit. (BUR→SPN returns
`BUR, JFK, HNL, GUM, SPN` — 4 legs — also in 1 ms.)

**Path modes** `TRAIL` (no repeated relationship) and `ACYCLIC` (no repeated node)
forbid doubling back. Both need **Cypher 25**; under `CYPHER 5` the parser rejects
them with `Invalid input 'ACYCLIC'`. Note the word order — the mode goes after
`=`, not after `MATCH`:

```cypher
CYPHER 25
MATCH p = ACYCLIC (:Airport {code: $origin})-[:ROUTE]->{1,3}(:Airport {code: $dest})
RETURN [n IN nodes(p) | n.code] AS route, length(p) AS legs
ORDER BY legs
LIMIT 10;
```

37 ms for LGA→DFW.

**What `ROUTE` does not answer.** It has no date and no times, so it cannot tell
you whether a connection is *bookable* — only whether the route pairs exist. Treat
it as a planner: use it to find candidate hubs cheaply, then verify against
`Schedule` with the explicit join above.

## Parameters

| Parameter | Example | Meaning |
|---|---|---|
| `$origin` | `"LGA"` | Origin IATA code |
| `$dest` | `"DFW"` | Destination IATA code |
| `$date` | `"2025-01-15"` | Departure date of the first leg |
| `$min_layover` | `45` | Minimum connection minutes |
| `$max_layover` | `300` | Maximum connection minutes |
| `$limit` | `10` | Max itineraries returned |

## Sample results

Real output from the loaded 2025 graph — LGA→DFW, 2025-01-15, 45-300 minute
layovers, same carrier:

```
Nonstop (20 total):
  AA1597   06:00 → 09:17   257 min block
  DL878    06:00 → 09:24   264 min block
  AA1490   07:37 → 10:55   258 min block

One-stop:
  DL550 → DL961    via ATL   dep 06:00   78 min layover
  UA2389 → UA1044  via ORD   dep 06:00   83 min layover
  AA628 → AA1203   via CLT   dep 06:00  196 min layover
```

Note the nonstop block times (~257 min) differ from `arrival − departure`
(~197 min) by exactly the one-hour Eastern→Central offset — a concrete instance
of the trap described at the top of this document.

## Notes and caveats

- **The shipped load test does not use these queries.**
  `neo4j_flight_load_test.py` still uses the older `UNION ALL` form with the
  `CASE` duration idiom and no carrier predicate. Migrating it is an open task.
- **Layover bounds are inconsistent across this repo** (300, 720, and 1200
  minutes appear in different files). Pick a value deliberately.
- **Without a carrier predicate, most returned itineraries are unsellable.**
  A 1-stop query with no carrier condition freely splices any airline to any
  other — including carriers that interline with nobody. But note the predicate
  cuts both ways: a *strict* `s1.reporting_airline = s2.reporting_airline` is too
  tight, because BTS reports operating carriers and so separates mainlines from
  their own wholly-owned regional feeders. `CONNECTS_TO` compares marketing
  carriers via `CARRIER_FAMILY` instead; the explicit join above still uses the
  strict form and will miss American Eagle connections.
- **No APOC.** Everything here is plain Cypher, for Aura compatibility.
