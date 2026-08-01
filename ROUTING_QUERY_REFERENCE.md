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
**bookable** connection on those dates — same carrier, layover within
45–300 minutes, no immediate backtrack. About **532,000 edges per day**, built in
**~7 seconds**. It is idempotent (`MERGE`), so re-running is safe.

The layover window is baked into the edges. Change `--min-layover`/`--max-layover`
and rebuild.

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

LGA→DFW, 2025-07-18, best-first, `LIMIT 5`:

| query | wall clock | itineraries |
|---|---|---|
| QPP `{1,1}` (1-stop) | **185 ms** | 135 |
| explicit 1-stop join | **182 ms** | 135 |
| QPP `{0,1}` | **66 ms** | 157 |
| QPP `{0,2}` | **102 ms** | 1,736 |
| QPP `{0,3}` | **492 ms** | 13,625 |

At 1-stop it is **at parity** with the hand-written join and returns an identical
135 itineraries. Beyond 1-stop the join has no equivalent — you would hand-write
another `MATCH` block per hop count, which is what the iterative-deepening client
loop in `neo4j_flight_load_test.py` exists to do.

Across 12 routes at `{0,2}`: **min 71 ms, median 113 ms, max 186 ms**.

Routes with no nonstop, where multi-hop is the *only* answer:

| route | wall clock | best itinerary |
|---|---|---|
| GUM→BOS | **1.2 ms** | GUM-HNL, HNL-DEN, DEN-BOS |
| FCA→SAV | **7.6 ms** | FCA-DEN, DEN-SAV |
| BOI→ALB | **28.5 ms** | BOI-MDW, MDW-ALB |

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
| 1 day | ~532,000 |
| full year (extrapolated) | ~194,000,000 |

A full year is ~9x the rest of the graph, and a date-specific search never needs
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
- **Without the carrier predicate, most returned itineraries are unsellable.**
  A 1-stop query with no `s1.reporting_airline = s2.reporting_airline` condition
  freely splices any airline to any other — including carriers that interline
  with nobody.
- **No APOC.** Everything here is plain Cypher, for Aura compatibility.
