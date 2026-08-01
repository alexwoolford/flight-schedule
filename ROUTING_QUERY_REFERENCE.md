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

## Recommended query: explicit 1-stop join

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

## Quantified path patterns: elegant, but slower here

Neo4j 5.9+ supports quantified path patterns (QPPs), which express "1 to N legs"
in one pattern — no `UNION ALL` per hop count and no iterative deepening in the
client:

```cypher
MATCH (origin:Airport {code: $origin})
      ((a:Airport)<-[:DEPARTS_FROM]-(leg:Schedule)-[:ARRIVES_AT]->(b:Airport)
        WHERE leg.flightdate IN [date($date), date($date) + duration('P1D')]
      ){1,2}
      (dest:Airport {code: $dest})
WHERE leg[0].flightdate = date($date)
  AND all(i IN range(0, size(leg) - 2) WHERE
        leg[i].reporting_airline = leg[i + 1].reporting_airline
    AND duration.inSeconds(leg[i].scheduled_arrival_time,
                           leg[i + 1].scheduled_departure_time).seconds / 60
          >= $min_layover
    AND duration.inSeconds(leg[i].scheduled_arrival_time,
                           leg[i + 1].scheduled_departure_time).seconds / 60
          <= $max_layover)
RETURN size(leg) - 1 AS stops,
       [f IN leg | f.reporting_airline + toString(f.flight_number_reporting_airline)] AS flights,
       [f IN leg | f.origin + '-' + f.dest] AS route,
       leg[0].scheduled_departure_time AS departs,
       leg[-1].scheduled_arrival_time AS arrives,
       reduce(t = 0, f IN leg | t + f.scheduled_duration_minutes) AS air_minutes
ORDER BY stops, departs
LIMIT $limit
```

Worth understanding:

- **`{1,2}`** covers direct flights *and* 1-stop in one pattern.
- **`leg`** is a *group variable* — outside the quantifier it is an ordered list of
  the matched `Schedule` nodes. That's what makes `leg[0]`, `leg[-1]`, and the
  `reduce()` over block times work.
- **A predicate inside the quantifier** (`leg.flightdate IN [...]`) applies per
  repetition, so it prunes during expansion.
- **`leg[0].flightdate = date($date)`** anchors the *first* leg to the search date
  while letting later legs spill into the next day.

### Why it is slow: the supernode juncture

`PROFILE`d on the full 2025 graph (6,898,743 flights), LGA→DFW on one date:

| query | wall clock | dbHits | rows expanded |
|---|---|---|---|
| explicit 1-stop join (above) | **658 ms** | 1,364,930 | — |
| QPP `{1,2}` | **44,566 ms** | 255,800,105 | 84,254,361 → 493,785 |
| QPP `{1,1}` (never crosses a hub) | **304 ms** | — | — |
| QPP `{1,2}`, *no* inter-repetition predicates | **20,800 ms** | — | — |

The cause is the **data model**, not the phrasing. There is no
`Schedule`→`Schedule` relationship, so each repetition must hop
`Schedule → Airport → Schedule` — through an `Airport` node. `Airport` carries
only `code`; it has **no date dimension**. So the moment expansion arrives at a
hub, the next hop fans out to that hub's departures for *the entire loaded
period* before any date predicate can apply. Measured directly: from the hubs LGA
reaches on one date, the second hop walks **51,943,377** `DEPARTS_FROM` edges, of
which **161,054** are on the search date — **99.69% wasted traversal**.

`Airport` out-degree over a year is avg **19,599**, median 2,733, max **321,372**
(ORD). For a single day it is avg 62.7, max 1,035 — but QPP cannot exploit that,
because the date lives on `Schedule`, one hop away.

The last two rows are the control that rules out predicate placement as the
explanation. Removing *every* inter-repetition predicate — QPP's best possible
case — still costs 20.8 s, while `{1,1}`, which never crosses a hub, costs 304 ms.
No rewrite of the QPP avoids the juncture.

**So itinerary search stays on the explicit join.** That is a deliberate choice
about where QPP fits this model, not an artifact of the repo predating QPP.

## Quantified path patterns over `ROUTE`: where they do win

The loader also writes an aggregated route network, which is the projection QPP is
actually suited to:

```
(:Airport)-[:ROUTE {flights, carriers, first_date, last_date}]->(:Airport)
```

One edge per distinct directed route rather than one per flight. It has no
supernodes — 352 airports, ~6,900 edges, out-degree avg ~20 / max 186, versus
19,599 for `DEPARTS_FROM` — so bounded expansion stays cheap, and none of these
questions need per-flight temporal data.

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
