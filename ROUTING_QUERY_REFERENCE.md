# Flight Routing Query Reference

## Overview

This document gives the recommended Cypher for multi-hop flight routing over this
graph, and explains the one modeling trap you need to know about before writing
your own.

## Read this first: durations and time zones

`Schedule` carries **two pairs** of timestamps. Picking the right pair is the
single most important thing on this page.

| property | type | frame | use it for |
|---|---|---|---|
| `scheduled_departure_time` | LOCAL DATETIME | local at the **origin** | "departs after 09:00 local" |
| `scheduled_arrival_time` | LOCAL DATETIME | local at the **destination** | "arrives before 15:00 local" |
| `scheduled_departure_utc` | LOCAL DATETIME | **UTC** | durations, sequencing, sorting across airports |
| `scheduled_arrival_utc` | LOCAL DATETIME | **UTC** | durations, sequencing, sorting across airports |
| `scheduled_duration_minutes` | INTEGER | — | block time (BTS `CRSElapsedTime`) |

**The rule: compare or subtract only within one frame.** Arithmetic goes in UTC;
wall-clock filters go in local time. Never mix the two.

The local pair is in **different timezones from each other**, so subtracting them
is meaningless: naive `arrival − departure` agrees with BTS's own block time for
only **48.9%** of flights on 2025-07-18 (10,453 of 21,376) and goes *negative* for
**940**. That is not a defect in the stored times — see below — it is simply the
wrong operation.

The UTC pair exists to make that operation correct. It is produced by
`--solve-offsets` and satisfies, for **100%** of flights:

```
scheduled_arrival_utc − scheduled_departure_utc == scheduled_duration_minutes
```

with **zero** flights arriving at or before their departure.

### Where the UTC times come from — no timezone database

`Airport` carries no timezone, and none is downloaded. The offsets are *recovered
from the loaded data*, which works because BTS gives local times at both ends plus
the timezone-independent block time. For any flight:

```
(arrival_local − departure_local) − block  =  offset(dest) − offset(origin)
```

Each directed airport pair therefore reveals the **difference** between its
endpoints' offsets. Treat airports as nodes and those differences as edges, BFS
from any airport to propagate relative offsets across the network, then pin the
result to absolute UTC with one known value (`PHX = −07:00`, chosen because Arizona
does not observe DST and so needs no seasonal branch).

On 2025-07-18 this solves all **341** airports as one connected component with
**0** conflicting deltas, every offset a whole hour, and all 22 spot-checked
against real-world values exactly — including `GUM`, which requires a dateline
wrap to come out at UTC+10 rather than −14.

Solved **per date**, because offsets are DST-dependent: 18 of 317 airports differ
between January and July, and mainland airports shift wholesale (ORD −6 → −5). A
single `Airport.utc_offset` property would be wrong for half the year, which is why
none exists.

### The local times are correct — only their dates are not

Worth stating plainly, because "48.9%" invites the wrong conclusion. The stored
**time-of-day is the correct local wall clock at its own airport**. Verified two
ways on 2025-07-18:

- `DL304 DTW→SNA` stores dep 08:45, arr 10:25, block 280 min. 08:45 Eastern is
  05:45 Pacific; 05:45 + 280 min = **10:25 Pacific** — exactly what is stored.
- Structurally, reconstructing local arrival from `scheduled_arrival_utc` plus the
  destination's solved offset reproduces the stored `scheduled_arrival_time`
  time-of-day for **21,376 of 21,376 flights (100.00%)**. Times that were
  arbitrarily wrong could not do this.

What is wrong is the **date**: both local timestamps are composed onto the origin's
`flightdate`, so a leg crossing local midnight is stamped a day early. On
2025-07-18, **893 of 21,376 flights (4.18%)** are such overnights. The UTC pair has
no such problem — 3,152 flights that day correctly land on the following UTC day.

Consequences, precisely:

| you want | use |
|---|---|
| a leg or journey **duration** | ✓ UTC pair, or `scheduled_duration_minutes` |
| **layover** at a hub | ✓ UTC pair (what `CONNECTS_TO` stores); the local pair also works at a single hub since both share its timezone |
| **"arrives before HH:MM local"** | ✓ `scheduled_arrival_time` with `localdatetime()` — no extra guard needed |
| ordering arrivals **across different airports** | ✓ UTC pair; ✗ the local pair, whose frames differ |
| **sequencing** two legs | ✓ UTC pair — or rely on `CONNECTS_TO`, which already enforces it |

### Deadline filters: one trap left, one repaired at load time

**1. `datetime()` vs `localdatetime()` — this one returns no error at all.**
`scheduled_arrival_time` is a `LOCAL DATETIME` (the loader writes
`to_timestamp_ntz`). `datetime('2025-07-18T15:00:00')` is a `ZONED DATETIME`.
Comparing the two is **not** an error and does **not** evaluate false — it
evaluates **NULL**, so `WHERE` discards every row:

```
MATCH (f:Schedule) WHERE f.flightdate = date('2025-07-18') AND f.dest = 'CHS' ...

  f.scheduled_arrival_time < datetime('...T15:00:00')       ->  0 of 82 matched, 82 NULL
  f.scheduled_arrival_time < localdatetime('...T15:00:00')  -> 39 of 82 matched,  0 NULL
```

A BUF→CHS search with 40 valid itineraries returns **zero** and looks like "no
route exists". Always use `localdatetime()` — or `datetime()` only after loading
a real timezone for each airport, which this graph does not have.

**2. Overnight legs used to pass a deadline they do not meet — fixed at load
time, no query-side guard.** The loader originally composed *both* timestamps onto
the origin's `flightdate`, so a red-eye landing at 00:18 the *next* day was stored
as 00:18 on the departure date and sailed through `< 15:00`.

`--solve-offsets` now rewrites `scheduled_arrival_time` as
`scheduled_arrival_utc + offset(dest)`, so the DATE is the destination's. The
time-of-day is unchanged — it was already correct. On 2025-07-18, **915 of 21,376
flights (4.28%)** move to the following local day, and a deadline filter with no
guard at all now wrongly accepts **0** of them. `AA983 MIA→RIC` departs 21:51 and
is stored `2025-07-19T00:18`, which is simply the truth.

Two things this replaced, both wrong, recorded so nobody reintroduces them:

- **A ±180-minute block-time tolerance** (what this repo shipped). It tried to
  *infer* the destination offset from the local subtraction, and could not span the
  widest gaps — `AA6` HNL→DFW needs 240–360 minutes and read as same-day.
- **`date(arrival_utc) = date(departure_utc)`.** Tempting and still wrong: it tests
  **UTC** midnight, not local. Measured on 2025-07-18 it excludes **3,135** ordinary
  evening flights (`AA976` CLT→FLL, 19:21→21:19 local, is 23:21→01:19 UTC) while
  admitting **876** genuine red-eyes.

The general lesson: the destination's UTC offset is not recoverable inside a query,
so a correct local arrival date can only be written by the loader, which knows it.

A leg may still legitimately arrive on an *earlier* local date than it departed:
`UA200 GUM→HNL` departs 07:15 on 07-18 and lands 18:25 on **07-17**, crossing the
dateline. Twenty more land at an earlier local *time* on the same day — `DL2250
ATL→BHM`, 09:40→09:32, a 52-minute flight westbound one hour. Both are correct;
never "repair" them.

### The `CASE`-based idiom is wrong — don't copy it

Earlier versions of this document recommended a `CASE` expression that treated
`arrival < departure` as a midnight crossing and added 1440 minutes. That
inference is invalid: on this data, `arrival < departure` usually means the flight
flew **westbound across a timezone**, not that it crossed midnight. The idiom
yields the correct duration for only about **50%** of flights.

Worst observed case: a short ATL→HSV hop scheduled 22:55 → 22:56 local (61 real
minutes, one timezone westbound). The `CASE` idiom returns **1439 minutes**, and a
`WHERE duration > 0 AND duration < 1440` guard does *not* filter it out.

There is now no reason to reach for any such workaround: subtract the **UTC**
timestamps, or read `scheduled_duration_minutes`. Both are exact.

### Beware ±180-minute block-time heuristics too

The same warning applies to a subtler workaround that this repo itself shipped.
Before UTC times existed, `CONNECTS_TO` detected next-day arrivals by testing
whether adding 1440 minutes reconciled the local subtraction with the block time,
to within ±180 minutes. That tolerance cannot span the widest US offset gaps:
`AA6` HNL→DFW (dep 17:36, arr 06:02 next day, 446-min block) needs 240–360 minutes
and so read as same-day, leaving **11,975 impossible edges** in the graph.

The lesson generalises. Any tolerance wide enough to absorb every real timezone gap
is also wide enough to swallow genuine same-day legs. Compare absolute instants
instead — that is what the UTC pair is for.

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
python load_bts_data.py --solve-offsets 2025-07-18       # required first
python load_bts_data.py --build-connections 2025-07-18
```

The offset step is a prerequisite, not an optimisation: the layover is computed
from `scheduled_departure_utc` / `scheduled_arrival_utc`, which it creates. See
"Durations and time zones" below.

That writes `(:Schedule)-[:CONNECTS_TO {layover_minutes}]->(:Schedule)` for every
**bookable** connection on those dates — same marketing carrier, layover within
45–300 minutes **of absolute time**, and no immediate backtrack.
**512,000-624,000 edges per day** (measured over 2025-07-14...20; mean 575,510,
lowest on Saturday), built in **~9 seconds** per date. Idempotent (`MERGE`), so
re-running is safe. Accepts several dates in one invocation.

Because the window is measured in UTC, an inbound leg that lands the next morning
is excluded arithmetically rather than by a heuristic. The previous local-time
build used a ±180-minute block-time test that could not span the widest US offset
gaps and left **11,975 impossible edges** — e.g. `AA6` HNL→DFW arriving 06:02 the
next day spliced to an 08:10 DFW departure the day before. Switching to UTC
removed exactly those (a strict subset: all 623,508 surviving edges for
2025-07-18 were already present, and nothing new was admitted).

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
WITH nodes(p) AS legs, relationships(p) AS conns
// No airport twice. The edge's backtrack guard is pairwise and does not compose
// over 3+ legs; without this, 18% of {0,3} paths revisit an airport and some
// return to the origin. See "Itineraries revisit airports" below.
WITH legs, conns, [legs[0].origin] + [x IN legs | x.dest] AS airports
WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                             WHERE NOT airports[i] IN airports[0..i]])
RETURN size(legs) - 1 AS stops,
       [f IN legs | f.reporting_airline + toString(f.flight_number_reporting_airline)] AS flights,
       airports AS route,
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
unsellable itinerary. The one thing the edge *cannot* enforce is a property of the
whole path — that no airport repeats — so that guard stays in the query.

### Arrive-before-a-deadline

"Find me a route from A to B that arrives before 3pm" — the query most real
requests actually are. It is the base QPP plus one line, and the only trap left is
using `localdatetime()` rather than `datetime()`; get that wrong and it returns
nothing at all. The overnight problem is handled by the loader, not here.

```cypher
MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {code: $origin})
WHERE first.flightdate = date($date)
MATCH p = (first)-[:CONNECTS_TO]->{0,2}(last:Schedule)
MATCH (last)-[:ARRIVES_AT]->(:Airport {code: $dest})
WITH nodes(p) AS legs, relationships(p) AS conns, last AS f
WITH legs, conns, f, [legs[0].origin] + [x IN legs | x.dest] AS airports
// No airport twice — see "Itineraries revisit airports" below.
WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                             WHERE NOT airports[i] IN airports[0..i]])
// localdatetime, NOT datetime: scheduled_arrival_time is a LOCAL DATETIME, and
// comparing it against a ZONED DATETIME yields NULL rather than false, which
// silently discards every row. See "Deadline filters" above.
  AND f.scheduled_arrival_time < localdatetime($deadline)
// No overnight guard is needed. scheduled_arrival_time now carries the
// DESTINATION's date (--solve-offsets rewrites it from the UTC instant), so a
// red-eye landing after midnight compares as the next day, which it is. Earlier
// revisions of this query needed a guard here; see "Deadline filters" above for
// why every query-side version of it was wrong.
RETURN size(legs) - 1 AS stops,
       [x IN legs | x.reporting_airline +
                    toString(x.flight_number_reporting_airline)] AS flights,
       airports AS route,
       f.scheduled_arrival_time AS arrives,
       // Elapsed journey from real block times plus real layovers. Never
       // subtract the endpoints: they are in different timezones.
       reduce(t = 0, x IN legs | t + x.scheduled_duration_minutes) +
       reduce(t = 0, c IN conns | t + c.layover_minutes) AS total_minutes
ORDER BY total_minutes
LIMIT $limit
```

`$deadline` is a local wall-clock string at the **destination**, e.g.
`"2025-07-18T15:00:00"`. Use `{1,2}` instead of `{0,2}` to require a connection.

Measured, `{1,2}`, `LIMIT 5`, warm — three routes with no nonstop:

| route | wall clock | best itinerary arriving before 15:00 |
|---|---|---|
| BUF→CHS | **44 ms** | `AA1204+AA1531` via CLT, dep 06:28 → arr 10:27 (3h59) |
| PVD→BOI | **37 ms** | `AA360+MQ3354` via ORD, dep 07:00 → arr 12:46 (7h46) |
| ALB→SNA | **28 ms** | `DL1498+DL304` via DTW, dep 06:10 → arr 10:25 (7h15) |

`AA360+MQ3354` is `CARRIER_FAMILY` doing its job: a real American Eagle
connection that strict operating-carrier equality would have dropped.

Cross-airport comparison used to be a limitation here — "which of these arrives
earliest in absolute time" was unanswerable across timezones. It no longer is:
order by `f.scheduled_arrival_utc` and the comparison is exact regardless of where
the destinations are. Keep `scheduled_arrival_time` for *display* and for the
deadline predicate, since a passenger's "before 3pm" means 3pm where they land.

### Depart-after-a-time

The mirror image of the deadline query, and the more common one on a travel site:
"leave after 08:00." It needs **neither** guard above — it constrains the *first*
leg's departure, which is local at the origin and is the one timestamp with no
date ambiguity at all. It does still need `localdatetime()`, for the same
type-mismatch reason.

```cypher
MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {code: $origin})
WHERE first.flightdate = date($date)
  // localdatetime, NOT datetime: comparing a LOCAL DATETIME against a ZONED one
  // yields NULL, so WHERE would discard every row. Same trap as the deadline
  // query; see "Deadline filters" above.
  AND first.scheduled_departure_time >= localdatetime($after)
MATCH p = (first)-[:CONNECTS_TO]->{0,2}(last:Schedule)
MATCH (last)-[:ARRIVES_AT]->(:Airport {code: $dest})
WITH nodes(p) AS legs, relationships(p) AS conns
// Acyclicity — see below. Not optional.
WITH legs, conns, [legs[0].origin] + [x IN legs | x.dest] AS airports
WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                             WHERE NOT airports[i] IN airports[0..i]])
RETURN size(legs) - 1 AS stops,
       [x IN legs | x.reporting_airline +
                    toString(x.flight_number_reporting_airline)] AS flights,
       airports AS route,
       legs[0].scheduled_departure_time AS departs,
       reduce(t = 0, x IN legs | t + x.scheduled_duration_minutes) +
       reduce(t = 0, c IN conns | t + c.layover_minutes) AS total_minutes
ORDER BY total_minutes
LIMIT $limit
```

`$after` is a local wall-clock string at the **origin**, e.g.
`"2025-07-18T08:00:00"`. Measured, `{0,3}`, `LIMIT 8`, warm, departing after
08:00 on 2025-07-18:

| route | wall clock | best itinerary |
|---|---|---|
| BOI→ALB | **22 ms** | `WN1382+WN3900` via MDW, dep 14:50, 375 min |
| PVD→BOI | **45 ms** | `OO5422+OO5290` via ORD, dep 16:29, 503 min |
| FCA→SAV | **11 ms** | `AA2939+AA2327` via DFW, dep 11:46, 533 min |
| LGA→DFW | **180 ms** | `AA3289` nonstop, dep 08:30, 227 min |

### Itineraries revisit airports unless you say otherwise

`CONNECTS_TO` carries a backtrack guard, but it is **pairwise** — it forbids
`s2.dest = s1.origin` on a single edge. That does not compose over three legs, so
a quantified path can leave an airport and come back to it. Measured on
2025-07-18, LGA→DFW at `{0,3}`: **2,115 of 11,488 paths (18.41%)** revisit an
airport, and **385 return to the origin outright**:

```
LGA -> MIA -> CLT -> LGA    AA970, AA985, AA1060
LGA -> MIA -> DFW -> LGA    AA970, AA1199, AA2708
```

Each of those is a legal chain of connections that no airline would sell. Over a
broader sample of 3-leg paths the rate is **1.74%**; it climbs with hop count and
with hub density, which is why it is invisible at `{0,1}` and material at `{0,3}`.

The fix is the projection-and-compare in the query above:

```cypher
WITH legs, conns, [legs[0].origin] + [x IN legs | x.dest] AS airports
WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                             WHERE NOT airports[i] IN airports[0..i]])
```

**Cypher's `ACYCLIC` and `TRAIL` path modes do not help here**, which is the
non-obvious part. They deduplicate the nodes and relationships *on the path*, and
the path's nodes are `Schedule` (flight) nodes — always distinct, since a given
flight appears once. The entity that repeats is an `Airport`, which is not on the
path at all; it is reached through `DEPARTS_FROM`/`ARRIVES_AT`. Verified on the
same route and date:

| path mode | paths | airport revisits |
|---|---|---|
| default (`WALK`) | 11,488 | 2,115 |
| `ACYCLIC` | 11,488 | 2,115 |
| `TRAIL` | 11,488 | 2,115 |

Identical. The guard has to be written against airport codes.

**Cost: none worth measuring.** A/B over six routes at `{0,3}`, three runs each,
minimum taken — the guard is within ±5%, i.e. inside run-to-run noise, and the
top-ranked itinerary is unchanged on every route (it prunes only paths that were
never going to rank):

| route | unguarded | guarded |
|---|---|---|
| LGA→DFW | 413 ms | 436 ms |
| ATL→SEA | 487 ms | 509 ms |
| ORD→LAX | 612 ms | 584 ms |
| BOI→ALB | 81 ms | 88 ms |
| PVD→BOI | 140 ms | 146 ms |
| FCA→SAV | 18 ms | 20 ms |

### Latency across real routes

The tables above are hand-picked routes. Across **40 origin/destination pairs
drawn from the graph's 60 busiest origins**, top-20 sorted, acyclicity guard on,
repeat-warm cache.

**Two conditions dominate the cost — the departure-time filter and how hub-heavy
the sample is — so both are varied here rather than chosen.** An earlier revision
gave only the `depart_after 08:00` row and did not carry the condition into
README.md or CLAUDE.md, where it was read as the unfiltered cost and understated
`{0,3}` by roughly 2.7x. The revision after that fixed the filter but reported a
single sample, which is how whole-day `{0,2}` came to be published as "0 / 40".
Ranges below are the spread over three runs:

| depth | filter | sample | p50 | p95 | over 200 ms |
|---|---|---|---|---|---|
| `{0,2}` | `depart_after 08:00` | top 60 origins | **23–28 ms** | **54–66 ms** | **0 / 40** |
| `{0,2}` | `depart_after 08:00` | top 20 origins | **41–44 ms** | **61–67 ms** | **0 / 40** |
| `{0,2}` | none (whole day) | top 60 origins | 45–48 ms | 171–220 ms | 0–2 / 40 |
| `{0,2}` | none (whole day) | top 20 origins | 118–125 ms | 194–253 ms | 1–3 / 40 |
| `{0,3}` | `depart_after 08:00` | top 60 origins | 70–81 ms | 212–223 ms | 2–3 / 40 |
| `{0,3}` | `depart_after 08:00` | top 20 origins | 140–154 ms | 232–243 ms | 12–14 / 40 |
| `{0,3}` | none (whole day) | top 60 origins | 251–280 ms | 573–672 ms | 25–27 / 40 |
| `{0,3}` | none (whole day) | top 20 origins | 492–506 ms | 635–657 ms | **39 / 40** |

**`{0,2}` with a departure-time filter is the only configuration that clears 200 ms
unconditionally.** Whole-day `{0,2}` sits *on* the budget rather than under it — p95
landed at 171, 192, 205 and 229 ms across four runs, with 0 to 3 pairs over — so it
must be quoted as a range. `{0,3}` misses on most pairs unfiltered and on up to a
third with a morning filter. If you need a hard 200 ms ceiling, serve `{0,2}`
filtered and treat `{0,3}` as a widen-on-demand second request.

**The sample is part of the result.** Concentrating the same 40 pairs on the top 20
origins instead of the top 60 roughly triples whole-day `{0,2}` p50 and takes
filtered `{0,3}` from 2 of 40 over budget to 13, with identical query, graph and
date. Dense hub-to-hub routes have the largest candidate sets (LGA→DFW enumerates
11,488 itineraries to return 20), so a benchmark that draws pairs from a wider
origin pool reports a faster system. State the pool.

These are repeat-warm, not first-hit: re-running `{0,3}` LGA→DFW twelve times in a
row converges to ~440 ms, consistent with the 436 ms in the guard-cost table above,
so the cost is path expansion rather than a cold page cache. Measured against the
full-year graph (6,898,743 `Schedule` nodes) on a date carrying 623,508
`CONNECTS_TO` edges — the same edge count as the CI fixture, so route density is
not what separates the two conditions.

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
  an all-skipped run look green. `tests/ci_verify_loaded.py <date>` runs first and
  fails if the graph does not hold what the fixture should have produced. It takes
  the date because node counts alone cannot tell whether `--solve-offsets` ran —
  a `Schedule` exists either way — so it also requires both `scheduled_*_utc`
  properties on every flight of that day.
- The regression tests assert the defect is *absent* **and** that the fixture
  could have exposed it — 915 legs landing on a later local day, 110,642
  cross-family edges, and 2 dateline airports are all asserted non-zero.
  Otherwise a dataset without those properties would pass while gating nothing.
  Verified by injecting one bad edge of each kind into 623,508 good ones: each
  trips its own assertion and nothing else.

Two defects that validation found, both now fixed in the builder:

**Inbound legs that land the next day.** Edges connected off a leg whose stored
arrival looked same-day but which really arrives the following morning —
`AA1009 LAX→ORD dep 22:59 arr 05:18` spliced to a 06:55 ORD departure. The builder
now compares `scheduled_arrival_utc` against `scheduled_departure_utc`: both are
absolute instants, so the layover is a positive interval by construction and needs
no tolerance at all.

The first attempt at this used a heuristic on the *local* pair — negative apparent
duration that reconciles with the block time once 1440 minutes are added, within
±180. It removed 17,502 edges and looked like a fix, but the tolerance cannot span
the widest US offset gaps, so **11,975 impossible edges survived it** (`AA6`
HNL→DFW needs 240–360 minutes to reconcile and read as same-day). Switching to UTC
removed exactly those and nothing else — verified as a strict subset: all 623,508
surviving edges were already present, zero new ones appeared.

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

**Being strict does not make those two safe, and the residual is not small.**
Measured over the seven built dates: **348,000 of 4,028,572 edges (8.64%)** are
`OO`→`OO` (283,368) or `YX`→`YX` (64,632). Every one passes the carrier predicate,
because the operating codes match — but SkyWest sells the same aircraft as Delta
Connection, United Express, American Eagle and Alaska SkyWest depending on the
route, so an `OO` arrival sold under one mainline connecting to an `OO` departure
sold under another is **not** a sellable itinerary. Which mainline applies is
**not recoverable from this feed at all**: it needs the marketing carrier, and BTS
On-Time Performance does not publish one. Treat that 8.64% as an upper bound on
edges the strict rule cannot vouch for — closing it requires a schedule source
with marketing carriers (OAG, ATPCO, a GDS feed), not a better query.

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
| 1 day | 514,000-625,000 (mean 577,228) |
| full year (365 dates, extrapolated) | ~211,000,000 |

A full year is ~10x the rest of the graph (measured: 20,703,162 other relationships), and a date-specific search never needs
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

Note these modes work **here** because the path's nodes *are* the airports. Over
`CONNECTS_TO` the path's nodes are flights, so neither mode prevents an airport
from repeating — see "Itineraries revisit airports" above.

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

Note the nonstop block times (~257 min) differ from local
`arrival − departure` (~197 min) by exactly the one-hour Eastern→Central offset.
That gap is why the local pair must never be subtracted; `scheduled_duration_minutes`
and the UTC pair both give 257.

## Notes and caveats

- **The load test drives the served query.** `neo4j_flight_load_test.py` calls
  `flight_search.py` and holds no Cypher of its own beyond one airport-sampling
  query, gated by `test_load_test_holds_no_cypher_of_its_own`. The older
  `UNION ALL` form with the `CASE` duration idiom is deleted, not migrated. (This
  bullet used to say the opposite; it was stale.)
- **Layover bounds have one authority: the `CONNECTS_TO` edge**, built at
  [45, 300] minutes in `create_connects_to()`. Since `flight_search.py` traverses
  that edge and the load test calls `flight_search.py`, no client restates it —
  the old 300/720/1200 spread across this repo is gone. The hand-rolled explicit
  join above still takes `$min_layover`/`$max_layover`, because it does the join
  itself.
- **Without a carrier predicate, most returned itineraries are unsellable.**
  A 1-stop query with no carrier condition freely splices any airline to any
  other — including carriers that interline with nobody. But the predicate cuts
  both ways: a *strict* `s1.reporting_airline = s2.reporting_airline` is too
  tight, because BTS reports operating carriers and so separates mainlines from
  their own wholly-owned regional feeders. `CONNECTS_TO` compares
  `CARRIER_FAMILY`-mapped codes instead; the explicit join above still uses the
  strict form and will miss American Eagle connections.
- **No APOC.** Everything here is plain Cypher, for Aura compatibility.

### Scope: three things this cannot answer

Stated plainly because none of them is a query problem.

- **Routing covers the 7 dates whose edges are built** — `2025-07-14 … 2025-07-20`,
  4,028,572 edges — out of 365 dates of loaded `Schedule` nodes. Every other date
  returns zero itineraries, correctly and with no error. `GET /dates` reports the
  real coverage; a full year would be ~211M edges.
- **No price, seat availability, booking class, or per-airport minimum connection
  time.** The flat [45, 300] window stands in for MCT everywhere. These queries
  answer "is this flyable as scheduled", not "is this purchasable".
- **8.64% of edges carry an unresolvable marketing carrier** — the `OO`/`YX`
  measurement above. Not a modelling gap; BTS On-Time Performance simply has no
  marketing-carrier column.
