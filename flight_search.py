#!/usr/bin/env python3
"""
Itinerary search over the flight graph.
=======================================

The callable interface to the graph. `api.py` puts HTTP in front of this, and
`neo4j_flight_load_test.py` drives it under load — neither holds a copy of the
Cypher, which is the point: there is one query to review and one place a fix
lands.

Search is **one quantified-path query** spanning 0 to `max_stops` stops, ranked
globally by total elapsed journey. Set `min_stops == max_stops` to search a single
depth exactly.

It is worth recording why it is not iterative deepening, since that is what this
repo did before and it sounds like the cheaper option. Deepening — direct flights
first, 1-stop only if fewer than `limit` came back, then 2-stop — was implemented
and measured against the single pass over the same 40 pairs drawn from the 60
busiest origins, repeat-warm, `limit=20`:

                               depart_after=08:00        whole day
    iterative deepening 0..2   p50  26 ms  p95 1323 ms   p50 103 ms  p95 249 ms
    single {0,2} pass          p50  39 ms  p95   63 ms   p50  96 ms  p95 204 ms

Deepening wins the median in the filtered case and loses the tail catastrophically
— 4 of 40 pairs over 200 ms either way, with a p95 twenty times worse — because a
route that needs depth pays for the shallow queries *and* the deep one, and the
deep query is no cheaper for having run them. Tail latency is what a serving
budget is written against, so the median is the wrong thing to optimise here.

It also gives up global ranking: a 1-stop that beats every nonstop is unreachable
once the nonstops fill `limit`. Nonstops still sort first in the single pass
whenever they exist, since a nonstop is essentially always the shortest total
journey — so deepening bought nothing and cost the tail.

Both columns are shown because a departure-time filter dominates the cost, and
quoting a figure without its filter is how this repo ended up publishing 36 ms
next to a 400 ms reality. See the latency table in README.md.

Everything else that makes an itinerary valid is enforced by the `CONNECTS_TO`
edge at load time — layover window, same marketing carrier, chronological
sequencing. See `create_connects_to()` in `load_bts_data.py`. The single
path-level property the edge cannot express is "no airport twice", so that guard
lives here (`_ACYCLIC_GUARD`) and must not be dropped: without it, 18.41% of
LGA->DFW itineraries at 3 stops revisit an airport and some fly back to where
they started.

    from flight_search import search_itineraries
    for it in search_itineraries("LGA", "BOI", "2025-07-18", depart_after="09:00"):
        print(it.flights, it.total_minutes)

Requires `--solve-offsets` and `--build-connections` to have run for the dates
being searched.
"""

import os
import threading
from dataclasses import dataclass, field
from datetime import date as date_cls
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Serving default. Two stops is as deep as a US domestic itinerary plausibly
# needs, and {0,3} costs far too much to be the default. Measured over 40 pairs
# from the 60 busiest origins, repeat-warm, limit=20, guard on:
#
#                     depart_after=08:00           whole day
#   {0,2}    p50  35 ms  p95  64 ms   0/40 >200   p50  85 ms  p95 175 ms   0/40
#   {0,3}    p50 116 ms  p95 243 ms   5/40 >200   p50 395 ms  p95 595 ms  34/40
#
# Two stops holds a 200 ms budget under both conditions; three stops misses on 85%
# of pairs with no time filter. Depth stays a caller's explicit request.
DEFAULT_MAX_STOPS = 2
DEFAULT_LIMIT = 20

# A pool size, not a connection count: the driver opens connections lazily and
# keeps up to this many. The old load test built one driver per simulated user,
# which put driver and TLS setup inside every measurement.
DEFAULT_POOL_SIZE = 50

_driver = None
_driver_lock = threading.Lock()


class SearchError(ValueError):
    """Invalid search input. Distinct from a driver or Cypher failure."""


def get_driver():
    """
    Return the process-wide driver, creating it on first use.

    One driver per process, not per request or per simulated user: it is a
    connection pool and a thread-safe one, so sharing it is both correct and the
    whole reason pooling helps. Double-checked under a lock so concurrent first
    requests cannot each build one.
    """
    global _driver
    if _driver is None:
        with _driver_lock:
            if _driver is None:
                # override=True so .env wins over a stale exported password from
                # another project — the failure mode that silently authenticates
                # against the wrong database (see CLAUDE.md).
                load_dotenv(override=True)
                uri = os.getenv("NEO4J_URI")
                if not uri:
                    raise RuntimeError(
                        "NEO4J_URI is not set. Copy .env.example to .env and fill "
                        "in your credentials."
                    )
                _driver = GraphDatabase.driver(
                    uri,
                    auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
                    max_connection_pool_size=int(
                        os.getenv("NEO4J_POOL_SIZE", DEFAULT_POOL_SIZE)
                    ),
                )
    return _driver


def get_database() -> str:
    """
    The database every query in this module runs against.

    `NEO4J_DATABASE` is set per-environment on purpose (see CLAUDE.md), so this
    must not be hard-coded or cached at import time.
    """
    return os.getenv("NEO4J_DATABASE", "neo4j")


def close_driver():
    """Close the shared driver. For interpreter/app shutdown."""
    global _driver
    with _driver_lock:
        if _driver is not None:
            _driver.close()
            _driver = None


@dataclass(frozen=True)
class Itinerary:
    """One bookable itinerary. Times are local wall clock at their own airport."""

    stops: int
    flights: List[str]
    route: List[str]
    carriers: List[str]
    departs: Any
    arrives: Any
    departs_utc: Any
    arrives_utc: Any
    air_minutes: int
    layover_minutes: List[int] = field(default_factory=list)
    total_minutes: int = 0

    def as_dict(self) -> Dict[str, Any]:
        """JSON-ready, with temporal types as ISO 8601 strings."""
        return {
            "stops": self.stops,
            "flights": list(self.flights),
            "route": list(self.route),
            "carriers": list(self.carriers),
            "departs": _iso(self.departs),
            "arrives": _iso(self.arrives),
            "departs_utc": _iso(self.departs_utc),
            "arrives_utc": _iso(self.arrives_utc),
            "air_minutes": self.air_minutes,
            "layover_minutes": list(self.layover_minutes),
            "total_minutes": self.total_minutes,
        }


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


# The path-level guard the CONNECTS_TO edge cannot express. `CONNECTS_TO` forbids
# an immediate backtrack (s2.dest <> s1.origin), but that is pairwise and does not
# compose: LGA->MIA->CLT->LGA satisfies it at every step. Cypher's ACYCLIC/TRAIL
# modes do not help either — they dedupe path *nodes*, which here are Schedule
# nodes and always distinct; the repeating entity is an Airport reached off-path.
# So compare the codes: keep the path only if no element of `airports` appeared
# earlier in it.
_ACYCLIC_GUARD = """
WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                             WHERE NOT airports[i] IN airports[0..i]])
"""

# One template, with the path pattern substituted per depth. The quantifier cannot
# be a query parameter in Cypher — hence the substitution — so both bounds are
# coerced to int before they get anywhere near this string.
_SEARCH_TEMPLATE = """
MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {{code: $origin}})
WHERE first.flightdate = date($date)
  {depart_filter}
{path_match}
MATCH (last)-[:ARRIVES_AT]->(:Airport {{code: $dest}})
  {arrive_filter}
WITH nodes(p) AS legs, relationships(p) AS conns
WITH legs, conns, [legs[0].origin] + [x IN legs | x.dest] AS airports
{acyclic_guard}
RETURN size(legs) - 1 AS stops,
       [x IN legs | x.reporting_airline +
                    toString(x.flight_number_reporting_airline)] AS flights,
       airports AS route,
       [x IN legs | x.reporting_airline] AS carriers,
       legs[0].scheduled_departure_time AS departs,
       legs[-1].scheduled_arrival_time AS arrives,
       legs[0].scheduled_departure_utc AS departs_utc,
       legs[-1].scheduled_arrival_utc AS arrives_utc,
       // Never subtract the local endpoints: they are clocks at different
       // airports. Real block times plus real layovers is exact, and is what
       // the ORDER BY below ranks on.
       reduce(t = 0, x IN legs | t + x.scheduled_duration_minutes) AS air_minutes,
       [c IN conns | c.layover_minutes] AS layover_minutes,
       reduce(t = 0, x IN legs | t + x.scheduled_duration_minutes) +
       reduce(t = 0, c IN conns | t + c.layover_minutes) AS total_minutes
ORDER BY total_minutes, departs_utc
LIMIT $limit
"""

# localdatetime(), NOT datetime(). scheduled_departure_time and
# scheduled_arrival_time are LOCAL DATETIMEs; comparing either against a ZONED
# datetime() yields NULL rather than false, so WHERE discards every row and a
# route with 40 valid itineraries returns zero with no error at all. This is the
# one silent trap left in the model — see "Deadline filters" in
# ROUTING_QUERY_REFERENCE.md.
_DEPART_FILTER = "AND first.scheduled_departure_time >= localdatetime($depart_after)"

# No overnight guard. scheduled_arrival_time carries the DESTINATION's calendar
# date because --solve-offsets rewrites it from the UTC instant, so a red-eye
# landing at 00:40 compares as the next day, which is when it lands. Every
# query-side version of this guard was wrong; the doc records why.
_ARRIVE_FILTER = "WHERE last.scheduled_arrival_time < localdatetime($arrive_before)"


def _normalise_airport(code: str, label: str) -> str:
    if not isinstance(code, str) or not code.strip():
        raise SearchError(f"{label} must be a non-empty airport code")
    normalised = code.strip().upper()
    if not (normalised.isalpha() and len(normalised) == 3):
        raise SearchError(
            f"{label} must be a 3-letter IATA code, got {code!r}. "
            "This graph is US-domestic BTS data."
        )
    return normalised


def _normalise_date(value) -> str:
    if isinstance(value, (date_cls, datetime)):
        return value.strftime("%Y-%m-%d")
    if not isinstance(value, str):
        raise SearchError(f"date must be YYYY-MM-DD, got {value!r}")
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise SearchError(f"date must be YYYY-MM-DD, got {value!r}") from None


def _normalise_bound(value, search_date: str, label: str) -> Optional[str]:
    """
    Accept "HH:MM", "HH:MM:SS" or a full "YYYY-MM-DDTHH:MM(:SS)" and return a
    string `localdatetime()` will parse.

    A bare time is resolved against `search_date` because that is what a caller
    passing `depart_after="09:00"` means. `arrive_before` is deliberately *not*
    rolled to the next day for red-eyes: a caller who wants to allow landing
    after midnight has to say so with a full timestamp, since silently extending
    a deadline past midnight would return itineraries the caller excluded.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if not isinstance(value, str) or not value.strip():
        raise SearchError(f"{label} must be HH:MM or YYYY-MM-DDTHH:MM")
    text = value.strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return f"{search_date}T{parsed.strftime('%H:%M:%S')}"
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.strftime("%Y-%m-%dT%H:%M:%S")
    raise SearchError(f"{label} must be HH:MM or YYYY-MM-DDTHH:MM, got {value!r}")


def build_search_query(
    min_stops: int, max_stops: int, depart_after=None, arrive_before=None
) -> str:
    """
    Render the Cypher for one depth. Exposed so tests can assert on the text
    without a database, and so `EXPLAIN`-ing what the service actually runs is
    a one-liner rather than a reconstruction.
    """
    min_stops, max_stops = int(min_stops), int(max_stops)
    if min_stops < 0 or max_stops < min_stops:
        raise SearchError(
            f"need 0 <= min_stops <= max_stops, got {min_stops}, {max_stops}"
        )
    if max_stops == 0:
        # Cypher rejects a quantifier bounded by zero — both {0,0} and {0} are
        # syntax errors, not empty matches — so the nonstop case is written as a
        # single-node path. That is not a workaround: `nodes(p)` is one leg and
        # `relationships(p)` is empty, exactly as the quantified form would bind
        # them, so every clause below is shared and there is no second RETURN to
        # drift out of step with the first.
        path_match = "MATCH p = (first)\nWITH p, first AS last"
    else:
        path_match = (
            "MATCH p = (first)-[:CONNECTS_TO]->"
            f"{{{min_stops},{max_stops}}}(last:Schedule)"
        )
    return _SEARCH_TEMPLATE.format(
        path_match=path_match,
        depart_filter=_DEPART_FILTER if depart_after is not None else "",
        arrive_filter=_ARRIVE_FILTER if arrive_before is not None else "",
        acyclic_guard=_ACYCLIC_GUARD,
    )


def search_itineraries(
    origin: str,
    dest: str,
    date: str,
    depart_after=None,
    arrive_before=None,
    max_stops: int = DEFAULT_MAX_STOPS,
    min_stops: int = 0,
    limit: int = DEFAULT_LIMIT,
    driver=None,
    database: Optional[str] = None,
) -> List[Itinerary]:
    """
    Find itineraries from `origin` to `dest` departing on `date`.

    `depart_after` is local at the origin, `arrive_before` local at the
    destination; both take "HH:MM" or a full "YYYY-MM-DDTHH:MM". Results are
    ordered by total elapsed journey — real block times plus real layovers —
    ascending, across all stop counts at once.

    Raises `SearchError` on bad input. A failure to reach Neo4j propagates as the
    driver's own exception, which is a different problem and should look like one.
    """
    origin = _normalise_airport(origin, "origin")
    dest = _normalise_airport(dest, "dest")
    if origin == dest:
        raise SearchError(f"origin and dest are both {origin}")
    search_date = _normalise_date(date)
    depart_after = _normalise_bound(depart_after, search_date, "depart_after")
    arrive_before = _normalise_bound(arrive_before, search_date, "arrive_before")

    min_stops, max_stops, limit = int(min_stops), int(max_stops), int(limit)
    if limit < 1:
        raise SearchError(f"limit must be >= 1, got {limit}")
    if min_stops < 0 or max_stops < min_stops:
        raise SearchError(
            f"need 0 <= min_stops <= max_stops, got {min_stops}, {max_stops}"
        )

    params = {
        "origin": origin,
        "dest": dest,
        "date": search_date,
        "limit": limit,
    }
    if depart_after is not None:
        params["depart_after"] = depart_after
    if arrive_before is not None:
        params["arrive_before"] = arrive_before

    driver = driver or get_driver()
    database = database or get_database()

    query = build_search_query(min_stops, max_stops, depart_after, arrive_before)
    with driver.session(database=database) as session:
        return [_to_itinerary(record) for record in session.run(query, **params)]


def _to_itinerary(record) -> Itinerary:
    return Itinerary(
        stops=record["stops"],
        flights=record["flights"],
        route=record["route"],
        carriers=record["carriers"],
        departs=record["departs"],
        arrives=record["arrives"],
        departs_utc=record["departs_utc"],
        arrives_utc=record["arrives_utc"],
        air_minutes=record["air_minutes"],
        layover_minutes=record["layover_minutes"],
        total_minutes=record["total_minutes"],
    )


def is_searchable(date, driver=None, database: Optional[str] = None) -> bool:
    """
    Whether `date` has `CONNECTS_TO` edges, i.e. whether searching it can return
    anything at all.

    Cheap (one indexed lookup with `LIMIT 1`) and meant to be called only when a
    search came back empty, to tell "no routes on this city pair" apart from "this
    date was never built". Those are very different answers and both look like an
    empty list.
    """
    driver = driver or get_driver()
    database = database or get_database()
    query = """
    MATCH (s:Schedule {flightdate: date($date)})-[:CONNECTS_TO]->()
    RETURN count(*) > 0 AS built LIMIT 1
    """
    with driver.session(database=database) as session:
        record = session.run(query, date=_normalise_date(date)).single()
    return bool(record and record["built"])


def searchable_dates(driver=None, database: Optional[str] = None) -> List[str]:
    """
    The dates itinerary search actually works on — those with `CONNECTS_TO` edges,
    not merely those with flights.

    Worth having as its own call because the distinction bites: the graph can hold
    a full year of `Schedule` nodes while only a handful of days have been through
    `--solve-offsets` and `--build-connections`, and searching any other day
    returns nothing with no indication why.
    """
    driver = driver or get_driver()
    database = database or get_database()
    query = """
    MATCH (s:Schedule)-[:CONNECTS_TO]->()
    RETURN DISTINCT s.flightdate AS d ORDER BY d
    """
    with driver.session(database=database) as session:
        return [record["d"].isoformat() for record in session.run(query)]
