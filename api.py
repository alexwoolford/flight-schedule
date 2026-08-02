#!/usr/bin/env python3
"""
HTTP interface to itinerary search.
===================================

A thin layer over `flight_search.py`: parameter parsing, error mapping, and one
pooled driver shared by every request. No Cypher lives here.

    uvicorn api:app --reload
    curl 'localhost:8000/itineraries?origin=LGA&dest=BOI&date=2025-07-18&depart_after=09:00'

Endpoints:

    GET /itineraries   search
    GET /dates         the dates search actually works on
    GET /health        liveness + whether the graph is reachable and loaded

The driver is opened once at startup and closed at shutdown, so no request pays
for connection or TLS setup. Requires `--solve-offsets` and `--build-connections`
to have run for the dates being searched; `GET /dates` reports which those are.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from neo4j.exceptions import Neo4jError, ServiceUnavailable

import flight_search
from flight_search import DEFAULT_LIMIT, DEFAULT_MAX_STOPS, SearchError

# Bound on `max_stops`. Three stops is already beyond what a US domestic
# itinerary plausibly needs, and the traversal cost grows fast enough that leaving
# it unbounded would let one request degrade the service for everyone: measured
# p95 595 ms at {0,3} against 175 ms at {0,2} with no departure-time filter (243 ms
# vs 64 ms with one), and it keeps climbing with depth.
MAX_ALLOWED_STOPS = 3
MAX_ALLOWED_LIMIT = 100


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Open the pool once, before the first request, so no request pays for
    # connection setup and a bad NEO4J_URI fails at boot rather than looking
    # like a slow search.
    flight_search.get_driver()
    yield
    flight_search.close_driver()


app = FastAPI(
    title="Flight itinerary search",
    description=(
        "Itinerary search over a Neo4j graph of real US DOT BTS scheduled "
        "flights. Answers 'is this flyable as scheduled', not 'is this "
        "purchasable' — the source data carries no price or seat availability."
    ),
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """
    Liveness plus a real check that the graph is queryable and non-empty.

    Deliberately not just `return {"ok": True}`: the failure this needs to catch
    is a running service in front of an empty or unreachable database, which is
    exactly what a static response hides.
    """
    try:
        driver = flight_search.get_driver()
        with driver.session(database=flight_search.get_database()) as s:
            record = s.run(
                "MATCH (s:Schedule)-[:CONNECTS_TO]->() "
                "RETURN count(*) AS edges LIMIT 1"
            ).single()
        edges = record["edges"] if record else 0
    except (ServiceUnavailable, Neo4jError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=f"graph unavailable: {exc}")
    if not edges:
        raise HTTPException(
            status_code=503,
            detail=(
                "no CONNECTS_TO edges — run load_bts_data.py --solve-offsets "
                "then --build-connections for the dates you want to search"
            ),
        )
    return {"status": "ok", "connects_to_edges": edges}


@app.get("/dates")
def dates():
    """
    The dates itinerary search works on — those with `CONNECTS_TO` edges, which is
    a strict subset of the dates that have flights. Both steps are per-date on
    purpose: offsets are DST-dependent, and a full year of edges would be ~228M.
    """
    try:
        return {"dates": flight_search.searchable_dates()}
    except (ServiceUnavailable, Neo4jError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=f"graph unavailable: {exc}")


@app.get("/itineraries")
def itineraries(
    origin: str = Query(..., description="3-letter IATA origin, e.g. LGA"),
    dest: str = Query(..., description="3-letter IATA destination, e.g. BOI"),
    date: str = Query(..., description="departure date, YYYY-MM-DD"),
    depart_after: Optional[str] = Query(
        None, description="earliest local departure at the origin, HH:MM"
    ),
    arrive_before: Optional[str] = Query(
        None, description="latest local arrival at the destination, HH:MM"
    ),
    max_stops: int = Query(DEFAULT_MAX_STOPS, ge=0, le=MAX_ALLOWED_STOPS),
    min_stops: int = Query(0, ge=0, le=MAX_ALLOWED_STOPS),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_ALLOWED_LIMIT),
):
    """
    Search itineraries, cheapest total journey first.

    `total_minutes` is real BTS block time plus real layovers, never a difference
    of the two local timestamps — those are clocks at different airports, and
    subtracting them is wrong for about half of all flights. `departs`/`arrives`
    are local wall clock at their own airport; `departs_utc`/`arrives_utc` are the
    absolute instants, which is what to compare across timezones.

    An empty `itineraries` list is a valid answer — plenty of city pairs genuinely
    have no same-carrier routing within the layover window on a given day. But an
    unbuilt date returns empty too, so on a zero-result search the response also
    carries `date_is_searchable`, which distinguishes the two.
    """
    try:
        results = flight_search.search_itineraries(
            origin=origin,
            dest=dest,
            date=date,
            depart_after=depart_after,
            arrive_before=arrive_before,
            max_stops=max_stops,
            min_stops=min_stops,
            limit=limit,
        )
    except SearchError as exc:
        # The caller's input is wrong, not the service — 400, with the reason.
        raise HTTPException(status_code=400, detail=str(exc))
    except (ServiceUnavailable, Neo4jError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=f"graph unavailable: {exc}")

    body = {
        "origin": origin.strip().upper(),
        "dest": dest.strip().upper(),
        "date": date,
        "count": len(results),
        "itineraries": [it.as_dict() for it in results],
    }
    if not results:
        # Only on an empty result, so the common path pays nothing. "No routes on
        # this pair" and "this date was never built" are both an empty list, and
        # the caller cannot tell them apart without this.
        try:
            body["date_is_searchable"] = flight_search.is_searchable(date)
        except (ServiceUnavailable, Neo4jError, RuntimeError):
            # Diagnostics must not turn a valid empty answer into an error.
            pass
    return body
