#!/usr/bin/env python3
"""
Fail loudly if the graph under test is not actually loaded.
===========================================================

The database-backed fixtures in `conftest.py` skip cleanly when Neo4j is
unreachable or the graph is empty. That is the right behaviour on a developer
laptop, and the wrong behaviour in CI: a broken connection or a silently
truncated load would surface as "all skipped", and the job would report success
having asserted nothing at all.

This script is the guard. It runs between the load steps and the assertions in
`.github/workflows/ci.yml`, and exits non-zero if the graph does not hold what
the fixture should have produced.

Not named `test_*.py` on purpose — it is a CI pre-flight check, not part of the
collected suite.

    python tests/ci_verify_loaded.py 2025-07-18

Dates are the days routing is exercised on — the ones `--solve-offsets` and
`--build-connections` were given. Node and edge floors are checked graph-wide;
UTC-property coverage is checked only on those dates, since both steps are
deliberately date-scoped.
"""

import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Floors rather than exact counts: the fixture is fixed, but the connection
# policy may legitimately move the edge total (--min-layover, --strict-carrier).
# Measured on tests/fixtures/bts_flights_2025_07_18.parquet with default policy:
# 21,376 Schedule / 341 Airport / 14 Carrier / 623,508 CONNECTS_TO. (A full
# month reaches 352 airports and 15 carriers — a single day misses a few.)
NODE_FLOORS = {"Schedule": 20000, "Airport": 300, "Carrier": 10}
EDGE_FLOORS = {
    "DEPARTS_FROM": 20000,
    "ARRIVES_AT": 20000,
    "OPERATED_BY": 20000,
    "CONNECTS_TO": 500000,
}

# Counting nodes is not enough. `--solve-offsets` is a separate step that writes
# the UTC properties every cross-airport comparison depends on, and a Schedule
# node exists whether or not it ran. If it were skipped or silently wrote only a
# subset, every floor above would still clear and the timezone assertions in
# test_timezone_offsets.py would skip rather than fail.
#
# Scoped to the dates under test, not the whole graph, because --solve-offsets is
# deliberately date-scoped: a database can legitimately hold a full year of
# Schedule nodes with UTC times on only the days routing is exercised on. A
# whole-graph check would fail on exactly that correct setup.
COVERAGE_QUERY = """
UNWIND $dates AS d
MATCH (s:Schedule {flightdate: date(d)})
RETURN d AS date,
       count(s) AS total,
       count(s.scheduled_departure_utc) AS dep_utc,
       count(s.scheduled_arrival_utc) AS arr_utc,
       count(s.scheduled_arrival_time) AS arr_local
"""


def main(dates) -> int:
    # override=True to match conftest.py and the loader: .env is authoritative
    # everywhere in this repo, and this script must resolve credentials the same
    # way as the tests it gates, or it can pass against a different database
    # than they check.
    load_dotenv(override=True)
    uri = os.getenv("NEO4J_URI")
    if not uri:
        print("NEO4J_URI is not set", file=sys.stderr)
        return 1
    if not dates:
        print(
            "usage: python tests/ci_verify_loaded.py DATE [DATE ...]\n"
            "  the dates --solve-offsets and --build-connections were given",
            file=sys.stderr,
        )
        return 1

    driver = GraphDatabase.driver(
        uri, auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    )
    try:
        with driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
            counts = {
                label: session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()[
                    "c"
                ]
                for label in NODE_FLOORS
            }
            counts.update(
                {
                    rel: session.run(
                        f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c"
                    ).single()["c"]
                    for rel in EDGE_FLOORS
                }
            )
            coverage = list(session.run(COVERAGE_QUERY, dates=dates))
    finally:
        driver.close()

    floors = {**NODE_FLOORS, **EDGE_FLOORS}
    short = {k: v for k, v in counts.items() if v < floors[k]}

    for name in floors:
        marker = "✗" if name in short else "✓"
        print(f"  {marker} {name:<14} {counts[name]:>9,}  (floor {floors[name]:,})")

    # A requested date with no flights at all yields no row here, which is its own
    # failure: it means the load never covered the day the assertions will query.
    gaps = [
        f"{d} has no Schedule nodes"
        for d in dates
        if d not in {row["date"] for row in coverage}
    ]
    for row in coverage:
        total = row["total"]
        missing = [
            prop for prop in ("dep_utc", "arr_utc", "arr_local") if row[prop] != total
        ]
        marker = "✗" if missing else "✓"
        print(f"  {marker} {row['date']}   {total:>9,} flights, UTC times on ", end="")
        print(", ".join(f"{row[p]:,} {p}" for p in ("dep_utc", "arr_utc", "arr_local")))
        gaps += [
            f"{row['date']} {p} set on {row[p]:,} of {total:,}"
            " — did --solve-offsets run for this date?"
            for p in missing
        ]

    if short or gaps:
        problems = [f"{k} {v:,} < {floors[k]:,}" for k, v in short.items()] + gaps
        print(
            "\nGraph is not loaded as expected: " + "; ".join(problems),
            file=sys.stderr,
        )
        return 1

    print("\n✅ Graph is loaded — routing assertions will run against real data")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
