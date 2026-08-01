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

    python tests/ci_verify_loaded.py
"""

import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Floors rather than exact counts: the fixture is fixed, but the connection
# policy may legitimately move the edge total (--min-layover, --strict-carrier).
# Measured on tests/fixtures/bts_flights_2025_07_18.parquet with default policy:
# 21,376 Schedule / 341 Airport / 14 Carrier / 625,220 CONNECTS_TO. (A full
# month reaches 352 airports and 15 carriers — a single day misses a few.)
NODE_FLOORS = {"Schedule": 20000, "Airport": 300, "Carrier": 10}
EDGE_FLOORS = {
    "DEPARTS_FROM": 20000,
    "ARRIVES_AT": 20000,
    "OPERATED_BY": 20000,
    "CONNECTS_TO": 500000,
}


def main() -> int:
    # override=True to match conftest.py and the loader: .env is authoritative
    # everywhere in this repo, and this script must resolve credentials the same
    # way as the tests it gates, or it can pass against a different database
    # than they check.
    load_dotenv(override=True)
    uri = os.getenv("NEO4J_URI")
    if not uri:
        print("NEO4J_URI is not set", file=sys.stderr)
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
    finally:
        driver.close()

    floors = {**NODE_FLOORS, **EDGE_FLOORS}
    short = {k: v for k, v in counts.items() if v < floors[k]}

    for name in floors:
        marker = "✗" if name in short else "✓"
        print(f"  {marker} {name:<14} {counts[name]:>9,}  (floor {floors[name]:,})")

    if short:
        print(
            "\nGraph is not loaded as expected: "
            + "; ".join(f"{k} {v:,} < {floors[k]:,}" for k, v in short.items()),
            file=sys.stderr,
        )
        return 1

    print("\n✅ Graph is loaded — routing assertions will run against real data")
    return 0


if __name__ == "__main__":
    sys.exit(main())
