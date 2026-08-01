#!/usr/bin/env python3
"""
Shared fixtures for the database-backed tests.
===============================================

These fixtures deliberately derive the date under test from whatever is loaded
in the graph rather than hard-coding one. The repo used to pin
`date('2024-03-01')` in a dozen places, which silently turned every temporal
test into a false negative as soon as a different year was loaded.
"""

import os

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase


@pytest.fixture(scope="session")
def neo4j_driver():
    """Neo4j driver, or skip the whole module if nothing is listening."""
    load_dotenv(override=True)
    uri = os.getenv("NEO4J_URI")
    if not uri:
        pytest.skip("NEO4J_URI is not set — copy .env.example to .env")

    driver = GraphDatabase.driver(
        uri,
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
    )
    try:
        driver.verify_connectivity()
    except Exception as exc:  # pragma: no cover - environment dependent
        driver.close()
        pytest.skip(f"Neo4j is not reachable at {uri}: {exc}")

    yield driver
    driver.close()


@pytest.fixture(scope="session")
def neo4j_database():
    """Target database name from .env."""
    load_dotenv(override=True)
    return os.getenv("NEO4J_DATABASE", "neo4j")


_NOT_LOADED = (
    "No Schedule nodes in the graph — run the loader first "
    "(see README.md, 'Load the graph')"
)


@pytest.fixture(scope="session")
def loaded_graph(neo4j_driver, neo4j_database):
    """
    Number of Schedule nodes, skipping the test if the graph is empty.

    An empty graph means the load step hasn't run, which is a setup state
    rather than a test failure. Every database-backed test should depend on
    this (directly or via `search_date`) so an unloaded graph produces one
    clear message instead of a mix of skips and assertion errors.
    """
    with neo4j_driver.session(database=neo4j_database) as session:
        count = session.run("MATCH (s:Schedule) RETURN count(s) AS count").single()[
            "count"
        ]
    if count == 0:
        pytest.skip(_NOT_LOADED)
    return count


@pytest.fixture(scope="session")
def loaded_days(neo4j_driver, neo4j_database, loaded_graph):
    """
    Number of distinct dates in the graph.

    Lets size assertions be expressed per day, so the same test is meaningful
    against a one-day CI fixture and a full-month local load.
    """
    with neo4j_driver.session(database=neo4j_database) as session:
        return session.run(
            "MATCH (s:Schedule) RETURN count(DISTINCT s.flightdate) AS days"
        ).single()["days"]


@pytest.fixture(scope="session")
def search_date(neo4j_driver, neo4j_database, loaded_graph):
    """
    An ISO date string for a day that actually has flights loaded.

    Picks the busiest day in the graph so route-specific assertions
    (LGA->ATL, LGA->hub->DFW, etc.) have data to find.
    """
    with neo4j_driver.session(database=neo4j_database) as session:
        record = session.run(
            """
            MATCH (s:Schedule)
            WHERE s.flightdate IS NOT NULL
            WITH s.flightdate AS flightdate, count(*) AS flights
            ORDER BY flights DESC, flightdate
            LIMIT 1
            RETURN toString(flightdate) AS flightdate
            """
        ).single()

    if record is None:
        pytest.skip(_NOT_LOADED)
    return record["flightdate"]


@pytest.fixture(scope="session")
def next_day(search_date):
    """The day after `search_date`, for cross-midnight range assertions."""
    import datetime

    return (
        datetime.date.fromisoformat(search_date) + datetime.timedelta(days=1)
    ).isoformat()
