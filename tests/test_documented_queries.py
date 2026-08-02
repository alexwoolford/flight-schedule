#!/usr/bin/env python3
"""
Execute every Cypher block in the docs against the loaded graph.

This exists because a hand-edited query in ROUTING_QUERY_REFERENCE.md shipped
with two consecutive `WHERE` clauses on one `WITH` -- a plain syntax error. Every
prose claim around it was correct and every other test passed; the query itself
had simply never been run in that exact form. A reader's first act is to
copy-paste it, so "the documented query parses and returns rows" is a property
worth gating.

The blocks are extracted from the markdown, not restated here. Restating them
would let the copy in the test and the copy in the doc drift, which is the
failure mode this is meant to prevent.
"""

import re
from pathlib import Path

import pytest

DOCS = ["ROUTING_QUERY_REFERENCE.md", "README.md"]

# A block is a runnable query only if it starts one. Blocks that open with WITH
# or AND are deliberate fragments quoted inline to explain a single clause.
RUNNABLE_PREFIXES = ("MATCH", "CYPHER", "PROFILE", "EXPLAIN")


# Pick the busiest pair on the search date in each of two shapes. The documented
# blocks want contradictory things from their bindings -- the "direct flights on a
# route" example needs a pair that flies nonstop, while the QPP examples only
# exercise layover arithmetic on a pair that genuinely requires a connection -- so
# no single route can serve them all. Each block is run against both.
#
# The connection-only route is what keeps the NULL-column assertion meaningful:
# on a nonstop trunk route the ranked results are all 0-stop, and `reduce` over an
# empty layover list is 0, never null. A property typo in the layover arithmetic
# is then invisible. Verified: dropping this route lets that mutation survive.
_ROUTE_QUERY = """
    MATCH (f:Schedule) WHERE f.flightdate = date($date)
    WITH collect(DISTINCT f.origin + '>' + f.dest) AS nonstop
    MATCH (s1:Schedule)-[:CONNECTS_TO]->(s2:Schedule)
    WHERE s1.flightdate = date($date)
      AND (s1.origin + '>' + s2.dest IN nonstop) = $has_nonstop
    WITH s1.origin AS origin, s2.dest AS dest, count(*) AS options
    RETURN origin, dest ORDER BY options DESC, origin, dest LIMIT 1
"""


@pytest.fixture(scope="module")
def query_bindings(neo4j_driver, neo4j_database, search_date):
    """Bindings for the documented placeholders, derived from the graph.

    Deliberately not hard-coded. The since-deleted test_performance_baseline.py
    pinned date('2024-03-01') and a specific route, and rotted the moment the
    loaded data changed -- it could not pass at all by the end.
    """
    routes = {}
    with neo4j_driver.session(database=neo4j_database) as session:
        for label, has_nonstop in (("nonstop", True), ("connecting", False)):
            record = session.run(
                _ROUTE_QUERY, date=search_date, has_nonstop=has_nonstop
            ).single()
            if record is None:
                pytest.skip(f"No {label} city pair with connections on {search_date}")
            routes[label] = (record["origin"], record["dest"])

    return [
        {
            "origin": origin,
            "dest": dest,
            "date": search_date,
            "limit": 5,
            "min_layover": 45,
            "max_layover": 300,
            # Late enough to admit itineraries on any route; the point is that
            # the query runs and returns rows, not that a cutoff is met.
            "deadline": f"{search_date}T23:59:00",
            "after": f"{search_date}T00:00:00",
            "min_flights": 500,
        }
        for origin, dest in routes.values()
    ]


def _blocks(doc):
    """Every ```cypher fenced block in `doc`, with its 1-based index."""
    text = Path(doc).read_text()
    return [
        (i, b.strip())
        for i, b in enumerate(re.findall(r"```cypher\n(.*?)```", text, re.S), 1)
    ]


def _runnable(doc):
    return [
        (doc, i, b) for i, b in _blocks(doc) if b.upper().startswith(RUNNABLE_PREFIXES)
    ]


ALL_RUNNABLE = [case for doc in DOCS for case in _runnable(doc)]


def test_docs_contain_runnable_blocks():
    """Guard the extractor itself

    If a docs refactor renames the fences or the files, the parametrised test
    below silently collects nothing and reports green while checking no queries
    at all. Assert the corpus is non-empty first.
    """
    assert len(ALL_RUNNABLE) >= 8, (
        f"Only found {len(ALL_RUNNABLE)} runnable cypher blocks across {DOCS}. "
        "Either the docs changed shape or the ```cypher fence extractor is "
        "broken -- in which case the query tests below are checking nothing."
    )


@pytest.mark.parametrize(
    "doc,index,query",
    ALL_RUNNABLE,
    ids=[f"{Path(d).stem}-block{i}" for d, i, _ in ALL_RUNNABLE],
)
def test_documented_query_runs(
    neo4j_driver, neo4j_database, loaded_graph, query_bindings, doc, index, query
):
    """A query printed in the docs parses and executes as written"""
    results = []
    with neo4j_driver.session(database=neo4j_database) as session:
        if (
            session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            == 0
        ):
            pytest.skip("No CONNECTS_TO edges — see --build-connections")
        for params in query_bindings:
            try:
                # Consume the result: a syntax error surfaces on run(), but a
                # runtime error (bad property, type mismatch) only on iteration.
                results.append((params, list(session.run(query, **params))))
            except Exception as exc:
                pytest.fail(
                    f"{doc} cypher block {index} does not execute as written "
                    f"for {params['origin']}->{params['dest']}:\n"
                    f"{exc}\n\n--- query ---\n{query}"
                )

    # Returning zero rows is the signature of the localdatetime/datetime NULL
    # trap, so an empty result is a failure here rather than a pass. Not every
    # block can return rows on every route -- a nonstop-only example finds
    # nothing on a connection-only pair, and vice versa -- so require rows on at
    # least one, and check NULLs on whichever ones produced them.
    #
    # If you are reading this because a block you just added fails: a documented
    # query that hard-codes an airport pair or a date is the other way to land
    # here, and it is also a real defect -- the loaded graph is whatever the
    # reader loaded, so a literal date('2025-01-15') returns nothing for most of
    # them. Use $origin / $dest / $date and this fixture will bind them.
    tried = ", ".join(f"{p['origin']}->{p['dest']}" for p, _ in results)
    assert any(rows for _, rows in results), (
        f"{doc} cypher block {index} parsed but returned no rows on any of "
        f"{tried} on {query_bindings[0]['date']}. A documented query that "
        "silently returns nothing is the exact symptom of the LOCAL/ZONED "
        f"DATETIME trap.\n--- query ---\n{query}"
    )

    # No NULL in any returned column. Cypher does not error on a misspelled
    # property -- `c.layover_mins` evaluates to null, and `0 + null` is null, so
    # a typo in a documented query propagates a null total and still returns
    # rows. Without this assertion that mutation survives; verified.
    for _, rows in results:
        for row in rows:
            nulls = [key for key, value in row.items() if value is None]
            assert not nulls, (
                f"{doc} cypher block {index} returned NULL for {nulls}. A "
                "misspelled property does not raise in Cypher -- it yields "
                "null and poisons any arithmetic it feeds. Check those names "
                f"against the schema.\n--- query ---\n{query}"
            )
