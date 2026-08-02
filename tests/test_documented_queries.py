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


@pytest.fixture(scope="module")
def query_params(neo4j_driver, neo4j_database, search_date):
    """Bindings for the documented placeholders, derived from the graph.

    Deliberately not hard-coded. `test_performance_baseline.py` pinned
    date('2024-03-01') and a specific route, and rotted the moment the loaded
    data changed -- it now cannot pass at all. The route is chosen as a real
    connecting city pair with no nonstop on the busiest loaded date, so the
    multi-hop queries exercise a genuine multi-hop answer rather than returning
    a direct flight and looking fine.
    """
    with neo4j_driver.session(database=neo4j_database) as session:
        record = session.run(
            """
            MATCH (f:Schedule) WHERE f.flightdate = date($date)
            WITH collect(DISTINCT f.origin + '>' + f.dest) AS nonstop
            MATCH (s1:Schedule)-[:CONNECTS_TO]->(s2:Schedule)
            WHERE s1.flightdate = date($date)
              AND NOT s1.origin + '>' + s2.dest IN nonstop
            WITH s1.origin AS origin, s2.dest AS dest, count(*) AS options
            RETURN origin, dest ORDER BY options DESC, origin, dest LIMIT 1
            """,
            date=search_date,
        ).single()
    if record is None:
        pytest.skip("No connection-only city pair on the busiest loaded date")

    return {
        "origin": record["origin"],
        "dest": record["dest"],
        "date": search_date,
        "limit": 5,
        "min_layover": 45,
        "max_layover": 300,
        # Late enough to admit itineraries on any route; the point is that the
        # query runs and returns rows, not that a specific cutoff is met.
        "deadline": f"{search_date}T23:59:00",
        "after": f"{search_date}T00:00:00",
        "min_flights": 500,
    }


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
    neo4j_driver, neo4j_database, loaded_graph, query_params, doc, index, query
):
    """A query printed in the docs parses and executes as written"""
    with neo4j_driver.session(database=neo4j_database) as session:
        if (
            session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            == 0
        ):
            pytest.skip("No CONNECTS_TO edges — see --build-connections")
        try:
            # Consume the result: a syntax error surfaces on run(), but a runtime
            # error (bad property, type mismatch) only surfaces on iteration.
            rows = list(session.run(query, **query_params))
        except Exception as exc:
            pytest.fail(
                f"{doc} cypher block {index} does not execute as written:\n"
                f"{exc}\n\n--- query ---\n{query}"
            )

    # Returning zero rows is the signature of the localdatetime/datetime NULL
    # trap, so an empty result is a failure here rather than a pass. The
    # parameters above name a real route on a date whose edges are built.
    assert rows, (
        f"{doc} cypher block {index} parsed but returned no rows for "
        f"{query_params['origin']}->{query_params['dest']} on "
        f"{query_params['date']}. A documented query that silently returns "
        "nothing is the exact symptom of the LOCAL/ZONED DATETIME trap.\n"
        f"--- query ---\n{query}"
    )

    # No NULL in any returned column. Cypher does not error on a misspelled
    # property -- `c.layover_mins` evaluates to null, and `0 + null` is null, so
    # a typo in a documented query propagates a null total and still returns
    # rows. Without this assertion that mutation survives; verified.
    for row in rows:
        nulls = [key for key, value in row.items() if value is None]
        assert not nulls, (
            f"{doc} cypher block {index} returned NULL for {nulls}. A "
            "misspelled property does not raise in Cypher -- it yields null and "
            "poisons any arithmetic it feeds. Check those names against the "
            "schema.\n"
            f"--- query ---\n{query}"
        )
