#!/usr/bin/env python3
"""
The search query's execution plan.
=================================

This replaces `test_performance_baseline.py`, which asserted wall-clock bounds
(`query_time < 500`) alongside `assert count >= 0`. Both halves were unusable: a
`>= 0` assertion on a count cannot fail, and a millisecond threshold on a shared
CI runner fails for reasons that have nothing to do with the query. Its remaining
checks queried `date('2024-03-01')`, which no fixture holds, and compared a LOCAL
DATETIME against `datetime()` — the NULL trap, so those rows were empty too.

The *plan* is the part worth gating, because it is deterministic. A query that
starts from an index seek and one that starts from a full label scan differ by
orders of magnitude at scale, and the difference shows up in `EXPLAIN` at any
graph size — including the one-day CI fixture, where a label scan is fast enough
that a latency assertion would never notice it.

`EXPLAIN` does not execute the query, so nothing here depends on how much data is
loaded; it only needs a reachable database to compile against.

Measured plan for `{0,2}` LGA->DFW with a departure filter (leaf last):

    ProduceResults / Projection / Top / Filter ...
      Expand(All)
        VarLengthExpand(All)          <- the CONNECTS_TO quantifier
          Expand(All)
            NodeIndexSeek             <- schedule_flightdate, NOT a label scan
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import flight_search  # noqa: E402

FIXTURE_DATE = "2025-07-18"

# Operators that mean the planner gave up on the indexes `setup_database_schema()`
# creates. AllNodesScan is worse still — no label filter at all.
#
# Scope, so this is not read as stronger than it is: the assertion is "an indexed
# entry point exists", not "*this* index is used". Rewriting the query to drop the
# flightdate seek still planned a NodeUniqueIndexSeek, because the planner fell
# back to airport_code_unique — a different indexed start, not a degradation.
# Verified to fail on a query that genuinely cannot seek: filtering Schedule on
# tail_number (no index) plans NodeByLabelScan.
SCAN_OPERATORS = {"NodeByLabelScan", "AllNodesScan"}


@pytest.fixture(scope="module")
def graph():
    """A connection to compile against, or skip."""
    try:
        driver = flight_search.get_driver()
        driver.verify_connectivity()
    except Exception as exc:
        pytest.skip(f"Neo4j not available: {exc}")
    return driver


def _plan_operators(driver, query, **params):
    """Every operator in the compiled plan, flattened. `EXPLAIN` only compiles."""
    with driver.session(database=flight_search.get_database()) as session:
        summary = session.run("EXPLAIN " + query, **params).consume()

    operators = []

    def walk(node):
        operators.append(node["operatorType"].split("@")[0])
        for child in node.get("children", []):
            walk(child)

    assert summary.plan is not None, "server returned no plan for EXPLAIN"
    walk(summary.plan)
    return operators


def _search_params(origin="LGA", dest="DFW", **extra):
    params = {
        "origin": origin,
        "dest": dest,
        "date": FIXTURE_DATE,
        "limit": 20,
    }
    params.update(extra)
    return params


class TestSearchPlan:
    """
    What the planner does with the query `flight_search` actually issues. Built
    via `build_search_query()` rather than a copy, so a change to the real query
    is a change to what is gated here.
    """

    @pytest.mark.parametrize("max_stops", [0, 1, 2, 3])
    def test_plan_starts_from_an_index_seek_not_a_scan(self, graph, max_stops):
        query = flight_search.build_search_query(0, max_stops)
        operators = _plan_operators(graph, query, **_search_params())

        # Anti-vacuity: an empty or trivial operator list would pass the
        # disjointness check below without asserting anything.
        assert len(operators) > 3, f"suspiciously small plan: {operators}"
        offenders = SCAN_OPERATORS.intersection(operators)
        assert not offenders, (
            f"{{0,{max_stops}}} plan falls back to {sorted(offenders)}: {operators}. "
            "Check that schedule_flightdate and the Airport code constraint exist "
            "— see setup_database_schema() in load_bts_data.py."
        )

    def test_seek_is_present_by_name(self, graph):
        # The paired positive: "no scan operator" would also hold for a plan that
        # somehow contained neither, so require the seek explicitly.
        operators = _plan_operators(
            graph, flight_search.build_search_query(0, 2), **_search_params()
        )
        assert any("IndexSeek" in op for op in operators), operators

    def test_multi_stop_plan_expands_the_quantifier(self, graph):
        """{0,2} must traverse CONNECTS_TO; {0,0} must not."""
        multi = _plan_operators(
            graph, flight_search.build_search_query(0, 2), **_search_params()
        )
        assert any("VarLengthExpand" in op or "Trail" in op for op in multi), multi

        nonstop = _plan_operators(
            graph, flight_search.build_search_query(0, 0), **_search_params()
        )
        assert not any(
            "VarLengthExpand" in op or "Trail" in op for op in nonstop
        ), f"nonstop search should not traverse CONNECTS_TO: {nonstop}"

    def test_ranking_does_not_materialise_every_path(self, graph):
        """
        `ORDER BY total_minutes LIMIT $limit` must plan as `Top`, not `Sort`.

        `Top` keeps only `limit` rows in its heap; `Sort` buffers the whole result
        set. On LGA->DFW at {0,3} that is the difference between 20 rows and
        11,488 — the ranking is global either way, but only one of them is
        bounded in memory.
        """
        operators = _plan_operators(
            graph, flight_search.build_search_query(0, 2), **_search_params()
        )
        assert "Top" in operators, f"expected Top, got {operators}"
        assert "Sort" not in operators, f"unbounded Sort in plan: {operators}"

    def test_filters_do_not_change_the_access_pattern(self, graph):
        """
        A departure-time filter cuts latency several-fold (see the latency table
        in flight_search.py), and it must do so by filtering, not by pushing the
        planner onto a different, worse starting point.
        """
        query = flight_search.build_search_query(
            0, 2, depart_after=f"{FIXTURE_DATE}T08:00:00", arrive_before=None
        )
        operators = _plan_operators(
            graph,
            query,
            **_search_params(depart_after=f"{FIXTURE_DATE}T08:00:00"),
        )
        assert not SCAN_OPERATORS.intersection(operators), operators
        assert any("IndexSeek" in op for op in operators), operators


class TestSupportingQueryPlans:
    """`is_searchable()` is called on every empty search result, so it has to be
    cheap. It is only cheap if it seeks."""

    def test_is_searchable_seeks_on_flightdate(self, graph):
        query = """
        MATCH (s:Schedule {flightdate: date($date)})-[:CONNECTS_TO]->()
        RETURN count(*) > 0 AS built LIMIT 1
        """
        operators = _plan_operators(graph, query, date=FIXTURE_DATE)
        assert not SCAN_OPERATORS.intersection(operators), operators
        assert any("IndexSeek" in op for op in operators), operators
