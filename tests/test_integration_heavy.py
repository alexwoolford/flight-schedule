#!/usr/bin/env python3
"""
Hub connection timing against a loaded graph.
=============================================

**One real test.** This file used to hold six, and five of them asserted nothing.
Measured against the dev graph (6,898,743 Schedule nodes, 2025-01-01…2025-12-31),
every one of those five queries returned **0 rows**, behind `assert count >= 0`:

- `test_popular_routes` — JFK→LAX on `date('2024-03-01')` (0; the graph holds
  2025), then EGPH→hub→LFMN, Edinburgh to Nice, in US-domestic BTS data (0).
- `test_time_filtering` — half on `date('2024-03-01')` (0), half on
  `s.date_of_operation` / `s.first_seen_time`, properties this graph has never
  had (0, plus an `UnknownPropertyKeyWarning` from the server).
- `test_airport_coverage` — EGLL, LFPG, EHAM, EDDF, EGPH, LFMN, EIDW, EGCC.
  **Zero of those eight `Airport` nodes exist.**
- `test_traveler_scenarios` — 2024 dates, Dublin→Amsterdam, and the banned
  `duration.between(...).minutes` idiom.
- `test_query_performance` — a 2-second wall-clock bound on the
  `date_of_operation` query above, i.e. on zero rows.

They were not merely stale. `assert n >= 0` cannot fail for a count, so each one
passed *because* it matched nothing, and the file reported green while checking
the schema of a different dataset. This is the same defect that got
`test_performance.py` and `test_performance_baseline.py` deleted; see CLAUDE.md.

Nothing was lost by removing them. The equivalent coverage is elsewhere in the
same CI gate and is falsifiable: nonstop and 1-stop route finding, morning-hour
filtering, hub identification and multi-constraint queries in
`test_graph_validation.py`; ranked itineraries, filters and journey arithmetic in
`test_flight_search_integration.py`; query *plans* rather than wall clock in
`test_query_plan.py`, which holds at any graph size.

The docstring here also used to say "NOT FOR CI", "requires 19M+ records" and
"5+ minutes each" — all three untrue: it is in the `integration-test` gate, it
passes against a one-day 21,376-flight fixture, and it runs in about a second.
The two session fixtures it declared privately are gone too; `conftest.py`'s
skip cleanly when Neo4j is unreachable, and these did not.
"""

from pathlib import Path


class TestConnectionTiming:
    """Layover arithmetic through the three biggest US hubs."""

    def test_connection_timing(self, neo4j_driver, neo4j_database, search_date):
        """Test connection timing validation

        This test used to skip unconditionally, which read as green. Three
        independent defects each forced its count to zero, and the trailing
        `if total == 0: pytest.skip(...)` swallowed all of them:

        - `date('2024-06-18')`, hard-coded, while the graph holds 2025 data.
          It now uses the `search_date` fixture, which reads the busiest loaded
          day out of the graph.
        - `s1.last_seen_time` / `s2.first_seen_time`, legacy property names that
          do not exist in the current schema -- the server returns an
          UnknownPropertyKeyWarning for both. The current names are
          `scheduled_arrival_time` / `scheduled_departure_time`.
        - `duration.between(...).minutes`, a component accessor that excludes
          whole days, so a 25.5-hour span reports as 90 minutes. Verified:
          `.minutes` gives 90 where `inSeconds(...).seconds / 60` gives 1530.

        The layover subtraction itself is sound even though the stored arrival
        and departure are in different timezones generally, because both
        timestamps here are local to the same hub. See the durations section of
        ROUTING_QUERY_REFERENCE.md.

        Mutation-tested: reinstating the 2024 date or the legacy property names
        each makes this fail. Reinstating `.minutes` does not, and that is
        expected rather than a gap -- the 45-480 filter excludes any span of a
        whole day or more, so the two accessors agree on all 2,955,915 hub pairs
        examined. `inSeconds` is kept because it is correct independently of the
        filter.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport)
                WHERE s1.flightdate = date($search_date)
                  AND s2.flightdate = date($search_date)
                  AND dep.code <> arr.code
                  AND hub.code <> dep.code AND hub.code <> arr.code
                  AND hub.code IN ['ATL', 'DFW', 'ORD']

                // inSeconds, NOT .minutes: the latter drops whole days.
                WITH duration.inSeconds(s1.scheduled_arrival_time,
                                        s2.scheduled_departure_time
                                        ).seconds / 60 AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 480

                RETURN
                    min(connection_time) AS min_connection,
                    max(connection_time) AS max_connection,
                    count(*) AS total_valid_connections
            """,
                search_date=search_date,
            )

            timing = result.single()
            total = timing["total_valid_connections"]

        # No skip on zero: that is the failure this test exists to catch. If the
        # three biggest US hubs yield no valid connection on the busiest loaded
        # day, either the data or the query is broken and the suite must say so.
        assert total > 1000, (
            f"Expected >1000 valid connections through ATL/DFW/ORD on "
            f"{search_date}, got {total}. Zero means the query no longer "
            "matches the schema (check property names) or the date has no data."
        )
        min_conn = int(timing["min_connection"])
        max_conn = int(timing["max_connection"])
        assert 45 <= min_conn <= max_conn <= 480, (
            f"Connection times must respect the 45-480 minute filter, got "
            f"min={min_conn} max={max_conn} on {search_date}"
        )

    def test_the_hubs_this_asserts_on_are_actually_loaded(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """Anti-vacuity: ATL/DFW/ORD must exist, or the test above proves nothing.

        The deleted tests in this file failed exactly here -- they filtered on
        eight European ICAO codes, none of which is an `Airport` node in
        US-domestic BTS data, so their queries could only ever return zero. The
        `> 1000` bound above is already safe from that, but a future edit to a
        smaller or differently-scoped fixture could reintroduce it, so state the
        precondition rather than relying on it.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            departures = {
                record["code"]: record["flights"]
                for record in session.run(
                    """
                    UNWIND ['ATL', 'DFW', 'ORD'] AS code
                    MATCH (s:Schedule)-[:DEPARTS_FROM]->(:Airport {code: code})
                    WHERE s.flightdate = date($search_date)
                    RETURN code, count(s) AS flights
                    """,
                    search_date=search_date,
                )
            }

        missing = {"ATL", "DFW", "ORD"} - departures.keys()
        assert not missing, (
            f"{sorted(missing)} have no departures on {search_date}, so the "
            "connection-timing assertion above would be filtering on airports "
            "that are not in the graph -- the defect that made five tests in "
            "this file vacuous. Check the loaded data, not the query."
        )
        thin = {code: n for code, n in departures.items() if n < 100}
        assert not thin, (
            f"Hub departure counts are implausibly low: {thin} on {search_date}. "
            "Measured on the one-day fixture: ORD 1,035, ATL 980, DFW 965."
        )


def test_no_vacuous_count_assertions_return():
    """`assert count >= 0` must not come back into this file.

    Five tests here passed for years on zero rows, because that is what a count
    assertion does. Guard the file against its own history rather than trusting
    a reviewer to notice. Needs no database.
    """
    # Skip the module docstring, which quotes the offending pattern deliberately.
    body = Path(__file__).read_text().split('"""', 2)[-1]
    offenders = [
        line.strip()
        for line in body.splitlines()
        if ">= 0" in line and line.strip().startswith("assert")
    ]
    assert not offenders, (
        f"vacuous count assertions are back: {offenders}. A count is never "
        "negative, so this passes on an empty result and hides a broken query. "
        "Assert a measured lower bound instead."
    )
