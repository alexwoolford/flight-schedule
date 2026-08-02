#!/usr/bin/env python3
"""
Test Graph Database Validation
==============================

Test cases to validate graph database structure and basic functionality.

The `neo4j_driver`, `neo4j_database` and `search_date` fixtures come from
`tests/conftest.py`. `search_date` is read out of the graph, so these tests pass
against any loaded year rather than a hard-coded one.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_bts_data import CARRIER_FAMILY  # noqa: E402


class TestBasicConnectivity:
    """Test basic database connectivity and data presence"""

    def test_database_connection(self, neo4j_driver, neo4j_database):
        """Test database connection works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("RETURN 'Connected' AS status")
            record = result.single()
            assert record["status"] == "Connected"

    def test_schedule_nodes_present(
        self, neo4j_driver, neo4j_database, loaded_graph, loaded_days
    ):
        """Every loaded day carries a full day's worth of flights"""
        # Asserted per day rather than as an absolute total, because the same
        # suite runs against a full month locally (~500K flights) and against
        # the one-day CI fixture (~21K). An absolute floor can only be right
        # for one of those. The invariant that holds for both is density: US
        # domestic BTS reports ~19-22K flights on every day of 2025, so a day
        # holding materially fewer means the load truncated — which is the
        # failure this test exists to catch.
        per_day = loaded_graph / loaded_days
        assert per_day > 15000, (
            f"Expected >15K Schedule nodes per loaded day (a full BTS day is "
            f"~20K), got {per_day:,.0f} across {loaded_days} day(s) "
            f"({loaded_graph:,} total) — the load looks truncated"
        )

    def test_relationships_present(self, neo4j_driver, neo4j_database, loaded_graph):
        """Each Schedule has exactly one of each per-flight relationship"""
        # Asserted per type rather than as a total, because ROUTE is an
        # aggregated Airport->Airport edge (one per distinct route, not per
        # flight) and would otherwise have to be subtracted out here.
        with neo4j_driver.session(database=neo4j_database) as session:
            counts = {
                rel_type: session.run(
                    f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS count"
                ).single()["count"]
                for rel_type in ("DEPARTS_FROM", "ARRIVES_AT", "OPERATED_BY")
            }
        for rel_type, count in counts.items():
            assert count == loaded_graph, (
                f"Expected one {rel_type} per Schedule ({loaded_graph:,}), "
                f"got {count:,}"
            )

    def test_route_projection_present(self, neo4j_driver, neo4j_database, loaded_graph):
        """The aggregated ROUTE network exists and is one edge per route"""
        with neo4j_driver.session(database=neo4j_database) as session:
            route_edges = session.run(
                "MATCH ()-[r:ROUTE]->() RETURN count(r) AS count"
            ).single()["count"]
            distinct_routes = session.run(
                "MATCH (s:Schedule) RETURN count(DISTINCT s.origin + '-' + s.dest) AS c"
            ).single()["c"]

        if route_edges == 0:
            pytest.skip(
                "No ROUTE edges — graph predates the ROUTE projection; re-run the loader"
            )
        # Exactly one edge per distinct directed route: proves the aggregation
        # is right and that a re-run MERGEd rather than duplicated.
        assert route_edges == distinct_routes, (
            f"Expected one ROUTE edge per distinct route ({distinct_routes:,}), "
            f"got {route_edges:,}"
        )

    def test_connects_to_edges_are_valid(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Every CONNECTS_TO edge is a genuinely bookable connection"""
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip(
                    "No CONNECTS_TO edges — build them with "
                    "`python load_bts_data.py --build-connections YYYY-MM-DD`"
                )
            # The whole value of this edge is that the connection rules are
            # already applied, so a query over it cannot produce an unsellable
            # itinerary. Assert the invariants rather than trusting the builder.
            # Carrier equality is asserted separately (regional affiliates are
            # legitimately allowed to differ), so it is not checked here.
            invalid = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.dest <> s2.origin
                   OR s2.dest = s1.origin
                   OR r.layover_minutes IS NULL
                   OR r.layover_minutes < 45
                   OR r.layover_minutes > 300
                RETURN count(r) AS count
                """
            ).single()["count"]

        assert invalid == 0, (
            f"{invalid:,} of {total:,} CONNECTS_TO edges violate the connection "
            "rules (hub mismatch, backtrack, or layover outside 45-300 min)"
        )

    def test_connects_to_has_no_overnight_inbound_legs(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """No connection depends on an inbound leg that lands the next day"""
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            # An inbound leg that lands the day AFTER the outbound departs cannot
            # be connected off, and the UTC instants settle it exactly: the
            # builder computes its layover as a positive UTC interval, so such an
            # edge cannot exist. Earlier revisions of this test asked the same
            # question with a block-time heuristic on the local pair, which
            # tolerated only +/-180 minutes of timezone skew and missed the
            # widest spans -- it passed while 11,975 such edges were in the graph.
            false_edges = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.scheduled_arrival_utc >= s2.scheduled_departure_utc
                RETURN count(r) AS count
                """
            ).single()["count"]
            # Anti-vacuity: the assertion above is only meaningful if the graph
            # actually contains legs landing on a later local day for the builder
            # to have excluded. On a dataset with none it would pass no matter
            # what the builder did. One BTS day holds ~900 (915 on 2025-07-18).
            overnight_legs = session.run(
                """
                MATCH (s:Schedule)
                WHERE date(s.scheduled_arrival_time) > s.flightdate
                RETURN count(*) AS count
                """
            ).single()["count"]

        assert false_edges == 0, (
            f"{false_edges:,} of {total:,} CONNECTS_TO edges depart at or before "
            "their inbound leg lands, in UTC. These are absolute instants, so "
            "this is not a timezone artefact — the connection is unflyable."
        )
        assert overnight_legs > 0, (
            "No leg in the graph lands on a later local day than it departed, so "
            "the assertion above proves nothing — this dataset cannot detect a "
            "regression in the builder's cross-midnight handling"
        )

    def test_connects_to_carrier_is_sellable(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Both legs are sold by the same airline, allowing regional affiliates"""
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            # BTS reports the OPERATING carrier, so an American Eagle feeder
            # shows up as MQ/OH even though the ticket says AA. Splicing two
            # *unrelated* carriers is still unsellable (Southwest interlines
            # with nobody), so map through CARRIER_FAMILY and then require
            # equality. Imported from the loader so there is one definition.
            mismatched = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WITH coalesce($family[s1.reporting_airline],
                              s1.reporting_airline) AS m1,
                     coalesce($family[s2.reporting_airline],
                              s2.reporting_airline) AS m2, r
                WHERE m1 <> m2
                RETURN count(r) AS count
                """,
                family=CARRIER_FAMILY,
            ).single()["count"]
            # Anti-vacuity, the other direction: a builder that used strict
            # operating-carrier equality would also pass the assertion above,
            # while silently dropping ~112K sellable AA<->MQ/OH connections a
            # day. Require that the family mapping is actually doing work.
            cross_family = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.reporting_airline <> s2.reporting_airline
                RETURN count(r) AS count
                """
            ).single()["count"]

        assert mismatched == 0, (
            f"{mismatched:,} of {total:,} CONNECTS_TO edges splice two "
            "unrelated carriers into an unsellable itinerary"
        )
        assert cross_family > 0, (
            "No connection joins two different operating carriers, so "
            "CARRIER_FAMILY is not being applied — mainline<->wholly-owned "
            "regional connections (AA<->MQ/OH) are being dropped. Expected "
            "if the edges were built with --strict-carrier, which is not the "
            "default policy this suite gates."
        )

    def test_connects_to_hubs_are_real(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Connections concentrate at real US hubs, not arbitrary airports"""
        # Externally validated against published route data: the busiest
        # connecting airports in the graph reproduce the real US hub set, and
        # the top 10 carry 71.6% of all connections (measured 2025-07-18). A
        # collapse here means the connection rules have degenerated even if
        # every individual edge still passes its invariants.
        real_hubs = {
            "ATL",
            "DFW",
            "DEN",
            "ORD",
            "CLT",
            "SEA",
            "LAS",
            "PHX",
            "MSP",
            "MDW",
            "BWI",
            "IAH",
            "EWR",
            "LAX",
            "SFO",
            "DTW",
            "SLC",
            "MIA",
            "JFK",
            "BOS",
            "MCO",
            "PHL",
            "FLL",
            "HNL",
            "DCA",
            "SAN",
            "TPA",
            "BNA",
            "AUS",
            "STL",
            "PDX",
            "ANC",
            "RDU",
            "HOU",
            "DAL",
            "SFB",
            "PIE",
            "OAK",
            "SJC",
            "MSY",
            "CLE",
            "PIT",
            "IND",
            "CVG",
            "SMF",
        }
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            top = list(
                session.run(
                    """
                    MATCH (s1:Schedule)-[r:CONNECTS_TO]->()
                    RETURN s1.dest AS hub, count(r) AS conns
                    ORDER BY conns DESC LIMIT 10
                    """
                )
            )

        hubs = [r["hub"] for r in top]
        unexpected = set(hubs) - real_hubs
        assert not unexpected, (
            f"Top connecting airports include implausible hubs: {unexpected}. "
            f"Top 10 were {hubs}"
        )
        share = sum(r["conns"] for r in top) / total
        assert share > 0.60, (
            f"Top 10 hubs carry only {share:.1%} of connections (expected "
            ">60%, measured 71.6%) — hub structure has degenerated"
        )

    def test_connects_to_supports_multi_hop_qpp(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """A variable-depth QPP over CONNECTS_TO returns coherent itineraries"""
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
                ).single()["count"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            # Pick a date that actually has connections rather than assuming one.
            date = session.run(
                """
                MATCH (s1:Schedule)-[:CONNECTS_TO]->()
                RETURN toString(s1.flightdate) AS d, count(*) AS c
                ORDER BY c DESC LIMIT 1
                """
            ).single()["d"]
            # Legs must chain end-to-end across the whole path: leg N's
            # destination is leg N+1's origin, at every depth the QPP returns.
            broken = session.run(
                """
                MATCH p = (first:Schedule)-[:CONNECTS_TO]->{1,3}(last:Schedule)
                WHERE first.flightdate = date($date)
                WITH nodes(p) AS legs LIMIT 2000
                WHERE any(i IN range(0, size(legs) - 2)
                          WHERE legs[i].dest <> legs[i + 1].origin)
                RETURN count(*) AS count
                """,
                date=date,
            ).single()["count"]

        assert broken == 0, f"{broken} QPP paths have non-contiguous legs"

    def test_sample_schedule_properties(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Test Schedule node properties"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                RETURN s.flightdate, s.reporting_airline, s.flight_number_reporting_airline,
                       s.origin, s.dest, s.scheduled_departure_time
                LIMIT 1
            """
            )
            record = result.single()
            assert record is not None, "Should have at least one Schedule node"
            assert record["s.flightdate"] is not None, "Schedule should have flightdate"
            assert (
                record["s.reporting_airline"] is not None
            ), "Schedule should have reporting_airline"
            assert (
                record["s.flight_number_reporting_airline"] is not None
            ), "Schedule should have flight_number_reporting_airline"


class TestTemporalQueries:
    """Test temporal query patterns that work"""

    def test_string_date_filter(self, neo4j_driver, neo4j_database, search_date):
        """Test date-based filtering works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date($search_date)
                RETURN count(s) AS count
            """,
                search_date=search_date,
            )
            count = result.single()["count"]
            assert count > 0, f"Expected flights on {search_date}, got {count}"

    def test_date_range_filter(
        self, neo4j_driver, neo4j_database, search_date, next_day
    ):
        """Test date range filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Specific date
            result1 = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date($search_date)
                RETURN count(s) AS count
            """,
                search_date=search_date,
            )
            specific_count = result1.single()["count"]

            # Date range that should match same day
            result2 = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate >= date($search_date)
                  AND s.flightdate < date($next_day)
                RETURN count(s) AS count
            """,
                search_date=search_date,
                next_day=next_day,
            )
            range_count = result2.single()["count"]

            assert (
                range_count == specific_count
            ), "Date range should match specific date"

    def test_time_string_extraction(self, neo4j_driver, neo4j_database, search_date):
        """Test time string extraction works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date($search_date)
                  AND s.scheduled_departure_time IS NOT NULL
                RETURN toString(s.scheduled_departure_time) AS time_str
                LIMIT 1
            """,
                search_date=search_date,
            )
            record = result.single()
            time_str = record["time_str"] if record else None

            assert time_str is not None, "Should extract time string"
            assert ":" in time_str, f"Time string should contain ':', got {time_str}"
            # LocalDateTime renders as e.g. "2025-07-18T14:30:00"
            assert (
                "T" in time_str
            ), f"DateTime string should contain 'T', got {time_str}"


class TestGraphTraversal:
    """Test graph traversal patterns"""

    def test_single_hop_traversal(self, neo4j_driver, neo4j_database, loaded_graph):
        """Test basic single hop graph traversal"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(a:Airport)
                RETURN s.reporting_airline, s.flight_number_reporting_airline, a.code
                LIMIT 5
            """
            )
            records = list(result)
            assert len(records) >= 1, "Should find schedule-airport relationships"
            assert (
                records[0]["s.reporting_airline"] is not None
            ), "Should have reporting_airline"
            assert records[0]["a.code"] is not None, "Should have airport code"

    def test_direct_route_finding(self, neo4j_driver, neo4j_database, search_date):
        """Test finding direct routes"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'ATL'})
                WHERE s.flightdate = date($search_date)
                RETURN count(s) AS direct_flights
            """,
                search_date=search_date,
            )
            direct = result.single()["direct_flights"]
            assert direct > 0, f"Should find LGA→ATL direct flights, got {direct}"

    def test_connection_finding(self, neo4j_driver, neo4j_database, search_date):
        """Test finding connection routes"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'DFW'})
                WHERE s1.flightdate = date($search_date)
                  AND s2.flightdate = date($search_date)
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
                  AND hub.code <> 'LGA' AND hub.code <> 'DFW'
                RETURN count(*) AS connections
            """,
                search_date=search_date,
            )
            connections = result.single()["connections"]
            assert (
                connections > 0
            ), f"Should find LGA→hub→DFW connections, got {connections}"


class TestNetworkAnalysis:
    """Test network analysis patterns"""

    def test_hub_identification(self, neo4j_driver, neo4j_database, search_date):
        """Test hub airport identification"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (hub:Airport)<-[:ARRIVES_AT]-(s:Schedule)
                WHERE s.flightdate = date($search_date)
                WITH hub, count(s) AS arrivals
                WHERE arrivals > 200
                RETURN count(hub) AS major_hubs
            """,
                search_date=search_date,
            )
            hubs = result.single()["major_hubs"]
            assert hubs >= 5, f"Should find >=5 major US hubs, got {hubs}"

    def test_carrier_analysis(self, neo4j_driver, neo4j_database, search_date):
        """Test carrier network analysis"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:OPERATED_BY]->(c:Carrier)
                WHERE s.flightdate = date($search_date)
                WITH c, count(s) AS flights
                WHERE flights > 100
                RETURN count(c) AS major_carriers
            """,
                search_date=search_date,
            )
            carriers = result.single()["major_carriers"]
            assert carriers >= 5, f"Should find >=5 major carriers, got {carriers}"


class TestDeadlineFilters:
    """
    Guard the two silent traps in an "arrives before HH:MM" filter.

    Both were found by writing the query the obvious way and getting a wrong
    answer with no error: the first returns nothing, the second returns
    itineraries that land the next day. See "Deadline filters" in
    ROUTING_QUERY_REFERENCE.md.
    """

    def test_arrival_is_localdatetime_not_zoned(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """Comparing the stored arrival to datetime() yields NULL, not false"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # The loader writes to_timestamp_ntz, so the stored value is a LOCAL
            # DATETIME. Cypher does not error when it is compared against a
            # ZONED DATETIME -- the predicate evaluates to NULL, so WHERE drops
            # every row and a route with valid itineraries reports none. This
            # test pins the type and both comparison behaviours so a loader
            # change to zoned timestamps fails here loudly rather than silently
            # flipping every deadline query's result set.
            record = session.run(
                """
                MATCH (f:Schedule)
                WHERE f.flightdate = date($search_date)
                  AND f.scheduled_arrival_time IS NOT NULL
                WITH f LIMIT 500
                WITH f, localdatetime($search_date + 'T15:00:00') AS local_cut,
                        datetime($search_date + 'T15:00:00') AS zoned_cut
                RETURN valueType(f.scheduled_arrival_time) AS stored_type,
                       count(*) AS total,
                       sum(CASE WHEN (f.scheduled_arrival_time < zoned_cut)
                                     IS NULL THEN 1 ELSE 0 END) AS zoned_nulls,
                       sum(CASE WHEN (f.scheduled_arrival_time < local_cut)
                                     IS NULL THEN 1 ELSE 0 END) AS local_nulls,
                       sum(CASE WHEN f.scheduled_arrival_time < local_cut
                                THEN 1 ELSE 0 END) AS local_matched
                """,
                search_date=search_date,
            ).single()

        assert record["stored_type"].startswith("LOCAL DATETIME"), (
            f"scheduled_arrival_time is {record['stored_type']}, expected a "
            "LOCAL DATETIME. Every deadline query in the docs compares it with "
            "localdatetime(); if the loader now writes a zoned type those "
            "comparisons are wrong and the docs need updating together."
        )
        assert record["zoned_nulls"] == record["total"], (
            "Comparing a LOCAL DATETIME against datetime() no longer yields "
            "NULL for every row. The documented trap may have changed "
            "behaviour; re-check ROUTING_QUERY_REFERENCE.md."
        )
        assert record["local_nulls"] == 0, (
            "Comparing against localdatetime() produced NULLs, so the "
            "recommended form is not reliable either"
        )
        assert record["local_matched"] > 0, (
            "No arrival on this date is before 15:00 local, so this test "
            "cannot demonstrate that the localdatetime form matches rows"
        )

    def test_deadline_query_needs_no_overnight_guard(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Overnight terminal legs fail a same-day deadline on their own

        This test previously asserted the OPPOSITE -- that overnight terminal legs
        wrongly passed a "< 15:00" filter -- and required every deadline query to
        carry a guard. That was a symptom of the loader stamping the arrival with
        the ORIGIN's date. `--solve-offsets` now rewrites the local arrival off the
        UTC instant, so its date is the destination's and the guard is unnecessary.
        See "Deadline filters" in ROUTING_QUERY_REFERENCE.md.

        Every query-side guard tried here was wrong, which is why the fix moved
        into the loader: a ±180-minute block-time tolerance cannot span the widest
        US offset gaps (HNL->DFW needs 240-360), and
        `date(arrival_utc) = date(departure_utc)` tests UTC midnight rather than
        local -- on 2025-07-18 it drops 3,135 ordinary evening flights and admits
        876 real red-eyes.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            record = session.run(
                """
                MATCH ()-[:CONNECTS_TO]->(f:Schedule)
                WITH DISTINCT f
                WITH f, date(f.scheduled_arrival_time) > f.flightdate
                        AS overnight,
                     f.scheduled_arrival_time
                       < localdatetime(toString(f.flightdate) + 'T15:00:00')
                     AS before_cutoff
                RETURN count(*) AS terminal_legs,
                       sum(CASE WHEN overnight THEN 1 ELSE 0 END) AS overnights,
                       sum(CASE WHEN overnight AND before_cutoff
                                THEN 1 ELSE 0 END) AS false_accepts
                """
            ).single()

        # Anti-vacuity: without overnight terminal legs in the graph this test
        # passes while checking nothing, and a regression in the date repair
        # would be undetectable. Assert the hazard exists before asserting it is
        # handled.
        assert record["overnights"] > 0, (
            f"None of the {record['terminal_legs']:,} terminal legs lands on the "
            "following local day, so this dataset cannot detect a regression in "
            "the arrival-date repair"
        )
        assert record["false_accepts"] == 0, (
            f"{record['false_accepts']:,} of {record['overnights']:,} overnight "
            "terminal legs still satisfy a same-day 15:00 deadline. The local "
            "arrival date is not the destination's -- run --solve-offsets, or "
            "the repair in write_utc_times() has regressed."
        )

    def test_documented_deadline_query_is_correct(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """The query shipped in ROUTING_QUERY_REFERENCE.md returns sound results

        Mutation-tested: swapping localdatetime() for datetime(), deleting the
        overnight guard, and computing the journey total by endpoint subtraction
        each make this fail. Three further mutants survive because they cannot
        change the result set -- {1,2}->{0,2} (the probe route has no nonstop),
        the contiguity check, and a query-side layover window (both already
        invariants of the edge).
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            total = session.run(
                "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
            ).single()["count"]
            if total == 0:
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            # Pick a real origin/dest/date triple out of the graph rather than
            # hard-coding one: a pair that needs a connection, on a date whose
            # edges are built. Hard-coded routes are what made the deleted
            # test_performance_baseline.py rot (it pinned date('2024-03-01')).
            #
            # The nonstop pairs are collected ONCE for the chosen date and
            # compared with IN. The natural phrasing -- a NOT EXISTS subquery on
            # each edge -- re-runs that lookup per row and takes 406s against a
            # 4M-edge graph, versus ~1.7s here.
            #
            # The route must also EXERCISE the hazards, not merely exist. An
            # all-Florida pair like MCO->TPA has no timezone spread and no
            # red-eye, so it passes even with the overnight guard deleted and
            # the journey total computed by endpoint subtraction -- verified by
            # mutation testing. Requiring >= 120 min of timezone skew and an
            # overnight terminal leg that falls before the cutoff makes every
            # assertion below load-bearing.
            probe = session.run(
                """
                MATCH (s1:Schedule)-[:CONNECTS_TO]->()
                WITH s1.flightdate AS d, count(*) AS n ORDER BY n DESC LIMIT 1
                CALL (d) {
                    MATCH (f:Schedule) WHERE f.flightdate = d
                    RETURN collect(DISTINCT f.origin + '>' + f.dest) AS nonstop
                }
                MATCH (s1:Schedule)-[:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.flightdate = d
                  AND NOT s1.origin + '>' + s2.dest IN nonstop
                WITH d, s1, s2,
                     duration.inSeconds(s1.scheduled_departure_time,
                                        s2.scheduled_arrival_time
                                        ).seconds / 60 AS apparent,
                     s1.scheduled_duration_minutes
                       + s2.scheduled_duration_minutes AS block,
                     date(s2.scheduled_arrival_time) > s2.flightdate AS overnight
                WITH d, s1, s2, apparent, block, overnight
                WITH d, s1.origin AS origin, s2.dest AS dest, count(*) AS options,
                     max(abs(apparent - block)) AS tz_skew,
                     sum(CASE WHEN overnight THEN 1 ELSE 0 END) AS overnights,
                     min(CASE WHEN NOT overnight
                              THEN s2.scheduled_arrival_time END) AS earliest_real
                WHERE options >= 10 AND tz_skew >= 120 AND overnights > 0
                  AND earliest_real IS NOT NULL
                // A fixed 15:00 cutoff is not valid for every route: MCO->ANC
                // crosses four timezones and has no arrival before 15:00 local
                // at all, so the test would fail on a correct query. Derive the
                // deadline instead -- two hours past the earliest genuinely
                // achievable arrival, which admits real itineraries while the
                // 00:0x overnight legs still fall below it.
                RETURN origin, dest, toString(d) AS date,
                       toString(earliest_real
                                + duration({minutes: 120})) AS deadline
                ORDER BY tz_skew DESC, options DESC LIMIT 1
                """
            ).single()
            if probe is None:
                pytest.skip("No connection-only city pair found in the graph")

            rows = list(
                session.run(
                    """
                    MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {code: $o})
                    WHERE first.flightdate = date($date)
                    MATCH p = (first)-[:CONNECTS_TO]->{1,2}(last:Schedule)
                    MATCH (last)-[:ARRIVES_AT]->(:Airport {code: $dst})
                    WITH nodes(p) AS legs, relationships(p) AS conns, last AS f
                    // One predicate, no overnight guard: the stored arrival now
                    // carries the DESTINATION's date, so a red-eye compares as
                    // the next day. The assertions below re-derive the true
                    // arrival independently and would catch it if it did not.
                    WHERE f.scheduled_arrival_time < localdatetime($deadline)
                    RETURN size(legs) - 1 AS stops,
                           [x IN legs | x.origin] AS origins,
                           [x IN legs | x.dest] AS dests,
                           toString(f.scheduled_arrival_time) AS arrival,
                           // Raw ingredients so the assertions can recompute
                           // every derived value instead of trusting the query.
                           [x IN legs | x.scheduled_duration_minutes] AS blocks,
                           [c IN conns | c.layover_minutes] AS layovers,
                           duration.inSeconds(f.scheduled_departure_time,
                                              f.scheduled_arrival_time
                                              ).seconds / 60 AS final_apparent,
                           f.scheduled_duration_minutes AS final_block,
                           // Independent ground truth for the overnight check:
                           // the true elapsed local days, from the UTC instants
                           // plus the block time, not from the stored local pair.
                           duration.inSeconds(legs[0].scheduled_departure_utc,
                                              f.scheduled_arrival_utc
                                              ).seconds / 60 AS utc_elapsed,
                           reduce(t = 0, x IN legs |
                                  t + x.scheduled_duration_minutes) +
                           reduce(t = 0, c IN conns |
                                  t + c.layover_minutes) AS total_minutes
                    // No LIMIT: the assertions below check EVERY row the
                    // predicates admit. With `LIMIT 10` the sound itineraries
                    // sort to the top and the ~60 defective ones hide beneath
                    // it, so removing a guard still passed -- confirmed by
                    // mutation testing. A few hundred rows is cheap.
                    ORDER BY total_minutes
                    """,
                    o=probe["origin"],
                    dst=probe["dest"],
                    date=probe["date"],
                    deadline=probe["deadline"],
                )
            )

        where = (
            f"{probe['origin']}->{probe['dest']} on {probe['date']} "
            f"arriving before {probe['deadline']}"
        )
        cutoff = probe["deadline"]
        assert rows, (
            f"The documented deadline query returned nothing for {where}, "
            "which has at least 10 connecting options. This is the exact "
            "symptom of the localdatetime/datetime NULL trap."
        )
        for row in rows:
            assert row["stops"] >= 1, f"{where}: {{1,2}} returned a nonstop"
            assert row["origins"][0] == probe["origin"], f"{where}: wrong origin"
            assert row["dests"][-1] == probe["dest"], f"{where}: wrong dest"
            # Legs must actually chain: each hop departs where the last landed.
            for i in range(len(row["dests"]) - 1):
                assert row["dests"][i] == row["origins"][i + 1], (
                    f"{where}: itinerary is not contiguous — leg {i} lands at "
                    f"{row['dests'][i]} but leg {i + 1} departs "
                    f"{row['origins'][i + 1]}"
                )
            assert row["arrival"] < cutoff, (
                f"{where}: returned an itinerary arriving {row['arrival']}, "
                f"past the deadline"
            )
            # Recompute the journey from its parts. A range check alone is too
            # weak: endpoint subtraction lands inside any plausible range on
            # plenty of routes, so it would pass while being wrong. Exact
            # equality against sum(blocks) + sum(layovers) does not.
            expected = sum(row["blocks"]) + sum(row["layovers"])
            # The stored arrival is what the query filtered on, so re-checking it
            # against the cutoff proves nothing. Re-derive the journey from the
            # UTC instants instead: they are real timestamps, so their difference
            # must equal the same blocks-plus-layovers total. Because the deadline
            # is derived from a genuinely achievable same-day arrival, an
            # itinerary that truly lands the next day cannot satisfy both this and
            # the cutoff above. This is the assertion that fails if the
            # arrival-date repair regresses to the origin's date.
            assert row["utc_elapsed"] == expected, (
                f"{where}: itinerary ending {row['dests'][-1]} reports arrival "
                f"{row['arrival']}, but its UTC instants span "
                f"{row['utc_elapsed']} min against {expected} min of block time "
                "plus layovers. The stored local arrival and the UTC pair "
                "disagree, so one is wrong — most likely the arrival-date repair "
                "in write_utc_times() has regressed."
            )
            assert row["total_minutes"] == expected, (
                f"{where}: journey reported as {row['total_minutes']} min but "
                f"its {len(row['blocks'])} block times and "
                f"{len(row['layovers'])} layovers sum to {expected} min. "
                "Subtracting the endpoint timestamps is wrong — they are in "
                "different timezones."
            )
            assert all(45 <= lay <= 300 for lay in row["layovers"]), (
                f"{where}: layovers {row['layovers']} fall outside the 45-300 "
                "minute window the edges were built with"
            )


class TestItineraryShape:
    """
    Guard properties of the itinerary as a whole, which no single edge can carry.

    CONNECTS_TO enforces everything local to one connection -- carrier, layover
    window, contiguity, no immediate backtrack. A path property like "no airport
    twice" is invisible to it, and that gap is real: 18% of LGA->DFW {0,3} paths
    revisit an airport. See "Itineraries revisit airports" in
    ROUTING_QUERY_REFERENCE.md.
    """

    # The documented guard, kept in one place so the tests below and the docs
    # cannot drift apart. Operates on airport CODES, not on path nodes.
    ACYCLIC_GUARD = """
        WITH legs, conns, [legs[0].origin] + [x IN legs | x.dest] AS airports
        WHERE size(airports) = size([i IN range(0, size(airports) - 1)
                                     WHERE NOT airports[i] IN airports[0..i]])
    """

    def _busiest_date(self, session):
        record = session.run(
            """
            MATCH (s:Schedule)-[:CONNECTS_TO]->()
            WITH s.flightdate AS d, count(*) AS n ORDER BY n DESC LIMIT 1
            RETURN toString(d) AS date
            """
        ).single()
        return record["date"] if record else None

    def _densest_route(self, session, date):
        """A route with enough 3-leg paths to actually contain a cycle.

        Chosen from the graph rather than hard-coded: a thin route may have no
        cyclic path at all, which would make the assertions below vacuous.
        """
        record = session.run(
            """
            MATCH (s1:Schedule)-[:CONNECTS_TO]->(:Schedule)-[:CONNECTS_TO]->(s3)
            WHERE s1.flightdate = date($date)
            WITH s1.origin AS origin, s3.dest AS dest, count(*) AS paths
            WHERE origin <> dest
            RETURN origin, dest ORDER BY paths DESC LIMIT 1
            """,
            date=date,
        ).single()
        return (record["origin"], record["dest"]) if record else (None, None)

    def test_unguarded_qpp_does_revisit_airports(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Anti-vacuity: the hazard the guard exists for is present in the graph

        Without this, test_guarded_qpp_never_revisits_an_airport could pass on a
        dataset where no cyclic path exists, gating nothing. Assert the graph can
        expose the bug before asserting the guard suppresses it.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
                ).single()["count"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            date = self._busiest_date(session)
            origin, dest = self._densest_route(session, date)
            if origin is None:
                pytest.skip("No 2-stop route found in the graph")

            record = session.run(
                """
                MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {code: $origin})
                WHERE first.flightdate = date($date)
                MATCH p = (first)-[:CONNECTS_TO]->{0,3}(last:Schedule)
                MATCH (last)-[:ARRIVES_AT]->(:Airport {code: $dest})
                WITH nodes(p) AS legs
                WITH [legs[0].origin] + [x IN legs | x.dest] AS ap
                WITH ap, size(ap) AS n,
                     size([i IN range(0, size(ap) - 1)
                           WHERE NOT ap[i] IN ap[0..i]]) AS uniq
                RETURN count(*) AS total,
                       sum(CASE WHEN n <> uniq THEN 1 ELSE 0 END) AS cyclic,
                       sum(CASE WHEN ap[0] IN ap[1..] THEN 1 ELSE 0 END)
                           AS back_to_origin
                """,
                origin=origin,
                dest=dest,
                date=date,
            ).single()

        where = f"{origin}->{dest} on {date}"
        assert record["total"] > 0, f"{where}: {{0,3}} returned no paths at all"
        assert record["cyclic"] > 0, (
            f"{where}: none of {record['total']:,} unguarded {{0,3}} paths "
            "revisits an airport, so this dataset cannot detect a routing query "
            "that omits the acyclicity guard. Pick a denser route or date."
        )

    def test_guarded_qpp_never_revisits_an_airport(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """The documented guard removes every airport revisit, and only those

        Mutation-tested: deleting the guard fails on the revisit assertion, and
        replacing it with Cypher's ACYCLIC or TRAIL path mode ALSO fails --
        those modes deduplicate path nodes, which here are Schedule nodes and are
        always distinct. The repeating entity is an Airport reached off-path.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
                ).single()["count"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            date = self._busiest_date(session)
            origin, dest = self._densest_route(session, date)
            if origin is None:
                pytest.skip("No 2-stop route found in the graph")

            rows = list(
                session.run(
                    f"""
                    MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {{code: $origin}})
                    WHERE first.flightdate = date($date)
                    MATCH p = (first)-[:CONNECTS_TO]->{{0,3}}(last:Schedule)
                    MATCH (last)-[:ARRIVES_AT]->(:Airport {{code: $dest}})
                    WITH nodes(p) AS legs, relationships(p) AS conns
                    {self.ACYCLIC_GUARD}
                    // Raw airport list so the assertion recomputes uniqueness
                    // itself rather than trusting the guard's own arithmetic.
                    RETURN airports, size(legs) - 1 AS stops
                    """,
                    origin=origin,
                    dest=dest,
                    date=date,
                )
            )

        where = f"{origin}->{dest} on {date}"
        assert rows, (
            f"{where}: the guarded {{0,3}} query returned nothing. The guard "
            "should prune cyclic paths, not all of them."
        )
        for row in rows:
            airports = row["airports"]
            assert len(airports) == len(set(airports)), (
                f"{where}: returned itinerary {' -> '.join(airports)} visits an "
                "airport twice. No airline sells this. The acyclicity guard is "
                "missing or ineffective — note that Cypher's ACYCLIC/TRAIL path "
                "modes do NOT work here, because the path's nodes are flights."
            )
            assert airports[0] == origin, f"{where}: wrong origin {airports[0]}"
            assert airports[-1] == dest, f"{where}: wrong dest {airports[-1]}"
            assert row["stops"] == len(airports) - 2, (
                f"{where}: {row['stops']} stops but {len(airports)} airports in "
                f"{' -> '.join(airports)}"
            )

    def test_depart_after_query_is_correct(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """The depart-after query in ROUTING_QUERY_REFERENCE.md is sound

        The mirror of the deadline query. It needs no overnight guard: it
        constrains the FIRST leg's departure, which is local at the origin and
        is the one timestamp carrying no date ambiguity. It does still need
        localdatetime() -- comparing a LOCAL DATETIME to a ZONED one yields NULL
        and silently drops every row.

        Mutation-tested: swapping localdatetime() for datetime() returns zero
        rows and fails; deleting the acyclicity guard fails; computing the
        journey total by endpoint subtraction fails.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
                ).single()["count"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            date = self._busiest_date(session)
            origin, dest = self._densest_route(session, date)
            if origin is None:
                pytest.skip("No 2-stop route found in the graph")
            after = f"{date}T08:00:00"

            rows = list(
                session.run(
                    f"""
                    MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {{code: $origin}})
                    WHERE first.flightdate = date($date)
                      AND first.scheduled_departure_time >= localdatetime($after)
                    MATCH p = (first)-[:CONNECTS_TO]->{{0,2}}(last:Schedule)
                    MATCH (last)-[:ARRIVES_AT]->(:Airport {{code: $dest}})
                    WITH nodes(p) AS legs, relationships(p) AS conns
                    {self.ACYCLIC_GUARD}
                    RETURN airports,
                           size(legs) - 1 AS stops,
                           toString(legs[0].scheduled_departure_time) AS departs,
                           // Raw ingredients: the assertions recompute every
                           // derived value instead of trusting the query.
                           [x IN legs | x.scheduled_duration_minutes] AS blocks,
                           [c IN conns | c.layover_minutes] AS layovers,
                           reduce(t = 0, x IN legs |
                                  t + x.scheduled_duration_minutes) +
                           reduce(t = 0, c IN conns |
                                  t + c.layover_minutes) AS total_minutes
                    // No LIMIT: assert every row the predicates admit. Under a
                    // LIMIT the sound itineraries sort to the top and defective
                    // ones hide beneath it.
                    ORDER BY total_minutes
                    """,
                    origin=origin,
                    dest=dest,
                    date=date,
                    after=after,
                )
            )

        where = f"{origin}->{dest} on {date} departing after 08:00"
        assert rows, (
            f"The depart-after query returned nothing for {where}. This is the "
            "symptom of the localdatetime/datetime NULL trap — a ZONED "
            "comparison evaluates NULL and WHERE discards every row."
        )
        for row in rows:
            assert row["departs"] >= after, (
                f"{where}: returned an itinerary departing {row['departs']}, "
                "before the requested time"
            )
            airports = row["airports"]
            assert len(airports) == len(set(airports)), (
                f"{where}: itinerary {' -> '.join(airports)} visits an airport " "twice"
            )
            expected = sum(row["blocks"]) + sum(row["layovers"])
            assert row["total_minutes"] == expected, (
                f"{where}: journey reported as {row['total_minutes']} min but "
                f"its block times and layovers sum to {expected} min. "
                "Subtracting the endpoint timestamps is wrong — they are in "
                "different timezones."
            )
            assert all(45 <= lay <= 300 for lay in row["layovers"]), (
                f"{where}: layovers {row['layovers']} fall outside the 45-300 "
                "minute window the edges were built with"
            )

    def test_results_are_ordered_by_journey_time(
        self, neo4j_driver, neo4j_database, loaded_graph
    ):
        """Ranked output is monotonic in total journey minutes

        A travel-site backend has to return options in a defensible order. The
        sound ranking key is sum(block times) + sum(layovers); endpoint
        subtraction is not, because the endpoints sit in different timezones.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS count"
                ).single()["count"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")
            date = self._busiest_date(session)
            origin, dest = self._densest_route(session, date)
            if origin is None:
                pytest.skip("No 2-stop route found in the graph")

            rows = list(
                session.run(
                    f"""
                    MATCH (first:Schedule)-[:DEPARTS_FROM]->(:Airport {{code: $origin}})
                    WHERE first.flightdate = date($date)
                    MATCH p = (first)-[:CONNECTS_TO]->{{0,2}}(last:Schedule)
                    MATCH (last)-[:ARRIVES_AT]->(:Airport {{code: $dest}})
                    WITH nodes(p) AS legs, relationships(p) AS conns
                    {self.ACYCLIC_GUARD}
                    WITH [x IN legs | x.scheduled_duration_minutes] AS blocks,
                         [c IN conns | c.layover_minutes] AS layovers,
                         reduce(t = 0, x IN legs |
                                t + x.scheduled_duration_minutes) +
                         reduce(t = 0, c IN conns |
                                t + c.layover_minutes) AS total_minutes
                    RETURN blocks, layovers, total_minutes
                    ORDER BY total_minutes
                    LIMIT 25
                    """,
                    origin=origin,
                    dest=dest,
                    date=date,
                )
            )

        where = f"{origin}->{dest} on {date}"
        assert len(rows) >= 2, f"{where}: need 2+ itineraries to check ordering"
        totals = [r["total_minutes"] for r in rows]
        assert totals == sorted(
            totals
        ), f"{where}: results are not ordered by journey time: {totals}"
        # And the key itself is the sound one, recomputed here.
        for row in rows:
            expected = sum(row["blocks"]) + sum(row["layovers"])
            assert row["total_minutes"] == expected, (
                f"{where}: ranking key {row['total_minutes']} != "
                f"sum(blocks) + sum(layovers) = {expected}"
            )
        assert totals[-1] > totals[0], (
            f"{where}: every itinerary has an identical {totals[0]}-minute "
            "journey, so this cannot demonstrate that ordering works"
        )


class TestBusinessLogic:
    """Test business logic patterns"""

    def test_morning_flights(self, neo4j_driver, neo4j_database, search_date):
        """Test morning flight filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                WHERE s.flightdate = date($search_date)
                  AND s.scheduled_departure_time IS NOT NULL
                  AND s.scheduled_departure_time.hour >= 6
                  AND s.scheduled_departure_time.hour < 9
                  AND dep.code IN ['ATL', 'DFW', 'DEN', 'ORD']
                RETURN count(s) AS morning_business_flights
            """,
                search_date=search_date,
            )
            morning = result.single()["morning_business_flights"]
            assert morning > 0, f"Should find morning business flights, got {morning}"

    def test_multi_constraint_query(self, neo4j_driver, neo4j_database, search_date):
        """Test complex multi-constraint business logic"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                MATCH (s)-[:OPERATED_BY]->(c:Carrier)
                WHERE s.flightdate = date($search_date)
                  AND dep.code <> arr.code
                  AND dep.code IN ['ATL', 'DFW']
                  AND arr.code IN ['ATL', 'DFW']
                RETURN count(DISTINCT c) AS carriers_on_route
            """,
                search_date=search_date,
            )
            route_carriers = result.single()["carriers_on_route"]
            assert (
                route_carriers > 0
            ), f"Should find carriers on business routes, got {route_carriers}"
