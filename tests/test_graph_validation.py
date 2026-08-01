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
            # s1's stored arrival is local at the hub with no timezone, so its
            # DATE is unreliable. scheduled_duration_minutes (BTS
            # CRSElapsedTime) is timezone-independent: a negative apparent
            # duration that reconciles with the block time once a day is added
            # means s1 really lands the NEXT morning, so a same-day connection
            # off it is not bookable. This caught 17,502 false edges (3.29%).
            false_edges = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->()
                WITH r, duration.inSeconds(s1.scheduled_departure_time,
                                           s1.scheduled_arrival_time
                                           ).seconds / 60 AS apparent,
                     s1.scheduled_duration_minutes AS block
                WHERE apparent < 0 AND abs(apparent + 1440 - block) <= 180
                RETURN count(r) AS count
                """
            ).single()["count"]
            # Anti-vacuity: the assertion above is only meaningful if the graph
            # actually contains overnight legs for the builder to have excluded.
            # On a dataset with none it would pass no matter what the builder
            # did. One BTS day holds ~900 of them (893 on 2025-07-18).
            overnight_legs = session.run(
                """
                MATCH (s:Schedule)
                WITH duration.inSeconds(s.scheduled_departure_time,
                                        s.scheduled_arrival_time
                                        ).seconds / 60 AS apparent,
                     s.scheduled_duration_minutes AS block
                WHERE apparent < 0 AND abs(apparent + 1440 - block) <= 180
                RETURN count(*) AS count
                """
            ).single()["count"]

        assert false_edges == 0, (
            f"{false_edges:,} of {total:,} CONNECTS_TO edges connect off an "
            "inbound leg that actually arrives the next calendar day"
        )
        assert overnight_legs > 0, (
            "No overnight legs in the graph at all, so the assertion above "
            "proves nothing — the dataset under test cannot detect a "
            "regression in the builder's overnight exclusion"
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
