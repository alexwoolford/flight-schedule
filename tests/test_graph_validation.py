#!/usr/bin/env python3
"""
Test Graph Database Validation
==============================

Test cases to validate graph database structure and basic functionality.

The `neo4j_driver`, `neo4j_database` and `search_date` fixtures come from
`tests/conftest.py`. `search_date` is read out of the graph, so these tests pass
against any loaded year rather than a hard-coded one.
"""

import pytest


class TestBasicConnectivity:
    """Test basic database connectivity and data presence"""

    def test_database_connection(self, neo4j_driver, neo4j_database):
        """Test database connection works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("RETURN 'Connected' AS status")
            record = result.single()
            assert record["status"] == "Connected"

    def test_schedule_nodes_present(self, loaded_graph):
        """Test that Schedule nodes exist"""
        # One BTS month is ~500K flights; a full year is ~6.9M. The floor is
        # set for a single-month load so the test works either way.
        assert (
            loaded_graph > 400000
        ), f"Expected >400K Schedule nodes (>=1 BTS month), got {loaded_graph:,}"

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
