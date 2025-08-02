#!/usr/bin/env python3
"""
Test Graph Database Validation
==============================

Test cases to validate graph database structure and basic functionality.
"""

import os

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase


@pytest.fixture(scope="session")
def neo4j_driver():
    """Neo4j database connection for tests"""
    load_dotenv(override=True)
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
    )
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def neo4j_database():
    """Neo4j database name"""
    load_dotenv(override=True)
    return os.getenv("NEO4J_DATABASE")


class TestBasicConnectivity:
    """Test basic database connectivity and data presence"""

    def test_database_connection(self, neo4j_driver, neo4j_database):
        """Test database connection works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("RETURN 'Connected' AS status")
            record = result.single()
            assert record["status"] == "Connected"

    def test_schedule_nodes_present(self, neo4j_driver, neo4j_database):
        """Test that Schedule nodes exist"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("MATCH (s:Schedule) RETURN count(s) AS count")
            count = result.single()["count"]
            assert (
                count > 500000
            ), f"Expected >500K Schedule nodes (BTS March 2024), got {count:,}"

    def test_relationships_present(self, neo4j_driver, neo4j_database):
        """Test that relationships exist"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("MATCH ()-[r]->() RETURN count(r) AS count LIMIT 1")
            rel_count = result.single()["count"]
            assert (
                rel_count > 1500000
            ), f"Expected >1.5M relationships (3x BTS March data), got {rel_count:,}"

    def test_sample_schedule_properties(self, neo4j_driver, neo4j_database):
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

    def test_string_date_filter(self, neo4j_driver, neo4j_database):
        """Test date-based filtering works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date('2024-03-01')
                RETURN count(s) AS count
            """
            )
            count = result.single()["count"]
            assert count > 0, f"Expected flights on 2024-03-01, got {count}"

    def test_date_range_filter(self, neo4j_driver, neo4j_database):
        """Test date range filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Specific date
            result1 = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date('2024-03-01')
                RETURN count(s) AS count
            """
            )
            specific_count = result1.single()["count"]

            # Date range that should match same day
            result2 = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate >= date('2024-03-01')
                  AND s.flightdate < date('2024-03-02')
                RETURN count(s) AS count
            """
            )
            range_count = result2.single()["count"]

            assert (
                range_count == specific_count
            ), "Date range should match specific date"

    def test_time_string_extraction(self, neo4j_driver, neo4j_database):
        """Test time string extraction works"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time IS NOT NULL
                RETURN toString(s.scheduled_departure_time) AS time_str
                LIMIT 1
            """
            )
            record = result.single()
            time_str = record["time_str"] if record else None

            assert time_str is not None, "Should extract time string"
            assert ":" in time_str, f"Time string should contain ':', got {time_str}"
            # Should be a proper datetime string like "2024-03-01T14:30:00Z"
            assert (
                "T" in time_str
            ), f"DateTime string should contain 'T', got {time_str}"


class TestGraphTraversal:
    """Test graph traversal patterns"""

    def test_single_hop_traversal(self, neo4j_driver, neo4j_database):
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

    def test_direct_route_finding(self, neo4j_driver, neo4j_database):
        """Test finding direct routes"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'ATL'})
                WHERE s.flightdate = date('2024-03-01')
                RETURN count(s) AS direct_flights
            """
            )
            direct = result.single()["direct_flights"]
            assert direct > 0, f"Should find LGA→ATL direct flights, got {direct}"

    def test_connection_finding(self, neo4j_driver, neo4j_database):
        """Test finding connection routes"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'DFW'})
                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
                  AND hub.code <> 'LGA' AND hub.code <> 'DFW'
                RETURN count(*) AS connections
            """
            )
            connections = result.single()["connections"]
            assert (
                connections > 0
            ), f"Should find LGA→hub→DFW connections, got {connections}"


class TestNetworkAnalysis:
    """Test network analysis patterns"""

    def test_hub_identification(self, neo4j_driver, neo4j_database):
        """Test hub airport identification"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (hub:Airport)<-[:ARRIVES_AT]-(s:Schedule)
                WHERE s.flightdate = date('2024-03-01')
                WITH hub, count(s) AS arrivals
                WHERE arrivals > 200
                RETURN count(hub) AS major_hubs
            """
            )
            hubs = result.single()["major_hubs"]
            assert hubs >= 5, f"Should find >=5 major US hubs, got {hubs}"

    def test_carrier_analysis(self, neo4j_driver, neo4j_database):
        """Test carrier network analysis"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:OPERATED_BY]->(c:Carrier)
                WHERE s.flightdate = date('2024-03-01')
                WITH c, count(s) AS flights
                WHERE flights > 100
                RETURN count(c) AS major_carriers
            """
            )
            carriers = result.single()["major_carriers"]
            assert carriers >= 5, f"Should find >=5 major carriers, got {carriers}"


class TestBusinessLogic:
    """Test business logic patterns"""

    def test_morning_flights(self, neo4j_driver, neo4j_database):
        """Test morning flight filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time IS NOT NULL
                  AND s.scheduled_departure_time.hour >= 6
                  AND s.scheduled_departure_time.hour < 9
                  AND dep.code IN ['ATL', 'DFW', 'DEN', 'ORD']
                RETURN count(s) AS morning_business_flights
            """
            )
            morning = result.single()["morning_business_flights"]
            assert morning > 0, f"Should find morning business flights, got {morning}"

    def test_multi_constraint_query(self, neo4j_driver, neo4j_database):
        """Test complex multi-constraint business logic"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                MATCH (s)-[:OPERATED_BY]->(c:Carrier)
                WHERE s.flightdate = date('2024-03-01')
                  AND dep.code <> arr.code
                  AND dep.code IN ['ATL', 'DFW']
                  AND arr.code IN ['ATL', 'DFW']
                RETURN count(DISTINCT c) AS carriers_on_route
            """
            )
            route_carriers = result.single()["carriers_on_route"]
            assert (
                route_carriers > 0
            ), f"Should find carriers on business routes, got {route_carriers}"
