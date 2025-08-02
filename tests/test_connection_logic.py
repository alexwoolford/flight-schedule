#!/usr/bin/env python3
"""
Test Connection Logic - CRITICAL for catching query errors
=========================================================

This test specifically validates that connection queries have correct temporal logic.
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


class TestConnectionLogic:
    """Critical tests for connection query logic"""

    def test_no_negative_connection_times(self, neo4j_driver, neo4j_database):
        """CRITICAL: Ensure no connections have negative connection times"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Test connection query that should NEVER return negative times
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport)

                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND dep.code <> arr.code
                  AND hub.code <> dep.code AND hub.code <> arr.code
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time  // CRITICAL: Ensure proper time ordering

                WITH s1, s2, dep, hub, arr,
                     s1.scheduled_arrival_time AS hub_arrival,
                     s2.scheduled_departure_time AS hub_departure

                WITH dep, hub, arr, hub_arrival, hub_departure,
                     duration.between(hub_arrival, hub_departure).minutes AS connection_minutes

                WHERE connection_minutes >= 45 AND connection_minutes <= 300

                RETURN
                    dep.code AS origin,
                    hub.code AS hub_code,
                    arr.code AS destination,
                    connection_minutes,
                    hub_arrival,
                    hub_departure
                LIMIT 100
            """
            )

            connections = list(result)

            # CRITICAL: All connection times must be positive
            for conn in connections:
                connection_time = conn["connection_minutes"]
                assert connection_time > 0, (
                    f"Connection {conn['origin']} -> {conn['hub_code']} -> {conn['destination']} "
                    f"has negative connection time: {connection_time} minutes. "
                    f"Arrival: {conn['hub_arrival']}, Departure: {conn['hub_departure']}"
                )

                # CRITICAL: Departure must be after arrival
                assert conn["hub_departure"] > conn["hub_arrival"], (
                    f"Connection {conn['origin']} -> {conn['hub_code']} -> {conn['destination']} "
                    f"has departure before arrival. "
                    f"Arrival: {conn['hub_arrival']}, Departure: {conn['hub_departure']}"
                )

            print(
                f"✅ Validated {len(connections)} connections - all have positive connection times"
            )

    def test_connection_query_finds_results(self, neo4j_driver, neo4j_database):
        """Test that we can find valid connections"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Test popular route using US airports in BTS data
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'DFW'})

                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND hub.code <> 'LGA' AND hub.code <> 'DFW'
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time

                WITH s1, s2, dep, hub, arr,
                     s1.scheduled_arrival_time AS hub_arrival,
                     s2.scheduled_departure_time AS hub_departure

                WITH dep, hub, arr, hub_arrival, hub_departure,
                     duration.between(hub_arrival, hub_departure).minutes AS connection_minutes

                WHERE connection_minutes >= 45 AND connection_minutes <= 300

                RETURN count(*) AS valid_connections
            """
            )

            count = result.single()["valid_connections"]
            assert count > 0, f"Should find valid LGA -> DFW connections, found {count}"
            print(f"✅ Found {count} valid LGA -> DFW connections")

    def test_wrong_query_logic_fails(self, neo4j_driver, neo4j_database):
        """Test that demonstrates why the old query logic was wrong"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # OLD WRONG QUERY (without time ordering check)
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'LGA'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'DFW'})

                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND hub.code <> 'LGA' AND hub.code <> 'DFW'
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  // MISSING: AND s2.scheduled_departure_time > s1.scheduled_arrival_time

                WITH s1, s2, dep, hub, arr,
                     s1.scheduled_arrival_time AS hub_arrival,
                     s2.scheduled_departure_time AS hub_departure

                WITH dep, hub, arr, hub_arrival, hub_departure,
                     duration.between(hub_arrival, hub_departure).minutes AS connection_minutes

                // This WHERE clause would fail with the browser error
                // WHERE connection_minutes >= 45 AND connection_minutes <= 300

                RETURN
                    count(CASE WHEN connection_minutes < 0 THEN 1 END) AS negative_connections,
                    count(*) AS total_connections
                LIMIT 1
            """
            )

            stats = result.single()
            negative_count = stats["negative_connections"]
            total_count = stats["total_connections"]

            # Demonstrate that the old logic produces negative connection times
            assert (
                negative_count > 0
            ), "Old query logic should produce negative connection times"
            print(
                f"❌ Old logic produces {negative_count} negative connections out of {total_count} total"
            )
            print(
                f"   This is why the browser query failed with 'Cannot divide DateTime by Long'"
            )
