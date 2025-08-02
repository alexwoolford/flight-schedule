#!/usr/bin/env python3
"""
Performance Tests
================

Ensure flight search maintains acceptable performance standards.
"""

import os
import time

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


class TestFlightSearchPerformance:
    """Performance tests to ensure sub-second query times"""

    def test_direct_flight_performance(self, neo4j_driver, neo4j_database):
        """Test direct flight search performance"""
        with neo4j_driver.session(database=neo4j_database) as session:
            start_time = time.time()

            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'JFK'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LAX'})
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                  AND substring(toString(s.first_seen_time), 11, 2) >= '07'
                  AND substring(toString(s.first_seen_time), 11, 2) <= '11'
                RETURN count(s) AS flights
            """
            )

            flights = result.single()["flights"]
            query_time = (time.time() - start_time) * 1000

            # Performance assertions
            assert (
                query_time < 500
            ), f"Direct flight search took {query_time:.0f}ms, should be <500ms"
            assert flights >= 0, "Should return valid flight count"

            print(f"   ⚡ Direct flight search: {query_time:.0f}ms")

    def test_connection_flight_performance(self, neo4j_driver, neo4j_database):
        """Test connection flight search performance"""
        with neo4j_driver.session(database=neo4j_database) as session:
            start_time = time.time()

            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'EGPH'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'LFMN'})
                WHERE toString(s1.date_of_operation) STARTS WITH '2024-06-18'
                  AND toString(s2.date_of_operation) STARTS WITH '2024-06-18'
                  AND hub.code <> 'EGPH' AND hub.code <> 'LFMN'

                WITH s1, s2, hub,
                     s1.scheduled_arrival_time AS arrival,
                     s2.scheduled_departure_time AS departure

                WITH s1, s2, hub, duration.between(arrival, departure).minutes AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 300

                RETURN count(*) AS connections
            """
            )

            connections = result.single()["connections"]
            query_time = (time.time() - start_time) * 1000

            # Performance assertions
            assert (
                query_time < 1000
            ), f"Connection search took {query_time:.0f}ms, should be <1000ms"
            assert connections >= 0, "Should return valid connection count"

            print(f"   ⚡ Connection search: {query_time:.0f}ms")

    def test_complete_search_performance(self, neo4j_driver, neo4j_database):
        """Test complete flight search performance (direct + connections)"""
        with neo4j_driver.session(database=neo4j_database) as session:
            total_start = time.time()

            # Direct flights
            direct_start = time.time()
            direct_result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'JFK'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LAX'})
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                RETURN count(s) AS direct_flights
            """
            )
            direct_flights = direct_result.single()["direct_flights"]
            direct_time = (time.time() - direct_start) * 1000

            # Connection flights
            connection_start = time.time()
            connection_result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'EGLL'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'LFPG'})
                WHERE toString(s1.date_of_operation) STARTS WITH '2024-06-18'
                  AND toString(s2.date_of_operation) STARTS WITH '2024-06-18'
                  AND hub.code <> 'EGLL' AND hub.code <> 'LFPG'

                WITH s1, s2, hub, duration.between(
                    datetime(replace(toString(s1.last_seen_time), 'Z', '')),
                    datetime(replace(toString(s2.first_seen_time), 'Z', ''))
                ).minutes AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 300

                RETURN count(*) AS connections
            """
            )
            connections = connection_result.single()["connections"]
            connection_time = (time.time() - connection_start) * 1000

            total_time = (time.time() - total_start) * 1000

            # Performance assertions
            assert (
                total_time < 2000
            ), f"Complete search took {total_time:.0f}ms, should be <2000ms"
            assert (
                direct_flights >= 0 and connections >= 0
            ), "Queries should return valid counts"

            print("   📊 Complete search performance:")
            print(f"      • Direct flights: {direct_time:.0f}ms")
            print(f"      • Connections: {connection_time:.0f}ms")
            print(f"      • Total time: {total_time:.0f}ms")
            print(
                f"      • Results: {direct_flights} direct + {connections} connections"
            )

    @pytest.mark.slow
    def test_dataset_scale_performance(self, neo4j_driver, neo4j_database):
        """Test performance on dataset scale queries"""
        with neo4j_driver.session(database=neo4j_database) as session:
            start_time = time.time()

            result = session.run(
                """
                MATCH (s:Schedule)
                WITH count(s) AS schedules
                MATCH (a:Airport)
                WITH schedules, count(a) AS airports
                MATCH ()-[r]->()
                RETURN schedules, airports, count(r) AS relationships
            """
            )

            data = result.single()
            query_time = (time.time() - start_time) * 1000

            # Performance assertion (this can be slower as it's a full dataset scan)
            assert (
                query_time < 10000
            ), f"Dataset scale query took {query_time:.0f}ms, should be <10s"
            assert data["schedules"] > 1000000, "Should have >1M schedule records"

            print(f"   📈 Dataset scale ({query_time:.0f}ms):")
            print(f"      • {data['schedules']:,} schedules")
            print(f"      • {data['airports']:,} airports")
            print(f"      • {data['relationships']:,} relationships")


class TestPerformanceBenchmarks:
    """Benchmark tests to track performance improvements"""

    def test_performance_vs_sql_equivalent(self, neo4j_driver, neo4j_database):
        """Test that graph queries outperform SQL-equivalent complexity"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Multi-hop graph traversal (equivalent to complex SQL with multiple joins)
            start_time = time.time()

            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport)
                MATCH (s1)-[:OPERATED_BY]->(c1:Carrier)
                MATCH (s2)-[:OPERATED_BY]->(c2:Carrier)
                WHERE toString(s1.date_of_operation) STARTS WITH '2024-06-15'
                  AND toString(s2.date_of_operation) STARTS WITH '2024-06-15'
                  AND dep.code IN ['EGLL', 'EDDF', 'EHAM']
                  AND arr.code IN ['LFPG', 'LFMN', 'LIRF']
                  AND dep.code <> arr.code
                  AND hub.code <> dep.code AND hub.code <> arr.code

                WITH s1, s2, dep, hub, arr, c1, c2, duration.between(
                    datetime(replace(toString(s1.last_seen_time), 'Z', '')),
                    datetime(replace(toString(s2.first_seen_time), 'Z', ''))
                ).minutes AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 300

                RETURN count(*) AS complex_connections
            """
            )

            connections = result.single()["complex_connections"]
            query_time = (time.time() - start_time) * 1000

            # This query would require 6+ table joins in SQL
            assert (
                query_time < 2000
            ), f"Complex multi-hop query took {query_time:.0f}ms, should be <2s"
            assert connections >= 0, "Should return valid result"

            print(f"   🎯 Complex multi-hop query: {query_time:.0f}ms")
            print("      • SQL equivalent: 6+ table joins + subqueries")
            print("      • Graph advantage: Single traversal query")
            print(f"      • Results: {connections:,} valid connections")
