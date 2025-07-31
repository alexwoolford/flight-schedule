#!/usr/bin/env python3
"""
Test Flight Search Functionality
===============================

Test cases to ensure flight search works correctly for different scenarios.
"""

import os
from datetime import datetime

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


class TestFlightSearch:
    """Test flight search functionality"""

    def test_popular_routes(self, neo4j_driver, neo4j_database):
        """Test flight search on popular routes"""
        test_date = "2024-06-18"

        with neo4j_driver.session(database=neo4j_database) as session:
            # Test 1: London to Paris (should have direct flights)
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'EGLL'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LFPG'})
                WHERE toString(s.date_of_operation) STARTS WITH $date
                RETURN count(s) AS direct_flights
            """,
                date=test_date,
            )

            london_paris = result.single()["direct_flights"]
            assert (
                london_paris > 0
            ), f"Expected London→Paris direct flights, got {london_paris}"

            # Test 2: Find Edinburgh to Nice connections
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'EGPH'})
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: 'LFMN'})
                WHERE toString(s1.date_of_operation) STARTS WITH $date
                  AND toString(s2.date_of_operation) STARTS WITH $date
                  AND hub.code <> 'EGPH' AND hub.code <> 'LFMN'
                
                WITH s1, s2, hub,
                     datetime(replace(toString(s1.last_seen_time), 'Z', '')) AS arrival,
                     datetime(replace(toString(s2.first_seen_time), 'Z', '')) AS departure
                
                WITH s1, s2, hub, duration.between(arrival, departure).minutes AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 480
                
                RETURN count(*) AS connections
            """,
                date=test_date,
            )

            connections = result.single()["connections"]
            assert (
                connections > 0
            ), f"Expected Edinburgh→Nice connections, got {connections}"

    def test_connection_timing(self, neo4j_driver, neo4j_database):
        """Test connection timing validation"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Test realistic connection times
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport)
                WHERE toString(s1.date_of_operation) STARTS WITH '2024-06-18'
                  AND toString(s2.date_of_operation) STARTS WITH '2024-06-18'
                  AND dep.code <> arr.code
                  AND hub.code <> dep.code AND hub.code <> arr.code
                
                WITH s1, s2, dep, hub, arr,
                     datetime(replace(toString(s1.last_seen_time), 'Z', '')) AS arrival,
                     datetime(replace(toString(s2.first_seen_time), 'Z', '')) AS departure
                
                WITH s1, s2, dep, hub, arr, duration.between(arrival, departure).minutes AS connection_time
                WHERE connection_time >= 45 AND connection_time <= 480
                
                RETURN 
                    min(connection_time) AS min_connection,
                    max(connection_time) AS max_connection,
                    count(*) AS total_valid_connections
            """
            )

            timing = result.single()
            min_conn = int(timing["min_connection"])
            max_conn = int(timing["max_connection"])
            total = timing["total_valid_connections"]

            assert (
                min_conn >= 45
            ), f"Minimum connection time should be >=45min, got {min_conn}min"
            assert (
                max_conn <= 480
            ), f"Maximum connection time should be <=480min, got {max_conn}min"
            assert total > 1000, f"Expected >1000 valid connections, got {total}"

    def test_time_filtering(self, neo4j_driver, neo4j_database):
        """Test departure time filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Test morning flights (6-12)
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                  AND substring(toString(s.first_seen_time), 11, 2) >= '06'
                  AND substring(toString(s.first_seen_time), 11, 2) < '12'
                RETURN count(s) AS morning_flights
            """
            )

            morning = result.single()["morning_flights"]
            assert morning > 1000, f"Expected >1000 morning flights, got {morning:,}"

            # Test specific hour filtering (around 10am)
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                  AND substring(toString(s.first_seen_time), 11, 2) = '10'
                RETURN count(s) AS ten_am_flights
            """
            )

            ten_am = result.single()["ten_am_flights"]
            assert ten_am > 100, f"Expected >100 10AM flights, got {ten_am:,}"

    def test_airport_coverage(self, neo4j_driver, neo4j_database):
        """Test coverage of major airports"""
        major_airports = [
            "EGLL",
            "LFPG",
            "EHAM",
            "EDDF",
            "EGPH",
            "LFMN",
            "EIDW",
            "EGCC",
        ]

        with neo4j_driver.session(database=neo4j_database) as session:
            for airport in major_airports:
                result = session.run(
                    """
                    MATCH (s:Schedule)-[:DEPARTS_FROM]->(a:Airport {code: $code})
                    WHERE toString(s.date_of_operation) STARTS WITH '2024-06'
                    RETURN count(s) AS departures
                """,
                    code=airport,
                )

                departures = result.single()["departures"]
                assert (
                    departures > 100
                ), f"Airport {airport} should have >100 departures, got {departures:,}"

    @pytest.mark.integration
    def test_traveler_scenarios(self, neo4j_driver, neo4j_database):
        """Test realistic traveler scenarios"""
        scenarios = [
            ("Business: London → Frankfurt, Tuesday 8am", "EGLL", "EDDF", "08"),
            ("Leisure: Edinburgh → Paris, Tuesday 10am", "EGPH", "LFPG", "10"),
            ("Weekend: Dublin → Amsterdam, Tuesday 9am", "EIDW", "EHAM", "09"),
        ]

        with neo4j_driver.session(database=neo4j_database) as session:
            for scenario_name, origin, destination, hour in scenarios:
                # Check for options (direct + connections)
                result = session.run(
                    """
                    // Direct flights
                    MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
                    MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
                    WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                      AND substring(toString(s.first_seen_time), 11, 2) >= $start_hour
                      AND substring(toString(s.first_seen_time), 11, 2) <= $end_hour
                    RETURN count(s) AS direct_options
                    
                    UNION ALL
                    
                    // Connection flights
                    MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
                    MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                    MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                    MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
                    WHERE toString(s1.date_of_operation) STARTS WITH '2024-06-18'
                      AND toString(s2.date_of_operation) STARTS WITH '2024-06-18'
                      AND substring(toString(s1.first_seen_time), 11, 2) >= $start_hour
                      AND substring(toString(s1.first_seen_time), 11, 2) <= $end_hour
                      AND hub.code <> $origin AND hub.code <> $destination
                    
                    WITH s1, s2, duration.between(
                        datetime(replace(toString(s1.last_seen_time), 'Z', '')),
                        datetime(replace(toString(s2.first_seen_time), 'Z', ''))
                    ).minutes AS connection_time
                    WHERE connection_time >= 45 AND connection_time <= 300
                    RETURN count(*) AS direct_options
                """,
                    origin=origin,
                    destination=destination,
                    start_hour=f"{max(0, int(hour)-2):02d}",
                    end_hour=f"{min(23, int(hour)+4):02d}",
                )

                options = sum(record["direct_options"] for record in result)
                assert (
                    options > 0
                ), f"{scenario_name}: Expected >0 options, got {options}"


class TestFlightSearchPerformance:
    """Performance tests for flight search"""

    @pytest.mark.slow
    def test_query_performance(self, neo4j_driver, neo4j_database):
        """Test that queries perform within acceptable time limits"""
        import time

        with neo4j_driver.session(database=neo4j_database) as session:
            # Test direct flight query performance
            start_time = time.time()
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'EGLL'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LFPG'})
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                RETURN count(s) AS flights
            """
            )
            flights = result.single()["flights"]
            direct_time = time.time() - start_time

            assert (
                direct_time < 2.0
            ), f"Direct query took {direct_time:.2f}s, should be <2s"
            assert flights > 0, "Should find direct flights"
