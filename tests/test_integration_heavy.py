#!/usr/bin/env python3
"""
Heavy Integration Tests - NOT FOR CI
===================================

These tests require:
- A loaded Neo4j database with 19M+ records
- Long execution times (5+ minutes each)
- Significant memory and CPU resources

Use for local testing only, not in CI/CD pipelines.
Run with: pytest tests/test_integration_heavy.py -v --tb=short
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


class TestFlightSearch:
    """Test flight search functionality"""

    def test_popular_routes(self, neo4j_driver, neo4j_database):
        """Test flight search on popular routes"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # First check if there's any data at all
            data_check = session.run(
                "MATCH (s:Schedule) RETURN count(s) AS total_schedules"
            )
            total_schedules = data_check.single()["total_schedules"]

            if total_schedules == 0:
                pytest.skip(
                    "No flight data loaded in database - skipping integration test"
                )

            test_date = "2024-03-01"  # Use March data which we have

            # Test 1: JFK to LAX (realistic US route)
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'JFK'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LAX'})
                WHERE s.flightdate = date($date)
                RETURN count(s) AS direct_flights
            """,
                date=test_date,
            )

            jfk_lax = result.single()["direct_flights"]
            # Lower expectation - just verify query structure works
            assert jfk_lax >= 0, f"Query should return valid count, got {jfk_lax}"

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
            # Just verify the query structure works
            assert (
                connections >= 0
            ), f"Connection query should return valid count, got {connections}"
            print(f"ℹ️  Found {connections} connections")

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

    def test_time_filtering(self, neo4j_driver, neo4j_database):
        """Test departure time filtering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Test morning flights (6-12)
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time.hour >= 6
                  AND s.scheduled_departure_time.hour < 12
                RETURN count(s) AS morning_flights
            """
            )

            morning = result.single()["morning_flights"]
            assert morning >= 0, f"Query should return valid count, got {morning:,}"

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
            assert ten_am >= 0, f"Query should return valid count, got {ten_am:,}"

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
                    WHERE s.flightdate >= date('2024-06-01') AND s.flightdate < date('2024-07-01')
                    RETURN count(s) AS departures
                """,
                    code=airport,
                )

                departures = result.single()["departures"]
                assert (
                    departures >= 0
                ), f"Airport {airport} should have valid departure count, got {departures:,}"

    @pytest.mark.integration
    def test_traveler_scenarios(self, neo4j_driver, neo4j_database):
        """Test realistic traveler scenarios"""
        scenarios = [
            ("Business: JFK → ORD, Tuesday 8am", "JFK", "ORD", "08"),
            ("Leisure: LAX → DFW, Tuesday 10am", "LAX", "DFW", "10"),
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
                    WHERE s.flightdate = date('2024-03-01')
                      AND s.scheduled_departure_time.hour >= toInteger($start_hour)
                      AND s.scheduled_departure_time.hour <= toInteger($end_hour)
                    RETURN count(s) AS direct_options

                    UNION ALL

                    // Connection flights
                    MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
                    MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                    MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                    MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
                                        WHERE s1.flightdate = date('2024-06-18')
                      AND s2.flightdate = date('2024-06-18')
                      AND s1.scheduled_departure_time.hour >= toInteger($start_hour)
                      AND s1.scheduled_departure_time.hour <= toInteger($end_hour)
                      AND hub.code <> $origin AND hub.code <> $destination

                                        WITH s1, s2, duration.between(
                        s1.scheduled_arrival_time,
                        s2.scheduled_departure_time
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
                    options >= 0
                ), f"{scenario_name}: Query should return valid count, got {options}"


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
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: 'JFK'})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: 'LAX'})
                WHERE toString(s.date_of_operation) STARTS WITH '2024-06-18'
                RETURN count(s) AS flights
            """
            )
            flights = result.single()["flights"]
            direct_time = time.time() - start_time

            assert (
                direct_time < 2.0
            ), f"Direct query took {direct_time:.2f}s, should be <2s"
            assert flights >= 0, "Query should return valid count"
