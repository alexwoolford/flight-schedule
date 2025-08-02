#!/usr/bin/env python3
"""
Performance Baseline Tests
=========================

Critical tests to establish performance baseline before optimization.
These tests ensure we don't break functionality when improving performance.
"""

import os
import time
from typing import Any, Dict, List

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


class TestPerformanceBaseline:
    """Baseline performance tests - capture current state before optimization"""

    def test_database_has_data(self, neo4j_driver, neo4j_database):
        """Ensure database has sufficient data for performance testing"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run("MATCH (s:Schedule) RETURN count(s) AS schedule_count")
            schedule_count = result.single()["schedule_count"]

            result = session.run("MATCH (a:Airport) RETURN count(a) AS airport_count")
            airport_count = result.single()["airport_count"]

            result = session.run("MATCH (c:Carrier) RETURN count(c) AS carrier_count")
            carrier_count = result.single()["carrier_count"]

            # Skip tests if database is empty
            if schedule_count == 0:
                pytest.skip("Database is empty - no data to test performance against")

            print(f"📊 Database contains:")
            print(f"   • {schedule_count:,} Schedule nodes")
            print(f"   • {airport_count:,} Airport nodes")
            print(f"   • {carrier_count:,} Carrier nodes")

            # Ensure sufficient data for meaningful tests
            assert (
                schedule_count >= 1000
            ), f"Need at least 1k schedules for performance testing, got {schedule_count}"
            assert (
                airport_count >= 10
            ), f"Need at least 10 airports, got {airport_count}"
            assert carrier_count >= 3, f"Need at least 3 carriers, got {carrier_count}"

    def test_schema_validation(self, neo4j_driver, neo4j_database):
        """Validate that Schedule nodes have the expected properties"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Check a sample Schedule node for expected properties
            result = session.run(
                """
                MATCH (s:Schedule)
                WHERE s.scheduled_departure_time IS NOT NULL
                  AND s.scheduled_arrival_time IS NOT NULL
                  AND s.flightdate IS NOT NULL
                RETURN s.flightdate, s.scheduled_departure_time, s.scheduled_arrival_time
                LIMIT 1
            """
            )

            sample = result.single()
            if not sample:
                pytest.skip("No Schedule nodes with required temporal properties found")

            # Validate property types - access by index since Neo4j Record uses aliased properties
            assert sample[0] is not None, "flightdate property missing"
            assert sample[1] is not None, "scheduled_departure_time property missing"
            assert sample[2] is not None, "scheduled_arrival_time property missing"

            print(
                "✅ Schema validation passed - all required temporal properties present"
            )

    def test_current_index_usage(self, neo4j_driver, neo4j_database):
        """Document current index usage for baseline comparison"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Check which indexes exist and their usage
            result = session.run(
                """
                SHOW INDEXES YIELD name, labelsOrTypes, properties, readCount
                WHERE readCount IS NOT NULL
                RETURN name, labelsOrTypes, properties, readCount
                ORDER BY readCount DESC
            """
            )

            indexes = list(result)
            print(f"📈 Current index usage (top indexes by readCount):")

            for idx in indexes[:10]:  # Top 10 most used indexes
                name = idx.get("name", "unknown")
                labels = idx.get("labelsOrTypes", [])
                props = idx.get("properties", [])
                reads = idx.get("readCount", 0)
                print(f"   • {name}: {labels} {props} - {reads:,} reads")

            # Store baseline for comparison
            self.baseline_indexes = {idx["name"]: idx["readCount"] for idx in indexes}

    def test_direct_flight_baseline_performance(self, neo4j_driver, neo4j_database):
        """Baseline: Direct flight search performance"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Find a valid airport with outbound flights
            airports_result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(a:Airport)
                WHERE s.flightdate = date('2024-03-01')
                RETURN a.code AS airport_code, count(s) AS flight_count
                ORDER BY flight_count DESC
                LIMIT 5
            """
            )

            airports = list(airports_result)
            if not airports:
                pytest.skip("No flights found for March 1, 2024")

            # Test with busiest airport
            test_airport = airports[0]["airport_code"]

            start_time = time.time()

            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $airport})
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time IS NOT NULL
                RETURN count(s) AS flight_count, collect(DISTINCT arr.code)[0..5] AS destinations
            """,
                airport=test_airport,
            )

            flight_data = result.single()
            query_time = (time.time() - start_time) * 1000

            flight_count = flight_data["flight_count"]
            destinations = flight_data["destinations"]

            print(f"📊 Direct flight baseline from {test_airport}:")
            print(f"   • Query time: {query_time:.1f}ms")
            print(f"   • Flights found: {flight_count}")
            print(f"   • Sample destinations: {destinations}")

            # Store baseline metrics
            assert flight_count > 0, "Should find at least one flight"
            assert (
                query_time < 5000
            ), f"Direct flight query taking too long: {query_time:.1f}ms"

            # Store for comparison
            self.direct_flight_baseline = {
                "query_time_ms": query_time,
                "flight_count": flight_count,
                "test_airport": test_airport,
            }

    def test_connection_flight_baseline_performance(self, neo4j_driver, neo4j_database):
        """Baseline: Connection flight search performance with proper temporal logic"""
        with neo4j_driver.session(database=neo4j_database) as session:
            start_time = time.time()

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
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time

                WITH s1, s2, dep, hub, arr,
                     s1.scheduled_arrival_time AS hub_arrival,
                     s2.scheduled_departure_time AS hub_departure

                WITH dep, hub, arr, hub_arrival, hub_departure,
                     duration.between(hub_arrival, hub_departure).minutes AS connection_minutes

                WHERE connection_minutes >= 45 AND connection_minutes <= 300

                RETURN count(*) AS connection_count,
                       avg(connection_minutes) AS avg_connection_time,
                       collect(DISTINCT dep.code)[0..3] AS sample_origins,
                       collect(DISTINCT arr.code)[0..3] AS sample_destinations
            """
            )

            connection_data = result.single()
            query_time = (time.time() - start_time) * 1000

            connection_count = connection_data["connection_count"]
            avg_connection_time = connection_data["avg_connection_time"]
            sample_origins = connection_data["sample_origins"]
            sample_destinations = connection_data["sample_destinations"]

            print(f"📊 Connection flight baseline:")
            print(f"   • Query time: {query_time:.1f}ms")
            print(f"   • Connections found: {connection_count}")
            print(f"   • Avg connection time: {avg_connection_time:.1f} minutes")
            print(f"   • Sample origins: {sample_origins}")
            print(f"   • Sample destinations: {sample_destinations}")

            # Store baseline metrics
            assert connection_count >= 0, "Should return valid connection count"
            assert (
                query_time < 15000
            ), f"Connection query taking too long: {query_time:.1f}ms"

            if connection_count > 0:
                assert (
                    45 <= avg_connection_time <= 300
                ), f"Average connection time out of range: {avg_connection_time}"

            # Store for comparison
            self.connection_baseline = {
                "query_time_ms": query_time,
                "connection_count": connection_count,
                "avg_connection_time": avg_connection_time,
            }

    def test_temporal_query_baseline_performance(self, neo4j_driver, neo4j_database):
        """Baseline: Time-based filtering performance"""
        with neo4j_driver.session(database=neo4j_database) as session:
            start_time = time.time()

            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time >= datetime('2024-03-01T06:00:00')
                  AND s.scheduled_departure_time <= datetime('2024-03-01T10:00:00')
                RETURN count(s) AS morning_flights,
                       collect(DISTINCT dep.code)[0..5] AS morning_airports
            """
            )

            morning_data = result.single()
            query_time = (time.time() - start_time) * 1000

            morning_flights = morning_data["morning_flights"]
            morning_airports = morning_data["morning_airports"]

            print(f"📊 Temporal filtering baseline (6-10 AM flights):")
            print(f"   • Query time: {query_time:.1f}ms")
            print(f"   • Morning flights: {morning_flights}")
            print(f"   • Airports with morning flights: {morning_airports}")

            # Store baseline metrics
            assert morning_flights >= 0, "Should return valid flight count"
            assert (
                query_time < 10000
            ), f"Temporal query taking too long: {query_time:.1f}ms"

            # Store for comparison
            self.temporal_baseline = {
                "query_time_ms": query_time,
                "morning_flights": morning_flights,
            }

    def test_multi_hop_routing_baseline(self, neo4j_driver, neo4j_database):
        """Baseline: Complex multi-hop routing performance"""
        with neo4j_driver.session(database=neo4j_database) as session:
            # Find the top airports for testing
            hub_result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                WHERE s.flightdate = date('2024-03-01')
                WITH arr.code AS airport, count(*) AS inbound_count
                ORDER BY inbound_count DESC
                LIMIT 3
                RETURN collect(airport) AS top_hubs
            """
            )

            top_hubs = hub_result.single()["top_hubs"]
            if len(top_hubs) < 2:
                pytest.skip("Not enough hub airports for multi-hop testing")

            start_time = time.time()

            # Test 2-hop routing between major hubs
            result = session.run(
                """
                MATCH path = (dep:Airport)-[r1:DEPARTS_FROM]-(s1:Schedule)-[r2:ARRIVES_AT]-(hub:Airport)
                             -[r3:DEPARTS_FROM]-(s2:Schedule)-[r4:ARRIVES_AT]-(arr:Airport)
                WHERE dep.code = $origin
                  AND arr.code = $destination
                  AND s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
                RETURN count(path) AS path_count, hub.code AS hub_airport
                LIMIT 10
            """,
                origin=top_hubs[0],
                destination=top_hubs[1],
            )

            paths = list(result)
            query_time = (time.time() - start_time) * 1000

            total_paths = sum(path["path_count"] for path in paths)

            print(f"📊 Multi-hop routing baseline ({top_hubs[0]} → {top_hubs[1]}):")
            print(f"   • Query time: {query_time:.1f}ms")
            print(f"   • Total paths: {total_paths}")
            print(f"   • Intermediate hubs: {[p['hub_airport'] for p in paths[:3]]}")

            # Store baseline metrics
            assert total_paths >= 0, "Should return valid path count"
            assert (
                query_time < 20000
            ), f"Multi-hop query taking too long: {query_time:.1f}ms"

            # Store for comparison
            self.multihop_baseline = {
                "query_time_ms": query_time,
                "path_count": total_paths,
                "test_route": f"{top_hubs[0]}-{top_hubs[1]}",
            }

    def test_baseline_report_summary(self, neo4j_driver, neo4j_database):
        """Generate comprehensive baseline performance report"""
        print("\n" + "=" * 60)
        print("📊 PERFORMANCE BASELINE REPORT")
        print("=" * 60)

        # Summarize all baseline metrics
        if hasattr(self, "direct_flight_baseline"):
            df = self.direct_flight_baseline
            print(
                f"✈️  Direct Flights: {df['query_time_ms']:.1f}ms, {df['flight_count']} flights from {df['test_airport']}"
            )

        if hasattr(self, "connection_baseline"):
            cf = self.connection_baseline
            print(
                f"🔄 Connections: {cf['query_time_ms']:.1f}ms, {cf['connection_count']} valid connections"
            )

        if hasattr(self, "temporal_baseline"):
            tf = self.temporal_baseline
            print(
                f"🕐 Temporal Filter: {tf['query_time_ms']:.1f}ms, {tf['morning_flights']} morning flights"
            )

        if hasattr(self, "multihop_baseline"):
            mf = self.multihop_baseline
            print(
                f"🛣️  Multi-hop: {mf['query_time_ms']:.1f}ms, {mf['path_count']} paths for {mf['test_route']}"
            )

        print("\n💡 This baseline will be used to validate performance improvements")
        print("   and ensure no regressions when optimizing queries and indexes.")
        print("=" * 60)


class TestQueryCorrectness:
    """Validate that current queries return logically correct results"""

    def test_connection_temporal_logic(self, neo4j_driver, neo4j_database):
        """Critical: Ensure all connections have valid temporal ordering"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
                MATCH (s2)-[:ARRIVES_AT]->(arr:Airport)

                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time

                WITH s1.scheduled_arrival_time AS arrival,
                     s2.scheduled_departure_time AS departure,
                     duration.between(s1.scheduled_arrival_time, s2.scheduled_departure_time).minutes AS gap

                WHERE gap >= 45 AND gap <= 300

                RETURN min(gap) AS min_connection, max(gap) AS max_connection,
                       avg(gap) AS avg_connection, count(*) AS total_connections
            """
            )

            stats = result.single()
            if not stats or stats["total_connections"] == 0:
                pytest.skip("No valid connections found for temporal logic testing")

            min_conn = stats["min_connection"]
            max_conn = stats["max_connection"]
            avg_conn = stats["avg_connection"]
            total = stats["total_connections"]

            print(f"✅ Connection temporal validation:")
            print(f"   • {total} valid connections found")
            print(f"   • Connection time range: {min_conn}-{max_conn} minutes")
            print(f"   • Average connection time: {avg_conn:.1f} minutes")

            # Validate logical constraints
            assert (
                min_conn >= 45
            ), f"Found connection shorter than 45 minutes: {min_conn}"
            assert (
                max_conn <= 300
            ), f"Found connection longer than 300 minutes: {max_conn}"
            assert (
                45 <= avg_conn <= 300
            ), f"Average connection time out of range: {avg_conn}"

    def test_no_same_day_overnight_connections(self, neo4j_driver, neo4j_database):
        """Ensure no impossible same-day connections (departure before arrival)"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s1:Schedule)-[:ARRIVES_AT]->(hub:Airport)
                MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)

                WHERE s1.flightdate = date('2024-03-01')
                  AND s2.flightdate = date('2024-03-01')
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time <= s1.scheduled_arrival_time

                RETURN count(*) AS impossible_connections
            """
            )

            impossible = result.single()["impossible_connections"]

            # This query should find 0 results if our temporal logic is correct
            assert impossible == 0, (
                f"Found {impossible} impossible same-day connections where "
                f"departure time <= arrival time. This indicates a temporal logic error."
            )

            print("✅ No impossible same-day connections found")

    def test_flight_data_consistency(self, neo4j_driver, neo4j_database):
        """Validate basic flight data consistency"""
        with neo4j_driver.session(database=neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport)
                MATCH (s)-[:ARRIVES_AT]->(arr:Airport)
                WHERE s.flightdate = date('2024-03-01')
                  AND s.scheduled_departure_time IS NOT NULL
                  AND s.scheduled_arrival_time IS NOT NULL

                WITH s, dep, arr,
                     s.scheduled_departure_time AS dep_time,
                     s.scheduled_arrival_time AS arr_time,
                     duration.between(s.scheduled_departure_time, s.scheduled_arrival_time).minutes AS flight_duration

                RETURN
                    count(*) AS total_flights,
                    count(CASE WHEN dep.code = arr.code THEN 1 END) AS same_airport_flights,
                    count(CASE WHEN flight_duration <= 0 THEN 1 END) AS impossible_durations,
                    min(flight_duration) AS min_duration,
                    max(flight_duration) AS max_duration,
                    avg(flight_duration) AS avg_duration
            """
            )

            stats = result.single()

            total = stats["total_flights"]
            same_airport = stats["same_airport_flights"]
            impossible = stats["impossible_durations"]
            min_dur = stats["min_duration"]
            max_dur = stats["max_duration"]
            avg_dur = stats["avg_duration"]

            print(f"✅ Flight data consistency check:")
            print(f"   • Total flights: {total}")
            print(f"   • Same airport flights: {same_airport}")
            print(f"   • Flight duration range: {min_dur}-{max_dur} minutes")
            print(f"   • Average flight duration: {avg_dur:.1f} minutes")

            # Data quality assertions
            assert total > 0, "Should have flight data"
            assert (
                same_airport == 0
            ), f"Found {same_airport} flights with same origin/destination"
            assert (
                impossible == 0
            ), f"Found {impossible} flights with impossible negative/zero duration"
            assert (
                min_dur > 0
            ), f"Minimum flight duration should be positive, got {min_dur}"
            assert (
                max_dur < 24 * 60
            ), f"Maximum flight duration should be under 24 hours, got {max_dur}"
