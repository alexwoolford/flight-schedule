"""
Neo4j Flight Load Test
======================
Production-grade load test for Neo4j flight routing queries.
Uses dynamic airport selection and README query patterns.

**Required .env file variables:**
- NEO4J_URI: Neo4j connection URI (e.g., bolt://localhost:7687)
- NEO4J_USERNAME: Neo4j username
- NEO4J_PASSWORD: Neo4j password
- NEO4J_DATABASE: Neo4j database name (default: neo4j)

**Usage:**
    locust -f neo4j_flight_load_test.py

**Test Distribution:**
- 70% Direct flight searches (simple count queries)
- 30% Comprehensive routing (direct + 1-stop with cross-day handling)
"""

import os
import random
import time

from dotenv import load_dotenv
from locust import User, between, task
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()


class Neo4jUser(User):
    """
    Neo4j load test user that properly reports to Locust metrics
    Uses a custom client approach to integrate with Locust's request tracking
    """

    wait_time = between(1, 3)

    def on_start(self):
        """Initialize Neo4j connection using .env file credentials"""
        # Load Neo4j credentials from .env file (never hard-code!)
        self.neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        self.neo4j_database = os.getenv("NEO4J_DATABASE", "neo4j")

        # Validate required environment variables
        if not all([self.neo4j_uri, self.neo4j_user, self.neo4j_password]):
            raise ValueError(
                "Missing required Neo4j environment variables. "
                "Please check your .env file contains: "
                "NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD"
            )

        print(
            f"🔗 Connecting to Neo4j: {self.neo4j_uri} (database: {self.neo4j_database})"
        )

        self.driver = GraphDatabase.driver(
            self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password)
        )

        # Get all actual airport codes from the database for realistic testing
        print("🔍 Loading airport codes from database...")
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(
                    "MATCH (a:Airport) RETURN DISTINCT a.code as code ORDER BY a.code"
                )
                self.airports = [record["code"] for record in result if record["code"]]
            print(f"✅ Loaded {len(self.airports)} airports from database")
            combinations = len(self.airports) * (len(self.airports) - 1)
            print(f"📊 Possible route combinations: {combinations:,}")
        except Exception as e:
            print(f"❌ Failed to load airports: {e}")
            # Fallback to major airports if database query fails
            self.airports = [
                "LAX",
                "JFK",
                "ORD",
                "DFW",
                "ATL",
                "SFO",
                "SEA",
                "BOS",
                "MIA",
                "DEN",
            ]
            print(f"⚠️  Using fallback airports: {len(self.airports)}")

        # Sample of airports for reference
        sample_airports = ", ".join(self.airports[:10])
        print(f"📋 Sample airports: {sample_airports}...")

        # Get actual date range from the database
        print("📅 Loading available flight dates from database...")
        with self.driver.session(database=self.neo4j_database) as session:
            result = session.run(
                """
                MATCH (s:Schedule)
                RETURN DISTINCT s.flightdate
                ORDER BY s.flightdate
            """
            )
            db_dates = [str(record["s.flightdate"]) for record in result]

        if db_dates:
            self.dates = db_dates
            print(f"✅ Loaded {len(self.dates)} actual flight dates from database")
            print(f"📆 Date range: {self.dates[0]} to {self.dates[-1]}")
        else:
            # If no dates found, this indicates a serious data problem
            raise Exception(
                "❌ No flight dates found in database! "
                "This suggests the database is empty or flight data wasn't loaded "
                "properly. Please check your Neo4j database contains Schedule "
                "nodes with flightdate properties."
            )

        print("✅ Neo4j user connected")

    def neo4j_request(self, name, query, params):
        """Execute Neo4j query and report to Locust properly"""
        start_time = time.time()
        try:
            with self.driver.session(database=self.neo4j_database) as session:
                result = session.run(query, **params)
                records = list(result)

            # Calculate timing
            total_time = time.time() - start_time

            # Report success to Locust (this is the key!)
            self.environment.events.request.fire(
                request_type="Neo4j",
                name=name,
                response_time=total_time * 1000,  # Locust expects milliseconds
                response_length=len(records),
                exception=None,
                context=self.context(),
            )

            return records

        except Exception as e:
            total_time = time.time() - start_time

            # Report failure to Locust
            self.environment.events.request.fire(
                request_type="Neo4j",
                name=name,
                response_time=total_time * 1000,
                response_length=0,
                exception=e,
                context=self.context(),
            )

            raise e

    def generate_random_route(self):
        """Generate a random origin-destination pair using realistic routes"""
        # Try to use generated airport pairs first (more realistic)
        try:
            import json

            with open("flight_test_scenarios.json", "r") as f:
                scenarios = json.load(f)
            airport_pairs = scenarios.get("airport_pairs", [])
            if airport_pairs:
                pair = random.choice(airport_pairs)  # nosec B311
                return pair["origin"], pair["dest"]
        except Exception:  # nosec B110
            pass

        # Fallback to random selection from actual airports
        origin = random.choice(self.airports)  # nosec B311
        dest = random.choice(self.airports)  # nosec B311
        # Ensure origin != destination
        while dest == origin:
            dest = random.choice(self.airports)  # nosec B311
        return origin, dest

    @task(70)  # 70% direct flights (most common search)
    def direct_flight_search(self):
        """Direct flight search - most realistic user behavior"""
        origin, dest = self.generate_random_route()
        search_date = random.choice(self.dates)  # nosec B311

        query = """
            MATCH (dep:Airport {code: $origin})<-[:DEPARTS_FROM]-(s:Schedule)
                  -[:ARRIVES_AT]->(arr:Airport {code: $dest})
            WHERE s.flightdate = date($search_date)
              AND s.scheduled_departure_time IS NOT NULL
            RETURN count(s) as flight_count
        """

        result = self.neo4j_request(
            f"Direct Flight Search ({origin}→{dest})",
            query,
            {"origin": origin, "dest": dest, "search_date": search_date},
        )

        # More realistic output
        count = result[0]["flight_count"] if result else 0
        if count > 0:
            print(f"✈️  Direct {origin}→{dest}: {count} flights available")
        else:
            print(f"❌ Direct {origin}→{dest}: No direct flights")

    @task(30)  # 30% comprehensive routing search - direct + connections
    def comprehensive_routing_search(self):
        """
        Advanced routing search with cross-day flight handling.
        Finds direct flights + 1-stop connections in a single query.
        Handles overnight flights properly (e.g., LAX-JFK red-eye).
        """
        origin, dest = self.generate_random_route()
        search_date = random.choice(self.dates)  # nosec B311

        # Advanced routing query with proper cross-day flight handling
        query = """
        // Find direct flights with cross-day handling
        MATCH (origin:Airport {code: $origin})<-[:DEPARTS_FROM]-(direct:Schedule)
              -[:ARRIVES_AT]->(dest:Airport {code: $dest})
        WHERE direct.flightdate = date($search_date)
          AND direct.scheduled_departure_time IS NOT NULL
          AND direct.scheduled_arrival_time IS NOT NULL

        // Calculate proper flight duration handling cross-day flights
        WITH direct, origin, dest,
             CASE
                 WHEN direct.scheduled_departure_time <= direct.scheduled_arrival_time THEN
                     // Same day flight
                     duration.between(direct.scheduled_departure_time, direct.scheduled_arrival_time).minutes
                 ELSE
                     // Cross-day flight (departure > arrival means arrival is next day)
                     duration.between(direct.scheduled_departure_time, time('23:59')).minutes + 1 +
                     duration.between(time('00:00'), direct.scheduled_arrival_time).minutes
             END AS flight_duration_minutes

        WHERE flight_duration_minutes > 0 AND flight_duration_minutes < 1440  // Reasonable flight time

        RETURN 'direct' AS route_type,
               [origin.code, dest.code] AS route_airports,
               [{
                   flight: direct.reporting_airline + toString(direct.flight_number_reporting_airline),
                   departure: direct.scheduled_departure_time,
                   arrival: direct.scheduled_arrival_time,
                   cross_day: direct.scheduled_departure_time > direct.scheduled_arrival_time
               }] AS flights,
               0 AS connections,
               flight_duration_minutes AS total_time_minutes

        UNION ALL

        // Find 1-stop connections with cross-day handling
        MATCH (origin:Airport {code: $origin})<-[:DEPARTS_FROM]-(s1:Schedule)-[:ARRIVES_AT]->(hub:Airport)
              <-[:DEPARTS_FROM]-(s2:Schedule)-[:ARRIVES_AT]->(dest:Airport {code: $dest})
        WHERE s1.flightdate = date($search_date)
          AND s2.flightdate IN [date($search_date), date($search_date) + duration('P1D')]
          AND s1.scheduled_arrival_time IS NOT NULL
          AND s2.scheduled_departure_time IS NOT NULL
          AND hub.code <> $origin AND hub.code <> $dest

        // Calculate proper flight durations and connection times
        WITH s1, s2, hub, origin, dest,
             // First flight duration
             CASE
                 WHEN s1.scheduled_departure_time <= s1.scheduled_arrival_time THEN
                     duration.between(s1.scheduled_departure_time, s1.scheduled_arrival_time).minutes
                 ELSE
                     duration.between(s1.scheduled_departure_time, time('23:59')).minutes + 1 +
                     duration.between(time('00:00'), s1.scheduled_arrival_time).minutes
             END AS s1_duration,
             // Second flight duration
             CASE
                 WHEN s2.scheduled_departure_time <= s2.scheduled_arrival_time THEN
                     duration.between(s2.scheduled_departure_time, s2.scheduled_arrival_time).minutes
                 ELSE
                     duration.between(s2.scheduled_departure_time, time('23:59')).minutes + 1 +
                     duration.between(time('00:00'), s2.scheduled_arrival_time).minutes
             END AS s2_duration,
             // Connection time calculation (handling cross-day scenarios)
             CASE
                 WHEN s1.flightdate = s2.flightdate THEN
                     // Same day connection
                     CASE
                         WHEN s1.scheduled_departure_time <= s1.scheduled_arrival_time THEN
                             duration.between(s1.scheduled_arrival_time, s2.scheduled_departure_time).minutes
                         ELSE
                             // S1 crosses to next day, s2 departure is after s1 arrival (next day)
                             duration.between(s1.scheduled_arrival_time, s2.scheduled_departure_time).minutes + 1440
                     END
                 ELSE
                     // Different day connection (s2 is next day)
                     CASE
                         WHEN s1.scheduled_departure_time <= s1.scheduled_arrival_time THEN
                             // S1 same day, S2 next day
                             duration.between(s1.scheduled_arrival_time, time('23:59')).minutes + 1 +
                             duration.between(time('00:00'), s2.scheduled_departure_time).minutes
                         ELSE
                             // S1 crosses day, S2 is next day
                             duration.between(s1.scheduled_arrival_time, s2.scheduled_departure_time).minutes
                     END
             END AS connection_minutes

        WHERE connection_minutes >= 45 AND connection_minutes <= 1200  // 45 min to 20 hours
          AND s1_duration > 0 AND s1_duration < 1440
          AND s2_duration > 0 AND s2_duration < 1440

        RETURN '1_stop' AS route_type,
               [origin.code, hub.code, dest.code] AS route_airports,
               [
                   {
                       flight: s1.reporting_airline + toString(s1.flight_number_reporting_airline),
                       departure: s1.scheduled_departure_time,
                       arrival: s1.scheduled_arrival_time,
                       cross_day: s1.scheduled_departure_time > s1.scheduled_arrival_time
                   },
                   {
                       flight: s2.reporting_airline + toString(s2.flight_number_reporting_airline),
                       departure: s2.scheduled_departure_time,
                       arrival: s2.scheduled_arrival_time,
                       cross_day: s2.scheduled_departure_time > s2.scheduled_arrival_time
                   }
               ] AS flights,
               1 AS connections,
               s1_duration + connection_minutes + s2_duration AS total_time_minutes

        ORDER BY connections, total_time_minutes
        LIMIT 10
        """

        result = self.neo4j_request(
            f"Comprehensive Routing ({origin}→{dest})",
            query,
            {"origin": origin, "dest": dest, "search_date": search_date},
        )

        # Show realistic airline-style results
        if result:
            direct_count = sum(1 for r in result if r["route_type"] == "direct")
            connection_count = sum(1 for r in result if r["route_type"] == "1_stop")

            route_display = " → ".join(result[0]["route_airports"])
            print(
                f"🛫 {route_display}: {len(result)} routes "
                f"({direct_count} direct, {connection_count} connections)"
            )

            # Show best option details
            best = result[0]
            total_hours = best["total_time_minutes"] / 60
            overnight_indicator = ""
            if any(f.get("cross_day", False) for f in best["flights"]):
                overnight_indicator = " 🌙"

            if best["route_type"] == "direct":
                flight = best["flights"][0]
                print(
                    f"  ✈️  Best: {flight['flight']} "
                    f"({total_hours:.1f}h){overnight_indicator}"
                )
            else:
                f1, f2 = best["flights"]
                hub = best["route_airports"][1]
                print(
                    f"  🔗 Best: {f1['flight']} → {f2['flight']} "
                    f"via {hub} ({total_hours:.1f}h){overnight_indicator}"
                )
        else:
            print(f"❌ Route {origin}→{dest}: No flights found")

    def on_stop(self):
        """Clean up connection"""
        if hasattr(self, "driver"):
            self.driver.close()


if __name__ == "__main__":
    print("🚀 NEO4J FLIGHT LOAD TEST")
    print("=========================")
    print("📊 Advanced flight search load testing:")
    print("   • 70% Direct flights (simple count queries)")
    print("   • 30% Comprehensive routing (direct + 1-stop with cross-day handling)")
    print("   • Dynamic date range from actual database data")
    print("")
    print("🎯 Dynamic airport + date selection from actual database")
    print("✅ Proper Locust integration for accurate RPS and response times")
    print("")
    print("▶️  Start: locust -f neo4j_flight_load_test.py")
