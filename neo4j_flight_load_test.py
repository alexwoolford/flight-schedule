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
- 60% Direct flight searches (random airport pairs)
- 35% Connection searches (README pattern)
- 5% Hub analysis
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

    @task(30)  # 30% connection search - when direct flights aren't available
    def connection_search(self):
        """Multi-hop connection search using README pattern"""
        origin, dest = self.generate_random_route()
        search_date = random.choice(self.dates)  # nosec B311

        # EXACT README query pattern for realistic connections
        query = """
            MATCH (dep:Airport {code: $origin})<-[:DEPARTS_FROM]-(s1:Schedule)
                  -[:ARRIVES_AT]->(hub:Airport)<-[:DEPARTS_FROM]-(s2:Schedule)
                  -[:ARRIVES_AT]->(arr:Airport {code: $dest})

            WHERE s1.flightdate = date($search_date)
              AND s2.flightdate = date($search_date)
              AND s1.scheduled_arrival_time IS NOT NULL
              AND s2.scheduled_departure_time IS NOT NULL
              AND s2.scheduled_departure_time > s1.scheduled_arrival_time
              AND hub.code <> $origin AND hub.code <> $dest

            WITH s1, s2, hub,
                 s1.scheduled_arrival_time AS hub_arrival,
                 s2.scheduled_departure_time AS hub_departure,
                 duration.between(
                     s1.scheduled_arrival_time,
                     s2.scheduled_departure_time
                 ).minutes AS connection_minutes

            WHERE connection_minutes >= 45 AND connection_minutes <= 300

            RETURN hub.code, connection_minutes,
                   hub_arrival, hub_departure,
                   s1.reporting_airline +
                   toString(s1.flight_number_reporting_airline) AS inbound_flight,
                   s2.reporting_airline +
                   toString(s2.flight_number_reporting_airline) AS outbound_flight
            ORDER BY connection_minutes
            LIMIT 8
        """

        result = self.neo4j_request(
            f"Connection Search ({origin}→{dest})",
            query,
            {"origin": origin, "dest": dest, "search_date": search_date},
        )

        # Show realistic connection results like a travel website
        if result:
            print(f"🔗 Connect {origin}→{dest}: {len(result)} routes found")
            for i, conn in enumerate(result[:2], 1):  # Show best 2 options
                hub_code = conn.get("hub.code", "Unknown")
                print(
                    f"  {i}. {conn['inbound_flight']} → "
                    f"{conn['outbound_flight']} via {hub_code} "
                    f"({conn['connection_minutes']}min layover)"
                )
        else:
            print(f"❌ Connect {origin}→{dest}: No connections found")

    def on_stop(self):
        """Clean up connection"""
        if hasattr(self, "driver"):
            self.driver.close()


if __name__ == "__main__":
    print("🚀 NEO4J FLIGHT LOAD TEST")
    print("=========================")
    print("📊 Realistic flight search load testing:")
    print("   • 70% Direct flights (most common user behavior)")
    print("   • 30% Multi-hop connections (README pattern)")
    print("   • Dynamic date range from actual database data")
    print("")
    print("🎯 Dynamic airport + date selection from actual database")
    print("✅ Proper Locust integration for accurate RPS and response times")
    print("")
    print("▶️  Start: locust -f neo4j_flight_load_test.py")
