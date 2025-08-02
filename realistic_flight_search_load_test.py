#!/usr/bin/env python3
"""
Realistic Flight Search Load Test
=================================

Production-grade load test simulating actual flight search behavior.
Each user searches for flights from A to B, getting both direct and connecting options.

Connection Pooling Strategy:
- Each Locust user maintains one Neo4j driver (with built-in connection pooling)
- Sessions are created per query (lightweight, reuses connections)
- This tests database performance, not connection overhead
"""

import json
import os
import random
import time
from datetime import date

from dotenv import load_dotenv
from locust import User, between, events, task
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()

# Load pre-generated realistic scenarios
try:
    with open("flight_test_scenarios.json", "r") as f:
        SCENARIOS = json.load(f)
        print(f"✅ Loaded {len(SCENARIOS['airport_pairs'])} flight scenarios")
except FileNotFoundError:
    print("❌ Run generate_flight_scenarios.py first!")
    exit(1)


class FlightSearchUser(User):
    """
    Simulates a user searching for flights.

    Each user represents a person looking for flights from origin to destination.
    Weight distribution mimics real user behavior:
    - 70% search popular routes (hub-to-hub, major routes)
    - 20% search medium routes (hub-to-spoke)
    - 10% search niche routes (spoke-to-spoke)
    """

    # Realistic user behavior - 2-8 seconds between searches
    wait_time = between(2, 8)

    def on_start(self):
        """Initialize Neo4j connection with pooling when user starts"""
        # Load connection details from environment variables
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE", "neo4j")

        # Validate required environment variables
        if not all([uri, username, password]):
            raise ValueError(
                "Missing required environment variables. Please ensure NEO4J_URI, "
                "NEO4J_USERNAME, and NEO4J_PASSWORD are set in your .env file."
            )

        # Create driver with connection pooling (default behavior)
        # Each user gets their own driver instance
        self.driver = GraphDatabase.driver(
            uri,
            auth=(username, password),
            # Connection pool settings (these are defaults, but explicit for clarity)
            max_connection_lifetime=3600,  # 1 hour
            max_connection_pool_size=100,  # Max connections per driver
            connection_acquisition_timeout=60,  # Seconds to wait for connection
        )
        self.database = database

        # Pre-parse scenarios for efficient access
        self.popular_routes = SCENARIOS.get("popular_routes", [])
        self.all_routes = SCENARIOS.get("airport_pairs", [])
        self.available_dates = [
            date.fromisoformat(d) for d in SCENARIOS.get("available_dates", [])
        ]

        print(
            f"🔗 User connected with {len(self.popular_routes)} popular routes available"
        )

    def on_stop(self):
        """Clean up Neo4j connection when user stops"""
        if hasattr(self, "driver"):
            self.driver.close()

    def _execute_flight_search(
        self, query_name: str, origin: str, dest: str, search_date: date
    ):
        """
        Execute sophisticated flight booking query (direct + multi-hop connections).

        This simulates what real travelers experience when booking flights:

        **Route Types**:
        - **Direct flights**: Always preferred when available (efficiency score: 100)
        - **One-stop connections**: Most common for longer routes (45-360 min layovers)
        - **Two-stop connections**: For complex routing to smaller markets
          (max 16 hrs total)

        **Realistic booking logic**:
        - Enforces minimum connection times (45+ minutes for transfers)
        - Penalizes long layovers in efficiency scoring
        - Prioritizes by: fewer hops → efficiency → departure time
        - Returns mix of options like Expedia/Kayak (15 routes max)

        **Connection timing validation**:
        - Ensures sufficient time to deplane, transfer gates, and board next flight
        - Validates temporal constraints
          (later departure > earlier arrival)
        - Limits unrealistic itineraries (16hr max, 300min max layovers)
        """

        # Realistic flight booking query - multi-hop routing with intelligent
        # prioritization
        query = """
        // Priority 1: Direct flights (always preferred when available)
        OPTIONAL MATCH (origin:Airport {code: $origin})
                       <-[:DEPARTS_FROM]-(direct:Schedule)
                       -[:ARRIVES_AT]->(dest:Airport {code: $dest})
        WHERE direct.flightdate = $search_date
          AND direct.scheduled_departure_time IS NOT NULL
          AND direct.scheduled_arrival_time IS NOT NULL

        WITH collect({
            route_type: "direct",
            hops: 0,
            departure_time: direct.scheduled_departure_time,
            arrival_time: direct.scheduled_arrival_time,
            total_duration: duration.between(
                direct.scheduled_departure_time, direct.scheduled_arrival_time
            ).minutes,
            flights: [
                direct.reporting_airline +
                toString(direct.flight_number_reporting_airline)
            ],
            route: [$origin, $dest],
            efficiency_score: 100  // Direct flights get highest efficiency
        }) AS direct_routes

        // Priority 2: One-stop connections (most common for longer routes)
        OPTIONAL MATCH (origin:Airport {code: $origin})
                       <-[:DEPARTS_FROM]-(leg1:Schedule)
                       -[:ARRIVES_AT]->(hub1:Airport)
                       <-[:DEPARTS_FROM]-(leg2:Schedule)
                       -[:ARRIVES_AT]->(dest:Airport {code: $dest})

        WHERE leg1.flightdate = $search_date
          AND leg2.flightdate = $search_date
          AND leg1.scheduled_arrival_time IS NOT NULL
          AND leg2.scheduled_departure_time IS NOT NULL
          AND leg2.scheduled_departure_time > leg1.scheduled_arrival_time
          // Connection possible
          AND hub1.code <> $origin AND hub1.code <> $dest  // Valid intermediate hub

        WITH direct_routes, leg1, leg2, hub1,
             duration.between(
                 leg1.scheduled_arrival_time, leg2.scheduled_departure_time
             ).minutes AS connection1,
             duration.between(
                 leg1.scheduled_departure_time, leg2.scheduled_arrival_time
             ).minutes AS total1

        WHERE connection1 >= 45 AND connection1 <= 360  // Realistic connection window

        WITH direct_routes, collect({
            route_type: "one_stop",
            hops: 1,
            departure_time: leg1.scheduled_departure_time,
            arrival_time: leg2.scheduled_arrival_time,
            total_duration: total1,
            connection_time: connection1,
            flights: [
                leg1.reporting_airline + toString(leg1.flight_number_reporting_airline),
                leg2.reporting_airline + toString(leg2.flight_number_reporting_airline)
            ],
            route: [$origin, hub1.code, $dest],
            efficiency_score: 90 - (connection1 / 10)  // Penalize long layovers
        }) AS one_stop_routes

        // Priority 3: Two-stop connections (for complex routing or smaller markets)
        OPTIONAL MATCH (origin:Airport {code: $origin})
                       <-[:DEPARTS_FROM]-(leg1:Schedule)
                       -[:ARRIVES_AT]->(hub1:Airport)
                       <-[:DEPARTS_FROM]-(leg2:Schedule)
                       -[:ARRIVES_AT]->(hub2:Airport)
                       <-[:DEPARTS_FROM]-(leg3:Schedule)
                       -[:ARRIVES_AT]->(dest:Airport {code: $dest})

        WHERE leg1.flightdate = $search_date
          AND leg2.flightdate = $search_date
          AND leg3.flightdate = $search_date
          AND leg1.scheduled_arrival_time IS NOT NULL
          AND leg2.scheduled_departure_time IS NOT NULL
          AND leg2.scheduled_arrival_time IS NOT NULL
          AND leg3.scheduled_departure_time IS NOT NULL
          AND leg2.scheduled_departure_time > leg1.scheduled_arrival_time
          // First connection valid
          AND leg3.scheduled_departure_time > leg2.scheduled_arrival_time
          // Second connection valid
          AND hub1.code <> $origin AND hub1.code <> $dest AND hub1.code <> hub2.code
          // Valid hubs
          AND hub2.code <> $origin AND hub2.code <> $dest

        WITH direct_routes, one_stop_routes, leg1, leg2, leg3, hub1, hub2,
             duration.between(
                 leg1.scheduled_arrival_time, leg2.scheduled_departure_time
             ).minutes AS connection1,
             duration.between(
                 leg2.scheduled_arrival_time, leg3.scheduled_departure_time
             ).minutes AS connection2,
             duration.between(
                 leg1.scheduled_departure_time, leg3.scheduled_arrival_time
             ).minutes AS total2

        WHERE connection1 >= 45 AND connection1 <= 300  // First connection timing
          AND connection2 >= 45 AND connection2 <= 300  // Second connection timing
          AND total2 <= 960  // Max reasonable total travel time (16 hours)

        WITH direct_routes, one_stop_routes, collect({
            route_type: "two_stop",
            hops: 2,
            departure_time: leg1.scheduled_departure_time,
            arrival_time: leg3.scheduled_arrival_time,
            total_duration: total2,
            connection_times: [connection1, connection2],
            flights: [
                leg1.reporting_airline + toString(leg1.flight_number_reporting_airline),
                leg2.reporting_airline + toString(leg2.flight_number_reporting_airline),
                leg3.reporting_airline + toString(leg3.flight_number_reporting_airline)
            ],
            route: [$origin, hub1.code, hub2.code, $dest],
            efficiency_score: 70 - ((connection1 + connection2) / 20)
            // Heavier penalty for multi-stop
        }) AS two_stop_routes

        // Combine all route options and prioritize like a real booking system
        WITH direct_routes + one_stop_routes + two_stop_routes AS all_routes
        UNWIND all_routes AS route

        RETURN route
        ORDER BY
            route.hops ASC,                    // Prefer fewer connections
            route.efficiency_score DESC,       // Then by efficiency
                                            // (considering layover penalties)
            route.departure_time ASC           // Then by departure time
        LIMIT 15  // Realistic number of options (like Expedia/Kayak)
        """

        start_time = time.time()
        try:
            # Create session for this query (lightweight, reuses pooled connections)
            with self.driver.session(database=self.database) as session:
                result = list(
                    session.run(
                        query,
                        {"origin": origin, "dest": dest, "search_date": search_date},
                    )
                )

            response_time = (time.time() - start_time) * 1000  # Convert to ms

            # Count results by type for insights
            direct_count = sum(1 for r in result if r["flight"]["type"] == "direct")
            connection_count = sum(
                1 for r in result if r["flight"]["type"] == "connection"
            )

            # Report to Locust with detailed metrics
            events.request.fire(
                request_type="Cypher",
                name=f"{query_name} ({direct_count}D+{connection_count}C)",
                response_time=response_time,
                response_length=len(result),
                exception=None,
            )

            return len(result)

        except Exception as e:
            total_time = (time.time() - start_time) * 1000
            events.request.fire(
                request_type="Cypher",
                name=query_name,
                response_time=total_time,
                response_length=0,
                exception=e,
            )
            print(f"❌ Error in {query_name}: {e}")
            return 0

    @task(7)  # 70% of searches - popular routes
    def search_popular_route(self):
        """Search popular routes (likely to have direct flights)"""
        if not self.popular_routes:
            return

        route = random.choice(self.popular_routes)
        search_date = random.choice(self.available_dates)

        self._execute_flight_search(
            "Popular Route Search", route["origin"], route["dest"], search_date
        )

    @task(2)  # 20% of searches - medium routes
    def search_medium_route(self):
        """Search medium routes (mix of direct and connections)"""
        # Filter for hub-to-spoke routes
        medium_routes = [
            r for r in self.all_routes if r.get("pattern") == "hub_to_spoke"
        ]
        if not medium_routes:
            return

        route = random.choice(medium_routes)
        search_date = random.choice(self.available_dates)

        self._execute_flight_search(
            "Medium Route Search", route["origin"], route["dest"], search_date
        )

    @task(1)  # 10% of searches - niche routes
    def search_niche_route(self):
        """Search niche routes (mostly connections)"""
        # Filter for spoke-to-spoke routes
        niche_routes = [
            r for r in self.all_routes if r.get("pattern") == "spoke_to_spoke"
        ]
        if not niche_routes:
            return

        route = random.choice(niche_routes)
        search_date = random.choice(self.available_dates)

        self._execute_flight_search(
            "Niche Route Search", route["origin"], route["dest"], search_date
        )

    @task(3)  # 30% additional searches - random exploration
    def search_random_route(self):
        """Random route search (simulates user exploration)"""
        if not self.all_routes:
            return

        route = random.choice(self.all_routes)
        search_date = random.choice(self.available_dates)

        self._execute_flight_search(
            "Random Route Search", route["origin"], route["dest"], search_date
        )


# Print configuration on startup
print("🚀 REALISTIC FLIGHT SEARCH LOAD TEST")
print("=" * 50)
print("📊 Scenario Configuration:")
print(f"   • Total airport pairs: {len(SCENARIOS.get('airport_pairs', []))}")
print(f"   • Popular routes: {len(SCENARIOS.get('popular_routes', []))}")
print(f"   • Hub airports: {len(SCENARIOS.get('hub_airports', []))}")
print(f"   • Available dates: {len(SCENARIOS.get('available_dates', []))}")
print("")
print("🎯 User Behavior Simulation:")
print("   • 70% Popular route searches (hub-to-hub, high-volume routes)")
print("   • 20% Medium route searches (hub-to-spoke)")
print("   • 10% Niche route searches (spoke-to-spoke)")
print("   • 30% Random exploration")
print("")
print("🔗 Connection Strategy:")
print("   • Each user: One Neo4j driver with connection pooling")
print("   • Each query: New session (reuses pooled connections)")
print("   • Tests database performance, not connection overhead")
print("")
print("📋 Query Strategy:")
print("   • Unified search: Returns both direct and 1-stop connecting flights")
print("   • Real user experience: Get all options for origin → destination")
print("   • Reasonable limits: 20 results, 45-300min connections")
