#!/usr/bin/env python3
"""
Flight Scenario Generator
=========================

Pre-generates realistic airport pairs and date ranges from actual data
to create representative load test scenarios.
"""

import json
import os
import random

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()


def generate_scenarios():
    """Generate realistic flight search scenarios from actual data"""
    print("🔍 Analyzing Flight Data to Generate Realistic Scenarios...")

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

    print(f"📡 Connecting to Neo4j at {uri}...")
    driver = GraphDatabase.driver(uri, auth=(username, password))

    scenarios = {
        "airport_pairs": [],
        "popular_routes": [],
        "hub_airports": [],
        "available_dates": [],
        "airport_stats": {},
    }

    with driver.session(database=database) as session:
        print("   📊 Analyzing airport activity...")

        # Get airport activity stats (flights per airport)
        result = session.run(
            """
            MATCH (a:Airport)
            OPTIONAL MATCH (a)<-[:DEPARTS_FROM]-(dep:Schedule)
            OPTIONAL MATCH (a)<-[:ARRIVES_AT]-(arr:Schedule)

            WITH a, count(DISTINCT dep) AS departures, count(DISTINCT arr) AS arrivals
            WHERE departures > 0 OR arrivals > 0

            RETURN a.code AS airport,
                   departures,
                   arrivals,
                   departures + arrivals AS total_activity
            ORDER BY total_activity DESC
        """
        )

        airport_activity = []
        for record in result:
            airport_data = {
                "code": record["airport"],
                "departures": record["departures"],
                "arrivals": record["arrivals"],
                "total_activity": record["total_activity"],
            }
            airport_activity.append(airport_data)
            scenarios["airport_stats"][record["airport"]] = airport_data

        print(f"   ✅ Found {len(airport_activity)} active airports")

        # Identify major hubs (top 20% by activity)
        hub_threshold = int(len(airport_activity) * 0.2)
        major_hubs = [a["code"] for a in airport_activity[:hub_threshold]]
        scenarios["hub_airports"] = major_hubs
        print(f"   ✅ Identified {len(major_hubs)} major hub airports")

        # Get actual available dates
        result = session.run(
            """
            MATCH (s:Schedule)
            RETURN DISTINCT s.flightdate AS date
            ORDER BY date
        """
        )

        available_dates = []
        for record in result:
            available_dates.append(record["date"])
        scenarios["available_dates"] = available_dates
        print(f"   ✅ Found {len(available_dates)} available dates")

        # Generate realistic airport pairs with different patterns
        print("   🎯 Generating airport pair scenarios...")

        # Pattern 1: Hub-to-Hub routes (high volume, likely direct flights)
        hub_to_hub_pairs = []
        for i, hub1 in enumerate(major_hubs[:15]):  # Top 15 hubs
            for hub2 in major_hubs[i + 1 : 15]:
                hub_to_hub_pairs.append((hub1, hub2))

        # Verify these pairs actually have flights
        verified_hub_pairs = []
        for origin, dest in hub_to_hub_pairs[:50]:  # Sample 50 pairs
            result = session.run(
                """
                MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s:Schedule)
                      -[:ARRIVES_AT]->(d:Airport {code: $dest})
                RETURN count(s) AS flight_count
                LIMIT 1
            """,
                origin=origin,
                dest=dest,
            )

            flight_count = result.single()["flight_count"]
            if flight_count > 0:
                verified_hub_pairs.append(
                    {
                        "origin": origin,
                        "dest": dest,
                        "pattern": "hub_to_hub",
                        "expected_flights": flight_count,
                    }
                )

        print(f"   ✅ Verified {len(verified_hub_pairs)} hub-to-hub routes")

        # Pattern 2: Hub-to-Spoke routes (medium volume, mix of direct/connecting)
        hub_to_spoke_pairs = []
        smaller_airports = [
            a["code"] for a in airport_activity[hub_threshold : hub_threshold * 3]
        ]

        for hub in major_hubs[:10]:
            for spoke in random.sample(
                smaller_airports, min(20, len(smaller_airports))
            ):
                hub_to_spoke_pairs.append((hub, spoke))
                hub_to_spoke_pairs.append((spoke, hub))  # Both directions

        # Verify hub-to-spoke pairs
        verified_hub_spoke_pairs = []
        for origin, dest in random.sample(
            hub_to_spoke_pairs, min(100, len(hub_to_spoke_pairs))
        ):
            result = session.run(
                """
                MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s:Schedule)
                      -[:ARRIVES_AT]->(d:Airport {code: $dest})
                RETURN count(s) AS direct_flights
            """,
                origin=origin,
                dest=dest,
            )

            direct_flights = result.single()["direct_flights"]

            # Also check for connection possibilities
            result = session.run(
                """
                MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s1:Schedule)
                      -[:ARRIVES_AT]->(hub:Airport)<-[:DEPARTS_FROM]-(s2:Schedule)
                      -[:ARRIVES_AT]->(d:Airport {code: $dest})
                WHERE s1.flightdate = s2.flightdate
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
                  AND hub.code <> $origin AND hub.code <> $dest
                RETURN count(*) AS connection_possibilities
                LIMIT 1
            """,
                origin=origin,
                dest=dest,
            )

            connections = result.single()["connection_possibilities"]

            if direct_flights > 0 or connections > 0:
                verified_hub_spoke_pairs.append(
                    {
                        "origin": origin,
                        "dest": dest,
                        "pattern": "hub_to_spoke",
                        "direct_flights": direct_flights,
                        "connection_possibilities": connections,
                    }
                )

        print(f"   ✅ Verified {len(verified_hub_spoke_pairs)} hub-to-spoke routes")

        # Pattern 3: Spoke-to-Spoke routes (lower volume, mostly connections)
        spoke_to_spoke_pairs = []
        medium_airports = [
            a["code"] for a in airport_activity[hub_threshold : hub_threshold * 2]
        ]

        for _ in range(50):  # Generate 50 random spoke-to-spoke pairs
            origin, dest = random.sample(medium_airports, 2)
            spoke_to_spoke_pairs.append((origin, dest))

        # Verify spoke-to-spoke pairs (focus on connections)
        verified_spoke_pairs = []
        for origin, dest in spoke_to_spoke_pairs:
            result = session.run(
                """
                MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s1:Schedule)
                      -[:ARRIVES_AT]->(hub:Airport)<-[:DEPARTS_FROM]-(s2:Schedule)
                      -[:ARRIVES_AT]->(d:Airport {code: $dest})
                WHERE s1.flightdate = s2.flightdate
                  AND s1.scheduled_arrival_time IS NOT NULL
                  AND s2.scheduled_departure_time IS NOT NULL
                  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
                  AND hub.code <> $origin AND hub.code <> $dest
                RETURN count(*) AS connection_possibilities
                LIMIT 1
            """,
                origin=origin,
                dest=dest,
            )

            connections = result.single()["connection_possibilities"]

            if connections > 0:
                verified_spoke_pairs.append(
                    {
                        "origin": origin,
                        "dest": dest,
                        "pattern": "spoke_to_spoke",
                        "connection_possibilities": connections,
                    }
                )

        print(f"   ✅ Verified {len(verified_spoke_pairs)} spoke-to-spoke routes")

        # Combine all verified pairs
        all_pairs = verified_hub_pairs + verified_hub_spoke_pairs + verified_spoke_pairs
        scenarios["airport_pairs"] = all_pairs

        # Create popular routes list (for higher frequency testing)
        popular_routes = [
            pair
            for pair in all_pairs
            if pair.get("expected_flights", 0) > 10 or pair.get("direct_flights", 0) > 5
        ]
        scenarios["popular_routes"] = popular_routes

        print("\n📊 SCENARIO GENERATION SUMMARY:")
        print(f"   • Total verified airport pairs: {len(all_pairs)}")
        print(f"   • Hub-to-hub routes: {len(verified_hub_pairs)}")
        print(f"   • Hub-to-spoke routes: {len(verified_hub_spoke_pairs)}")
        print(f"   • Spoke-to-spoke routes: {len(verified_spoke_pairs)}")
        print(f"   • Popular routes (high frequency): {len(popular_routes)}")
        print(f"   • Major hub airports: {len(major_hubs)}")
        print(f"   • Available date range: {len(available_dates)} days")

    driver.close()

    # Save scenarios to file
    with open("flight_test_scenarios.json", "w") as f:
        json.dump(scenarios, f, indent=2, default=str)

    print("\n✅ Scenarios saved to flight_test_scenarios.json")
    return scenarios


if __name__ == "__main__":
    generate_scenarios()
