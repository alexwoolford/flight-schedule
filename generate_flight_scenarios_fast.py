#!/usr/bin/env python3
"""
Fast Flight Scenario Generator
==============================

Lightweight version that doesn't hang on large datasets.
Pre-generates realistic airport pairs without expensive verification queries.
"""

import json
import os
import random

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()


def generate_scenarios_fast():
    """Generate realistic flight search scenarios quickly from actual data"""
    print("🔍 Generating Flight Scenarios (Fast Mode)...")

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
        print("   📊 Getting basic airport stats...")

        # Simple airport activity query (no expensive joins)
        result = session.run(
            """
            MATCH (a:Airport)
            OPTIONAL MATCH (a)<-[:DEPARTS_FROM]-(dep:Schedule)
            WITH a, count(dep) AS departures
            WHERE departures > 100
            RETURN a.code AS airport, departures
            ORDER BY departures DESC
            LIMIT 100
            """
        )

        airport_activity = []
        for record in result:
            airport_data = {
                "code": record["airport"],
                "departures": record["departures"],
                "total_activity": record["departures"],
            }
            airport_activity.append(airport_data)
            scenarios["airport_stats"][record["airport"]] = airport_data

        print(f"   ✅ Found {len(airport_activity)} active airports")

        # If no data found, use common US airports as fallback
        if len(airport_activity) == 0:
            print("   ⚠️  No data found, using fallback airport list...")
            fallback_airports = [
                {"code": "ATL", "departures": 1000, "total_activity": 1000},
                {"code": "DFW", "departures": 900, "total_activity": 900},
                {"code": "DEN", "departures": 800, "total_activity": 800},
                {"code": "ORD", "departures": 700, "total_activity": 700},
                {"code": "LAX", "departures": 600, "total_activity": 600},
                {"code": "PHX", "departures": 500, "total_activity": 500},
                {"code": "LAS", "departures": 400, "total_activity": 400},
                {"code": "SEA", "departures": 350, "total_activity": 350},
                {"code": "MIA", "departures": 300, "total_activity": 300},
                {"code": "CLT", "departures": 250, "total_activity": 250},
                {"code": "BOS", "departures": 200, "total_activity": 200},
                {"code": "MSP", "departures": 180, "total_activity": 180},
                {"code": "DTW", "departures": 160, "total_activity": 160},
                {"code": "JFK", "departures": 150, "total_activity": 150},
                {"code": "LGA", "departures": 140, "total_activity": 140},
            ]
            airport_activity = fallback_airports
            for airport_data in fallback_airports:
                scenarios["airport_stats"][airport_data["code"]] = airport_data

        # Identify major hubs (top 20 airports)
        major_hubs = [a["code"] for a in airport_activity[:20]]
        scenarios["hub_airports"] = major_hubs
        print(f"   ✅ Identified {len(major_hubs)} major hub airports")

        # Get sample of available dates (faster)
        result = session.run(
            """
            MATCH (s:Schedule)
            RETURN DISTINCT s.flightdate AS date
            ORDER BY date
            LIMIT 100
            """
        )

        available_dates = []
        for record in result:
            available_dates.append(record["date"])

        # If no dates found, use fallback dates
        if len(available_dates) == 0:
            print("   ⚠️  No dates found, using fallback date range...")
            from datetime import date, timedelta

            base_date = date(2024, 3, 1)
            available_dates = [
                base_date + timedelta(days=i) for i in range(0, 30, 2)
            ]  # Every other day for a month

        scenarios["available_dates"] = available_dates
        print(f"   ✅ Found {len(available_dates)} available dates")

        # Generate airport pairs without expensive verification
        print("   🎯 Generating airport pair scenarios...")

        # Pattern 1: Hub-to-Hub routes (no verification needed)
        hub_to_hub_pairs = []
        for i, hub1 in enumerate(major_hubs[:10]):
            for hub2 in major_hubs[i + 1 : 10]:
                hub_to_hub_pairs.append(
                    {
                        "origin": hub1,
                        "dest": hub2,
                        "pattern": "hub_to_hub",
                        "expected_flights": "unknown",  # Skip expensive verification
                    }
                )

        print(f"   ✅ Generated {len(hub_to_hub_pairs)} hub-to-hub routes")

        # Pattern 2: Hub-to-Spoke routes
        hub_to_spoke_pairs = []
        smaller_airports = [a["code"] for a in airport_activity[20:60]]

        for hub in major_hubs[:5]:
            # Sample smaller airports
            sample_size = min(10, len(smaller_airports))
            if sample_size > 0:
                sampled_airports = random.sample(
                    smaller_airports, sample_size
                )  # nosec B311
                for spoke in sampled_airports:
                    hub_to_spoke_pairs.append(
                        {
                            "origin": hub,
                            "dest": spoke,
                            "pattern": "hub_to_spoke",
                            "direct_flights": "unknown",
                        }
                    )
                    hub_to_spoke_pairs.append(
                        {
                            "origin": spoke,
                            "dest": hub,
                            "pattern": "spoke_to_hub",
                            "direct_flights": "unknown",
                        }
                    )

        print(f"   ✅ Generated {len(hub_to_spoke_pairs)} hub-to-spoke routes")

        # Pattern 3: Random pairs from all airports
        random_pairs = []
        all_airports = [a["code"] for a in airport_activity]

        for _ in range(30):  # Generate 30 random pairs
            if len(all_airports) >= 2:
                origin, dest = random.sample(all_airports, 2)  # nosec B311
                random_pairs.append(
                    {
                        "origin": origin,
                        "dest": dest,
                        "pattern": "random",
                        "direct_flights": "unknown",
                    }
                )

        print(f"   ✅ Generated {len(random_pairs)} random routes")

        # Combine all patterns
        all_pairs = hub_to_hub_pairs + hub_to_spoke_pairs + random_pairs
        scenarios["airport_pairs"] = all_pairs
        scenarios["popular_routes"] = hub_to_hub_pairs[:20]  # Top 20 hub routes

    driver.close()

    # Save scenarios to file
    output_file = "flight_test_scenarios.json"
    with open(output_file, "w") as f:
        json.dump(scenarios, f, indent=2, default=str)

    print("\n🎉 Flight Scenarios Generated Successfully!")
    print(f"   📁 Saved to: {output_file}")
    print(f"   ✈️  Total airport pairs: {len(scenarios['airport_pairs'])}")
    print(f"   🏢 Hub airports: {len(scenarios['hub_airports'])}")
    print(f"   📅 Available dates: {len(scenarios['available_dates'])}")
    print(f"   🎯 Popular routes: {len(scenarios['popular_routes'])}")

    return scenarios


if __name__ == "__main__":
    try:
        generate_scenarios_fast()
        print("\n✅ Scenario generation completed successfully!")
    except Exception as e:
        print(f"\n❌ Error generating scenarios: {e}")
        exit(1)
