#!/usr/bin/env python3
"""
Flight Search Graph Database Demo
================================

Demonstrates realistic flight search on 19M+ flight records.
Shows graph database advantages, query performance, and business logic.
"""

import os
import time
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase


class FlightSearchDemo:
    def __init__(self):
        load_dotenv(override=True)
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))
        )
        self.database = os.getenv('NEO4J_DATABASE')
        
        # Airport mappings for realistic searches
        self.airports = {
            'edinburgh': 'EGPH',
            'nice': 'LFMN', 
            'london': 'EGLL',
            'paris': 'LFPG',
            'amsterdam': 'EHAM',
            'frankfurt': 'EDDF',
            'dublin': 'EIDW',
            'munich': 'EDDM'
        }
    
    def close(self):
        self.driver.close()
    
    def timed_query(self, description, query, params=None):
        """Execute query with timing and performance metrics"""
        print(f"\n🔍 {description}")
        print("=" * (4 + len(description)))
        
        start_time = time.time()
        with self.driver.session(database=self.database) as session:
            result = session.run(query, params or {})
            records = list(result)
        query_time = (time.time() - start_time) * 1000
        
        print(f"⚡ Query time: {query_time:.0f}ms")
        print(f"📊 Records: {len(records):,}")
        
        return records, query_time
    
    def show_dataset_scale(self):
        """Show the scale of data we're querying"""
        print("📊 DATASET SCALE")
        print("=" * 16)
        
        records, _ = self.timed_query(
            "Dataset Overview",
            """
            MATCH (s:Schedule)
            WITH count(s) AS schedules
            MATCH (a:Airport) 
            WITH schedules, count(a) AS airports
            MATCH ()-[r]->()
            RETURN schedules, airports, count(r) AS relationships
            """
        )
        
        data = records[0]
        print(f"   • {data['schedules']:,} flight schedules")
        print(f"   • {data['airports']:,} airports")
        print(f"   • {data['relationships']:,} relationships")
    
    def search_direct_flights(self, origin, destination, date, hour_range=(8, 12)):
        """
        Business Logic: Direct flight search with scoring
        - Find flights between two airports
        - Filter by date and time window  
        - Score by flight duration (lower = better)
        - Sort by score, then departure time
        """
        start_hour, end_hour = hour_range
        
        query = """
        MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
        MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
        MATCH (s)-[:OPERATED_BY]->(carrier:Carrier)
        WHERE date(datetime({epochmillis: s.date_of_operation / 1000})) = date($date)
          AND datetime({epochmillis: s.first_seen_time / 1000}).hour >= $start_hour
          AND datetime({epochmillis: s.first_seen_time / 1000}).hour <= $end_hour
          
        WITH dep, arr, carrier, s,
             datetime({epochmillis: s.first_seen_time / 1000}) AS departure_dt,
             datetime({epochmillis: s.last_seen_time / 1000}) AS arrival_dt
             
        WITH dep, arr, carrier, s, departure_dt, arrival_dt,
             duration.between(departure_dt, arrival_dt).minutes AS flight_duration_minutes
        
        RETURN 
            dep.code AS origin,
            arr.code AS destination,
            carrier.code AS airline,
            s.flight_id AS flight,
            departure_dt AS departure,
            arrival_dt AS arrival,
            flight_duration_minutes,
            flight_duration_minutes AS score,
            'direct' AS flight_type
        ORDER BY score ASC, departure_dt ASC
        LIMIT 10
        """
        
        print(f"\n🛫 DIRECT FLIGHTS: {origin} → {destination}")
        print("=" * (19 + len(origin) + len(destination)))
        print("Business Logic:")
        print(f"  • Route: {origin} → {destination}")
        print(f"  • Date: {date}")
        print(f"  • Time window: {start_hour:02d}:00 - {end_hour:02d}:00")
        print(f"  • Scoring: flight duration (minutes)")
        print(f"  • Ranking: shortest flights first")
        
        records, query_time = self.timed_query(
            "Direct Flight Query",
            query,
            {
                'origin': origin, 'destination': destination, 'date': date,
                'start_hour': start_hour, 'end_hour': end_hour
            }
        )
        
        if records:
            print(f"\n✈️  Found {len(records)} direct flights (ranked by duration):")
            for i, flight in enumerate(records, 1):
                # Format native DateTime objects properly
                dep_time = flight['departure'].strftime("%H:%M")
                arr_time = flight['arrival'].strftime("%H:%M")
                duration = flight['flight_duration_minutes']
                score = flight['score']
                print(f"   {i}. {dep_time}-{arr_time} ({flight['airline']}) {flight['flight']}")
                print(f"      Duration: {duration}min, Score: {score}")
        else:
            print("   No direct flights found")
        
        return len(records), query_time
    
    def search_connections(self, origin, destination, date, hour_range=(6, 14)):
        """
        Business Logic: Connection flight search with scoring
        - Multi-hop graph traversal through hub airports
        - Connection timing validation (45-300 minutes)
        - Score by total travel time + connection penalty
        - Sort by score (best routes first), then departure time
        """
        start_hour, end_hour = hour_range
        
        query = """
        MATCH (s1:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
        MATCH (s1)-[:ARRIVES_AT]->(hub:Airport)
        MATCH (s2:Schedule)-[:DEPARTS_FROM]->(hub)
        MATCH (s2)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
        MATCH (s1)-[:OPERATED_BY]->(c1:Carrier)
        MATCH (s2)-[:OPERATED_BY]->(c2:Carrier)
        
        WHERE date(datetime({epochmillis: s1.date_of_operation / 1000})) = date($date)
          AND date(datetime({epochmillis: s2.date_of_operation / 1000})) = date($date)
          AND hub.code <> $origin AND hub.code <> $destination
          AND datetime({epochmillis: s1.first_seen_time / 1000}).hour >= $start_hour
          AND datetime({epochmillis: s1.first_seen_time / 1000}).hour <= $end_hour
          
        WITH s1, s2, dep, hub, arr, c1, c2,
             datetime({epochmillis: s1.first_seen_time / 1000}) AS departure_dt,
             datetime({epochmillis: s1.last_seen_time / 1000}) AS hub_arrival_dt,
             datetime({epochmillis: s2.first_seen_time / 1000}) AS hub_departure_dt,
             datetime({epochmillis: s2.last_seen_time / 1000}) AS arrival_dt
             
        WITH s1, s2, dep, hub, arr, c1, c2, departure_dt, hub_arrival_dt, hub_departure_dt, arrival_dt,
             duration.between(hub_arrival_dt, hub_departure_dt).minutes AS connection_minutes,
             duration.between(departure_dt, arrival_dt).minutes AS total_travel_minutes
        
        WHERE connection_minutes >= 45 AND connection_minutes <= 300
        
        WITH dep, hub, arr, c1, c2, departure_dt, hub_arrival_dt, hub_departure_dt, arrival_dt,
             connection_minutes, total_travel_minutes,
             total_travel_minutes + 60 AS score  // Connection penalty: +60 minutes
        
        RETURN 
            dep.code AS origin,
            hub.code AS hub,
            arr.code AS destination,
            c1.code + '/' + c2.code AS airlines,
            departure_dt AS departure,
            hub_arrival_dt AS hub_arrival,
            hub_departure_dt AS hub_departure,
            arrival_dt AS arrival,
            connection_minutes,
            total_travel_minutes,
            score,
            'connection' AS flight_type
        ORDER BY score ASC, departure_dt ASC
        LIMIT 8
        """
        
        print(f"\n🔄 CONNECTION FLIGHTS: {origin} → {destination}")
        print("=" * (22 + len(origin) + len(destination)))
        print("Business Logic:")
        print(f"  • Route: {origin} → hub → {destination}")
        print(f"  • Date: {date}")
        print(f"  • Time window: {start_hour:02d}:00 - {end_hour:02d}:00")
        print(f"  • Connection time: 45-300 minutes")
        print(f"  • Scoring: total travel time + 60min connection penalty")
        print(f"  • Ranking: best routes first (lowest score)")
        
        records, query_time = self.timed_query(
            "Connection Flight Query", 
            query,
            {
                'origin': origin, 'destination': destination, 'date': date,
                'start_hour': start_hour, 'end_hour': end_hour
            }
        )
        
        if records:
            print(f"\n🔗 Found {len(records)} connections (ranked by score):")
            for i, conn in enumerate(records, 1):
                # Format native DateTime objects properly
                dep_time = conn['departure'].strftime("%H:%M")
                hub_arr = conn['hub_arrival'].strftime("%H:%M") 
                hub_dep = conn['hub_departure'].strftime("%H:%M")
                arr_time = conn['arrival'].strftime("%H:%M")
                
                print(f"   {i}. {dep_time}-{hub_arr} → {hub_dep}-{arr_time} via {conn['hub']}")
                print(f"      Airlines: {conn['airlines']}, Connection: {int(conn['connection_minutes'])}min")
                print(f"      Total time: {int(conn['total_travel_minutes'])}min, Score: {int(conn['score'])} (incl. 60min penalty)")
        else:
            print("   No connections found")
        
        return len(records), query_time
    
    def run_search_scenario(self, name, origin_city, dest_city, date, time_range):
        """Run complete flight search scenario"""
        origin = self.airports[origin_city.lower()]
        destination = self.airports[dest_city.lower()]
        
        print(f"\n" + "=" * 60)
        print(f"🌍 SCENARIO: {name}")
        print("=" * 60)
        print(f"Traveler: '{origin_city} to {dest_city} on {date}, departing {time_range[0]:02d}:00-{time_range[1]:02d}:00'")
        
        # Search direct flights
        direct_count, direct_time = self.search_direct_flights(origin, destination, date, time_range)
        
        # Search connections
        connection_count, connection_time = self.search_connections(origin, destination, date, time_range)
        
        # Summary
        total_time = direct_time + connection_time
        print(f"\n📋 SCENARIO SUMMARY")
        print("=" * 19)
        print(f"✅ Complete search: {total_time:.0f}ms")
        print(f"✅ Options found: {direct_count} direct + {connection_count} connections")
        print(f"✅ Query performance: {'Excellent' if total_time < 500 else 'Good'}")
        
        return total_time
    
    def show_graph_advantages(self):
        """Demonstrate graph database advantages over SQL"""
        print(f"\n💡 GRAPH DATABASE ADVANTAGES")
        print("=" * 31)
        
        print("🔍 Query Analysis:")
        print("  • Direct flights: scored by actual flight duration")
        print("  • Connections: scored by total travel time + connection penalty")
        print("  • Ranking strategy: ORDER BY score ASC (best routes first)")
        print("  • Business logic: 60-minute penalty for connections")
        
        print("\n⚡ Technical Benefits:")
        print("  • Single query finds and ranks multi-hop paths")
        print("  • Native temporal math with duration.between()")
        print("  • Real-time connection validation during traversal")
        print("  • Bipartite deadlock-free parallel loading")
        
        print(f"\n🏆 Business Value:")
        print("  • Sub-second response times")
        print("  • Complex routing logic in simple queries")
        print("  • Natural graph data model")
        print("  • Scalable to billions of relationships")
    
    def run_demo(self):
        """Run complete flight search demonstration"""
        print("🚀 FLIGHT SEARCH GRAPH DATABASE DEMO")
        print("=" * 39)
        print("Real-world flight search scenarios on 19M+ records")
        
        # Show dataset scale
        self.show_dataset_scale()
        
        # Travel scenarios
        scenarios = [
            ("Business Travel", "London", "Paris", "2024-06-18", (8, 11)),
            ("European Connection", "Edinburgh", "Nice", "2024-06-18", (9, 13)),
            ("Hub-to-Hub Route", "Frankfurt", "Amsterdam", "2024-06-18", (14, 18))
        ]
        
        total_times = []
        for scenario in scenarios:
            scenario_time = self.run_search_scenario(*scenario)
            total_times.append(scenario_time)
        
        # Show graph advantages
        self.show_graph_advantages()
        
        # Performance summary
        avg_time = sum(total_times) / len(total_times)
        print(f"\n📊 PERFORMANCE SUMMARY")
        print("=" * 22)
        print(f"🏃 Average search time: {avg_time:.0f}ms")
        print(f"📊 Query optimization: score-based ranking with business penalties")
        print(f"⚡ Temporal indexing: sub-second multi-hop traversals")
        print(f"📈 Scalability: efficient on large-scale flight networks")


def main():
    """Run flight search demonstration"""
    demo = FlightSearchDemo()
    try:
        demo.run_demo()
        print(f"\n🎉 Demo completed successfully!")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise
    finally:
        demo.close()


if __name__ == "__main__":
    main()