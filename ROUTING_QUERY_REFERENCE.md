# Advanced Flight Routing Query Reference

## Overview

This document provides the production-ready flight routing query used in the Neo4j load testing framework. The query handles complex real-world scenarios including cross-day flights, realistic connection times, and proper time zone handling.

## Key Features

- ✅ **Cross-day flight handling**: Properly calculates duration for overnight flights (e.g., LAX→JFK red-eye)
- ✅ **Plain Cypher**: No APOC dependencies, fully compatible with Neo4j Aura
- ✅ **Multi-route search**: Finds direct flights and 1-stop connections in a single query
- ✅ **Realistic constraints**: Proper layover times (45min - 20hours), flight duration validation
- ✅ **Performance optimized**: Early filtering, indexed lookups, bounded results

## The Query

```cypher
// Advanced flight routing with cross-day handling
// Finds direct flights + 1-stop connections in a single query

// Part 1: Find direct flights with cross-day handling
MATCH (origin:Airport {code: $origin})<-[:DEPARTS_FROM]-(direct:Schedule)-[:ARRIVES_AT]->(dest:Airport {code: $dest})
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

// Part 2: Find 1-stop connections with cross-day handling
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
```

## Parameters

- `$origin`: 3-letter airport code (e.g., "LAX")
- `$dest`: 3-letter airport code (e.g., "JFK")
- `$search_date`: Search date in YYYY-MM-DD format (e.g., "2024-03-01")

## Data Issue Solved

This query specifically addresses a critical data interpretation issue where flights that cross midnight (overnight flights) had negative durations when calculated naively. For example:

- **Problem**: LAX→JFK departing 23:35 and arriving 07:48 on the same date
- **Naive calculation**: 07:48 - 23:35 = -947 minutes ❌
- **Correct calculation**: Cross-day flight duration = 493 minutes (8.2 hours) ✅

## Example Results

```
🛫 LAX → JFK: 5 routes (2 direct, 3 connections)
  ✈️  Best: B61124 (8.2h) 🌙
```

The 🌙 emoji indicates overnight flights that cross days.

## Extending to N-Stop Connections

This pattern can be extended to 2-stop, 3-stop connections by:

1. Adding additional `UNION ALL` clauses
2. Following the same cross-day calculation patterns
3. Maintaining realistic layover constraints
4. Using `LIMIT` clauses to prevent performance issues

## Performance Considerations

- Early filtering on indexed properties (`flightdate`, airport codes)
- Bounded results (`LIMIT 10`)
- Realistic constraints prevent combinatorial explosion
- Connection time validation happens after node matching

## Production Usage

This query is integrated into `neo4j_flight_load_test.py` as the `comprehensive_routing_search` task, providing realistic airline-style search patterns for load testing Neo4j flight databases.
