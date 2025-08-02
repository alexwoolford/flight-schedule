// Neo4j Index Creation Script for Flight Schedule Graph
// Run this script after loading the flight schedule graph

// =======================
// CONSTRAINT CREATION
// =======================

// Unique constraints for primary keys
CREATE CONSTRAINT airport_code_unique IF NOT EXISTS FOR (a:Airport) REQUIRE a.code IS UNIQUE;
CREATE CONSTRAINT carrier_code_unique IF NOT EXISTS FOR (c:Carrier) REQUIRE c.code IS UNIQUE;
CREATE CONSTRAINT schedule_id_unique IF NOT EXISTS FOR (s:Schedule) REQUIRE s.schedule_id IS UNIQUE;
// Note: No Day or CabinClass constraints needed - using bitmap properties on Schedule nodes

// =======================
// PERFORMANCE INDEXES
// =======================

// Primary lookup indexes
CREATE INDEX airport_code_lookup IF NOT EXISTS FOR (a:Airport) ON (a.code);
CREATE INDEX carrier_code_lookup IF NOT EXISTS FOR (c:Carrier) ON (c.code);
CREATE INDEX schedule_id_lookup IF NOT EXISTS FOR (s:Schedule) ON (s.schedule_id);

// Date range indexes for schedule validity queries
CREATE INDEX schedule_date_range IF NOT EXISTS FOR (s:Schedule) ON (s.effective_date, s.discontinued_date);

// Time-based indexes for departure/arrival filtering
CREATE INDEX schedule_departure_time IF NOT EXISTS FOR (s:Schedule) ON (s.published_departure_time);
CREATE INDEX schedule_arrival_time IF NOT EXISTS FOR (s:Schedule) ON (s.published_arrival_time);
CREATE INDEX schedule_time_range IF NOT EXISTS FOR (s:Schedule) ON (s.published_departure_time, s.published_arrival_time);

// Bitmap indexes for legacy bitmap queries (if still needed)
CREATE INDEX schedule_service_days IF NOT EXISTS FOR (s:Schedule) ON (s.service_days_bitmap);
CREATE INDEX schedule_cabin_bitmap IF NOT EXISTS FOR (s:Schedule) ON (s.cabin_bitmap);

// Composite indexes for common query patterns
CREATE INDEX schedule_effective_bitmap IF NOT EXISTS FOR (s:Schedule) ON (s.effective_date, s.service_days_bitmap);
CREATE INDEX schedule_times_cabin IF NOT EXISTS FOR (s:Schedule) ON (s.published_departure_time, s.cabin_bitmap);

// Note: Day and cabin filtering done via bitmap properties on Schedule nodes
// (service_days_bitmap, cabin_bitmap) - no separate node indexes needed

// =======================
// RELATIONSHIP INDEXES
// =======================

// No relationship property indexes needed for current implementation
// Relationships used: DEPARTS_FROM, ARRIVES_AT, OPERATED_BY (no properties)

// =======================
// FULL-TEXT SEARCH INDEXES
// =======================

// Airport name and city search
CREATE FULLTEXT INDEX airport_search IF NOT EXISTS FOR (a:Airport) ON EACH [a.name, a.city, a.state];

// Carrier name search
CREATE FULLTEXT INDEX carrier_search IF NOT EXISTS FOR (c:Carrier) ON EACH [c.name];

// =======================
// VERIFICATION QUERIES
// =======================

// Use these queries to verify indexes are working:

/*
// Check all indexes
SHOW INDEXES;

// Verify constraint uniqueness
MATCH (a:Airport)
WITH a.code AS code, count(*) AS cnt
WHERE cnt > 1
RETURN code, cnt;

// Test index usage with EXPLAIN
EXPLAIN MATCH (a:Airport {code: 'JFK'}) RETURN a;

// Performance test - should use index
EXPLAIN MATCH (s:Schedule)
WHERE s.effective_date <= date('2023-01-15')
  AND s.discontinued_date >= date('2023-01-15')
RETURN count(s);
