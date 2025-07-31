// =============================================================================
// NEO4J CONSTRAINTS FOR FLIGHT SCHEDULE SYSTEM
// =============================================================================
// These constraints ensure data integrity and create implicit indexes
// for faster lookups during both loading and querying.
//
// LOADING STRATEGY:
// 1. Create constraints BEFORE bulk loading for optimal performance
// 2. Constraints create implicit indexes that speed up MERGE operations
// 3. Uniqueness is enforced, preventing duplicate data
// =============================================================================

// Schedule Constraints
// -------------------
// Primary identifier for flight schedules
CREATE CONSTRAINT schedule_id_unique IF NOT EXISTS
FOR (s:Schedule) REQUIRE s.schedule_id IS UNIQUE;

// Ensure critical schedule properties exist
CREATE CONSTRAINT schedule_properties IF NOT EXISTS  
FOR (s:Schedule) REQUIRE (s.schedule_id, s.effective_date, s.discontinued_date) IS NOT NULL;

// Airport Constraints  
// ------------------
// IATA codes must be unique (e.g., 'LAX', 'JFK')
CREATE CONSTRAINT airport_code_unique IF NOT EXISTS
FOR (a:Airport) REQUIRE a.code IS UNIQUE;

// Ensure airport code exists
CREATE CONSTRAINT airport_code_exists IF NOT EXISTS
FOR (a:Airport) REQUIRE a.code IS NOT NULL;

// Carrier Constraints
// ------------------  
// Airline codes must be unique (e.g., 'AA', 'UA')
CREATE CONSTRAINT carrier_code_unique IF NOT EXISTS
FOR (c:Carrier) REQUIRE c.code IS UNIQUE;

// Ensure carrier code exists
CREATE CONSTRAINT carrier_code_exists IF NOT EXISTS
FOR (c:Carrier) REQUIRE c.code IS NOT NULL;

// Note: Day and cabin information is stored as bitmap properties 
// directly on Schedule nodes (service_days_bitmap, cabin_bitmap)
// No separate Day or CabinClass nodes are used in this implementation

// =============================================================================
// PERFORMANCE BENEFITS:
// 
// 1. MERGE Operations: Constraints create implicit indexes that make 
//    MERGE operations much faster during bulk loading
//
// 2. Uniqueness Enforcement: Prevents duplicate nodes during parallel 
//    loading, which could cause relationship inconsistencies
//
// 3. Query Performance: Implicit indexes speed up lookups in queries
//
// 4. Data Integrity: Ensures clean, consistent data from the start
// =============================================================================