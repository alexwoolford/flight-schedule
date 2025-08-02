# BTS Flight Data Schema Analysis

## 📊 Data Overview
- **Source**: US Bureau of Transportation Statistics (BTS)
- **Sample**: June 2024 (611,132 flights)
- **Total Columns**: 110 columns
- **Data Quality**: Excellent (most core columns 98-100% populated)

## 🎯 Mapping to Your 3-Node Graph Model

### 🏢 1. AIRLINE NODES (Carrier)

**Primary Data:**
- `reporting_airline` - Airline code (AA, DL, WN, etc.) - **100% populated** ✅
- `dot_id_reporting_airline` - DOT unique numeric ID - **100% populated** ✅
- `iata_code_reporting_airline` - IATA code (same as reporting_airline) - **100% populated** ✅

**Sample Airlines:** 9E (Endeavor), AA (American), DL (Delta), WN (Southwest), B6 (JetBlue), F9 (Frontier)

### 🏢 2. AIRPORT NODES

**Primary Data:**
- `origin` / `dest` - Airport codes (JFK, LAX, ORD, etc.) - **100% populated** ✅
- `originairportid` / `destairportid` - Unique numeric IDs - **100% populated** ✅

**Additional Airport Data:**
- `origincityname` / `destcityname` - City names (New York, NY) - **100% populated** ✅
- `originstate` / `deststate` - State codes (NY, CA) - **100% populated** ✅
- `originstatefips` / `deststatefips` - FIPS codes - **100% populated** ✅

**Sample Airports:** JFK, LAX, ORD, ATL, DEN, PHX

### ✈️ 3. SCHEDULE NODES (Core Flight Data)

**Required Identifiers:**
- `flightdate` - Flight date (2024-06-07) - **100% populated** ✅
- `flight_number_reporting_airline` - Flight number (4800) - **100% populated** ✅
- Links to airline and airports via foreign keys

**🕐 TEMPORAL DATA (Your Key Requirement!):**
- `crsdeptime` - Scheduled departure time (07:00:00) - **100.0% populated** ✅
- `crsarrtime` - Scheduled arrival time (09:00:00) - **99.6% populated** ✅
- `deptime` - Actual departure time (06:50:00) - **98.6% populated** ⚠️
- `arrtime` - Actual arrival time (08:41:00) - **98.0% populated** ⚠️
- `crselapsedtime` - Scheduled duration (120 minutes) - **100.0% populated** ✅
- `actualelapsedtime` - Actual duration (111 minutes) - **98.4% populated** ⚠️

**Sample Flight Record:**
```
Flight: 9E 4800 (Endeavor Air)
Date: 2024-06-07
Route: CHS → JFK (Charleston to JFK)
Scheduled: 07:00:00 → 09:00:00 (2h duration)
Actual: 06:50:00 → 08:41:00 (1h 51m duration)
Distance: 636 miles
Status: On-time (arrived 19 minutes early)
```

**Performance Metadata:**
- `distance` - Flight distance in miles - **100% populated** ✅
- `tail_number` - Aircraft identifier (N272PQ) - **99.8% populated** ✅
- `cancelled` - Cancellation flag (0/1) - **100% populated** ✅
- `diverted` - Diversion flag (0/1) - **100% populated** ✅
- `depdelay` / `arrdelay` - Delay in minutes (negative = early) - **98%+ populated** ⚠️

## 🚀 Recommended Columns for Neo4j Loading

### Essential Columns (Required)
```python
AIRLINE_COLUMNS = [
    'reporting_airline',           # Primary key
    'dot_id_reporting_airline'     # Unique numeric ID
]

AIRPORT_COLUMNS = [
    'origin', 'dest',              # Airport codes  
    'originairportid', 'destairportid'  # Unique IDs
]

SCHEDULE_CORE = [
    'flightdate',                  # Flight date
    'flight_number_reporting_airline',  # Flight number
    'reporting_airline',           # Links to airline
    'origin', 'dest',             # Links to airports
    
    # Temporal data (your requirement!)
    'crsdeptime', 'crsarrtime',   # Scheduled times
    'deptime', 'arrtime',         # Actual times  
    'crselapsedtime', 'actualelapsedtime'  # Durations
]
```

### Useful Metadata (Optional)
```python
SCHEDULE_METADATA = [
    'distance',                    # Flight distance
    'tail_number',                # Aircraft ID
    'cancelled', 'diverted',      # Status flags
    'depdelay', 'arrdelay',       # Performance metrics
    'airtime',                    # Actual flying time
    'taxiout', 'taxiin'          # Ground operations
]

AIRPORT_METADATA = [
    'origincityname', 'destcityname',  # City names
    'originstate', 'deststate'         # State codes
]
```

### Skip These Columns
- `div1-div5*` columns - Diversion details (<1% populated)
- `*delay` breakdown columns - Cause analysis (24.5% populated)  
- `firstdeptime`, `totaladdgtime` - Gate returns (0.8% populated)
- `*timeblk` columns - Time groupings (can be derived)
- `unnamed:_109` - Empty column

## ✅ Data Quality Assessment

**Excellent Quality (>99% populated):**
- All airline identifiers
- All airport identifiers
- Flight dates and numbers
- Scheduled times
- Distance and basic metadata

**Good Quality (95-99% populated):**
- Actual departure/arrival times (98%+)
- Tail numbers (99.8%)
- Performance delays (98%+)

**Sparse Data (<25% populated):**
- Delay cause breakdowns
- Diversion details
- Gate return information

## 🎯 Neo4j Loading Strategy

1. **Create unique constraints** on primary keys
2. **Load in order**: Airlines → Airports → Schedules  
3. **Use temporal types** for all time columns
4. **Index temporal fields** for fast queries
5. **Create relationships**: 
   - `(Schedule)-[:OPERATED_BY]->(Airline)`
   - `(Schedule)-[:DEPARTS_FROM]->(Airport)`
   - `(Schedule)-[:ARRIVES_AT]->(Airport)`

**Result**: Rich temporal graph with 100% real data! 🚀