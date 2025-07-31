# AGENTS.md - Lessons Learned

## 🤖 For Future AI Assistants Working on This Project

### 📋 Project Context
This is a **Neo4j flight schedule system** for fast flight queries.

### 🔐 Data Classification

#### PRIVATE (never commit):
- **PDFs in `private_data/customer_docs/`**: Schedule model, implementation docs, customer questions
- **Server reports in `private_data/server_reports/`**: Output from `neo4j-admin server report` (for troubleshooting)

#### PUBLIC (but gitignored due to size):
- **Flight data in `data/`**: Web-scraped airline schedules (reproducible)
- **Sample files**: Can be regenerated from download scripts

### 🏗️ Project Structure
```
flight-schedule-system/
├── private_data/           # NEVER commits (customer-specific)
│   ├── customer_docs/      # PDFs, customer documents  
│   └── server_reports/     # Neo4j admin reports
├── data/                   # Large data files (gitignored)
│   ├── .gitkeep           # Preserves folder structure
│   └── *.parquet          # Flight schedule data
├── src/                    # Source code (commits)
├── setup.py               # Main setup script (commits)
├── README.md               # Main documentation (commits)
└── .env                    # Credentials (gitignored, in root)
```

### 🚨 Common Mistakes to Avoid

#### 1. Neo4j Connection Issues
- **Default**: `bolt://localhost:7687` (configurable via .env)
- **Password**: Read from `.env` file (should be in root)
- **Database**: Use `flights` by default (configurable via .env)

#### 2. Data Loading Errors
- **Temporal data**: DateTime properties stored as epoch microseconds, convert using `datetime({epochmillis: value / 1000})`
- **Column names**: Use actual column names from parquet files (`icao_operator`, `adep`, `ades`, etc.)
- **File paths**: Flight data is in `data/flight_list/` folder, sample in `data/`

#### 3. File Organization
- **Customer docs**: PDFs, implementation details → `private_data/`
- **Server reports**: Neo4j admin output → `private_data/`  
- **Flight data**: Web-scraped, reproducible → `data/` (gitignored)
- **Code**: Our optimization work → `src/` (commits)

### 🔧 Key Technical Details

#### Graph Schema:
- **Nodes**: `Schedule`, `Airport`, `Carrier`
- **Relationships**: `DEPARTS_FROM`, `ARRIVES_AT`, `OPERATED_BY`
- **Schedule Properties**: Contains temporal data (`first_seen_time`, `last_seen_time`, `date_of_operation`)

#### Critical Indexes:
```cypher
CREATE CONSTRAINT airport_code_unique FOR (a:Airport) REQUIRE a.code IS UNIQUE;
CREATE CONSTRAINT carrier_code_unique FOR (c:Carrier) REQUIRE c.code IS UNIQUE;
CREATE CONSTRAINT schedule_id_unique FOR (s:Schedule) REQUIRE s.schedule_id IS UNIQUE;
CREATE INDEX schedule_date_operations FOR (s:Schedule) ON (s.date_of_operation);
CREATE INDEX schedule_temporal FOR (s:Schedule) ON (s.first_seen_time, s.last_seen_time);
```

#### Sample Optimized Query:
```cypher
MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
MATCH (s)-[:OPERATED_BY]->(carrier:Carrier)
WHERE date(datetime({epochmillis: s.date_of_operation / 1000})) = date($date)
  AND datetime({epochmillis: s.first_seen_time / 1000}).hour >= $start_hour
  AND datetime({epochmillis: s.first_seen_time / 1000}).hour <= $end_hour
RETURN s.flight_id, carrier.code, 
       datetime({epochmillis: s.first_seen_time / 1000}) AS departure,
       datetime({epochmillis: s.last_seen_time / 1000}) AS arrival
ORDER BY s.first_seen_time
```

### 📊 Performance Results
- **Direct flights**: 73-431ms per query
- **Connection searches**: 143-612ms per query  
- **Average search time**: ~655ms for complete scenarios
- **Dataset scale**: 4.8M+ flight schedules, 991 airports, 14.4M+ relationships

### 🎯 Success Metrics
- ✅ Score-based flight ranking with business logic
- ✅ Sub-second query times on large datasets
- ✅ Deadlock-free parallel data loading
- ✅ Customer data protected

---
*Created: $(date)*
*Update this file when you learn something new!*
