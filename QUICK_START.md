# 🚀 One-Liner Quick Start

Get from zero to a fully populated Neo4j graph with load testing in **one command**:

## True One-Liner Setup

```bash
curl -fsSL https://raw.githubusercontent.com/alexwoolford/flight-schedule/main/setup-and-run.sh | bash
```

**Or clone and run locally:**

```bash
git clone https://github.com/alexwoolford/flight-schedule.git && cd flight-schedule && ./setup-and-run.sh
```

## What This Does (Fully Automated)

1. **✅ Environment Setup**: Creates conda environment with all dependencies
2. **📥 Data Download**: Downloads real BTS flight data (7-8M+ records, all 12 months of 2024)
3. **🔗 Neo4j Integration**: Connects to your Neo4j instance (Aura, self-hosted, etc.)
4. **⚡ Data Loading**: Loads ALL flight data using optimized Spark pipeline (~15-30 minutes)
5. **🧪 Testing**: Validates system with comprehensive test suite
6. **🚀 Load Testing**: Sets up production-ready Locust framework

## Prerequisites

- **Conda** installed ([Get it here](https://docs.conda.io/en/latest/miniconda.html))
- **Neo4j instance** accessible (Aura, self-hosted, cloud)
- **16GB+ RAM** recommended
- **~10GB disk space** for flight data

## What You'll Need During Setup

The script will prompt you for:
- Neo4j URI (e.g., `bolt://localhost:7687` or `neo4j+s://your-aura.neo4j.io`)
- Username (usually `neo4j`)
- Password
- Database name (`neo4j` for Aura, `flights` for self-hosted)

## After Setup Completes

**🎯 Start Load Testing:**
```bash
locust -f neo4j_flight_load_test.py
# Visit: http://localhost:8089
```

**📊 Query Your Data:**
```cypher
// Advanced routing with cross-day flight handling
MATCH (origin:Airport {code: 'LGA'})<-[:DEPARTS_FROM]-(direct:Schedule)
      -[:ARRIVES_AT]->(dest:Airport {code: 'DFW'})
WHERE direct.flightdate = date('2024-03-01')
  AND direct.scheduled_departure_time IS NOT NULL

WITH direct,
     CASE
         WHEN direct.scheduled_departure_time <= direct.scheduled_arrival_time THEN
             duration.between(direct.scheduled_departure_time, direct.scheduled_arrival_time).minutes
         ELSE
             // Cross-day flight handling (red-eye flights)
             duration.between(direct.scheduled_departure_time, time('23:59')).minutes + 1 +
             duration.between(time('00:00'), direct.scheduled_arrival_time).minutes
     END AS flight_duration

WHERE flight_duration > 0 AND flight_duration < 1440
RETURN direct.reporting_airline + toString(direct.flight_number_reporting_airline) AS flight,
       direct.scheduled_departure_time AS departure,
       direct.scheduled_arrival_time AS arrival,
       flight_duration AS duration_minutes,
       direct.scheduled_departure_time > direct.scheduled_arrival_time AS is_red_eye
LIMIT 5
```

## Time Estimates

| Phase | Time | Notes |
|-------|------|-------|
| Environment Setup | 2-3 min | Conda environment creation |
| Data Download | 10-15 min | Real BTS data (government servers) |
| Data Loading | 15-30 min | Spark → Neo4j (~4K records/sec, 7-8M records) |
| Validation | 1-2 min | Test suite execution |
| **Total** | **~35-50 minutes** | **Fully hands-off after credential input** |

## What You Get

- **7-8M+ Flight Schedules** (All 12 months of 2024 real data)
- **331 US Airports** with actual codes
- **15 Airlines** with real flight numbers
- **21M+ Relationships** (optimized graph structure)
- **Production Load Testing** (331 airports = 109K+ route combinations)
- **Sub-second Queries** with proper indexing

## Troubleshooting

**Connection issues?**
```bash
# Check your .env file
cat .env

# Test connection manually
python -c "
from dotenv import load_dotenv
from neo4j import GraphDatabase
import os
load_dotenv()
driver = GraphDatabase.driver(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')))
print('✅ Connected!')
"
```

**Need help?**
- Check the setup log: `tail -f logs/setup_*.log`
- Review full documentation: [README.md](README.md)
- Issues? [GitHub Issues](https://github.com/alexwoolford/flight-schedule/issues)

---

**🎯 Goal**: Get you from `git clone` to production-ready graph queries in under 20 minutes!
