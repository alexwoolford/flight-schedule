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
2. **📥 Data Download**: Downloads real BTS flight data (586K+ records, ~2GB)
3. **🔗 Neo4j Integration**: Connects to your Neo4j instance (Aura, self-hosted, etc.)
4. **⚡ Data Loading**: Loads flight data using optimized Spark pipeline (~5 minutes)
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
// Find connections LGA → DFW
MATCH (dep:Airport {code: 'LGA'})<-[:DEPARTS_FROM]-(s1:Schedule)
      -[:ARRIVES_AT]->(hub:Airport)<-[:DEPARTS_FROM]-(s2:Schedule)
      -[:ARRIVES_AT]->(arr:Airport {code: 'DFW'})
WHERE s1.flightdate = date('2024-03-01')
  AND s2.flightdate = date('2024-03-01')
  AND s2.scheduled_departure_time > s1.scheduled_arrival_time
RETURN hub.code, s1.reporting_airline + toString(s1.flight_number_reporting_airline) as flight1,
       s2.reporting_airline + toString(s2.flight_number_reporting_airline) as flight2
LIMIT 5
```

## Time Estimates

| Phase | Time | Notes |
|-------|------|-------|
| Environment Setup | 2-3 min | Conda environment creation |
| Data Download | 10-15 min | Real BTS data (government servers) |
| Data Loading | 5-10 min | Spark → Neo4j (~4K records/sec) |
| Validation | 1-2 min | Test suite execution |
| **Total** | **~20 minutes** | **Fully hands-off after credential input** |

## What You Get

- **586K+ Flight Schedules** (March 2024 real data)
- **331 US Airports** with actual codes
- **15 Airlines** with real flight numbers
- **1.76M Relationships** (optimized graph structure)
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
