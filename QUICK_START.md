# 🚀 One-Liner Quick Start

Get from zero to a fully populated Neo4j graph with load testing in **one command**:

## Setup

```bash
git clone https://github.com/alexwoolford/flight-schedule.git && cd flight-schedule && ./setup-and-run.sh
```

Read the script before piping it anywhere — it prompts for credentials and writes
a `.env` file.

## What This Does (Fully Automated)

1. **✅ Environment Setup**: Creates conda environment with all dependencies
2. **📥 Data Download**: Downloads real BTS flight data (all 12 months of 2025)
3. **🔗 Neo4j Integration**: Connects to your Neo4j instance (Aura, self-hosted, etc.)
4. **⚡ Data Loading**: Loads ALL flight data using a Spark pipeline (~30 minutes)
5. **🧪 Testing**: Runs the unit test suite

## Prerequisites

- **Conda** installed ([Get it here](https://docs.conda.io/en/latest/miniconda.html))
- **Neo4j 5.25+** accessible (Aura, self-hosted, cloud)
- **16GB+ RAM** recommended
- **~10GB disk space** for flight data
- **Internet access on first load** — Spark downloads the Neo4j connector JAR

## What You'll Need During Setup

The script will prompt you for:
- Neo4j URI (e.g., `bolt://localhost:7687` or `neo4j+s://your-aura.neo4j.io`)
- Username (usually `neo4j`)
- Password
- Database name (`neo4j` for Aura and Community Edition, `flights` for self-hosted Enterprise)

If you use `flights`, create it first — the loader does not create databases:

```cypher
CREATE DATABASE flights IF NOT EXISTS;   // run against the `system` database
```

## After Setup Completes

**🎯 Start Load Testing:**
```bash
locust -f neo4j_flight_load_test.py
# Visit: http://localhost:8089
```

**📊 Query Your Data — direct flights:**
```cypher
MATCH (:Airport {code: 'LGA'})<-[:DEPARTS_FROM]-(s:Schedule)-[:ARRIVES_AT]->(:Airport {code: 'DFW'})
WHERE s.flightdate = date('2025-01-15')
RETURN s.reporting_airline + toString(s.flight_number_reporting_airline) AS flight,
       s.scheduled_departure_time AS departs,
       s.scheduled_arrival_time AS arrives,
       s.scheduled_duration_minutes AS minutes
ORDER BY departs
LIMIT 5;
```

> ⚠️ Use `scheduled_duration_minutes` for flight length. Do **not** subtract
> `scheduled_departure_time` from `scheduled_arrival_time`: departure is local
> time at the origin and arrival is local time at the destination, so the
> difference is wrong whenever the two airports are in different timezones.
> `scheduled_duration_minutes` is BTS's own reported block time, which is
> timezone- and DST-independent.

See [README.md](README.md) for the multi-hop routing query.

## Time Estimates

| Phase | Time | Notes |
|-------|------|-------|
| Environment Setup | 2-3 min | Conda environment creation |
| Data Download | 10-15 min | Real BTS data (government servers) |
| Data Loading | ~30 min | Spark → Neo4j, 6.9M flights + 20.7M relationships |
| Validation | 1-2 min | Unit test suite |
| **Total** | **~45-50 minutes** | **Hands-off after credential input** |

## What You Get

From the full 2025 BTS On-Time Performance dataset (7,001,619 flights):

- **6,898,743 `Schedule` nodes** (cancelled flights filtered at load)
- **352 `Airport` nodes** with real IATA codes
- **14 `Carrier` nodes** with real flight numbers
- **20,696,229 relationships** — `DEPARTS_FROM`, `ARRIVES_AT`, `OPERATED_BY`

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
