# Neo4j Flight Query Load Testing Guide

## 🚀 Quick Start

### 1. Install Dependencies
```bash
# Update your conda environment to include load testing dependencies
conda env update -f environment.yml

# Or if you need to install them individually:
# conda activate flight-schedule
# conda install -c conda-forge locust faker psutil
```

### 2. Configure Database Connection
The load test automatically loads credentials from your `.env` file using `python-dotenv`:
```bash
# Copy .env.example to .env if you haven't already
cp .env.example .env

# Edit .env with your actual Neo4j credentials
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_actual_password
NEO4J_DATABASE=neo4j
```

**Note**: The load test script automatically calls `load_dotenv()` - no manual environment setup required!

### 3. Start Load Test
```bash
locust -f neo4j_flight_load_test.py
```

### 4. Open Web UI
Navigate to: http://localhost:8089

## 📊 Test Scenarios

### Light Load Testing (Baseline)
- **Users**: 10
- **Spawn Rate**: 2 users/second
- **Purpose**: Baseline performance, verify everything works
- **Expected**: <100ms average response time

### Medium Load Testing (Realistic)
- **Users**: 50
- **Spawn Rate**: 5 users/second
- **Purpose**: Realistic user load simulation
- **Expected**: <200ms average response time

### Heavy Load Testing (Stress)
- **Users**: 100-200
- **Spawn Rate**: 10 users/second
- **Purpose**: Find breaking point and maximum throughput
- **Expected**: Find the limit where response times degrade

### Spike Testing
- **Users**: Start with 10, spike to 100, back to 10
- **Purpose**: Test recovery and stability under sudden load changes

## 🎯 Query Distribution

Our load test simulates realistic user behavior:

What the harness actually runs — two Locust tasks:

| Weight | Task | Query |
|---|---|---|
| 70% | `direct_flight_search` | Counts direct flights on a random route and date |
| 30% | `comprehensive_routing_search` | Direct + 1-stop connections via a hub |

> ⚠️ There is **no** "popular / medium / niche" tiering in the code, and no
> 2-stop search. Earlier versions of this table described a distribution that
> summed to 130% and does not exist.
>
> ⚠️ **Route sampling is skewed.** `_load_airports()` orders airports
> lexicographically and keeps the first 100, so the sampling universe runs
> ABE…ELM and excludes most major hubs (ORD, LAX, JFK, LGA, SFO, EWR, MIA, SEA,
> PHX, LAS, ATL, …). Most random routes have no flights, so the majority of the
> 70%-weighted task returns zero rows. Treat throughput figures accordingly.

## 📈 Key Metrics to Monitor

### Response Time Metrics
- **Average Response Time**: Should stay under 200ms for good UX
- **95th Percentile**: No more than 500ms (worst-case user experience)
- **99th Percentile**: Should not exceed 1000ms
- **Max Response Time**: Watch for outliers

### Throughput Metrics
- **RPS (Requests Per Second)**: How many queries/second your system handles
- **Current Users**: Number of concurrent users
- **Total Requests**: Cumulative request count

### Error Metrics
- **Failure Rate**: Should be <1% under normal load
- **Error Types**: Connection errors vs query errors
- **Error Distribution**: Which query types are failing

## 🎛️ Advanced Configuration

### Environment Variables
```bash
# Set database connection via environment
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="password"
export NEO4J_DATABASE="neo4j"

# Run with environment variables
locust -f neo4j_flight_load_test.py
```

### Headless Mode (CI/CD)
```bash
# Run without web UI for automated testing
locust -f neo4j_flight_load_test.py \
  --headless \
  --users 50 \
  --spawn-rate 5 \
  --run-time 5m \
  --html report.html
```

### Distributed Load Testing
```bash
# Master node
locust -f neo4j_flight_load_test.py --master

# Worker nodes (run on different machines)
locust -f neo4j_flight_load_test.py --worker --master-host=<master-ip>
```

## 🔍 Performance Expectations by Query Type

### Direct Flight Queries (70% of load)
```cypher
MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s:Schedule)-[:ARRIVES_AT]->(d:Airport {code: $dest})
WHERE s.flightdate = date($flight_date)
```
> ⚠️ Do **not** add `AND s.cancelled = 0`. Cancelled flights are filtered out
> during loading and `cancelled` is never stored as a property, so that
> predicate matches nothing and the query returns zero rows.
- **Expected**: 20-50ms
- **Bottlenecks**: Airport code lookups, date filtering
- **Optimization**: Ensure indexes on `Airport.code`, `Schedule.flightdate`

### Connection Queries (30% of load)
```cypher
MATCH (dep:Airport)<-[:DEPARTS_FROM]-(s1:Schedule)-[:ARRIVES_AT]->(hub:Airport)
      <-[:DEPARTS_FROM]-(s2:Schedule)-[:ARRIVES_AT]->(arr:Airport)
```
- **Expected**: 50-150ms
- **Bottlenecks**: Complex join patterns, time calculations
- **Optimization**: Composite indexes on `(flightdate, scheduled_departure_time)`

> The 2-stop and analytics query types previously listed here are **not in the
> harness**. The two tasks above are all it runs.

## 🚨 Warning Signs

### Performance Degradation
- Average response time > 500ms
- 95th percentile > 1000ms
- High CPU usage on Neo4j server
- Memory usage climbing steadily

### System Overload
- Error rate > 5%
- Connection timeouts
- Query timeouts
- Database connection pool exhaustion

## 🛠️ Troubleshooting

### High Response Times
1. Check Neo4j query logs for slow queries
2. Verify all recommended indexes exist
3. Monitor system resources (CPU, memory, I/O)
4. Consider Neo4j configuration tuning

### Connection Errors
1. Check Neo4j connection pool settings
2. Verify network connectivity
3. Monitor database connection limits
4. Consider connection pooling in load test

### Memory Issues
1. Monitor Neo4j heap size
2. Check for memory leaks in queries
3. Consider query result pagination
4. Review Neo4j memory configuration

## 📊 Interpreting Results

Run your own baseline before trusting any target — the numbers below are shapes
to look for, not measurements from this repo.

**Healthy:** response times stable as users climb, error rate near zero,
throughput rising roughly linearly with concurrency.

**Saturated:** latency climbing while throughput plateaus or falls, error rate
rising, timeouts appearing on the routing task first.

### Two things that distort the reported numbers

- **Per-route stats names.** Each airport pair is passed as Locust's request
  `name`, producing thousands of near-single-sample entries. Percentiles per
  entry are therefore meaningless, and `quick_load_test_analysis.py` matches
  none of these names — it labels every row "Other". Read the aggregate row.
- **Throughput ceiling is built in.** With `wait_time = between(1, 3)` and about
  1.3 requests per iteration, 100 users cannot exceed roughly 65 req/s no matter
  how fast the database is. If you want to find the database's limit, lower
  `wait_time`.

## 🎯 Setting Performance Goals

Establish your own baseline on your own hardware, then set targets relative to
it. Query latency here depends heavily on dataset size, page cache size relative
to store size, and whether the route you sampled has any flights at all.

For reference, measured directly (not through Locust) against the full 2025
graph of 6.9M flights on a local instance, warm:

| Query | Latency |
|---|---|
| Direct flights, one route + date (20 rows) | ~110 ms |
| 1-stop connections via any hub, same carrier (10 rows) | ~200 ms |

Your numbers will differ. Measure before and after any change rather than
comparing against these.

## 🚀 Next Steps

1. **Baseline Testing**: Start with light load to establish baseline
2. **Gradual Increase**: Incrementally increase load to find breaking point
3. **Optimization**: Use results to optimize queries and indexes
4. **Production Sizing**: Use peak RPS to size production infrastructure
5. **Monitoring Setup**: Implement continuous performance monitoring

---

Happy load testing! 🚀📊
