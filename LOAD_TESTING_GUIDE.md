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

| Weight | Task | Locust stat name | What it calls |
|---|---|---|---|
| 70% | `nonstop_lookup` | `nonstop lookup` | `search_itineraries(max_stops=0, limit=10)` |
| 30% | `itinerary_search` | `itinerary search {0,2}` | `search_itineraries(max_stops=2, limit=20)` |

Both go through `flight_search.py`, the same code path `api.py` serves. The
harness holds no Cypher of its own beyond one airport-sampling query, which is
gated by `test_load_test_holds_no_cypher_of_its_own`.

> ⚠️ There is **no** "popular / medium / niche" tiering in the code, and no
> 2-stop-only task. Earlier versions of this table described a distribution that
> summed to 130% and does not exist, and named two functions
> (`direct_flight_search`, `comprehensive_routing_search`) that no longer do.
>
> ✅ **Sampling is by flight volume**, from the `TOP_ORIGINS = 60` busiest origins
> on the sampled date. It used to order airports lexicographically and keep the
> first 100 — a universe of ABE…ELM that excluded ORD, LAX, JFK, LGA, SFO, EWR
> and most other hubs, so ~95% of the 70%-weighted task returned zero rows.
> Throughput figures published before that fix measured empty results.
>
> ⚠️ **Dates are sampled from `CONNECTS_TO` coverage**, not from all `Schedule`
> dates, so a sampled date always has routing edges. With 7 dates built, the
> harness only ever exercises those.

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

Both tasks issue the **same** quantified-path query from `flight_search.py`,
differing only in `max_stops` and `limit`. The Cypher is not restated here — see
`flight_search.build_search_query()` and `ROUTING_QUERY_REFERENCE.md`. The two
hand-rolled `MATCH` patterns that used to sit in this section described a
`Schedule → Airport → Schedule` traversal the harness no longer performs, and
which is **200-400x slower** than traversing `CONNECTS_TO` because `Airport` is a
supernode with no date property.

### Nonstop lookup (70% of load) — `max_stops=0`, `limit=10`
- **Bottlenecks**: airport code lookup, date filtering
- **Optimization**: the `flightdate` index plus the `Airport.code` constraint's
  backing index; `test_query_plan.py` gates that the plan starts from a seek
  rather than a `NodeByLabelScan`

### Itinerary search (30% of load) — `max_stops=2`, `limit=20`
- **Bottlenecks**: quantifier expansion, the per-path acyclicity guard, ranking
- **Optimization**: `ORDER BY total_minutes LIMIT $limit` must plan as `Top`, not
  `Sort` — a bounded heap rather than buffering every path. Also gated.
- **Serve `{0,2}`, not `{0,3}`**: measured over 40 pairs, `{0,2}` holds a 200 ms
  p95 filtered or unfiltered; `{0,3}` is p95 595 ms unfiltered with 34 of 40 pairs
  over 200 ms.

> ⚠️ Do **not** add `AND s.cancelled = 0` to any of these. Cancelled flights are
> filtered out during loading and `cancelled` is never stored as a property, so
> that predicate matches nothing and the query returns zero rows.
>
> The 2-stop-only and analytics query types previously listed here are **not in
> the harness**. The two tasks above are all it runs.

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

### One thing that still distorts the reported numbers

- **Throughput ceiling is built in.** With `wait_time = between(1, 3)` and about
  1.3 requests per iteration, 100 users cannot exceed roughly 65 req/s no matter
  how fast the database is. If you want to find the database's limit, lower
  `wait_time`.

**Fixed, but worth knowing if you compare against old runs:** each airport pair
used to be passed as Locust's request `name`, producing up to 29,700
near-single-sample stat entries whose percentiles were computed over 1-2 samples
each — and `quick_load_test_analysis.py` matched none of those names, labelling
every row "Other". The two task names are now constants (`NONSTOP_TASK`,
`SEARCH_TASK`), so percentiles aggregate properly and the analysis script matches.
A driver was also constructed **per simulated user**, putting driver and TLS setup
inside every measurement; there is now one pooled process-wide driver from
`flight_search.get_driver()`. Numbers published before those two fixes are not
comparable to numbers after them.

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
