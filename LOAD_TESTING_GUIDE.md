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

### 1.5. Generate Flight Scenarios (Required)
```bash
# Generate realistic flight scenarios from your loaded data
# This creates flight_test_scenarios.json from your actual flight database
python generate_flight_scenarios.py

# ⚠️ IMPORTANT: This file is NOT committed to git (it's generated)
# You must run this after loading flight data and before load testing
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
locust -f realistic_flight_search_load_test.py --host=bolt://localhost:7687
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

| Query Type | Percentage | Complexity | Purpose |
|------------|------------|------------|---------|
| **Popular Route Search** | 70% | Simple-Moderate | Hub-to-hub routes (likely direct flights) |
| **Medium Route Search** | 20% | Moderate | Hub-to-spoke routes (mix of direct/connections) |
| **Niche Route Search** | 10% | Complex | Spoke-to-spoke routes (mostly connections) |
| **Random Exploration** | 30% | Variable | Simulates user browsing behavior |

> **Note**: Each search returns both direct flights AND 1-stop connections in a single unified query, just like real flight booking websites.

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
locust -f realistic_flight_search_load_test.py
```

### Headless Mode (CI/CD)
```bash
# Run without web UI for automated testing
locust -f realistic_flight_search_load_test.py \
  --headless \
  --users 50 \
  --spawn-rate 5 \
  --run-time 5m \
  --html report.html
```

### Distributed Load Testing
```bash
# Master node
locust -f realistic_flight_search_load_test.py --master

# Worker nodes (run on different machines)
locust -f realistic_flight_search_load_test.py --worker --master-host=<master-ip>
```

## 🔍 Performance Expectations by Query Type

### Direct Flight Queries (50% of load)
```cypher
MATCH (o:Airport {code: $origin})<-[:DEPARTS_FROM]-(s:Schedule)-[:ARRIVES_AT]->(d:Airport {code: $dest})
WHERE s.flightdate = $flight_date AND s.cancelled = 0
```
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

### Multi-hop Queries (15% of load)
```cypher
MATCH path = (dep)<-[:DEPARTS_FROM]-(s1)-[:ARRIVES_AT]->(hub1)
             <-[:DEPARTS_FROM]-(s2)-[:ARRIVES_AT]->(hub2)
             <-[:DEPARTS_FROM]-(s3)-[:ARRIVES_AT]->(arr)
```
- **Expected**: 100-300ms
- **Bottlenecks**: Triple joins, complex filtering
- **Optimization**: Consider query hints, limit result sets

### Analytics Queries (5% of load)
```cypher
MATCH (a:Airport)<-[:DEPARTS_FROM|:ARRIVES_AT]-(s:Schedule)
```
- **Expected**: 50-200ms
- **Bottlenecks**: Aggregations, counting
- **Optimization**: Consider caching for frequent analytics

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

### Successful Load Test Results
```
Target: 50 users, 5/sec spawn rate, 5 minutes
✅ Average Response Time: 120ms
✅ 95th Percentile: 250ms
✅ RPS: 45-50 queries/second
✅ Error Rate: 0.1%
✅ All query types performing within expected ranges
```

### Problematic Results
```
Target: 100 users, 10/sec spawn rate, 5 minutes
❌ Average Response Time: 800ms
❌ 95th Percentile: 2000ms
❌ RPS: 25 queries/second (degraded)
❌ Error Rate: 15%
❌ Multi-hop queries timing out
```

## 🎯 Performance Goals

### Minimum Acceptable Performance
- **Direct flights**: <200ms average
- **Connections**: <300ms average
- **Multi-hop**: <500ms average
- **Analytics**: <400ms average
- **Overall error rate**: <2%

### Optimal Performance Targets
- **Direct flights**: <50ms average
- **Connections**: <150ms average
- **Multi-hop**: <300ms average
- **Analytics**: <200ms average
- **Overall error rate**: <0.5%

## 🚀 Next Steps

1. **Baseline Testing**: Start with light load to establish baseline
2. **Gradual Increase**: Incrementally increase load to find breaking point
3. **Optimization**: Use results to optimize queries and indexes
4. **Production Sizing**: Use peak RPS to size production infrastructure
5. **Monitoring Setup**: Implement continuous performance monitoring

---

Happy load testing! 🚀📊
