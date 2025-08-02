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

#### 🚫 ABSOLUTELY NO SYNTHETIC DATA - ZERO TOLERANCE

**⛔ CRITICAL WARNING ⛔**: The user has EXPLICITLY FORBIDDEN any synthetic, generated, or fake data of ANY KIND. ZERO TOLERANCE POLICY.

**🔴 PROJECT FAILURES - LEARN FROM THESE:**
- `download_opensky_data.py` - DELETED for generating fake data using `np.random`
- `download_real_flight_data.py` - DELETED for generating fake schedule IDs like "REAL000033"
- Both violations caused major trust issues and project delays

**❌ NEVER EVER DO ANY OF THIS:**
- `np.random`, `random.choice()`, or ANY randomization
- Generate ANY schedule IDs (even "REAL000033" type patterns)
- Create fake flight schedules, routes, or times
- Generate synthetic timestamps, dates, or temporal data
- Create placeholder data "for testing" or "demos"
- Use made-up airline codes, airport codes, or flight numbers
- Simulate or synthesize ANY flight data
- Create "sample" data of any kind

**✅ ONLY ACCEPTABLE DATA:**
- Historical flight data from FlightAware, OpenSky Network (actual recorded flights)
- Government aviation databases (BTS, FAA, Eurocontrol)
- Airline operational data (actual schedules, not simulated)
- Verifiable flight tracking records with real timestamps

**🛑 IF YOU ARE EVEN CONSIDERING GENERATING DATA: STOP. ASK THE USER INSTEAD.**

**VERIFICATION**: Every flight record must correspond to a real flight that actually operated on the specified date/time.

#### 🚫 NEVER DELETE THE .env FILE

**⛔ CRITICAL WARNING ⛔**: NEVER delete, overwrite, or modify the user's `.env` file.

**❌ FORBIDDEN:**
- Deleting `.env` file for any reason
- Overwriting `.env` file with different credentials
- Modifying database settings in `.env` without explicit permission
- Changing `NEO4J_DATABASE` from `flights` to `neo4j` or any other value

**✅ ALLOWED:**
- Reading `.env` file to understand current configuration
- Suggesting `.env` changes to the user (but never implementing them)
- Using existing `.env` values in your code

**Why**: The user has configured their environment specifically and expects it to remain unchanged. The database name is `flights`, not `neo4j`.

#### 1. Neo4j Connection Issues
- **Default**: `bolt://localhost:7687` (configurable via .env)
- **Password**: Read from `.env` file (should be in root)
- **Database**: Use `flights` by default (configurable via .env)

#### 2. Data Loading Errors
- **Temporal data**: DateTime properties stored as native Neo4j DateTime objects, use directly (e.g., `s.date_of_operation`, `s.first_seen_time.hour`)
- **Column names**: Use actual column names from parquet files (`icao_operator`, `adep`, `ades`, etc.)
- **File paths**: Flight data is in `data/bts_flight_data/` folder

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

#### 🚀 Spark Loading Best Practice:
**CRITICAL**: Always create constraints and indexes BEFORE Spark data loading:

1. **Constraints** (for node merging without duplicates):
   ```bash
   # Use the dedicated constraint script
   cat src/queries/create_constraints.cypher | cypher-shell
   ```

2. **Indexes** (for query performance):
   ```bash
   # Use the dedicated index script
   cat src/queries/create_indexes.cypher | cypher-shell
   ```

3. **Use Neo4j Parallel Spark Loader** (prevents deadlocks):
   ```python
   # REQUIRED for bulk loading - install first:
   pip install neo4j-parallel-spark-loader

   # Import and use for relationships:
   from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe

   # Group data to avoid deadlocks
   grouped_df = group_and_batch_spark_dataframe(
       df, source_col="schedule_id", target_col="airport_code", num_groups=10
   )
   ```

**Why**:
- Constraints create implicit indexes that speed up `MERGE` operations by 3-5x during bulk loading
- Parallel loader prevents Neo4j deadlocks when loading relationships in parallel
- Without constraints: slow loading + potential duplicates
- Without parallel loader: deadlocks + failed loads

#### Critical Indexes:
```cypher
CREATE CONSTRAINT airport_code_unique FOR (a:Airport) REQUIRE a.code IS UNIQUE;
CREATE CONSTRAINT carrier_code_unique FOR (c:Carrier) REQUIRE c.code IS UNIQUE;
CREATE CONSTRAINT schedule_id_unique FOR (s:Schedule) REQUIRE s.schedule_id IS UNIQUE;
CREATE INDEX schedule_date_operations FOR (s:Schedule) ON (s.date_of_operation);
CREATE INDEX schedule_temporal FOR (s:Schedule) ON (s.first_seen_time, s.last_seen_time);
```

#### Sample Query:
```cypher
MATCH (s:Schedule)-[:DEPARTS_FROM]->(dep:Airport {code: $origin})
MATCH (s)-[:ARRIVES_AT]->(arr:Airport {code: $destination})
MATCH (s)-[:OPERATED_BY]->(carrier:Carrier)
WHERE date(s.date_of_operation) = date($date)
  AND s.first_seen_time.hour >= $start_hour
  AND s.first_seen_time.hour <= $end_hour
RETURN s.flight_id, carrier.code,
       s.first_seen_time AS departure,
       s.last_seen_time AS arrival
ORDER BY s.first_seen_time
```

## 📋 Logging Requirements

**MANDATORY**: All scripts must write comprehensive logs to the `logs/` folder:

### Logging Structure
- **Directory**: `logs/` (with `.gitkeep` for git tracking)
- **Format**: `logs/{script_name}_{timestamp}.log`
- **Content**: Timestamps, operation details, errors, performance metrics

### What to Log
- **Data Operations**: Download progress, file sizes, record counts
- **Spark Operations**: Session config, read/write operations, timing
- **Neo4j Operations**: Connection status, constraint creation, load progress
- **Errors**: Full stack traces, context, recovery attempts
- **Performance**: Timing for each phase, memory usage, partition counts

### Implementation Template
```python
import logging
from datetime import datetime

# Setup logging for each script
log_file = f"logs/{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Also print to console
    ]
)
```

### Database Operations During Development
- **Fast Iteration**: Drop and recreate database instead of `MATCH (n) DETACH DELETE n`
- **Production**: Use proper deletion commands
- **Never**: Drop databases in production environments

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

## 🛡️ Code Quality & Pre-Commit Workflow

### ⚠️ MANDATORY: Always Run Pre-Commit Checks Before Committing

**🚨 CRITICAL RULE**: NEVER commit code without running ALL pre-commit checks and tests.

### 📋 Complete Pre-Commit Checklist

**1. Run All Tests**
```bash
# Fast CI tests (always run these)
python -m pytest tests/test_ci_unit.py tests/test_flight_search_unit.py -v

# Connection logic tests (critical for temporal validation)
python -m pytest tests/test_connection_logic.py -v

# Full test suite (when time permits)
python -m pytest tests/ -v
```

**2. Run All Pre-Commit Hooks**
```bash
# Fix formatting automatically
black .
isort .

# Run all quality checks
pre-commit run --all-files

# NEVER use --no-verify unless it's an emergency
# If checks fail, FIX the issues, don't bypass them
```

**3. Manual Quality Checks**
```bash
# Check for long lines that need manual fixing
flake8 . | grep E501 | head -10

# Check for unused imports/variables
flake8 . | grep F401
flake8 . | grep F841

# Security scan results (low severity issues are usually OK)
bandit -r . --skip B101 --exclude tests/
```

**4. Performance & Integration Tests (for major changes)**
```bash
# Performance baseline tests
python -m pytest tests/test_performance_baseline.py -v

# Integration tests (require loaded database)
python -m pytest tests/test_integration_heavy.py -v
```

### 🔧 Common Pre-Commit Fixes

**Line Length Violations (E501)**
- Break long lines at logical points (operators, commas)
- Use parentheses for multi-line expressions
- Consider shorter variable names for very long chains

**Unused Imports (F401)**
```python
# Remove unused imports at the top of files
# Check if import is actually used in the code
```

**F-String Issues (F541)**
```python
# Change f"static text" to "static text"
# Only use f-strings when you have {placeholders}
```

**Unused Variables (F841)**
```python
# Remove variables that are assigned but never used
# Use underscore for intentionally unused variables: _ = value
```

### 📝 Commit Message Standards

**Good Commit Messages:**
```
feat: optimize query performance and index strategy

• README connection query: 239ms → 110ms (44% improvement)
• Add 5 temporal indexes for optimal performance
• Update load_bts_data.py with optimized index creation
• All existing functionality preserved

✅ 40-60% performance improvement achieved
✅ Data integrity maintained
```

**Bad Commit Messages:**
```
fix stuff
update code
wip
```

### 🚫 What NOT to Commit

- Files with failing pre-commit hooks (unless emergency)
- Code with known test failures
- Temporary debugging files
- Large datasets (use .gitignore)
- Credentials or sensitive information
- Work-in-progress code without proper testing

### ⚡ Emergency Bypass (Use Sparingly)

If you MUST commit with failing checks:
```bash
git commit --no-verify -m "emergency: critical fix for production issue

Reason for bypass: [explain emergency]
TODO: Fix code quality issues in follow-up commit"
```

**RULE**: Emergency bypass MUST be followed by a cleanup commit within 24 hours.

---
*Created: $(date)*
*Update this file when you learn something new!*
