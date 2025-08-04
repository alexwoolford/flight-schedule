# AGENTS.md - Lessons Learned

## 🤖 For Future AI Assistants Working on This Project

### 📋 Project Context
This is a **Neo4j flight schedule system** for fast flight queries.

### 🚨 CRITICAL: .gitignore File Handling Policy
**ABSOLUTE RULE**: NEVER delete files that are excluded by `.gitignore` during cleanup operations.

**🎯 UNDERSTANDING THE REPOSITORY STRUCTURE**:
This repository contains TWO types of files:
1. **Repository files** - Code, docs, configs that should be tracked in git
2. **Development context files** - Customer data, generated reports, caches that are useful locally but excluded by `.gitignore`

**✅ ALWAYS DO**:
- Respect `.gitignore` boundaries completely
- When asked to "cleanup for commit" - ONLY deal with tracked files or files that should be tracked
- Use `git status --porcelain` to see what git cares about
- Use `git check-ignore <file>` to verify if a file is ignored before touching it

**❌ NEVER DO**:
- Delete files from the filesystem just because they seem "non-essential"
- Remove `.gitignore`d files during "cleanup" operations
- Touch files in `private_data/`, generated caches, or other ignored directories
- Delete customer development context (Neo4j reports, analysis files, etc.)

**🔍 CLEANUP CHECKLIST**:
```bash
# CORRECT: Check what git actually tracks
git status --porcelain
git ls-files | grep -E "\.(png|jpg|json|log|tmp)$"

# WRONG: Don't blindly delete files from filesystem
# rm -rf some_directory/  # ❌ Could delete customer data!
```

**📁 PROTECTED DIRECTORIES** (always .gitignore'd, never delete):
- `private_data/` - Customer-specific data and reports
- `__pycache__/` - Python caches (auto-regenerate)
- `.mypy_cache/` - Type checking cache (auto-regenerate)
- `htmlcov/` - Coverage reports (regenerate with tests)
- `logs/` - Application logs

**⚠️ IF YOU VIOLATE THIS POLICY**: You may delete irreplaceable customer data, development context, or hours of generated analysis results.

### 🐍 Dependency Management Policy
**CRITICAL**: This is a **conda-only project**.

**✅ DO:**
- Add dependencies to `environment.yml`
- Use `conda env update -f environment.yml` to install new packages
- Use the `pip:` section within `environment.yml` for pip-only packages

**❌ NEVER:**
- Create `requirements.txt`, `requirements-dev.txt`, or `requirements-*.txt` files
- Use `pip install` directly (outside of conda environment)
- Mix conda and pip package management approaches

**Rationale**: Conda provides better dependency resolution and environment isolation than mixing package managers. This avoids version conflicts and ensures reproducible environments across all development and deployment scenarios.

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
├── setup-and-run.sh       # Main setup script (commits)
├── README.md               # Main documentation (commits)
└── .env                    # Credentials (gitignored, in root)
```

### 📄 Generated Files Policy
**CRITICAL**: Some files are generated from source data and should NOT be committed:

**✅ GENERATE (don't commit):**
- `flight_test_scenarios.json` - Generated from actual loaded flight data
- `data/*.parquet` - Downloaded BTS flight data files
- `logs/*.log` - Runtime log files
- Neo4j server reports

**❌ NEVER commit generated files because:**
- They change based on source data
- They're environment-specific
- They're large and change frequently
- They can be regenerated from scripts

**🔧 Regeneration Commands:**
```bash
# Regenerate flight scenarios from loaded data
python generate_flight_scenarios.py

# Regenerate flight data from BTS
python download_bts_flight_data.py --year 2024 --month 3
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
**CRITICAL**: Schema (constraints and indexes) are automatically managed by the Python loading scripts:

1. **Automatic Schema Management**:
   - `load_bts_data.py` creates all necessary constraints and indexes
   - Schema is defined in Python code for consistency
   - No need for separate .cypher files or manual schema creation

2. **Use Neo4j Parallel Spark Loader** (prevents deadlocks):
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

**🔥 ABSOLUTE REQUIREMENT**: Before committing ANY new code, you MUST run the complete CI.yml test battery to ensure it will pass GitHub Actions.

### 📋 Complete Pre-Commit Checklist

**1. Run Complete CI Test Suite (MANDATORY)**
```bash
# Run the EXACT same tests that CI.yml runs
pytest tests/test_ci_unit.py tests/test_flight_search_unit.py tests/test_download_bts_unit.py tests/test_load_bts_unit.py tests/test_system_validation_unit.py tests/test_data_transformations.py tests/test_business_rules.py tests/test_error_scenarios.py -v --cov=. --cov-report=xml --cov-report=term-missing

# ALL tests must pass - no exceptions!
```

**2. Run ALL CI.yml Quality Checks (MANDATORY)**
```bash
# 1. Black formatting check (must pass)
black --check --diff .
# If it fails, run: black .

# 2. Isort import sorting check (must pass)
isort --check-only --diff .
# If it fails, run: isort .

# 3. Flake8 critical linting (must pass)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# 4. Flake8 secondary check (warnings OK, exit-zero)
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=88 --statistics

# 5. MyPy type checking (continue-on-error, but run it)
mypy --install-types --non-interactive .

# 6. Bandit security check (continue-on-error, but run it)
bandit -r . -x tests/ -ll

# 7. Safety dependency check (continue-on-error, but run it)
safety check
```

**3. Additional Tests for Major Changes**
```bash
# Connection logic tests (critical for temporal validation)
python -m pytest tests/test_connection_logic.py -v

# Performance baseline tests
python -m pytest tests/test_performance_baseline.py -v

# Integration tests (require loaded database)
python -m pytest tests/test_integration_heavy.py -v

# Full test suite (comprehensive verification)
python -m pytest tests/ -v
```

**4. Pre-Commit Hooks (After Manual Checks)**
```bash
# Run all pre-commit hooks
pre-commit run --all-files

# NEVER use --no-verify unless it's an emergency
# If checks fail, FIX the issues, don't bypass them
```

### 🚨 CI.yml Compliance Status Check

**BEFORE COMMITTING**, verify your changes pass CI by running this quick command:
```bash
echo "🔍 CI.yml Compliance Check" && \
pytest tests/test_ci_unit.py tests/test_flight_search_unit.py tests/test_download_bts_unit.py tests/test_load_bts_unit.py tests/test_system_validation_unit.py tests/test_data_transformations.py tests/test_business_rules.py tests/test_error_scenarios.py --quiet && \
black --check . && \
isort --check-only . && \
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics && \
echo "✅ CI.yml WILL PASS - Safe to commit!"
```

If ANY step fails, you MUST fix it before committing. The CI will fail otherwise.

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

## 🔐 Database Credentials & Environment Variables

**🚨 ABSOLUTE SECURITY REQUIREMENT**: NEVER EVER hard-code database credentials, IP addresses, passwords, or connection strings in source code - ANYWHERE in the project. This is a security vulnerability and will be rejected in code review.

### ✅ ALWAYS DO
```python
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Correct: Use environment variables
uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")
database = os.getenv("NEO4J_DATABASE", "neo4j")

# Validate required variables
if not all([uri, username, password]):
    raise ValueError("Missing required Neo4j environment variables")
```

### ❌ NEVER DO - ALL OF THESE ARE SECURITY VIOLATIONS
```python
# WRONG: Hard-coded credentials - MAJOR security risk!
uri = "bolt://10.0.1.27:7687"           # ❌ Hard-coded IP + port
username = "neo4j"                      # ❌ Hard-coded username
password = "secretpassword"             # ❌ Hard-coded password
password = "V1ctoria"                   # ❌ ANY hard-coded password
database = "flights"                    # ❌ Hard-coded database name

# ALSO WRONG: Connection strings with embedded credentials
uri = "bolt://user:pass@server:7687"    # ❌ Credentials in URI
DATABASE_URL = "neo4j://host:7687"      # ❌ Hard-coded connection URL

# WRONG: Even IP addresses without credentials
SERVER_IP = "10.0.1.27"                # ❌ Hard-coded IP address
NEO4J_HOST = "production.company.com"   # ❌ Hard-coded hostname
```

**⚠️ REMEMBER**: If you can see credentials/IPs/hostnames in the source code, so can anyone with access to the repository!

### 📋 Required Environment Variables
Every script connecting to Neo4j MUST use these variables from `.env`:
- `NEO4J_URI` - Database connection URI (e.g. bolt://localhost:7687)
- `NEO4J_USERNAME` - Database username (usually "neo4j")
- `NEO4J_PASSWORD` - Database password (never commit this!)
- `NEO4J_DATABASE` - Database name (default: "neo4j" - compatible with Aura)

### 🛠️ Setup Template
```bash
# Copy example and edit with real values
cp .env.example .env
# Edit .env with your actual credentials (never commit .env!)
```

### 📝 MANDATORY Code Review Checklist
**🔒 SECURITY CHECK** - Before committing ANY code that connects to databases/services:

- [ ] **NO hard-coded credentials**: Search file for any IP addresses, passwords, usernames, URIs
- [ ] **Uses `python-dotenv`**: `from dotenv import load_dotenv` and `load_dotenv()` called
- [ ] **Uses `os.getenv()`**: ALL connection parameters use environment variables
- [ ] **Validates environment variables**: Checks that required variables exist
- [ ] **Proper error handling**: Clear error messages if .env file misconfigured
- [ ] **No credentials in comments**: Check comments don't contain sensitive info
- [ ] **No debug prints**: Remove any debug statements that might log credentials

**⚠️ SECURITY SCAN**: Run this command before committing:
```bash
grep -rn "bolt://.*:" --include="*.py" .
grep -rn "password.*=" --include="*.py" . | grep -v "os.getenv"
```

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
