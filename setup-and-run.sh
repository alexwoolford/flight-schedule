#!/bin/bash

# 🚀 Neo4j Flight Data Pipeline - Complete End-to-End Setup
#
# This script sets up everything from scratch:
# 1. Conda environment creation
# 2. Flight data download (real BTS data)
# 3. Data loading into your Neo4j instance
# 4. Load testing framework setup
#
# Usage:
#   ./setup-and-run.sh
#
# Prerequisites:
#   - Conda installed
#   - Neo4j instance accessible (Aura, self-hosted, etc.)
#   - 16GB+ RAM recommended
#   - ~10GB disk space for data

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/setup_${TIMESTAMP}.log"

# Create logs directory
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"
}

log_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}🚀 $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "SECTION: $1"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
    log "SUCCESS: $1"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
    log "WARNING: $1"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
    log "ERROR: $1"
}

# Error handler
error_exit() {
    log_error "Setup failed on line $1"
    echo ""
    echo -e "${RED}Setup failed! Check the log file for details:${NC}"
    echo -e "${YELLOW}  tail -f $LOG_FILE${NC}"
    exit 1
}

trap 'error_exit $LINENO' ERR

# Welcome message
echo ""
echo -e "${GREEN}🎯 Neo4j Flight Data Pipeline - Complete Setup${NC}"
echo -e "${GREEN}===============================================${NC}"
echo ""
echo "This script will set up everything you need:"
echo "  • Conda environment with all dependencies"
echo "  • Download real BTS flight data (586K+ records)"
echo "  • Load data into your Neo4j instance"
echo "  • Production-ready load testing framework"
echo ""
echo -e "${YELLOW}Prerequisites:${NC}"
echo "  • Conda installed"
echo "  • Neo4j instance accessible (credentials required)"
echo "  • 16GB+ RAM recommended"
echo "  • ~10GB disk space for flight data"
echo ""
echo -e "${BLUE}Logs will be saved to: $LOG_FILE${NC}"
echo ""

# Check prerequisites
log_section "Checking Prerequisites"

# Check conda
if ! command -v conda &> /dev/null; then
    log_error "Conda not found. Please install Conda first:"
    echo "  https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi
log_success "Conda found: $(conda --version)"

# Check available disk space (need ~10GB)
AVAILABLE_SPACE=$(df . | tail -1 | awk '{print $4}')
if [ "$AVAILABLE_SPACE" -lt 10485760 ]; then  # 10GB in KB
    log_warning "Low disk space detected. You need ~10GB for flight data."
    echo "Continue anyway? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi
log_success "Sufficient disk space available"

# Get Neo4j credentials
log_section "Neo4j Configuration"

ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ]; then
    log_success "Found existing .env file"
    echo ""
    echo "Current Neo4j configuration:"
    grep "^NEO4J_" "$ENV_FILE" | sed 's/NEO4J_PASSWORD=.*/NEO4J_PASSWORD=***/' || true
    echo ""
    echo "Use existing configuration? (Y/n)"
    read -r use_existing
    if [[ "$use_existing" =~ ^[Nn]$ ]]; then
        CREATE_NEW_ENV=true
    else
        CREATE_NEW_ENV=false
    fi
else
    CREATE_NEW_ENV=true
fi

if [ "$CREATE_NEW_ENV" = true ]; then
    echo ""
    echo "Please provide your Neo4j connection details:"
    echo ""

    # Neo4j URI
    echo -n "Neo4j URI (e.g., bolt://localhost:7687 or neo4j+s://your-aura-instance.neo4j.io): "
    read -r NEO4J_URI

    # Username
    echo -n "Username (default: neo4j): "
    read -r NEO4J_USERNAME
    NEO4J_USERNAME=${NEO4J_USERNAME:-neo4j}

    # Password
    echo -n "Password: "
    read -rs NEO4J_PASSWORD
    echo ""

    # Database name
    echo -n "Database name (default: neo4j for Aura, flights for self-hosted): "
    read -r NEO4J_DATABASE
    if [[ "$NEO4J_URI" == *"neo4j.io"* ]]; then
        NEO4J_DATABASE=${NEO4J_DATABASE:-neo4j}  # Aura default
    else
        NEO4J_DATABASE=${NEO4J_DATABASE:-flights}  # Self-hosted default
    fi

    # Create .env file
    cat > "$ENV_FILE" << EOF
# Neo4j Connection Settings
NEO4J_URI=$NEO4J_URI
NEO4J_USERNAME=$NEO4J_USERNAME
NEO4J_PASSWORD=$NEO4J_PASSWORD
NEO4J_DATABASE=$NEO4J_DATABASE
EOF

    log_success "Created .env file with Neo4j credentials"
fi

# Test Neo4j connection
log_section "Testing Neo4j Connection"

# Load environment variables
source "$ENV_FILE"

# Simple connection test using Python
cat > test_connection.py << 'EOF'
import sys
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

try:
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    )

    with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
        result = session.run("RETURN 'Connection successful!' as message")
        record = result.single()
        print(f"✅ {record['message']}")

        # Get database info
        result = session.run("CALL db.info()")
        info = result.single()
        print(f"✅ Connected to: {info['name']} (Neo4j {info['version']})")

    driver.close()
    sys.exit(0)

except Exception as e:
    print(f"❌ Connection failed: {e}")
    sys.exit(1)
EOF

# Try connection test (will install basic deps if needed)
if python test_connection.py 2>> "$LOG_FILE"; then
    log_success "Neo4j connection successful"
else
    log_error "Neo4j connection failed. Please check your credentials and try again."
    rm -f test_connection.py
    exit 1
fi

rm -f test_connection.py

# Setup Conda Environment
log_section "Setting Up Conda Environment"

CONDA_ENV_NAME="neo4j"

# Check if environment exists
if conda env list | grep -q "^$CONDA_ENV_NAME "; then
    log_success "Conda environment '$CONDA_ENV_NAME' already exists"
    echo "Recreate environment? (y/N)"
    read -r recreate
    if [[ "$recreate" =~ ^[Yy]$ ]]; then
        log "Removing existing environment..."
        conda env remove -n "$CONDA_ENV_NAME" -y >> "$LOG_FILE" 2>&1
        CREATE_ENV=true
    else
        CREATE_ENV=false
    fi
else
    CREATE_ENV=true
fi

if [ "$CREATE_ENV" = true ]; then
    log "Creating conda environment from environment.yml..."
    conda env create -f environment.yml >> "$LOG_FILE" 2>&1
    log_success "Conda environment created"
fi

# Activate environment for rest of script
log "Activating conda environment..."
eval "$(conda shell.bash hook)"
conda activate "$CONDA_ENV_NAME" >> "$LOG_FILE" 2>&1
log_success "Environment activated: $(conda env list | grep '*' | awk '{print $1}')"

# Download Flight Data
log_section "Downloading BTS Flight Data"

DATA_DIR="$SCRIPT_DIR/data/bts_flight_data"
if [ -d "$DATA_DIR" ] && [ "$(ls -A $DATA_DIR 2>/dev/null | wc -l)" -gt 0 ]; then
    log_success "Flight data already exists in $DATA_DIR"
    echo "Download fresh data? (y/N)"
    read -r download_fresh
    if [[ "$download_fresh" =~ ^[Yy]$ ]]; then
        DOWNLOAD_DATA=true
    else
        DOWNLOAD_DATA=false
    fi
else
    DOWNLOAD_DATA=true
fi

if [ "$DOWNLOAD_DATA" = true ]; then
    log "Downloading real BTS flight data (this may take 10-15 minutes)..."
    python download_bts_flight_data.py >> "$LOG_FILE" 2>&1
    log_success "Flight data downloaded to $DATA_DIR"

    # Show what was downloaded
    echo ""
    echo "Downloaded files:"
    ls -lh "$DATA_DIR"/*.parquet 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
    echo ""
fi

# Load Data into Neo4j
log_section "Loading Data into Neo4j"

# Check if data already exists
log "Checking if data is already loaded..."
cat > check_data.py << 'EOF'
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

try:
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    )

    with driver.session(database=os.getenv("NEO4J_DATABASE")) as session:
        # Count schedules
        result = session.run("MATCH (s:Schedule) RETURN count(s) as count")
        schedule_count = result.single()["count"]

        # Count airports
        result = session.run("MATCH (a:Airport) RETURN count(a) as count")
        airport_count = result.single()["count"]

        print(f"{schedule_count},{airport_count}")

    driver.close()

except Exception as e:
    print("0,0")
EOF

DATA_COUNTS=$(python check_data.py 2>/dev/null)
SCHEDULE_COUNT=$(echo "$DATA_COUNTS" | cut -d',' -f1)
AIRPORT_COUNT=$(echo "$DATA_COUNTS" | cut -d',' -f2)

rm -f check_data.py

if [ "$SCHEDULE_COUNT" -gt 100000 ]; then
    log_success "Data already loaded: $SCHEDULE_COUNT schedules, $AIRPORT_COUNT airports"
    echo "Reload data? (y/N)"
    read -r reload_data
    if [[ "$reload_data" =~ ^[Yy]$ ]]; then
        LOAD_DATA=true
    else
        LOAD_DATA=false
    fi
else
    LOAD_DATA=true
fi

if [ "$LOAD_DATA" = true ]; then
    log "Loading flight data into Neo4j (this may take 5-10 minutes)..."

    # Use March 2024 data (586K+ records) for optimal performance
    MARCH_FILE="$DATA_DIR/bts_flights_2024_03.parquet"
    if [ -f "$MARCH_FILE" ]; then
        python load_bts_data.py --single-file bts_flights_2024_03.parquet --data-path data/bts_flight_data >> "$LOG_FILE" 2>&1
        log_success "Flight data loaded successfully"
    else
        log_error "March 2024 data file not found. Please check the download step."
        exit 1
    fi

    # Verify load
    FINAL_COUNTS=$(python check_data.py 2>/dev/null)
    FINAL_SCHEDULE_COUNT=$(echo "$FINAL_COUNTS" | cut -d',' -f1)
    FINAL_AIRPORT_COUNT=$(echo "$FINAL_COUNTS" | cut -d',' -f2)

    echo ""
    echo "Data loading results:"
    echo "  • Schedules: $FINAL_SCHEDULE_COUNT"
    echo "  • Airports: $FINAL_AIRPORT_COUNT"
    echo ""

    if [ "$FINAL_SCHEDULE_COUNT" -lt 100000 ]; then
        log_error "Data loading appears incomplete. Check logs for errors."
        exit 1
    fi
fi

# Generate Flight Scenarios for Load Testing
log_section "Setting Up Load Testing Framework"

log "Generating flight scenarios from actual data..."
python generate_flight_scenarios.py >> "$LOG_FILE" 2>&1
log_success "Flight scenarios generated"

# Test the load testing framework
log "Testing load testing framework..."
python -c "
import neo4j_flight_load_test
print('✅ Load test framework imports successfully')

# Quick validation
user = neo4j_flight_load_test.Neo4jUser()
user.on_start()
print(f'✅ Connected to Neo4j with {len(user.airports)} airports loaded')
user.on_stop()
print('✅ Load test framework ready')
" >> "$LOG_FILE" 2>&1

log_success "Load testing framework validated"

# Run basic tests
log_section "Running System Validation"

log "Running core unit tests..."
python -m pytest tests/test_ci_unit.py tests/test_flight_search_unit.py -v >> "$LOG_FILE" 2>&1
log_success "Core tests passed"

log "Running load testing framework tests..."
python -m pytest tests/test_load_testing_framework.py -v >> "$LOG_FILE" 2>&1
log_success "Load testing framework tests passed"

# Success summary
log_section "Setup Complete!"

echo ""
echo -e "${GREEN}🎉 SUCCESS! Your Neo4j flight data pipeline is ready!${NC}"
echo ""
echo -e "${BLUE}📊 System Summary:${NC}"
echo "  • Environment: conda env '$CONDA_ENV_NAME' activated"
echo "  • Database: $FINAL_SCHEDULE_COUNT flight schedules, $FINAL_AIRPORT_COUNT airports"
echo "  • Data source: Real BTS government flight data (March 2024)"
echo "  • Load testing: Production-ready Locust framework"
echo ""
echo -e "${BLUE}🚀 Next Steps:${NC}"
echo ""
echo -e "${YELLOW}1. Start Load Testing:${NC}"
echo "   locust -f neo4j_flight_load_test.py"
echo "   # Then visit: http://localhost:8089"
echo ""
echo -e "${YELLOW}2. Run Sample Queries:${NC}"
echo "   # Neo4j Browser: http://localhost:7474 (if local)"
echo "   # Or use your Neo4j Aura/hosted instance interface"
echo ""
echo -e "${YELLOW}3. Explore the Data:${NC}"
echo "   python -c \"from neo4j import GraphDatabase; # Use your .env credentials\""
echo ""
echo -e "${YELLOW}4. Run Integration Tests:${NC}"
echo "   pytest tests/test_integration_heavy.py -v"
echo ""
echo -e "${BLUE}📋 Important Files:${NC}"
echo "  • Configuration: .env"
echo "  • Flight data: data/bts_flight_data/"
echo "  • Load test: neo4j_flight_load_test.py"
echo "  • Documentation: README.md"
echo "  • This log: $LOG_FILE"
echo ""

log_success "End-to-end setup completed successfully in $(date)"

echo -e "${GREEN}Happy querying! 🛫✈️🛬${NC}"
echo ""
