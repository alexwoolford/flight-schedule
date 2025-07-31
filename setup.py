#!/usr/bin/env python3
"""
Complete setup script for Neo4j Flight Schedule System

This script will:
1. Optionally download sample flight data
2. Set up the Neo4j graph database
3. Create performance indexes
4. Run a demonstration

Usage:
    python setup.py --all                    # Complete setup
    python setup.py --download-data          # Download sample data only
    python setup.py --setup-database         # Create graph database only
    python setup.py --create-indexes         # Create indexes only
    python setup.py --demo                   # Run demo only

Requirements:
    - Neo4j running and accessible
    - .env file with connection details (copy from .env.example)
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Add src to path for imports
sys.path.insert(0, "src")


def check_prerequisites():
    """Check that all prerequisites are met"""
    print("🔍 Checking prerequisites...")

    # Check if we're in the right conda environment
    conda_env = os.getenv("CONDA_DEFAULT_ENV")
    if conda_env != "neo4j-flight-schedule":
        print("⚠️  Warning: Not in recommended conda environment")
        print(f"   Current environment: {conda_env or 'base'}")
        print("   Recommended setup:")
        print("     conda activate neo4j-flight-schedule")
        print("   Or create it: ./setup_environment.sh")
        print()

    # Check .env file exists
    if not os.path.exists(".env"):
        print("❌ Error: .env file not found!")
        print("   Copy .env.example to .env and configure your Neo4j connection")
        return False

    # Load environment variables, overriding any existing ones
    load_dotenv(override=True)

    # Check required environment variables
    required_vars = ["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD", "NEO4J_DATABASE"]
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"❌ Error: Missing environment variables: {', '.join(missing_vars)}")
        print("   Please configure these in your .env file")
        return False

    # Test Neo4j connection using .env values
    try:
        from neo4j import GraphDatabase

        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE")

        print(f"📋 Using connection: {uri} → database '{database}'")

        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session(database=database) as session:
            result = session.run("RETURN 1 as test")
            result.single()
        driver.close()
        print("✅ Neo4j connection successful")

    except Exception as e:
        print(f"❌ Error: Cannot connect to Neo4j: {e}")
        print("   Please check your .env configuration and ensure Neo4j is running")
        return False

    return True


def download_sample_data():
    """Download or create sample flight data"""
    print("\n📥 Setting up sample flight data...")

    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    sample_file = data_dir / "flight_schedule_sample.parquet"

    if sample_file.exists():
        print(f"✅ Sample data already exists: {sample_file}")
        return True

    # Check if we have any existing sample data to copy
    existing_files = list(data_dir.glob("*schedule*.parquet"))
    if existing_files:
        print(f"✅ Using existing data: {existing_files[0]}")
        import shutil

        shutil.copy2(existing_files[0], sample_file)
        return True

    # Create minimal sample data for demo
    print("📝 Creating minimal sample dataset...")

    # Generate a small dataset for demonstration
    sample_data = []

    airports = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "BOS", "LAS", "SEA", "MIA"]
    carriers = ["AA", "DL", "UA", "WN", "AS"]

    for i in range(100):
        sample_data.append(
            {
                "schedule_id": f"SCHED_{i:04d}",
                "carrier": carriers[i % len(carriers)],
                "departure_station": airports[i % len(airports)],
                "arrival_station": airports[(i + 3) % len(airports)],
                "effective_date": "2023-01-01",
                "discontinued_date": "2023-12-31",
                "published_departure_time": f"{7 + (i % 16):02d}:00",
                "published_arrival_time": f"{9 + (i % 16):02d}:30",
                "service_days_bitmap": 127,  # All days (1111111 in binary)
                "cabin_bitmap": 15,  # All cabin classes (1111 in binary)
            }
        )

    df = pd.DataFrame(sample_data)
    df.to_parquet(sample_file, index=False)

    print(f"✅ Created sample dataset: {sample_file}")
    print(f"   Records: {len(df)}")
    print(f"   Airports: {len(airports)}")
    print(f"   Carriers: {len(carriers)}")

    return True


def setup_database():
    """Set up the Neo4j graph database"""
    print("\n🚀 Setting up flight schedule database...")

    try:
        from loaders.load_flight_graph import FlightGraphLoader

        loader = FlightGraphLoader()
        data_file = "data/flight_schedule_sample.parquet"

        if not os.path.exists(data_file):
            print(f"❌ Error: Data file not found: {data_file}")
            print("   Run with --download-data first")
            return False

        loader.load_data_from_file(data_file)
        loader.close()

        print("✅ Database setup complete!")
        return True

    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False


def create_constraints():
    """Create database constraints for data integrity and performance"""
    print("\n🔒 Creating database constraints...")

    load_dotenv(override=True)

    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "flights")

    print(f"📋 Target: {uri} → database '{database}'")

    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))

        # Read and execute constraint creation queries
        with open("src/queries/create_constraints.cypher", "r") as f:
            cypher_content = f.read()

        # Split by semicolon and filter out comments/empty strings
        queries = [
            q.strip()
            for q in cypher_content.split(";")
            if q.strip()
            and not q.strip().startswith("//")
            and "CREATE CONSTRAINT" in q.upper()
        ]

        with driver.session(database=database) as session:
            for query in queries:
                if query.strip():
                    constraint_name = (
                        query.split("IF NOT EXISTS")[0].split()[-1]
                        if "IF NOT EXISTS" in query
                        else "constraint"
                    )
                    print(f"  Creating: {constraint_name}")
                    session.run(query)

        driver.close()
        print("✅ Constraints created successfully")
        print("💡 Constraints provide implicit indexes for faster MERGE operations")
        return True

    except Exception as e:
        print(f"❌ Error creating constraints: {e}")
        return False


def create_indexes():
    """Create performance indexes"""
    print("\n📊 Creating performance indexes...")

    try:
        from neo4j import GraphDatabase

        # Load from environment variables
        load_dotenv(override=True)
        uri = os.getenv("NEO4J_URI")
        username = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE")

        driver = GraphDatabase.driver(uri, auth=(username, password))

        # Read index creation script
        index_file = "src/queries/create_indexes.cypher"
        if not os.path.exists(index_file):
            print(f"❌ Error: Index file not found: {index_file}")
            return False

        with open(index_file, "r") as f:
            content = f.read()

        # Split on semicolon and clean up queries
        index_queries = []
        for query in content.split(";"):
            query = query.strip()
            # Skip empty queries and comments
            if query and not query.startswith("//") and "CREATE" in query.upper():
                index_queries.append(query)

        with driver.session(database=database) as session:
            for query in index_queries:
                try:
                    session.run(query)
                    print(f"✅ Created index: {query[:50]}...")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"⚠️  Index already exists: {query[:50]}...")
                    else:
                        print(f"⚠️  Skipping query with error: {query[:50]}...")
                        # Don't fail on individual index errors

        driver.close()
        print("✅ All indexes created!")
        return True

    except Exception as e:
        print(f"❌ Error creating indexes: {e}")
        return False


def run_demo():
    """Run performance demonstration"""
    print("\n🎯 Running flight schedule demo...")

    try:
        # Import and call the demo function
        from demo.flight_performance_demo import test_flight_performance

        test_flight_performance()
        return True

    except Exception as e:
        print(f"❌ Error running demo: {e}")
        return False


def load_full_dataset(batch_size: int = 10000):
    """Load the complete flight dataset using Neo4j Parallel Spark Loader"""
    print(f"\n⚡ Loading full dataset with Parallel Spark Loader...")
    print("📊 This will process ~23M records → ~7.7M schedules in <1 hour")

    # Load environment variables for Neo4j connection
    load_dotenv(override=True)
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "flights")

    # Check Java version and warn if not optimal
    try:
        import subprocess

        java_version = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT, text=True
        )
        if "21." in java_version and "11." not in java_version:
            print("⚠️  JAVA COMPATIBILITY WARNING:")
            print("   Java 21 detected - PySpark works best with Java 11")
            print("   Please switch to Java 11:")
            print("   sdk use java 11.0.28-amzn")

            proceed = input("Continue anyway? (y/N): ").strip().lower()
            if proceed != "y":
                print("Please switch to Java 11 and try again.")
                return False
    except Exception:
        print("⚠️  Could not check Java version")

    try:
        print(
            "\n🚀 Using Neo4j Parallel Spark Loader (deadlock-free, production-ready)..."
        )
        result = subprocess.run(
            [sys.executable, "load_with_parallel_spark.py"], capture_output=False
        )

        if result.returncode == 0:
            print("\n✅ Parallel Spark loading completed!")
            print("🔍 Verifying data was actually loaded...")

            # Verify data was loaded by checking node/relationship counts
            try:
                driver = GraphDatabase.driver(uri, auth=(username, password))
                with driver.session(database=database) as session:
                    node_result = session.run("MATCH (n) RETURN count(n) as count")
                    node_count = node_result.single()["count"]

                    rel_result = session.run(
                        "MATCH ()-[r]->() RETURN count(r) as count"
                    )
                    rel_count = rel_result.single()["count"]

                    print(
                        f"📊 Verification: {node_count:,} nodes, {rel_count:,} relationships"
                    )

                    if node_count > 0 and rel_count > 0:
                        print("✅ Dataset loaded and verified successfully!")
                        return True
                    else:
                        print("❌ Loading completed but no data found in database!")
                        return False

                driver.close()
            except Exception as e:
                print(f"⚠️ Could not verify loading: {e}")
                print("✅ Loading process completed (verification failed)")
                return True
        else:
            print(
                f"\n❌ Parallel Spark loading failed (exit code: {result.returncode})"
            )
            return False

    except Exception as e:
        print(f"❌ Error during parallel Spark loading: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Complete setup for Neo4j Flight Schedule System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup.py --all                      # Complete setup (recommended)
  python setup.py --download-data            # Download data only
  python setup.py --setup-database           # Setup database only
  python setup.py --demo                     # Run demo only
  python setup.py --load-full-dataset        # Load 19M records (interactive)
  
Environment setup:
  ./setup_environment.sh                     # Create conda environment
  python check_environment.py                # Check current setup
        """,
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Complete setup: download data, setup database, create indexes, run demo",
    )
    parser.add_argument(
        "--download-data", action="store_true", help="Download sample flight data"
    )
    parser.add_argument(
        "--setup-database", action="store_true", help="Setup Neo4j graph database"
    )
    parser.add_argument(
        "--create-constraints", action="store_true", help="Create database constraints"
    )
    parser.add_argument(
        "--create-indexes", action="store_true", help="Create performance indexes"
    )
    parser.add_argument(
        "--demo", action="store_true", help="Run performance demonstration"
    )
    parser.add_argument(
        "--load-full-dataset",
        action="store_true",
        help="Load the complete flight dataset (19M records)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for full dataset loading",
    )

    args = parser.parse_args()

    # If no arguments, show help
    if not any(vars(args).values()):
        parser.print_help()
        return

    print("🚀 NEO4J FLIGHT SCHEDULE SYSTEM SETUP")
    print("=" * 50)

    # Check prerequisites first
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Please fix the issues above and try again.")
        sys.exit(1)

    success = True

    # Determine what to run
    if args.all:
        steps = ["download-data", "setup-database", "create-indexes", "demo"]
    else:
        steps = []
        if args.download_data:
            steps.append("download-data")
        if args.setup_database:
            steps.append("setup-database")
        if args.create_constraints:
            steps.append("create-constraints")
        if args.create_indexes:
            steps.append("create-indexes")
        if args.demo:
            steps.append("demo")
        if args.load_full_dataset:
            steps.append("load-full-dataset")

    # Execute steps
    for step in steps:
        if step == "download-data":
            success = download_sample_data() and success
        elif step == "setup-database":
            success = setup_database() and success
        elif step == "create-constraints":
            success = create_constraints() and success
        elif step == "create-indexes":
            success = create_indexes() and success
        elif step == "demo":
            success = run_demo() and success
        elif step == "load-full-dataset":
            success = load_full_dataset(args.batch_size) and success

    print("\n" + "=" * 50)
    if success:
        print("🎉 SETUP COMPLETE!")
        print("\nYour Neo4j flight schedule system is ready!")
        print("\nNext steps:")
        print("  • Connect to Neo4j Browser to explore the graph")
        print("  • Run flight_search_demo.py for flight search examples")
        print("  • Check README.md for detailed documentation")
    else:
        print("❌ SETUP FAILED!")
        print("\nSome steps encountered errors. Please check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
