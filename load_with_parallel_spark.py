#!/usr/bin/env python3
"""
Neo4j Parallel Spark Loader - Deadlock-free loading for large datasets
Uses neo4j-parallel-spark-loader to avoid locking issues during parallel writes
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None

try:
    from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe
    
    PARALLEL_LOADER_AVAILABLE = True
except ImportError:
    PARALLEL_LOADER_AVAILABLE = False


def create_spark_session(app_name: str = "ParallelFlightLoader"):
    """Create Spark session optimized for parallel Neo4j loading"""
    if not SPARK_AVAILABLE:
        raise ImportError("PySpark not available. Install with: pip install pyspark")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.jars.packages", "org.neo4j:neo4j-connector-apache-spark_2.12:4.1.5_for_spark_3")
        .getOrCreate()
    )

    return spark


def load_with_parallel_spark(data_path="data/flight_list", batch_size=50000, single_file=None):
    """Load flight data using Neo4j Parallel Spark Loader to avoid deadlocks
    
    Args:
        data_path: Path to parquet files directory
        batch_size: Batch size for processing  
        single_file: Optional - load only this single file for testing
    """

    if not SPARK_AVAILABLE:
        print("❌ PySpark not available!")
        print("   Install with: conda install pyspark=3.5")
        return

    if not PARALLEL_LOADER_AVAILABLE:
        print("❌ Neo4j Parallel Spark Loader not available!")
        print("   Install with: pip install neo4j-parallel-spark-loader")
        return

    # Load environment variables
    load_dotenv(override=True)
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USERNAME", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    neo4j_database = os.getenv("NEO4J_DATABASE", "flights")

    print("🚀 PARALLEL SPARK LOADER - DEADLOCK-FREE LOADING")
    print("=" * 55)

    # First, ensure constraints exist for optimal performance
    print("🔒 Creating constraints first (required for fast node creation)...")
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        constraints = [
            "CREATE CONSTRAINT schedule_id_unique IF NOT EXISTS FOR (s:Schedule) REQUIRE s.schedule_id IS UNIQUE",
            "CREATE CONSTRAINT airport_code_unique IF NOT EXISTS FOR (a:Airport) REQUIRE a.code IS UNIQUE", 
            "CREATE CONSTRAINT carrier_code_unique IF NOT EXISTS FOR (c:Carrier) REQUIRE c.code IS UNIQUE"
        ]
        
        with driver.session(database=neo4j_database) as session:
            for constraint in constraints:
                constraint_name = constraint.split()[2]
                try:
                    session.run(constraint)
                    print(f"   ✅ {constraint_name}")
                except Exception as e:
                    if "already exists" in str(e) or "equivalent" in str(e):
                        print(f"   ✅ {constraint_name} (already exists)")
                    else:
                        print(f"   ⚠️  {constraint_name}: {e}")
        
        driver.close()
        print("✅ Constraints ready - will speed up node creation by 3-5x")
        
    except Exception as e:
        print(f"⚠️  Could not create constraints: {e}")
        print("Proceeding without constraints (will be slower)")

    # Create Spark session
    print("📊 Creating Spark session...")
    try:
        spark = create_spark_session()
        print(f"✅ Spark {spark.version} session created")
    except Exception as e:
        print(f"❌ Error creating Spark session: {e}")
        return

    # Configure Neo4j connection for the parallel loader
    spark.conf.set("neo4j.url", neo4j_uri)
    spark.conf.set("neo4j.authentication.basic.username", neo4j_user)
    spark.conf.set("neo4j.authentication.basic.password", neo4j_password)
    spark.conf.set("neo4j.database", neo4j_database)

    try:
        # Handle single file vs all files loading
        if single_file:
            print(f"🧪 SINGLE FILE MODE: Loading only {single_file}")
            single_file_path = Path(f"data/flight_list/{single_file}")
            if not single_file_path.exists():
                print(f"❌ Single file not found: {single_file_path}")
                return
            parquet_files = [single_file_path]
        else:
            # Load all files
            data_dir = Path("data/flight_list")
            parquet_files = list(data_dir.glob("*.parquet"))

            if not parquet_files:
                print("❌ No parquet files found in data/flight_list/")
                print("   Using sample data instead...")
                data_file = "data/flight_schedule_sample.parquet"
                if not os.path.exists(data_file):
                    print("❌ Sample data also not found!")
                    return
                parquet_files = [Path(data_file)]

        print(f"📁 Found {len(parquet_files)} data file(s)")

        # Read and transform data
        print("📖 Reading flight data...")
        import time

        start_time = time.time()

        if len(parquet_files) == 1:
            df = spark.read.parquet(str(parquet_files[0]))
        else:
            df = spark.read.parquet(str(data_dir / "*.parquet"))

        total_records = df.count()
        read_time = time.time() - start_time

        print(f"✅ Read {total_records:,} records in {read_time:.1f}s")

        # Transform flight data for relationship loading
        print("🔄 Transforming data for relationship loading...")

        # Create schedule DataFrame with temporal properties preserved as ISO strings
        # Keep datetime objects as native types - Neo4j connector handles DateTime conversion
        flight_df = df.select(
            col("id").alias("schedule_id"),
            col("icao_operator").alias("carrier_code"),
            col("adep").alias("departure_airport"),
            col("ades").alias("arrival_airport"),
            
            # Keep as native datetime objects - no conversion to strings!
            col("dof").alias("date_of_operation"),          # Native datetime → Neo4j DateTime
            col("first_seen").alias("first_seen_time"),     # Native datetime → Neo4j DateTime  
            col("last_seen").alias("last_seen_time"),       # Native datetime → Neo4j DateTime
            col("unix_time").alias("timestamp"),            # Keep as integer (unix seconds)
            
            col("flt_id").alias("flight_id"),
            col("registration").alias("aircraft_registration"),
            col("typecode").alias("aircraft_type")
        ).filter(
            col("icao_operator").isNotNull()
            & col("adep").isNotNull()
            & col("ades").isNotNull()
            & col("dof").isNotNull()
        )

        processed_count = flight_df.count()
        print(f"✅ Processed {processed_count:,} valid flight records")

        if processed_count == 0:
            print("❌ No valid records to process")
            return

        # First, create all nodes that relationships will reference
        print("\n📦 Creating nodes first (required for relationships)...")
        
        # 1. Create Schedule nodes with temporal properties
        print("   Creating Schedule nodes with temporal properties...")
        schedule_nodes_df = flight_df.select(
            "schedule_id",
            "date_of_operation", 
            "first_seen_time",
            "last_seen_time", 
            "timestamp",
            "flight_id",
            "aircraft_registration",
            "aircraft_type"
        ).distinct()
        
        schedule_nodes_df.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("labels", ":Schedule") \
            .option("node.keys", "schedule_id") \
            .mode("append") \
            .save()
        print(f"   ✅ Created Schedule nodes with temporal properties")
        
        # 2. Create Airport nodes  
        print("   Creating Airport nodes...")
        departure_airports = flight_df.select(col("departure_airport").alias("code")).distinct()
        arrival_airports = flight_df.select(col("arrival_airport").alias("code")).distinct()
        all_airports = departure_airports.union(arrival_airports).distinct()
        
        all_airports.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("labels", ":Airport") \
            .option("node.keys", "code") \
            .mode("append") \
            .save()
        print(f"   ✅ Created Airport nodes")
        
        # 3. Create Carrier nodes
        print("   Creating Carrier nodes...")
        carrier_nodes_df = flight_df.select(col("carrier_code").alias("code")).distinct()
        carrier_nodes_df.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("labels", ":Carrier") \
            .option("node.keys", "code") \
            .mode("append") \
            .save()
        print(f"   ✅ Created Carrier nodes")

        # Now create relationships using parallel loader to avoid deadlocks
        print("\n🔗 Creating relationships with deadlock-free parallel processing...")

        # 1. DEPARTS_FROM relationships (Schedule -> Airport) - Bipartite scenario
        print("   Processing DEPARTS_FROM relationships...")
        departure_df = flight_df.select("schedule_id", "departure_airport").distinct()
        
        # Group and batch the DataFrame to avoid deadlocks
        grouped_departure_df = group_and_batch_spark_dataframe(
            departure_df,
            source_col="schedule_id",
            target_col="departure_airport", 
            num_groups=10
        )
        
        # Write DEPARTS_FROM relationships in batches to avoid deadlocks
        print("     Creating Schedule->Airport DEPARTS_FROM relationships...")
        grouped_departure_df.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("relationship", "DEPARTS_FROM") \
            .option("relationship.source.labels", ":Schedule") \
            .option("relationship.source.node.keys", "schedule_id") \
            .option("relationship.target.labels", ":Airport") \
            .option("relationship.target.node.keys", "departure_airport:code") \
            .option("relationship.save.strategy", "keys") \
            .mode("append") \
            .save()
        print("     ✅ DEPARTS_FROM relationships created")

        # 2. ARRIVES_AT relationships (Schedule -> Airport) - Bipartite scenario  
        print("   Processing ARRIVES_AT relationships...")
        arrival_df = flight_df.select("schedule_id", "arrival_airport").distinct()
        
        grouped_arrival_df = group_and_batch_spark_dataframe(
            arrival_df,
            source_col="schedule_id",
            target_col="arrival_airport",
            num_groups=10
        )
        
        print("     Creating Schedule->Airport ARRIVES_AT relationships...")
        grouped_arrival_df.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("relationship", "ARRIVES_AT") \
            .option("relationship.source.labels", ":Schedule") \
            .option("relationship.source.node.keys", "schedule_id") \
            .option("relationship.target.labels", ":Airport") \
            .option("relationship.target.node.keys", "arrival_airport:code") \
            .option("relationship.save.strategy", "keys") \
            .mode("append") \
            .save()
        print("     ✅ ARRIVES_AT relationships created")

        # 3. OPERATED_BY relationships (Schedule -> Carrier) - Bipartite scenario
        print("   Processing OPERATED_BY relationships...")
        carrier_df = flight_df.select("schedule_id", "carrier_code").distinct()
        
        grouped_carrier_df = group_and_batch_spark_dataframe(
            carrier_df,
            source_col="schedule_id", 
            target_col="carrier_code",
            num_groups=10
        )
        
        print("     Creating Schedule->Carrier OPERATED_BY relationships...")
        grouped_carrier_df.write.format("org.neo4j.spark.DataSource") \
            .option("url", neo4j_uri) \
            .option("authentication.basic.username", neo4j_user) \
            .option("authentication.basic.password", neo4j_password) \
            .option("database", neo4j_database) \
            .option("relationship", "OPERATED_BY") \
            .option("relationship.source.labels", ":Schedule") \
            .option("relationship.source.node.keys", "schedule_id") \
            .option("relationship.target.labels", ":Carrier") \
            .option("relationship.target.node.keys", "carrier_code:code") \
            .option("relationship.save.strategy", "keys") \
            .mode("append") \
            .save()
        print("     ✅ OPERATED_BY relationships created")

        total_time = time.time() - start_time

        print(f"\n🎉 SUCCESS!")
        print(f"   Total records: {total_records:,}")
        print(f"   Processed records: {processed_count:,}")
        print(f"   Total time: {total_time:.1f}s")
        print(f"   Rate: {processed_count/total_time:,.0f} records/second")
        print(f"   ✅ No deadlocks - parallel loading completed successfully!")

    except Exception as e:
        print(f"❌ Error during parallel loading: {e}")
        import traceback

        traceback.print_exc()
    finally:
        print("🔄 Stopping Spark session...")
        spark.stop()


def test_parallel_loader_requirements():
    """Test if parallel loader requirements are met"""
    print("🧪 TESTING PARALLEL LOADER REQUIREMENTS")
    print("=" * 45)

    issues = []

    # Test PySpark
    if SPARK_AVAILABLE:
        print("✅ PySpark available")
    else:
        print("❌ PySpark not available")
        issues.append("Install PySpark: conda install pyspark=3.5")

    # Test parallel loader
    if PARALLEL_LOADER_AVAILABLE:
        print("✅ Neo4j Parallel Spark Loader available")
    else:
        print("❌ Neo4j Parallel Spark Loader not available")
        issues.append("Install parallel loader: pip install neo4j-parallel-spark-loader")

    # Test Neo4j configuration
    load_dotenv(override=True)
    neo4j_uri = os.getenv("NEO4J_URI")
    if neo4j_uri:
        print(f"✅ Neo4j URI configured: {neo4j_uri}")
    else:
        print("❌ Neo4j URI not configured")
        issues.append("Configure .env file with Neo4j connection details")

    # Test data availability
    if os.path.exists("data/flight_schedule_sample.parquet"):
        print("✅ Sample data available")
    else:
        print("❌ Sample data not found")
        issues.append("Run: python setup.py --download-data")

    if issues:
        print(f"\n❌ {len(issues)} issues found:")
        for issue in issues:
            print(f"   • {issue}")
        return False
    else:
        print(f"\n🎉 All requirements met - ready for parallel loading!")
        return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Neo4j Parallel Spark Loader - Deadlock-free loading"
    )
    parser.add_argument(
        "--check-requirements",
        action="store_true",
        help="Check if all requirements are met",
    )
    parser.add_argument(
        "--single-file",
        help="Load single parquet file for testing (e.g., flight_list_202403.parquet)",
    )

    args = parser.parse_args()

    if args.check_requirements:
        test_parallel_loader_requirements()
    else:
        load_with_parallel_spark(single_file=args.single_file)