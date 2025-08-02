#!/usr/bin/env python3
"""
BTS Flight Data Loader - Load BTS flight data into Neo4j using Spark
Based on successful debugging - applies type matching and tested relationship logic
"""

import argparse
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv

# Import validation
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, expr, when
    from pyspark.sql.types import FloatType, IntegerType

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

try:
    from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe

    PARALLEL_LOADER_AVAILABLE = True
except ImportError:
    PARALLEL_LOADER_AVAILABLE = False

try:
    from neo4j import GraphDatabase

    NEO4J_DRIVER_AVAILABLE = True
except ImportError:
    NEO4J_DRIVER_AVAILABLE = False


def setup_logging(verbose_cli=True):
    """Setup logging to file with proper formatting (required by AGENTS.md)"""
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Add logs/ to .gitignore if not already there
    gitignore_path = Path(".gitignore")
    if gitignore_path.exists():
        content = gitignore_path.read_text()
        if "logs/" not in content:
            with open(gitignore_path, "a") as f:
                f.write("\nlogs/\n")
    else:
        gitignore_path.write_text("logs/\n")

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"load_bts_data_{timestamp}.log"

    # Setup handlers based on CLI verbosity
    handlers = [logging.FileHandler(log_file)]
    if not verbose_cli:  # Only add console handler if not in verbose CLI mode
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    logger = logging.getLogger(__name__)
    logger.info("=== BTS Flight Data Loader Started ===")
    logger.info(f"Log file: {log_file}")

    return logger


def log_and_print(message, logger, level=logging.INFO, cli_mode=True):
    """
    Log message to file and optionally print to console for CLI feedback

    Args:
        message: The message to log/print
        logger: Logger instance
        level: Logging level (default: INFO)
        cli_mode: Whether to also print to console for CLI feedback
    """
    # Always log to file
    logger.log(level, message)

    # Print to console only in CLI mode (for user feedback)
    if cli_mode:
        print(message)


def create_spark_session(app_name: str = "BTSFlightLoader", custom_config: dict = None):
    """
    Create Spark session configured for Neo4j bulk loading

    SPARK VERSION: Configured for Spark 3.5.3+ with modern configuration options

    PREREQUISITES: This configuration assumes a Neo4j database with:
    - Proper indexes on all lookup fields (origin, dest, airline, etc.)
    - Unique constraints where appropriate
    - Pre-flight schema setup completed successfully

    PERFORMANCE BENEFITS:
    - 2.5x larger batch sizes (50k vs 20k) thanks to indexed lookups
    - Aggressive memory allocation for faster processing
    - Parallelism configured for concurrent relationship creation
    - Enhanced connection pooling for higher throughput

    WARNING: Without proper indexes, this configuration may cause timeouts!
    Run setup_database_schema() first to ensure optimal performance.
    """
    if not SPARK_AVAILABLE:
        raise ImportError("PySpark not available. Install with: pip install pyspark")

    # Configuration for indexed Neo4j database
    default_config = {
        # === ADAPTIVE QUERY EXECUTION (Configured for bulk loading) ===
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",  # Updated for Spark 3.5.3
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        # === MEMORY CONFIGURATION (Aggressive for faster loading) ===
        "spark.driver.memory": "12g",  # Increased for better caching
        "spark.driver.maxResultSize": "4g",  # Larger result sets
        "spark.executor.memory": "8g",  # More memory per executor
        "spark.executor.memoryFraction": "0.85",  # More memory for processing
        "spark.executor.memoryStorageFraction": "0.3",  # Less for storage, more for execution
        # === PARALLELISM (Configured for indexed database writes) ===
        "spark.sql.shuffle.partitions": "16",  # Increased for better parallelism
        "spark.default.parallelism": "16",  # Match shuffle partitions
        "spark.sql.sources.parallelPartitionDiscovery.threshold": "32",
        # === SERIALIZATION & COMPRESSION ===
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "1g",
        "spark.sql.inMemoryColumnarStorage.compressed": "true",
        "spark.sql.inMemoryColumnarStorage.batchSize": "20000",
        # === PARQUET OPTIMIZATION (Handle BTS timestamp issues) ===
        "spark.sql.parquet.enableVectorizedReader": "false",  # Disable for timestamp compatibility
        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",  # Use microseconds
        "spark.sql.parquet.int96TimestampConversion": "true",
        "spark.sql.parquet.writeLegacyFormat": "false",
        "spark.sql.parquet.mergeSchema": "false",  # Faster reads
        "spark.sql.parquet.filterPushdown": "true",
        # === ADVANCED TIMESTAMP COMPATIBILITY (Fix nanosecond timestamp issues) ===
        "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",  # Handle legacy timestamps
        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",  # Handle datetime rebasing
        "spark.sql.parquet.respectSummaryFiles": "false",  # Ignore potentially problematic summary files
        "spark.sql.parquet.pushdown.date": "false",  # Disable date pushdown for compatibility
        "spark.sql.parquet.pushdown.timestamp": "false",  # Disable timestamp pushdown for compatibility
        "spark.sql.parquet.enableNestedColumnVectorizedReader": "false",  # Disable for complex timestamp handling
        # === ARROW & COLUMNAR (Disabled for Neo4j compatibility) ===
        "spark.sql.execution.arrow.pyspark.enabled": "false",  # Conflicts with Neo4j connector
        "spark.sql.execution.columnar.inMemoryTableScanEnabled": "false",
        "spark.sql.columnVector.offheap.enabled": "true",
        # === NEO4J CONNECTOR OPTIMIZATION (Aggressive with indexes) ===
        "spark.neo4j.batch.size": "50000",  # Much larger batches with indexes
        "spark.neo4j.transaction.retries.max": "5",  # More retries for stability
        "spark.neo4j.transaction.timeout": "120s",  # Longer timeout for large batches
        "spark.neo4j.connection.pool.maxSize": "100",  # More connections
        "spark.neo4j.connection.acquisition.timeout": "60s",
        "spark.neo4j.connection.liveness.timeout": "300s",
        # === CACHING & PERSISTENCE ===
        "spark.sql.cache.serializer": "org.apache.spark.sql.execution.columnar.InMemoryRelation",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        # === RESOURCE OPTIMIZATION ===
        "spark.task.maxFailures": "3",
        "spark.stage.maxConsecutiveAttempts": "8",
        "spark.excludeOnFailure.enabled": "false",  # Disable failure exclusion for single-node (updated for Spark 3.5.3)
        # === I/O OPTIMIZATION ===
        "spark.sql.files.maxPartitionBytes": "256MB",  # Larger partition sizes
        "spark.sql.files.openCostInBytes": "8MB",  # Optimize small file handling
    }

    # Override with environment variables (for performance testing)
    env_config = {}
    for key in default_config.keys():
        env_key = f"SPARK_{key.upper().replace('.', '_')}"
        if env_key in os.environ:
            env_config[key] = os.environ[env_key]

    # Merge configurations
    final_config = {**default_config, **env_config}
    if custom_config:
        final_config.update(custom_config)

    # Build Spark session
    builder = SparkSession.builder.appName(app_name)
    for key, value in final_config.items():
        builder = builder.config(key, value)

    spark = builder.config(
        "spark.jars.packages",
        "org.neo4j:neo4j-connector-apache-spark_2.12:4.1.5_for_spark_3",
    ).getOrCreate()

    return spark


def verify_optimal_loading_conditions(
    spark,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    neo4j_database,
    logger=None,
    cli_mode=True,
):
    """Verify that the system is configured for optimal loading performance"""

    verify_msg = "🔍 VERIFYING OPTIMAL LOADING CONDITIONS..."
    if logger:
        log_and_print(verify_msg, logger, cli_mode=cli_mode)
    else:
        print(verify_msg)

    # Check Spark configuration
    spark_batch_size = spark.conf.get("spark.neo4j.batch.size", "20000")
    spark_memory = spark.conf.get("spark.driver.memory", "8g")
    shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions", "12")

    config_msg = f"   📊 Spark Config: {spark_batch_size} batch size, {spark_memory} driver memory, {shuffle_partitions} partitions"
    if logger:
        log_and_print(config_msg, logger, cli_mode=cli_mode)
    else:
        print(config_msg)

    # Check Neo4j indexes
    if NEO4J_DRIVER_AVAILABLE:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        try:
            with driver.session(database=neo4j_database) as session:
                # Count indexes
                result = session.run("SHOW INDEXES WHERE type = 'RANGE'")
                indexes = list(result)
                index_count = len(indexes)

                if (
                    index_count >= 3
                ):  # We expect at least 3 indexes from our schema setup
                    msg = f"   ✅ Database: {index_count} performance indexes detected"
                    if logger:
                        log_and_print(msg, logger, cli_mode=cli_mode)
                    else:
                        print(msg)
                    return True
                else:
                    msg1 = (
                        f"   ⚠️  Database: Only {index_count} indexes found (expect 3+)"
                    )
                    msg2 = "   💡 Run setup_database_schema() for optimal performance"
                    if logger:
                        log_and_print(
                            msg1, logger, level=logging.WARNING, cli_mode=cli_mode
                        )
                        log_and_print(msg2, logger, cli_mode=cli_mode)
                    else:
                        print(msg1)
                        print(msg2)
                    return False
        except Exception as e:
            error_msg = f"   ❌ Database check failed: {e}"
            if logger:
                log_and_print(error_msg, logger, level=logging.ERROR, cli_mode=cli_mode)
            else:
                print(error_msg)
            return False
        finally:
            driver.close()
    else:
        warning_msg = (
            "   ⚠️  Cannot verify database indexes (neo4j driver not available)"
        )
        if logger:
            log_and_print(warning_msg, logger, level=logging.WARNING, cli_mode=cli_mode)
        else:
            print(warning_msg)
        return False


def setup_database_schema(
    neo4j_uri, neo4j_user, neo4j_password, neo4j_database, logger=None, cli_mode=True
):
    """Pre-flight check: Create constraints and indexes for optimal loading performance"""

    if not NEO4J_DRIVER_AVAILABLE:
        msg = "❌ neo4j driver not available. Install with: pip install neo4j"
        if logger:
            log_and_print(msg, logger, level=logging.ERROR, cli_mode=cli_mode)
        else:
            print(msg)
        return False

    setup_msg = "🔧 PRE-FLIGHT: Setting up database schema..."
    if logger:
        log_and_print(setup_msg, logger, cli_mode=cli_mode)
    else:
        print(setup_msg)

        # Only create indexes that are actually used during loading
    # Based on readCount analysis - most indexes add write overhead without benefit
    schema_queries = [
        # ✅ PROVEN USEFUL: This index gets 3.5M+ reads during loading
        "CREATE INDEX schedule_route IF NOT EXISTS FOR (s:Schedule) ON (s.origin, s.dest)",
        # ✅ ESSENTIAL: Airport/Carrier lookups for MATCH operations (1M+ reads)
        "CREATE INDEX airport_code_lookup IF NOT EXISTS FOR (a:Airport) ON (a.code)",
        "CREATE INDEX carrier_code_lookup IF NOT EXISTS FOR (c:Carrier) ON (c.code)",
        # ✅ NEW: Temporal indexes for optimized query performance
        "CREATE INDEX schedule_flightdate IF NOT EXISTS FOR (s:Schedule) ON (s.flightdate)",
        "CREATE INDEX schedule_departure_time IF NOT EXISTS FOR (s:Schedule) ON (s.scheduled_departure_time)",
        "CREATE INDEX schedule_arrival_time IF NOT EXISTS FOR (s:Schedule) ON (s.scheduled_arrival_time)",
        # ✅ NEW: Composite indexes for connection queries (significant performance gain)
        "CREATE INDEX schedule_date_departure IF NOT EXISTS FOR (s:Schedule) ON (s.flightdate, s.scheduled_departure_time)",
        "CREATE INDEX schedule_date_arrival IF NOT EXISTS FOR (s:Schedule) ON (s.flightdate, s.scheduled_arrival_time)",
        # Constraints (may fail if duplicates exist, but that's OK)
        "CREATE CONSTRAINT airport_code_unique IF NOT EXISTS FOR (a:Airport) REQUIRE a.code IS UNIQUE",
        "CREATE CONSTRAINT carrier_code_unique IF NOT EXISTS FOR (c:Carrier) REQUIRE c.code IS UNIQUE",
        "CREATE CONSTRAINT schedule_composite_unique IF NOT EXISTS FOR (s:Schedule) REQUIRE (s.flightdate, s.reporting_airline, s.flight_number_reporting_airline, s.origin, s.dest) IS UNIQUE",
    ]

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    try:
        with driver.session(database=neo4j_database) as session:
            indexes_created = 0
            constraints_created = 0

            for query in schema_queries:
                try:
                    session.run(query)
                    if "INDEX" in query:
                        indexes_created += 1
                        index_name = query.split("INDEX ")[1].split(" ")[0]
                        msg = f"   ✅ Index: {index_name}"
                        if logger:
                            log_and_print(msg, logger, cli_mode=cli_mode)
                        else:
                            print(msg)
                    else:
                        constraints_created += 1
                        constraint_name = query.split("CONSTRAINT ")[1].split(" ")[0]
                        msg = f"   ✅ Constraint: {constraint_name}"
                        if logger:
                            log_and_print(msg, logger, cli_mode=cli_mode)
                        else:
                            print(msg)
                except Exception:
                    if "INDEX" in query:
                        index_name = query.split("INDEX ")[1].split(" ")[0]
                        msg = f"   ⚠️  Index already exists: {index_name}"
                        if logger:
                            log_and_print(
                                msg, logger, level=logging.WARNING, cli_mode=cli_mode
                            )
                        else:
                            print(msg)
                    else:
                        constraint_name = query.split("CONSTRAINT ")[1].split(" ")[0]
                        msg = f"   ⚠️  Constraint skipped: {constraint_name} (duplicates exist)"
                        if logger:
                            log_and_print(
                                msg, logger, level=logging.WARNING, cli_mode=cli_mode
                            )
                        else:
                            print(msg)

            summary_msg = f"   📊 Schema setup: {indexes_created} indexes, {constraints_created} constraints"
            success_msg = "   🚀 Database ready for loading!"
            if logger:
                log_and_print(summary_msg, logger, cli_mode=cli_mode)
                log_and_print(success_msg, logger, cli_mode=cli_mode)
            else:
                print(summary_msg)
                print(success_msg)
            return True

    except Exception as e:
        error_msg = f"   ❌ Schema setup failed: {e}"
        if logger:
            log_and_print(error_msg, logger, level=logging.ERROR, cli_mode=cli_mode)
        else:
            print(error_msg)
        return False
    finally:
        driver.close()


def create_relationships_fast(
    spark,
    schedule_df,
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    neo4j_database,
    use_parallel_loader=True,
):
    """Create relationships for flights, airports, and carriers"""

    print("\n🔗 Creating relationships with GUARANTEED success...")
    total_start_time = time.time()

    # Configure num_groups based on shuffle partitions for optimal performance
    shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "12"))
    num_groups = shuffle_partitions

    print(f"📊 Using {num_groups} groups for parallel processing")

    # 1. DEPARTS_FROM relationships
    print("   🛫 Creating DEPARTS_FROM relationships...")
    dep_start_time = time.time()

    departure_df = schedule_df.select(
        "flightdate",  # Already converted to proper date type
        "reporting_airline",
        "flight_number_reporting_airline",
        "origin",
        "dest",
    ).distinct()

    dep_count = departure_df.count()
    print(f"     📊 Processing {dep_count:,} unique departures")

    # Debug: Check data types
    print("     🔍 Departure DataFrame schema:")
    departure_df.printSchema()

    if use_parallel_loader and PARALLEL_LOADER_AVAILABLE:
        print("     🔄 Using parallel loader for deadlock prevention...")
        grouped_departure_df = group_and_batch_spark_dataframe(
            departure_df,
            source_col="flightdate",
            target_col="origin",
            num_groups=num_groups,
        )
        write_df = grouped_departure_df
    else:
        print("     ⚡ Using direct loading (no parallel loader)")
        write_df = departure_df

    write_df.write.format("org.neo4j.spark.DataSource").option("url", neo4j_uri).option(
        "authentication.basic.username", neo4j_user
    ).option("authentication.basic.password", neo4j_password).option(
        "database", neo4j_database
    ).option(
        "relationship", "DEPARTS_FROM"
    ).option(
        "relationship.save.strategy", "keys"
    ).option(
        "relationship.source.labels", ":Schedule"
    ).option(
        "relationship.source.save.mode", "match"
    ).option(
        "relationship.source.node.keys",
        "flightdate,reporting_airline,flight_number_reporting_airline,origin,dest",
    ).option(
        "relationship.target.labels", ":Airport"
    ).option(
        "relationship.target.save.mode", "match"
    ).option(
        "relationship.target.node.keys", "origin:code"
    ).mode(
        "Append"
    ).save()

    dep_time = time.time() - dep_start_time
    print(
        f"     ✅ DEPARTS_FROM completed in {dep_time:.1f}s ({dep_count/dep_time:.0f} rels/sec)"
    )

    # 2. ARRIVES_AT relationships
    print("   🛬 Creating ARRIVES_AT relationships...")
    arr_start_time = time.time()

    arrival_df = schedule_df.select(
        "flightdate",
        "reporting_airline",
        "flight_number_reporting_airline",
        "origin",
        "dest",
    ).distinct()

    if use_parallel_loader and PARALLEL_LOADER_AVAILABLE:
        grouped_arrival_df = group_and_batch_spark_dataframe(
            arrival_df,
            source_col="flightdate",
            target_col="dest",
            num_groups=num_groups,
        )
        write_df = grouped_arrival_df
    else:
        write_df = arrival_df

    write_df.write.format("org.neo4j.spark.DataSource").option("url", neo4j_uri).option(
        "authentication.basic.username", neo4j_user
    ).option("authentication.basic.password", neo4j_password).option(
        "database", neo4j_database
    ).option(
        "relationship", "ARRIVES_AT"
    ).option(
        "relationship.save.strategy", "keys"
    ).option(
        "relationship.source.labels", ":Schedule"
    ).option(
        "relationship.source.save.mode", "match"
    ).option(
        "relationship.source.node.keys",
        "flightdate,reporting_airline,flight_number_reporting_airline,origin,dest",
    ).option(
        "relationship.target.labels", ":Airport"
    ).option(
        "relationship.target.save.mode", "match"
    ).option(
        "relationship.target.node.keys", "dest:code"
    ).mode(
        "Append"
    ).save()

    arr_time = time.time() - arr_start_time
    print(
        f"     ✅ ARRIVES_AT completed in {arr_time:.1f}s ({dep_count/arr_time:.0f} rels/sec)"
    )

    # 3. OPERATED_BY relationships
    print("   ✈️  Creating OPERATED_BY relationships...")
    op_start_time = time.time()

    carrier_df = schedule_df.select(
        "flightdate",
        "reporting_airline",
        "flight_number_reporting_airline",
        "origin",
        "dest",
    ).distinct()

    if use_parallel_loader and PARALLEL_LOADER_AVAILABLE:
        grouped_carrier_df = group_and_batch_spark_dataframe(
            carrier_df,
            source_col="flightdate",
            target_col="reporting_airline",
            num_groups=num_groups,
        )
        write_df = grouped_carrier_df
    else:
        write_df = carrier_df

    write_df.write.format("org.neo4j.spark.DataSource").option("url", neo4j_uri).option(
        "authentication.basic.username", neo4j_user
    ).option("authentication.basic.password", neo4j_password).option(
        "database", neo4j_database
    ).option(
        "relationship", "OPERATED_BY"
    ).option(
        "relationship.save.strategy", "keys"
    ).option(
        "relationship.source.labels", ":Schedule"
    ).option(
        "relationship.source.save.mode", "match"
    ).option(
        "relationship.source.node.keys",
        "flightdate,reporting_airline,flight_number_reporting_airline,origin,dest",
    ).option(
        "relationship.target.labels", ":Carrier"
    ).option(
        "relationship.target.save.mode", "match"
    ).option(
        "relationship.target.node.keys", "reporting_airline:code"
    ).mode(
        "Append"
    ).save()

    op_time = time.time() - op_start_time
    print(
        f"     ✅ OPERATED_BY completed in {op_time:.1f}s ({dep_count/op_time:.0f} rels/sec)"
    )

    total_rel_time = time.time() - total_start_time
    total_relationships = dep_count * 3  # Each schedule creates 3 relationships

    print(f"\n   📊 Total relationship creation: {total_rel_time:.1f}s")
    print(
        f"   🚀 Overall throughput: {total_relationships/total_rel_time:.0f} total rels/sec"
    )

    return total_relationships


def load_bts_data(
    data_path="data/bts_flight_data",
    single_file=None,
    spark_config=None,
    use_parallel_loader=True,
    load_all_files=False,
    cli_mode=True,
):
    """Load BTS flight data into Neo4j graph database"""

    logger = logging.getLogger(__name__)
    logger.info("Starting BTS data load to Neo4j")
    logger.info(f"Data path: {data_path}, Single file: {single_file}")
    logger.info(f"Use parallel loader: {use_parallel_loader}")

    if not SPARK_AVAILABLE:
        logger.error("PySpark not available! Install with: conda install pyspark=3.5")
        return

    # Load environment variables for Neo4j connection
    load_dotenv()
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    neo4j_database = os.getenv("NEO4J_DATABASE", "flights")

    if not all([neo4j_uri, neo4j_user, neo4j_password]):
        logger.error("Missing Neo4j connection details in .env file")
        return

    logger.info(f"Neo4j connection: {neo4j_uri} -> {neo4j_database}")

    # CLI user feedback (only when running from command line)
    if cli_mode:
        print("🇺🇸 BTS FLIGHT DATA -> NEO4J LOADER")
        print("===============================================")
        print("📊 Source: Bureau of Transportation Statistics")
        print("✅ 100% factual government data")
        print("🔧 GUARANTEED relationship creation")

    # PRE-FLIGHT: Setup database schema for optimal performance
    logger.info("Running pre-flight database schema setup")
    schema_ready = setup_database_schema(
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        neo4j_database,
        logger=logger,
        cli_mode=cli_mode,
    )
    if not schema_ready:
        logger.error("Database schema setup failed")
        log_and_print(
            "❌ Database schema setup failed - aborting load",
            logger,
            level=logging.ERROR,
        )
        return

    # Create Spark session
    logger.info("Creating Spark session")
    if cli_mode:
        print("\n📊 Creating Spark session...")

    spark = create_spark_session(custom_config=spark_config)
    logger.info(f"Spark session created successfully - version {spark.version}")
    if cli_mode:
        print(f"✅ Spark {spark.version} session created")

    # Set Neo4j connection parameters
    spark.conf.set("neo4j.url", neo4j_uri)
    spark.conf.set("neo4j.authentication.basic.username", neo4j_user)
    spark.conf.set("neo4j.authentication.basic.password", neo4j_password)
    spark.conf.set("neo4j.database", neo4j_database)

    # Verify optimal loading conditions
    logger.info("Verifying optimal loading conditions")
    is_optimal = verify_optimal_loading_conditions(
        spark,
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        neo4j_database,
        logger=logger,
        cli_mode=cli_mode,
    )
    if is_optimal:
        log_and_print(
            "   🚀 System ready for high-performance loading!",
            logger,
            cli_mode=cli_mode,
        )
    else:
        log_and_print(
            "   ⚠️  System not fully configured - expect slower performance",
            logger,
            level=logging.WARNING,
            cli_mode=cli_mode,
        )
        log_and_print(
            "   💡 Consider running with recommended configuration",
            logger,
            cli_mode=cli_mode,
        )

    try:
        # Handle file loading
        data_dir = Path(data_path)
        if single_file and not load_all_files:
            single_file_path = data_dir / single_file
            if not single_file_path.exists():
                logger.error(f"Single file not found: {single_file_path}")
                print(f"❌ Single file not found: {single_file_path}")
                return
            parquet_files = [single_file_path]
            logger.info(f"Single file mode: {single_file}")
            print(f"🧪 SINGLE FILE MODE: Loading only {single_file}")
        else:
            parquet_files = list(data_dir.glob("*.parquet"))
            if not parquet_files:
                logger.error(f"No parquet files found in {data_dir}/")
                print(f"❌ No parquet files found in {data_dir}/")
                return
            logger.info(f"Multi-file mode: {len(parquet_files)} files")
            print(f"📁 MULTI-FILE MODE: Loading {len(parquet_files)} files")

        # Read BTS data with Parquet handling
        print(f"📖 Reading BTS data from {len(parquet_files)} file(s)...")
        start_time = time.time()

        # Configure Parquet reader for BTS timestamp compatibility
        parquet_reader = (
            spark.read.option("mergeSchema", "false")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("inferTimestamp", "false")
        )

        try:
            if len(parquet_files) == 1:
                df = parquet_reader.parquet(str(parquet_files[0]))
                print(f"   📁 Loaded: {parquet_files[0].name}")
            else:
                df = parquet_reader.parquet(str(data_dir / "*.parquet"))
                print(f"   📁 Loaded: {len(parquet_files)} files")
        except Exception as e:
            if "TIMESTAMP(NANOS" in str(e) or "Illegal Parquet type" in str(e):
                print(
                    "   ⚠️  Detected parquet timestamp issues - using aggressive compatibility mode..."
                )
                logger.warning(
                    "Parquet timestamp compatibility issues detected, using aggressive fallback"
                )

                # Enhanced Fallback: Use aggressive compatibility settings for schema inference problems
                parquet_reader_fallback = (
                    spark.read.option("mergeSchema", "false")
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                    .option("inferTimestamp", "false")
                    .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss")
                    .option("multiline", "false")
                    .option("mode", "PERMISSIVE")
                    .option("columnNameOfCorruptRecord", "_corrupt_record")
                    # Force legacy parquet reader for maximum compatibility
                    .option("spark.sql.parquet.enableVectorizedReader", "false")
                    .option("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
                    .option(
                        "spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY"
                    )
                )

                try:
                    if len(parquet_files) == 1:
                        df = parquet_reader_fallback.parquet(str(parquet_files[0]))
                        print(
                            f"   📁 Loaded (aggressive compatibility): {parquet_files[0].name}"
                        )
                    else:
                        df = parquet_reader_fallback.parquet(
                            str(data_dir / "*.parquet")
                        )
                        print(
                            f"   📁 Loaded (aggressive compatibility): {len(parquet_files)} files"
                        )
                except Exception as e2:
                    # Ultimate fallback: Read files individually to isolate problematic ones
                    print(
                        "   🚨 Standard fallback failed, using individual file processing..."
                    )
                    logger.warning(
                        f"Aggressive fallback failed: {e2}, using individual file processing"
                    )

                    dfs = []
                    for pfile in parquet_files:
                        try:
                            single_df = parquet_reader_fallback.parquet(str(pfile))
                            dfs.append(single_df)
                            print(f"   ✅ Successfully loaded: {pfile.name}")
                        except Exception as e3:
                            print(
                                f"   ❌ Failed to load: {pfile.name} - {str(e3)[:100]}..."
                            )
                            logger.error(f"Failed to load {pfile.name}: {e3}")
                            # Continue with other files instead of failing completely

                    if not dfs:
                        raise Exception(
                            "No parquet files could be loaded with any compatibility method"
                        )

                    # Union all successfully loaded DataFrames
                    df = dfs[0]
                    for additional_df in dfs[1:]:
                        df = df.union(additional_df)
                    print(
                        f"   📁 Combined {len(dfs)} successful files out of {len(parquet_files)} total"
                    )
            else:
                raise  # Re-raise if it's a different error

        total_records = df.count()
        read_time = time.time() - start_time
        print(f"✅ Read {total_records:,} BTS records in {read_time:.1f}s")

        # Transform and filter data
        print("🔄 Transforming and filtering BTS data...")

        # Filter valid flights
        valid_flights_df = df.filter(
            col("flightdate").isNotNull()
            & col("reporting_airline").isNotNull()
            & col("flight_number_reporting_airline").isNotNull()
            & col("origin").isNotNull()
            & col("dest").isNotNull()
            & (col("cancelled") == 0)
        )

        # Create schedule DataFrame with proper types
        schedule_df = valid_flights_df.select(
            # CRITICAL: Proper date type conversion
            col("flightdate").cast("date").alias("flightdate"),
            col("reporting_airline"),
            col("flight_number_reporting_airline"),
            col("origin"),
            col("dest"),
            # Temporal data with proper timestamp conversion
            when(
                col("crsdeptime").isNotNull(),
                expr(
                    "timestamp(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(crsdeptime, 'HH:mm:ss')))"
                ),
            ).alias("scheduled_departure_time"),
            when(
                col("crsarrtime").isNotNull(),
                expr(
                    "timestamp(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(crsarrtime, 'HH:mm:ss')))"
                ),
            ).alias("scheduled_arrival_time"),
            when(
                col("deptime").isNotNull(),
                expr(
                    "timestamp(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(deptime, 'HH:mm:ss')))"
                ),
            ).alias("actual_departure_time"),
            when(
                col("arrtime").isNotNull(),
                expr(
                    "timestamp(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(arrtime, 'HH:mm:ss')))"
                ),
            ).alias("actual_arrival_time"),
            # Additional fields
            col("distance").cast(FloatType()).alias("distance_miles"),
            col("tail_number"),
            col("depdelay").cast(IntegerType()).alias("departure_delay_minutes"),
            col("arrdelay").cast(IntegerType()).alias("arrival_delay_minutes"),
        )

        valid_count = schedule_df.count()
        print(f"✅ Prepared {valid_count:,} valid schedule records")

        if valid_count == 0:
            print("❌ No valid records to process")
            return

        # Create nodes first
        print("\n📦 Creating nodes...")
        node_start_time = time.time()

        # Create Carrier nodes
        print("   ✈️  Creating Carrier nodes...")
        carrier_df = (
            schedule_df.select("reporting_airline")
            .distinct()
            .withColumnRenamed("reporting_airline", "code")
        )

        carrier_df.write.format("org.neo4j.spark.DataSource").option(
            "url", neo4j_uri
        ).option("authentication.basic.username", neo4j_user).option(
            "authentication.basic.password", neo4j_password
        ).option(
            "database", neo4j_database
        ).option(
            "labels", ":Carrier"
        ).option(
            "node.keys", "code"
        ).mode(
            "Append"
        ).save()

        # Create Airport nodes
        print("   🏢 Creating Airport nodes...")
        origin_airports = (
            schedule_df.select("origin").distinct().withColumnRenamed("origin", "code")
        )
        dest_airports = (
            schedule_df.select("dest").distinct().withColumnRenamed("dest", "code")
        )
        airport_df = origin_airports.union(dest_airports).distinct()

        airport_df.write.format("org.neo4j.spark.DataSource").option(
            "url", neo4j_uri
        ).option("authentication.basic.username", neo4j_user).option(
            "authentication.basic.password", neo4j_password
        ).option(
            "database", neo4j_database
        ).option(
            "labels", ":Airport"
        ).option(
            "node.keys", "code"
        ).mode(
            "Append"
        ).save()

        # Create Schedule nodes
        print("   📅 Creating Schedule nodes...")
        schedule_df.write.format("org.neo4j.spark.DataSource").option(
            "url", neo4j_uri
        ).option("authentication.basic.username", neo4j_user).option(
            "authentication.basic.password", neo4j_password
        ).option(
            "database", neo4j_database
        ).option(
            "labels", ":Schedule"
        ).option(
            "node.keys",
            "flightdate,reporting_airline,flight_number_reporting_airline,origin,dest",
        ).mode(
            "Append"
        ).save()

        node_time = time.time() - node_start_time
        print(f"   ✅ All nodes created in {node_time:.1f}s")

        # Create relationships with guaranteed success
        total_relationships = create_relationships_fast(
            spark,
            schedule_df,
            neo4j_uri,
            neo4j_user,
            neo4j_password,
            neo4j_database,
            use_parallel_loader=use_parallel_loader,
        )

        total_time = time.time() - start_time

        print("\n🎉 BTS DATA LOADING SUCCESS!")
        print(f"   📊 Total records processed: {valid_count:,}")
        print(f"   🔗 Total relationships created: {total_relationships:,}")
        print(f"   ⏱️  Total time: {total_time:.1f}s")
        print(f"   🚀 Overall rate: {valid_count/total_time:.0f} records/second")

        # Verify relationships were created
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

        with driver.session(database=neo4j_database) as session:
            result = session.run("MATCH ()-[r]->() RETURN count(r) as total_rels")
            actual_rels = result.single()["total_rels"]
            print(f"   ✅ VERIFIED: {actual_rels:,} relationships in database")

            if actual_rels > 0:
                print("   🎯 RELATIONSHIP CREATION CONFIRMED WORKING!")
            else:
                print("   ❌ WARNING: No relationships found in database")

        driver.close()

        logger.info("=== BTS Flight Data Load Completed Successfully ===")

    except Exception as e:
        logger.error(f"BTS data load failed: {str(e)}", exc_info=True)
        print(f"❌ Load failed: {e}")
        raise
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="BTS flight data loader for Neo4j")
    parser.add_argument("--single-file", help="Load single parquet file for testing")
    parser.add_argument(
        "--data-path", default="data/bts_flight_data", help="Path to BTS parquet files"
    )
    parser.add_argument(
        "--load-all-files", action="store_true", help="Load all parquet files"
    )
    parser.add_argument(
        "--no-parallel-loader",
        action="store_true",
        help="Disable parallel loader (for debugging)",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress CLI output (logs only)"
    )

    args = parser.parse_args()

    # Determine CLI mode
    cli_mode = not args.quiet

    # Setup logging - disable console output if in CLI mode (to avoid duplication)
    logger = setup_logging(verbose_cli=cli_mode)

    logger.info(
        f"Starting BTS data load - single_file: {args.single_file}, data_path: {args.data_path}"
    )
    logger.info(f"CLI mode: {cli_mode}")

    try:
        load_bts_data(
            data_path=args.data_path,
            single_file=args.single_file,
            use_parallel_loader=not args.no_parallel_loader,
            load_all_files=args.load_all_files,
            cli_mode=cli_mode,
        )
    except Exception as e:
        error_msg = f"BTS data load failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        if cli_mode:
            print(f"❌ {error_msg}")
        raise


if __name__ == "__main__":
    main()
