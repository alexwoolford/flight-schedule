#!/usr/bin/env python3
"""
BTS Flight Data Loader - Load BTS flight data into Neo4j using Spark
Based on successful debugging - applies type matching and tested relationship logic
"""

import argparse
import logging
import os
import time
from collections import defaultdict, deque
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

# BTS On-Time Performance reports only the OPERATING carrier
# (reporting_airline); there is no marketing-carrier column in the feed. These
# regionals are wholly-owned subsidiaries whose flights are scheduled, marketed
# and sold exclusively under the parent's code, so a parent<->child connection
# is a single-ticket itinerary in the real world. Comparing raw operating codes
# instead drops ~112,501 sellable connections per day (measured 2025-07-18),
# because e.g. an AA flight never connects to its own American Eagle feeder.
#
# Only carriers that fly for exactly ONE mainline belong here. Deliberately
# ABSENT: SkyWest (OO) and Republic (YX), which each fly for several mainlines
# (OO for DL/UA/AA/AS, YX for AA/DL/UA) and are independently owned, so BTS
# carries nothing to say which one a given flight was sold under; and Mesa (YV),
# which left American in April 2023, now flies for United, and is owned by
# Republic rather than a mainline. Inferring any of these would be fabricating
# data — see rule 1 in CLAUDE.md.
#
# Of the entries below only MQ and OH appear in the loaded 2025 US-domestic
# data; the rest are correct mappings kept for other periods and are inert here.
CARRIER_FAMILY = {
    "MQ": "AA",  # Envoy Air -> American Eagle
    "OH": "AA",  # PSA Airlines -> American Eagle
    "PT": "AA",  # Piedmont Airlines -> American Eagle
    "9E": "DL",  # Endeavor Air -> Delta Connection
    "QX": "AS",  # Horizon Air -> Alaska
}

# Anchor for the UTC-offset solve (see solve_airport_offsets). BFS over the
# directed-pair graph recovers every airport's offset *relative* to one reference,
# so one absolute value has to be supplied to place them all on the UTC scale.
#
# Phoenix is the right choice precisely because Arizona does not observe DST: PHX
# is UTC-7 all year, so this constant needs no seasonal branch. Anchoring on an
# airport that does observe DST (JFK, ORD) would require knowing which side of the
# transition each loaded date falls on -- exactly the complexity this avoids.
# Verified against known offsets in both seasons: with PHX=-420, a July week and a
# January week each resolve with 0 conflicts and every spot check exact
# (Jul: JFK -4, ORD -5, LAX -7, HNL -10, ANC -8, GUM +10;
#  Jan: JFK -5, ORD -6, LAX -8, HNL -10, ANC -9).
OFFSET_ANCHOR = ("PHX", -420)

# Minimum flights per directed airport pair before its offset delta is treated as
# self-checking. A pair flown three or more times has had several independent
# flights agree on the delta; a pair flown once has no cross-check against a data
# error in that single row.
#
# This is a preference, not a filter -- see the two-tier solve in
# solve_airport_offsets(). Measured 2025-07-18: the >=3 tier alone covers 225 of
# the day's 341 airports, and treating it as a hard cutoff left 386 flights with
# no UTC timestamp at all. Thin pairs are used to reach the remaining 116
# low-frequency stations (STT, BET, BRW, SCC...) and are still cross-checked
# against the solution, so a bad one raises rather than propagating.
OFFSET_MIN_FLIGHTS_PER_PAIR = 3


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
        "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64MB",
        # Updated for Spark 3.5.3
        "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        # === MEMORY CONFIGURATION (Aggressive for faster loading) ===
        "spark.driver.memory": "12g",  # Increased for better caching
        "spark.driver.maxResultSize": "4g",  # Larger result sets
        "spark.executor.memory": "8g",  # More memory per executor
        "spark.executor.memoryFraction": "0.85",  # More memory for processing
        "spark.executor.memoryStorageFraction": "0.3",
        # Less for storage, more for execution
        # === PARALLELISM (Configured for indexed database writes) ===
        "spark.sql.shuffle.partitions": "16",
        # Increased for better parallelism
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
        # === TIMEZONE SEMANTICS: both keys are load-bearing, not tuning ===
        #
        # BTS times are LOCAL wall clock at their own airport, and
        # download_bts_flight_data.py writes them as `pa.timestamp("us")` with
        # isAdjustedToUTC=false -- i.e. genuinely zone-less. Reading them as
        # TimestampNTZType is therefore the *correct* interpretation, not a
        # convenience.
        #
        # This is pinned rather than inherited because Spark 3.5's default
        # happens to be "true": relying on it means the graph silently depends
        # on a default we never chose.
        #
        # THE TWO KEYS BELOW ARE EACH INDEPENDENTLY SUFFICIENT. Measured on
        # tests/fixtures/bts_flights_2025_07_18.parquet, TZ=America/Denver,
        # hashing all 21,376 rows x 4 timestamps after the real transform:
        #
        #   inferTimestampNTZ | session.timeZone | result
        #   ------------------+------------------+----------------------------
        #   true              | UTC              | correct (44ee57ce...)
        #   false             | UTC              | correct (44ee57ce...)
        #   true              | <unset>          | correct (44ee57ce...)
        #   false             | <unset>          | CORRUPT  (f2d10acd...)
        #
        # So this is genuine defence in depth, not redundancy: either key alone
        # holds the line, and only losing both bakes the loader machine's offset
        # into every timestamp. In that state:
        #
        #   DL31 ATL scheduled_departure_time  2025-07-18 20:30 -> 2025-07-17 13:30
        #   flightdate for all 21,376 rows     2025-07-18       -> 2025-07-17
        #
        # The second line is the damaging one: `flightdate` is part of the
        # 5-field composite key, so the whole day moves and --solve-offsets
        # 2025-07-18 then finds nothing. That +7h shift is the same signature as
        # the fabricated README block removed in f20f20c.
        #
        # Because either key masks the other, no end-to-end test can catch the
        # removal of just one -- that is what
        # test_timezone_semantics_are_pinned_not_inherited asserts on the config
        # dict directly. The TZ-varied load step in ci.yml is the backstop for
        # the both-gone case and for mechanisms we have not thought of.
        "spark.sql.parquet.inferTimestampNTZ.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.parquet.int96TimestampConversion": "true",
        "spark.sql.parquet.writeLegacyFormat": "false",
        "spark.sql.parquet.mergeSchema": "false",  # Faster reads
        "spark.sql.parquet.filterPushdown": "true",
        # === ADVANCED TIMESTAMP COMPATIBILITY
        # (Fix nanosecond timestamp issues) ===
        "spark.sql.legacy.parquet.int96RebaseModeInRead": "CORRECTED",
        # Handle legacy timestamps
        "spark.sql.legacy.parquet.datetimeRebaseModeInRead": "CORRECTED",
        # Handle datetime rebasing
        "spark.sql.parquet.respectSummaryFiles": "false",
        # Ignore potentially problematic summary files
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
        # 5.4.0+ is REQUIRED: it is the first version that maps Spark
        # timestamp_ntz to a Neo4j LocalDateTime. Earlier versions (e.g. 4.1.5)
        # silently write timestamp_ntz as a raw epoch INTEGER, which breaks every
        # temporal query and duration.between() call.
        "org.neo4j:neo4j-connector-apache-spark_2.12:5.5.0_for_spark_3",
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
    # NOTE: Do NOT add a plain index on (:Airport {code}) or (:Carrier {code}).
    # The uniqueness constraints below create their own backing indexes, and Neo4j
    # rejects a constraint with IndexAlreadyExists if a plain index on the identical
    # label+property already exists (IF NOT EXISTS does not suppress that error).
    # Airport/Carrier lookups are served by the constraints' backing indexes.
    schema_queries = [
        # ✅ PROVEN USEFUL: This index gets 3.5M+ reads during loading
        "CREATE INDEX schedule_route IF NOT EXISTS FOR (s:Schedule) ON (s.origin, s.dest)",
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
                except Exception as e:
                    # Do NOT swallow these. A missing uniqueness constraint silently
                    # turns a second load into duplicate dimension nodes, so a
                    # failure here must abort the load rather than be logged away.
                    kind = "Index" if "INDEX" in query else "Constraint"
                    name = query.split(f"{kind.upper()} ")[1].split(" ")[0]
                    msg = f"   ❌ {kind} failed: {name}: {e}"
                    if logger:
                        log_and_print(
                            msg, logger, level=logging.ERROR, cli_mode=cli_mode
                        )
                    else:
                        print(msg)
                    return False

            # Verify the constraints actually exist. Creation returning without an
            # exception is not proof: assert against SHOW CONSTRAINTS.
            expected_constraints = {
                "airport_code_unique",
                "carrier_code_unique",
                "schedule_composite_unique",
            }
            actual = {
                record["name"]
                for record in session.run("SHOW CONSTRAINTS YIELD name RETURN name")
            }
            missing = expected_constraints - actual
            if missing:
                msg = (
                    f"   ❌ Schema verification failed: missing constraints "
                    f"{sorted(missing)}. Loading would create duplicates."
                )
                if logger:
                    log_and_print(msg, logger, level=logging.ERROR, cli_mode=cli_mode)
                else:
                    print(msg)
                return False

            summary_msg = f"   📊 Schema setup: {indexes_created} indexes, {constraints_created} constraints"
            verified_msg = (
                f"   ✅ Verified {len(expected_constraints)} constraints exist"
            )
            success_msg = "   🚀 Database ready for loading!"
            if logger:
                log_and_print(summary_msg, logger, cli_mode=cli_mode)
                log_and_print(verified_msg, logger, cli_mode=cli_mode)
                log_and_print(success_msg, logger, cli_mode=cli_mode)
            else:
                print(summary_msg)
                print(verified_msg)
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


def create_route_projection(neo4j_uri, neo4j_user, neo4j_password, neo4j_database):
    """
    Build the aggregated (:Airport)-[:ROUTE]->(:Airport) network.

    One edge per distinct (origin, dest) pair rather than one per flight: ~6,900
    edges for a full year against ~20.7M for the three per-Schedule types.

    Why it exists: multi-leg traversal over Schedule has to pass through Airport,
    which is a supernode (out-degree up to ~321K across a year) and carries no
    date, so a quantified path pattern fans out to a full year of flights at
    every hop. This projection has out-degree ~20, so reachability and hop-count
    questions are cheap over it. See ROUTING_QUERY_REFERENCE.md.

    Deliberately computed in Cypher over the graph rather than as a Spark
    groupBy over the file being loaded. The aggregates (flights, carriers,
    first_date, last_date) describe *all* loaded data, and MERGE overwrites
    properties rather than accumulating them — so a per-file aggregate would
    make a --single-file run silently replace a full-year ROUTE network with
    one month's numbers. Deriving from the graph is correct for any load order
    and for incremental loads.
    """
    print("   🗺️  Creating ROUTE relationships (aggregated route network)...")
    route_start_time = time.time()

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    try:
        with driver.session(database=neo4j_database) as session:
            # Aggregate per directed pair, then MERGE one edge each. Airport
            # nodes already exist from the node-creation step, so this matches
            # rather than creates them.
            result = session.run(
                """
                MATCH (s:Schedule)
                WITH s.origin AS origin, s.dest AS dest,
                     count(*) AS flights,
                     count(DISTINCT s.reporting_airline) AS carriers,
                     min(s.flightdate) AS first_date,
                     max(s.flightdate) AS last_date
                MATCH (o:Airport {code: origin})
                MATCH (d:Airport {code: dest})
                MERGE (o)-[r:ROUTE]->(d)
                SET r.flights = flights,
                    r.carriers = carriers,
                    r.first_date = first_date,
                    r.last_date = last_date
                RETURN count(*) AS routes
                """
            )
            route_count = result.single()["routes"]
    finally:
        driver.close()

    route_time = time.time() - route_start_time
    print(f"     ✅ ROUTE completed in {route_time:.1f}s ({route_count:,} edges)")
    return route_count


# Per-directed-pair offset delta.
#
# Overnight legs need no special handling, which is worth stating because it is
# not obvious. Their stored arrival is stamped a day early, so the raw
# arrival-minus-departure is wrong by exactly -1440 minutes -- and `(... + 2880)
# % 1440` folds that away. Measured on 2025-07-18: excluding them changes nothing,
# 341 airports and 0 conflicts either way.
#
# An earlier version of this query did exclude them, via a heuristic that tested
# whether adding a day reconciled the subtraction with the block time to within
# 180 minutes. That heuristic is now deleted rather than kept as belt-and-braces:
# it cannot span the widest US offset gaps (HNL->DFW needs 240-360 minutes), so
# leaving it in the file invited reuse somewhere it would be load-bearing and
# wrong -- which is exactly how 11,975 impossible CONNECTS_TO edges got built.
#
# Returns the flight count so the solve can prefer well-supported pairs and fall
# back to thin ones only where it must; it does not filter on support here.
# Contradictory pairs are dropped outright -- there are none on a single date, so
# any that appear are a data problem, and the caller's conflict assertion is what
# surfaces them.
_OFFSET_DELTA_QUERY = """
    MATCH (s:Schedule)
    WHERE s.flightdate = date($date)
      AND s.scheduled_duration_minutes IS NOT NULL
    WITH s.origin AS origin, s.dest AS dest,
         (duration.inSeconds(s.scheduled_departure_time,
                             s.scheduled_arrival_time).seconds / 60
          - s.scheduled_duration_minutes + 2880) % 1440 AS delta
    WITH origin, dest, count(*) AS flights, collect(DISTINCT delta) AS deltas
    WHERE size(deltas) = 1
    RETURN origin, dest, flights, deltas[0] AS delta
"""


def solve_airport_offsets(session, search_date, min_flights=None):
    """Recover every airport's UTC offset (in minutes) for one date.

    No external timezone database is needed, and none is used. BTS gives local
    departure time at the origin, local arrival time at the destination, and the
    timezone-independent block time (CRSElapsedTime). For any flight,

        (arrival_local - departure_local) - block = offset(dest) - offset(origin)

    so each directed airport pair yields the *difference* between its endpoints'
    offsets. Treating airports as nodes and those differences as weighted edges,
    a BFS from any starting airport propagates relative offsets across the whole
    network; fixing one known absolute value converts them to true UTC offsets.

    Solved per-date on purpose. Offsets are DST-dependent -- 18 of 317 airports
    differ between January and July, and mainland airports shift wholesale (ORD
    -6 -> -5) -- so there is no single correct value to store on an Airport node.
    A week straddling the 2025-03-09 transition yields 603 of 5,037 pairs (12.0%)
    with contradictory deltas; any single day yields 0.

    Returns {airport_code: offset_minutes}. Raises on any inconsistency: with real
    BTS data the solve is exact, so a conflict means a data problem, not a case to
    paper over.
    """
    if min_flights is None:
        min_flights = OFFSET_MIN_FLIGHTS_PER_PAIR

    pairs = session.run(_OFFSET_DELTA_QUERY, date=search_date).data()
    if not pairs:
        raise RuntimeError(
            f"No usable airport pairs on {search_date} — is the date loaded?"
        )

    # Undirected: delta forward, -delta reverse. offset(dest) - offset(origin).
    # Two tiers. Well-supported pairs (>= min_flights) are self-checking: several
    # independent flights had to agree on the delta. Thin pairs are not, so they
    # are held back and used only to reach airports the first tier misses -- on
    # 2025-07-18 that is 116 low-frequency stations (STT, BET, BRW, SCC...), and
    # skipping them would silently leave 386 flights with no UTC time at all.
    # Every thin edge is still cross-checked against the solution, so a bad one
    # raises rather than propagating.
    strong = defaultdict(list)
    weak = defaultdict(list)
    for row in pairs:
        bucket = strong if row["flights"] >= min_flights else weak
        bucket[row["origin"]].append((row["dest"], row["delta"]))
        bucket[row["dest"]].append((row["origin"], -row["delta"]))

    # Root the BFS at the best-connected airport, so the traversal is driven by
    # data rather than by a hard-coded hub. Deliberately NOT the anchor: the root
    # only has to be well connected, while the anchor has to have a known,
    # DST-free offset.
    root = max(strong, key=lambda code: len(strong[code]))
    relative = {root: 0}
    conflicts = []

    def bfs(adjacency, sources):
        """Propagate offsets outward, recording rather than raising on conflict."""
        queue = deque(sources)
        while queue:
            code = queue.popleft()
            for neighbour, delta in adjacency.get(code, ()):
                implied = relative[code] + delta
                if neighbour not in relative:
                    relative[neighbour] = implied
                    queue.append(neighbour)
                elif (relative[neighbour] - implied) % 1440 != 0:
                    conflicts.append(
                        f"{code}->{neighbour}: have {relative[neighbour]}, "
                        f"implied {implied}"
                    )

    bfs(strong, [root])

    # Then pull in whatever only the thin pairs can reach. Alternating lets an
    # airport reached by a thin edge serve as a bridge back into strong pairs.
    combined = defaultdict(list)
    for adjacency in (strong, weak):
        for code, edges in adjacency.items():
            combined[code].extend(edges)
    while True:
        frontier = [
            c
            for c in relative
            if any(n not in relative for n, _ in combined.get(c, ()))
        ]
        if not frontier:
            break
        bfs(combined, frontier)

    if conflicts:
        raise RuntimeError(
            f"{len(conflicts)} contradictory airport offset(s) on {search_date}. "
            "Every directed pair should agree exactly; disagreement means the "
            "underlying times are inconsistent, not that the solve needs a "
            f"tolerance. First few: {conflicts[:5]}"
        )

    unreached = set(combined) - set(relative)
    if unreached:
        raise RuntimeError(
            f"Airport offset graph for {search_date} is disconnected: "
            f"{len(unreached)} airport(s) unreachable from {root}, e.g. "
            f"{sorted(unreached)[:10]}. A single day of US domestic BTS is one "
            "component; a split means the date is only partially loaded."
        )

    # Shift the relative solution onto the absolute UTC scale.
    anchor_code, anchor_offset = OFFSET_ANCHOR
    if anchor_code not in relative:
        raise RuntimeError(
            f"Offset anchor {anchor_code} has no flights on {search_date}, so "
            "the relative solution cannot be placed on the UTC scale. "
            f"{anchor_code} is a top-10 airport by volume and is present on any "
            "real BTS day; its absence means the load is incomplete."
        )
    shift = anchor_offset - relative[anchor_code]

    offsets = {}
    for code, value in relative.items():
        offset = (value + shift) % 1440
        # Wrap to (-720, 720]. Everything east of the dateline lands here
        # directly; GUM and SPN would otherwise come out as -840 rather than
        # +600 -- exactly 24h off, which is the dateline, not an error.
        if offset > 720:
            offset -= 1440
        if offset % 60 != 0:
            raise RuntimeError(
                f"{code} solved to {offset} minutes, which is not a whole hour. "
                "Every US airport offset is a whole hour; a fractional result "
                "means the block times and clock times disagree."
            )
        offsets[code] = offset

    return offsets


def write_utc_times(
    neo4j_uri, neo4j_user, neo4j_password, neo4j_database, dates, min_flights=None
):
    """Solve offsets per date and store absolute UTC timestamps on Schedule.

    Adds two properties, leaving the existing local-time ones untouched:

      scheduled_departure_utc = scheduled_departure_time - offset(origin)
      scheduled_arrival_utc   = scheduled_departure_utc + block_minutes

    The local times are correct as local wall clock and some filters legitimately
    want them ("arrives before 3pm local"). What they cannot support is
    subtraction, because both are composed onto the origin's flightdate: on
    2025-07-18, arrival-minus-departure matches the BTS block time for only
    10,453 of 21,376 flights (48.9%) and 934 flights appear to arrive before they
    depart. The UTC pair fixes durations, journey totals, and cross-midnight
    sequencing.

    Arrival is derived by *adding the block time* rather than by converting the
    stored local arrival. Both routes agree, but adding is the one that cannot
    inherit a wrong date -- the stored arrival's DATE is unreliable for exactly
    the overnight legs this is meant to repair.

    It then *repairs* `scheduled_arrival_time` in place, rewriting it as
    `arrival_utc + offset(dest)`. Only the DATE changes: the loader composes both
    timestamps onto the origin's `flightdate`, so a leg crossing local midnight
    was stamped a day early (893 of 21,376 on 2025-07-18). The time-of-day was
    already the correct destination-local wall clock and is preserved exactly --
    `TestUtcTimestamps` asserts the round-trip on 100% of rows.

    This is what makes a deadline filter correct with no guard at all. The
    ±180-minute block-time heuristic this replaces was an attempt to *infer* the
    destination offset at query time, which is not recoverable there; it missed
    the widest gaps (HNL->DFW needs 240-360) and so left real red-eyes passing a
    deadline they miss. `date(arrival_utc) = date(departure_utc)` is not a
    substitute either -- that tests UTC midnight, and on 2025-07-18 it wrongly
    excludes 3,135 ordinary evening flights while admitting 876 genuine
    overnights. Fixing the stored date is the only correct fix, and it can only
    be done here, where the offsets are known.
    """
    print("   🕐 Solving airport UTC offsets and writing UTC timestamps...")
    start_time = time.time()

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    solved = {}
    try:
        with driver.session(database=neo4j_database) as session:
            for search_date in dates:
                offsets = solve_airport_offsets(
                    session, search_date, min_flights=min_flights
                )

                # Offsets go in as a parameter map rather than onto Airport nodes:
                # they are date-specific (DST), so a single Airport.utc_offset
                # would be wrong for half the year.
                written = session.run(
                    """
                    MATCH (s:Schedule)
                    WHERE s.flightdate = date($date)
                      AND s.scheduled_duration_minutes IS NOT NULL
                      AND $off[s.origin] IS NOT NULL
                      AND $off[s.dest] IS NOT NULL
                    CALL (s) {
                        WITH s.scheduled_departure_time
                             - duration({minutes: $off[s.origin]}) AS dep_utc
                        WITH dep_utc, dep_utc + duration({minutes:
                                 s.scheduled_duration_minutes}) AS arr_utc
                        SET s.scheduled_departure_utc = dep_utc,
                            s.scheduled_arrival_utc = arr_utc,
                            // Rewrite the local arrival off the UTC instant so
                            // its DATE is the destination's, not the origin's.
                            // The time-of-day is unchanged; only overnight legs
                            // move, and they move onto the day they land.
                            s.scheduled_arrival_time = arr_utc
                                + duration({minutes: $off[s.dest]})
                    } IN TRANSACTIONS OF 25000 ROWS
                    """,
                    date=search_date,
                    off=offsets,
                ).consume()

                print(
                    f"     • {search_date}: {len(offsets)} airports, "
                    f"{written.counters.properties_set // 3:,} flights"
                )
                solved[search_date] = offsets
    finally:
        driver.close()

    elapsed = time.time() - start_time
    print(f"     ✅ UTC timestamps written in {elapsed:.1f}s")
    return solved


def create_connects_to(
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    neo4j_database,
    dates,
    min_layover=45,
    max_layover=300,
    strict_carrier=False,
    rebuild=False,
):
    """
    Build (:Schedule)-[:CONNECTS_TO {layover_minutes}]->(:Schedule) for `dates`.

    This is the edge that makes quantified path patterns work for itinerary
    search. A QPP over Schedule->Airport->Schedule has to cross the Airport
    supernode at every repetition, and because the date lives on Schedule rather
    than Airport, 99.69% of the Schedule nodes it binds are discarded. Measured
    LGA->DFW for one date: 3,783,541 candidates bound, 11,695 relevant.
    Materialising the connection as a single edge removes the juncture, so each
    repetition is one relationship hop with small out-degree.

    Only valid connections get an edge:

    - same marketing carrier — CARRIER_FAMILY maps wholly-owned regionals onto
      their mainline parent, so AA->MQ (American Eagle) connects but unrelated
      carriers never splice. `strict_carrier=True` compares raw operating codes
      instead, which drops ~112K sellable connections per day.
    - layover within [min_layover, max_layover], measured in UTC
    - no immediate backtrack to the first leg's origin

    Sequencing is enforced here, at load time, rather than in the search query.
    That is deliberate: `lay >= min_layover` with a positive minimum is what
    guarantees the second leg departs after the first one lands, so every path
    over these edges is chronologically valid by construction. Sequencing is
    transitive, so it needs no path-level check; airport revisits are not, and
    the `s2.dest <> s1.origin` guard here is only pairwise — a multi-leg search
    must still compare airport codes itself.

    **Requires `--solve-offsets` to have run for each date first.** The layover
    is computed from `scheduled_*_utc`, which that step creates.

    Encoding the policy in the edge is the tradeoff — change the layover window
    or the carrier rule and this must be rebuilt. `MERGE` will not remove edges
    that a previous, looser build already wrote, so pass `rebuild=True` to clear
    each date first.

    Scoped to specific dates on purpose. One day of 2025 is ~625K edges; a full
    year would be ~228M, which is ~11x the rest of the graph and not what a demo
    or a same-day search needs.
    """
    print("   🔀 Creating CONNECTS_TO relationships (bookable connections)...")
    start_time = time.time()

    if strict_carrier:
        carrier_match = "s2.reporting_airline = s1.reporting_airline"
    else:
        carrier_match = (
            "coalesce($family[s2.reporting_airline], s2.reporting_airline) = "
            "coalesce($family[s1.reporting_airline], s1.reporting_airline)"
        )

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    total = 0
    try:
        with driver.session(database=neo4j_database) as session:
            for search_date in dates:
                if rebuild:
                    session.run(
                        """
                        MATCH (s1:Schedule)-[r:CONNECTS_TO]->()
                        WHERE s1.flightdate = date($date)
                        CALL (r) {
                            DELETE r
                        } IN TRANSACTIONS OF 25000 ROWS
                        """,
                        date=search_date,
                    ).consume()

                # Driven from Schedule.flightdate (indexed) rather than through
                # the Airport supernode: seeks the day's flights, then joins on
                # dest = origin. Batched so one day does not build a single
                # oversized transaction.
                #
                # The layover is computed in UTC, which is what makes it correct.
                # This previously subtracted the two LOCAL timestamps and guarded
                # against next-day arrivals with a block-time heuristic (since
                # deleted). That heuristic allowed only +/-180 minutes of
                # timezone skew and so missed the widest spans: 11,975 edges
                # survived where the inbound leg lands the next morning, e.g.
                # AA6 HNL->DFW dep 17:36 arr 06:02+1 (446-min block, 240-360 min
                # to reconcile) spliced to an 08:10 DFW departure the previous
                # day. Comparing scheduled_*_utc needs no tolerance and no
                # heuristic, because both sides are absolute instants.
                #
                # Requires --solve-offsets to have run for this date; the IS NOT
                # NULL guard below would otherwise silently produce no edges, so
                # the count check after this raises instead.
                session.run(
                    f"""
                    MATCH (s1:Schedule) WHERE s1.flightdate = date($date)
                      AND s1.scheduled_arrival_utc IS NOT NULL
                    MATCH (s2:Schedule) WHERE s2.flightdate = date($date)
                      AND s2.origin = s1.dest
                      AND s2.scheduled_departure_utc IS NOT NULL
                      AND {carrier_match}
                      AND s2.dest <> s1.origin
                    WITH s1, s2,
                         duration.inSeconds(s1.scheduled_arrival_utc,
                                            s2.scheduled_departure_utc
                                            ).seconds / 60 AS lay
                    WHERE lay >= $min_layover AND lay <= $max_layover
                    CALL (s1, s2, lay) {{
                        MERGE (s1)-[r:CONNECTS_TO]->(s2)
                        SET r.layover_minutes = lay
                    }} IN TRANSACTIONS OF 25000 ROWS
                    """,
                    date=search_date,
                    min_layover=min_layover,
                    max_layover=max_layover,
                    family=CARRIER_FAMILY,
                ).consume()

                count = session.run(
                    """
                    MATCH (s1:Schedule)-[r:CONNECTS_TO]->()
                    WHERE s1.flightdate = date($date)
                    RETURN count(r) AS count
                    """,
                    date=search_date,
                ).single()["count"]

                # Zero edges on a loaded date means the UTC properties are
                # missing, not that the day has no connections. Without this the
                # build would report success having written nothing -- the same
                # class of silent-empty failure that conftest's skip-on-no-DB
                # once hid in CI.
                if count == 0:
                    flights = session.run(
                        """
                        MATCH (s:Schedule) WHERE s.flightdate = date($date)
                        RETURN count(s) AS flights,
                               count(s.scheduled_departure_utc) AS with_utc
                        """,
                        date=search_date,
                    ).single()
                    if flights["flights"] == 0:
                        raise RuntimeError(
                            f"No flights loaded for {search_date}; nothing to "
                            "connect."
                        )
                    if flights["with_utc"] == 0:
                        raise RuntimeError(
                            f"{search_date} has {flights['flights']:,} flights "
                            "but none carry scheduled_departure_utc. Run "
                            f"--solve-offsets {search_date} first."
                        )

                total += count
                print(f"     • {search_date}: {count:,} connections")
    finally:
        driver.close()

    elapsed = time.time() - start_time
    print(f"     ✅ CONNECTS_TO completed in {elapsed:.1f}s ({total:,} edges)")
    return total


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
        # MERGE the relationship rather than CREATE, so a re-run does not
        # duplicate edges. See the node writes for the full rationale.
        "Overwrite"
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
        "Overwrite"  # MERGE, not CREATE — idempotent re-runs
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
        "Overwrite"  # MERGE, not CREATE — idempotent re-runs
    ).save()

    op_time = time.time() - op_start_time
    print(
        f"     ✅ OPERATED_BY completed in {op_time:.1f}s ({dep_count/op_time:.0f} rels/sec)"
    )

    # 4. ROUTE relationships — the aggregated route network.
    route_count = create_route_projection(
        neo4j_uri, neo4j_user, neo4j_password, neo4j_database
    )

    total_rel_time = time.time() - total_start_time
    # 3 per-Schedule relationships (DEPARTS_FROM, ARRIVES_AT, OPERATED_BY) plus
    # the aggregated Airport->Airport route network.
    total_relationships = dep_count * 3 + route_count

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
    neo4j_database = os.getenv("NEO4J_DATABASE", "neo4j")

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
        # Raise rather than `return`: a bare return here aborted the load while
        # still exiting 0, so a caller — notably the `integration-test` job in
        # .github/workflows/ci.yml — saw a successful load step against a
        # database that had received nothing. Any automation gating on this
        # loader needs a non-zero exit when it does not load.
        raise RuntimeError(
            "Database schema setup failed - aborting load. The most common "
            "cause is bad Neo4j credentials or an unreachable server; check "
            "NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD."
        )

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
            if (
                "TIMESTAMP(NANOS" in str(e)
                or "Illegal Parquet type" in str(e)
                or "ClassCastException" in str(e)
                or "MutableLong cannot be cast" in str(e)
                or "MutableDouble" in str(e)
            ):
                print(
                    "   ⚠️  Detected parquet compatibility issues - using aggressive compatibility mode..."
                )
                logger.warning(
                    f"Parquet compatibility issues detected: {str(e)[:200]}... using aggressive fallback"
                )

                # Enhanced Fallback: Use aggressive compatibility settings for schema inference problems
                #
                # NOTE: the `spark.sql.*` keys below are passed as *reader*
                # options, where they are inert -- DataFrameReader.option() sets
                # per-source options, not session config. They are left in place
                # because removing them would change nothing and this fallback
                # only runs on already-broken input. What actually governs this
                # path is the session config in create_spark_session(), which is
                # the argument for pinning the timezone keys there rather than
                # per-read: the fallback inherits them for free.
                parquet_reader_fallback = (
                    spark.read.option(
                        "mergeSchema", "true"
                    )  # Enable for data type differences
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
                            if "ClassCastException" in str(
                                e3
                            ) or "MutableLong cannot be cast" in str(e3):
                                print(
                                    f"   ❌ Failed to load: {pfile.name} - Data type mismatch (likely schema change)"
                                )
                                print(
                                    "      Suggestion: This file may have incompatible data types. "
                                    "Consider re-downloading or excluding this file."
                                )
                            else:
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
            # Temporal data.
            #
            # These are LOCAL wall-clock times at their respective airports:
            # BTS CRSDepTime is local at origin, CRSArrTime is local at dest.
            # They are therefore written as TIMESTAMP_NTZ, which the Neo4j
            # connector stores as LocalDateTime. Using plain `timestamp()` here
            # would produce Spark TimestampType, which the connector writes as a
            # UTC ZonedDateTime — silently baking in the *loader machine's*
            # timezone and producing a different graph on a laptop than in a
            # container.
            #
            # to_timestamp_ntz secures the *write* side. The matching read side
            # is `spark.sql.parquet.inferTimestampNTZ.enabled` in
            # create_spark_session() — both are required, and that comment
            # carries the measured cost of losing either.
            #
            # Do NOT subtract these two to get a flight duration: they are in
            # different timezones, so the result is wrong for ~50% of flights.
            # Use scheduled_duration_minutes (below) instead.
            when(
                col("crsdeptime").isNotNull(),
                expr(
                    "to_timestamp_ntz(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(crsdeptime, 'HH:mm:ss')))"
                ),
            ).alias("scheduled_departure_time"),
            when(
                col("crsarrtime").isNotNull(),
                expr(
                    "to_timestamp_ntz(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(crsarrtime, 'HH:mm:ss')))"
                ),
            ).alias("scheduled_arrival_time"),
            when(
                col("deptime").isNotNull(),
                expr(
                    "to_timestamp_ntz(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(deptime, 'HH:mm:ss')))"
                ),
            ).alias("actual_departure_time"),
            when(
                col("arrtime").isNotNull(),
                expr(
                    "to_timestamp_ntz(concat(date_format(flightdate, 'yyyy-MM-dd'), ' ', date_format(arrtime, 'HH:mm:ss')))"
                ),
            ).alias("actual_arrival_time"),
            # BTS-reported scheduled block time, in minutes. This is the
            # authoritative flight duration: it is timezone-independent and
            # DST-proof, unlike arrival-minus-departure. 100% populated.
            col("crselapsedtime")
            .cast(IntegerType())
            .alias("scheduled_duration_minutes"),
            col("actualelapsedtime")
            .cast(IntegerType())
            .alias("actual_duration_minutes"),
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
            # "Overwrite" maps to MERGE on node.keys in the Neo4j connector;
            # "Append" maps to CREATE, which ignores node.keys and duplicates
            # rows on any re-run. Overwrite makes the load idempotent.
            "Overwrite"
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
            "Overwrite"  # MERGE on code — see Carrier write above
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
            "Overwrite"  # MERGE on the 5-part composite key — see Carrier write above
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


def _neo4j_credentials(logger):
    """Read Neo4j connection details from the environment (rule 5: never inline).

    Note load_dotenv() is called without override=True, matching the rest of this
    module: an exported NEO4J_PASSWORD from another project beats .env and shows
    up as an auth failure here rather than as a missing key.
    """
    load_dotenv()
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")
    if not all([uri, user, password]):
        msg = "Missing Neo4j credentials — copy .env.example to .env"
        logger.error(msg)
        print(f"❌ {msg}")
        raise SystemExit(1)
    return uri, user, password, database


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
    parser.add_argument(
        "--build-connections",
        metavar="YYYY-MM-DD",
        nargs="+",
        help=(
            "Build CONNECTS_TO edges for the given date(s) and exit. Requires an "
            "already-loaded graph; does not start Spark. ~625K edges per day."
        ),
    )
    parser.add_argument(
        "--solve-offsets",
        metavar="YYYY-MM-DD",
        nargs="+",
        help=(
            "Recover airport UTC offsets for the given date(s) from the loaded "
            "block times and write scheduled_departure_utc / "
            "scheduled_arrival_utc, then exit. Requires an already-loaded graph; "
            "does not start Spark. Run this BEFORE --build-connections."
        ),
    )
    parser.add_argument(
        "--rebuild-connections",
        action="store_true",
        help=(
            "Delete existing CONNECTS_TO edges for each date before building. "
            "MERGE cannot remove edges written by an earlier, looser rule, so "
            "use this after changing the layover window or carrier matching."
        ),
    )
    parser.add_argument(
        "--strict-carrier",
        action="store_true",
        help=(
            "Require an identical operating carrier code instead of treating "
            "wholly-owned regionals as their mainline parent. Drops ~112K "
            "sellable connections per day; see CARRIER_FAMILY."
        ),
    )
    parser.add_argument(
        "--min-layover",
        type=int,
        default=45,
        help="Minimum connection minutes for --build-connections (default 45)",
    )
    parser.add_argument(
        "--max-layover",
        type=int,
        default=300,
        help="Maximum connection minutes for --build-connections (default 300)",
    )

    args = parser.parse_args()

    # Determine CLI mode
    cli_mode = not args.quiet

    # Setup logging - disable console output if in CLI mode (to avoid duplication)
    logger = setup_logging(verbose_cli=cli_mode)

    # The UTC solve and the CONNECTS_TO build both work purely over the loaded
    # graph, so they run standalone rather than as steps in the Spark pipeline.
    # Offsets must be written first: once scheduled_arrival_utc exists, the
    # connection build can eventually compare UTC directly instead of inferring
    # overnight legs from the block time.
    if args.solve_offsets or args.build_connections:
        creds = _neo4j_credentials(logger)

        if args.solve_offsets:
            try:
                write_utc_times(*creds, args.solve_offsets)
            except Exception as e:
                error_msg = f"UTC offset solve failed: {str(e)}"
                logger.error(error_msg, exc_info=True)
                print(f"❌ {error_msg}")
                raise

        if args.build_connections:
            try:
                create_connects_to(
                    *creds,
                    args.build_connections,
                    min_layover=args.min_layover,
                    max_layover=args.max_layover,
                    strict_carrier=args.strict_carrier,
                    rebuild=args.rebuild_connections,
                )
            except Exception as e:
                error_msg = f"CONNECTS_TO build failed: {str(e)}"
                logger.error(error_msg, exc_info=True)
                print(f"❌ {error_msg}")
                raise
        return

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
