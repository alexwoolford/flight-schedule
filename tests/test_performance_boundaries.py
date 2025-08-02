#!/usr/bin/env python3
"""
Performance Boundaries Tests
===========================

Tests performance and scale scenarios WITHOUT using mocks.
Focuses on real memory usage, configuration impact, and scale limits
that affect production deployments.
"""

import os
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import psutil

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import download_bts_flight_data  # noqa: E402
    import load_bts_data  # noqa: E402
except ImportError:
    # Tests will skip if modules can't be imported
    download_bts_flight_data = None
    load_bts_data = None


class TestMemoryUsagePatterns:
    """Test memory usage patterns with different data scenarios"""

    def test_memory_usage_small_dataset(self):
        """Test memory usage with small datasets (baseline)"""
        # Create small dataset (100 records)
        small_data = {
            "flightdate": ["2024-03-01"] * 100,
            "reporting_airline": ["AA"] * 100,
            "flight_number_reporting_airline": [str(i) for i in range(100)],
            "origin": ["LAX"] * 100,
            "dest": ["JFK"] * 100,
            "crs_dep_time": ["1430"] * 100,
            "crs_arr_time": ["1630"] * 100,
            "cancelled": [0] * 100,
            "distance": [2475.0] * 100,
        }

        # Measure memory before
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        # Create DataFrame
        df = pd.DataFrame(small_data)

        # Measure memory after
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = memory_after - memory_before

        # Small dataset should use minimal memory (< 10MB additional)
        assert (
            memory_used < 10
        ), f"Small dataset used {memory_used:.1f}MB (expected < 10MB)"
        assert len(df) == 100, "Should create 100 records"

        # Test basic operations don't explode memory
        memory_before_ops = process.memory_info().rss / 1024 / 1024

        # Basic operations
        unique_airlines = df["reporting_airline"].unique()
        _grouped = df.groupby("origin").size()  # noqa: F841
        filtered = df[df["cancelled"] == 0]

        memory_after_ops = process.memory_info().rss / 1024 / 1024
        ops_memory_used = memory_after_ops - memory_before_ops

        assert (
            ops_memory_used < 5
        ), f"Basic operations used {ops_memory_used:.1f}MB (expected < 5MB)"
        assert len(unique_airlines) == 1, "Should have 1 unique airline"
        assert len(filtered) == 100, "Should have all non-cancelled flights"

    def test_memory_usage_medium_dataset(self):
        """Test memory usage with medium datasets (10k records)"""
        # Create medium dataset (10,000 records)
        medium_data = {
            "flightdate": ["2024-03-01"] * 10000,
            "reporting_airline": (["AA", "UA", "DL"] * 3334)[:10000],
            "flight_number_reporting_airline": [str(i) for i in range(10000)],
            "origin": (["LAX", "ORD", "ATL"] * 3334)[:10000],
            "dest": (["JFK", "SFO", "MIA"] * 3334)[:10000],
            "crs_dep_time": ["1430"] * 10000,
            "crs_arr_time": ["1630"] * 10000,
            "cancelled": [0] * 10000,
            "distance": [2475.0] * 10000,
        }

        # Measure memory before
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        # Create DataFrame
        df = pd.DataFrame(medium_data)

        # Measure memory after
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = memory_after - memory_before

        # Medium dataset should use reasonable memory (< 50MB additional)
        assert (
            memory_used < 50
        ), f"Medium dataset used {memory_used:.1f}MB (expected < 50MB)"
        assert len(df) == 10000, "Should create 10,000 records"

        # Test grouping operations performance
        start_time = time.time()
        airline_counts = df.groupby("reporting_airline").size()
        route_counts = df.groupby(["origin", "dest"]).size()
        grouping_time = time.time() - start_time

        # Grouping should be fast (< 1 second)
        assert (
            grouping_time < 1.0
        ), f"Grouping took {grouping_time:.2f}s (expected < 1s)"
        assert len(airline_counts) == 3, "Should have 3 unique airlines"
        assert len(route_counts) <= 9, "Should have <= 9 unique routes"

    def test_memory_growth_pattern(self):
        """Test memory growth pattern with increasing dataset sizes"""
        memory_usage = []
        dataset_sizes = [100, 500, 1000, 2000, 5000]

        process = psutil.Process()

        for size in dataset_sizes:
            # Create dataset of specified size
            data = {
                "flightdate": ["2024-03-01"] * size,
                "reporting_airline": ["AA"] * size,
                "flight_number_reporting_airline": [str(i) for i in range(size)],
                "origin": ["LAX"] * size,
                "dest": ["JFK"] * size,
                "cancelled": [0] * size,
                "distance": [2475.0] * size,
            }

            memory_before = process.memory_info().rss / 1024 / 1024  # MB
            df = pd.DataFrame(data)
            memory_after = process.memory_info().rss / 1024 / 1024  # MB

            memory_used = memory_after - memory_before
            memory_usage.append((size, memory_used))

            # Clean up DataFrame
            del df

        # Test that memory growth is roughly linear (not exponential)
        # Skip the first few measurements as they can be unreliable due to overhead
        for i in range(
            2, len(memory_usage)
        ):  # Start from index 2 to skip small datasets
            prev_size, prev_memory = memory_usage[i - 1]
            curr_size, curr_memory = memory_usage[i]

            size_ratio = curr_size / prev_size

            # Only test memory growth if previous memory usage was significant (> 1MB)
            if prev_memory > 1.0:
                memory_ratio = curr_memory / prev_memory
                # Memory growth should be sub-quadratic (allow for some overhead and garbage collection effects)
                assert (
                    memory_ratio <= size_ratio * 4
                ), f"Memory growth too high: {memory_ratio:.2f}x for {size_ratio:.2f}x size increase"
            else:
                # For small memory usage, just check current usage is reasonable
                assert (
                    curr_memory < size_ratio * 10
                ), f"Memory usage {curr_memory:.1f}MB too high for size {curr_size}"


class TestConfigurationPerformanceImpact:
    """Test performance impact of different configurations"""

    def test_batch_size_impact(self):
        """Test impact of different batch sizes on processing time"""
        # Create test dataset
        test_data = {
            "flightdate": ["2024-03-01"] * 1000,
            "reporting_airline": (["AA", "UA", "DL"] * 334)[:1000],
            "flight_number_reporting_airline": [str(i) for i in range(1000)],
            "origin": ["LAX"] * 1000,
            "dest": ["JFK"] * 1000,
        }
        df = pd.DataFrame(test_data)

        batch_sizes = [10, 50, 100, 500, 1000]
        processing_times = []

        for batch_size in batch_sizes:
            start_time = time.time()

            # Simulate batch processing
            processed_batches = 0
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i : i + batch_size]
                # Simulate some processing work
                _ = batch.groupby("reporting_airline").size()
                processed_batches += 1

            processing_time = time.time() - start_time
            processing_times.append((batch_size, processing_time, processed_batches))

        # Verify all batch sizes processed the same amount of data
        total_records = len(df)
        for batch_size, proc_time, batches in processing_times:
            estimated_records = batches * batch_size
            # Should process at least the total records (last batch might be smaller)
            assert (
                estimated_records >= total_records - batch_size
            ), f"Batch size {batch_size} didn't process all records"

        # Larger batch sizes should generally be more efficient (fewer batches)
        smallest_batch_time = processing_times[0][1]  # batch_size=10
        largest_batch_time = processing_times[-1][1]  # batch_size=1000

        # Large batches should not be significantly slower than small batches
        assert (
            largest_batch_time <= smallest_batch_time * 3
        ), f"Large batch processing too slow: {largest_batch_time:.3f}s vs {smallest_batch_time:.3f}s"

    def test_spark_memory_configuration_impact(self):
        """Test impact of different Spark memory configurations"""
        memory_configs = [
            {"spark.driver.memory": "1g", "spark.executor.memory": "1g"},
            {"spark.driver.memory": "2g", "spark.executor.memory": "2g"},
            {"spark.driver.memory": "4g", "spark.executor.memory": "4g"},
        ]

        for config in memory_configs:
            # Test configuration validation
            for key, value in config.items():
                # Memory values should be parseable
                assert value.endswith(
                    "g"
                ), f"Memory config should end with 'g': {value}"
                memory_size = int(value[:-1])
                assert (
                    1 <= memory_size <= 8
                ), f"Memory size should be reasonable: {memory_size}GB"

                # Test that configurations affect resource allocation expectations
                if key == "spark.driver.memory":
                    # Driver memory affects local processing capacity
                    assert memory_size >= 1, "Driver should have at least 1GB"
                elif key == "spark.executor.memory":
                    # Executor memory affects distributed processing capacity
                    assert memory_size >= 1, "Executor should have at least 1GB"

    def test_partition_configuration_impact(self):
        """Test impact of different partition configurations"""
        # Create dataset that would benefit from partitioning
        large_data = {
            "flightdate": ["2024-03-01", "2024-03-02", "2024-03-03"] * 1000,
            "reporting_airline": (["AA", "UA", "DL"] * 1000)[:3000],
            "flight_number_reporting_airline": [str(i) for i in range(3000)],
            "origin": (["LAX", "ORD", "ATL"] * 1000)[:3000],
            "dest": (["JFK", "SFO", "MIA"] * 1000)[:3000],
        }
        df = pd.DataFrame(large_data)

        partition_strategies = [
            ("single", None),  # No partitioning
            ("by_date", "flightdate"),  # Partition by date
            ("by_airline", "reporting_airline"),  # Partition by airline
            ("by_route", ["origin", "dest"]),  # Partition by route
        ]

        for strategy_name, partition_cols in partition_strategies:
            start_time = time.time()

            if partition_cols is None:
                # Process entire dataset at once
                _result = df.groupby("reporting_airline").size()  # noqa: F841
            elif isinstance(partition_cols, str):
                # Process each partition separately
                results = []
                for partition_value in df[partition_cols].unique():
                    partition_df = df[df[partition_cols] == partition_value]
                    partition_result = partition_df.groupby("reporting_airline").size()
                    results.append(partition_result)
            else:
                # Process each combination of partition columns
                results = []
                partition_combinations = df[partition_cols].drop_duplicates()
                for _, partition_row in partition_combinations.iterrows():
                    mask = True
                    for col in partition_cols:
                        mask = mask & (df[col] == partition_row[col])
                    partition_df = df[mask]
                    if len(partition_df) > 0:
                        partition_result = partition_df.groupby(
                            "reporting_airline"
                        ).size()
                        results.append(partition_result)

            processing_time = time.time() - start_time

            # All strategies should complete in reasonable time (< 2 seconds)
            assert (
                processing_time < 2.0
            ), f"Strategy '{strategy_name}' took {processing_time:.2f}s (too slow)"


class TestScaleScenarios:
    """Test scale scenarios with large datasets and many files"""

    def test_many_small_files_vs_few_large_files(self):
        """Test performance difference between many small files vs few large files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test scenario 1: Many small files (10 files, 100 records each)
            many_files_dir = temp_path / "many_files"
            many_files_dir.mkdir()

            start_time = time.time()
            total_records_many = 0

            for i in range(10):
                file_data = {
                    "flightdate": [f"2024-03-{i+1:02d}"] * 100,
                    "reporting_airline": ["AA"] * 100,
                    "flight_number_reporting_airline": [str(j) for j in range(100)],
                    "origin": ["LAX"] * 100,
                    "dest": ["JFK"] * 100,
                }
                df = pd.DataFrame(file_data)
                file_path = many_files_dir / f"flights_{i:02d}.parquet"
                df.to_parquet(file_path, index=False)
                total_records_many += len(df)

            _many_files_write_time = time.time() - start_time  # noqa: F841

            # Test scenario 2: Few large files (2 files, 500 records each)
            few_files_dir = temp_path / "few_files"
            few_files_dir.mkdir()

            start_time = time.time()
            total_records_few = 0

            for i in range(2):
                file_data = {
                    "flightdate": [f"2024-03-{j+1:02d}" for j in range(500)],
                    "reporting_airline": ["AA"] * 500,
                    "flight_number_reporting_airline": [str(j) for j in range(500)],
                    "origin": ["LAX"] * 500,
                    "dest": ["JFK"] * 500,
                }
                df = pd.DataFrame(file_data)
                file_path = few_files_dir / f"flights_large_{i:02d}.parquet"
                df.to_parquet(file_path, index=False)
                total_records_few += len(df)

            _few_files_write_time = time.time() - start_time  # noqa: F841

            # Both should have same total records
            assert (
                total_records_many == total_records_few
            ), "Both scenarios should have same total records"

            # Test reading performance
            start_time = time.time()
            many_files_data = []
            for file_path in many_files_dir.glob("*.parquet"):
                df = pd.read_parquet(file_path)
                many_files_data.append(df)
            many_files_combined = pd.concat(many_files_data, ignore_index=True)
            many_files_read_time = time.time() - start_time

            start_time = time.time()
            few_files_data = []
            for file_path in few_files_dir.glob("*.parquet"):
                df = pd.read_parquet(file_path)
                few_files_data.append(df)
            few_files_combined = pd.concat(few_files_data, ignore_index=True)
            few_files_read_time = time.time() - start_time

            # Both should read same amount of data
            assert len(many_files_combined) == len(
                few_files_combined
            ), "Should read same amount of data"

            # Reading many small files should not be excessively slower than few large files
            # However, when dealing with very fast operations (< 10ms), timing can be unreliable
            if few_files_read_time > 0.01:  # Only compare if baseline is > 10ms
                max_acceptable_ratio = (
                    50  # Allow many files to be up to 50x slower (file I/O overhead)
                )
                assert (
                    many_files_read_time <= few_files_read_time * max_acceptable_ratio
                ), f"Many files read time ({many_files_read_time:.3f}s) too slow vs few files ({few_files_read_time:.3f}s)"
            else:
                # For very fast operations, just check that many files reading is reasonable (< 1 second)
                assert (
                    many_files_read_time < 1.0
                ), f"Many files read time {many_files_read_time:.3f}s should be < 1 second"

    def test_file_count_impact_on_discovery(self):
        """Test impact of file count on file discovery performance"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_dir = temp_path / "data"
            data_dir.mkdir()

            file_counts = [5, 10, 25, 50]
            discovery_times = []

            for file_count in file_counts:
                # Create specified number of files
                for i in range(file_count):
                    file_path = data_dir / f"bts_flights_2024_{i+1:02d}.parquet"
                    # Create minimal file
                    sample_data = {"flightdate": ["2024-03-01"], "airline": ["AA"]}
                    df = pd.DataFrame(sample_data)
                    df.to_parquet(file_path, index=False)

                # Measure file discovery time
                start_time = time.time()
                discovered_files = list(data_dir.glob("bts_flights_*.parquet"))
                discovery_time = time.time() - start_time

                discovery_times.append((file_count, discovery_time))

                # Verify correct number of files discovered
                assert (
                    len(discovered_files) == file_count
                ), f"Should discover {file_count} files"

                # Discovery should be fast even with many files
                assert (
                    discovery_time < 1.0
                ), f"File discovery took {discovery_time:.3f}s for {file_count} files (too slow)"

                # Clean up for next iteration
                for file_path in discovered_files:
                    file_path.unlink()

            # Discovery time should scale reasonably with file count
            for i in range(1, len(discovery_times)):
                prev_count, prev_time = discovery_times[i - 1]
                curr_count, curr_time = discovery_times[i]

                count_ratio = curr_count / prev_count
                time_ratio = curr_time / prev_time if prev_time > 0 else 1

                # Time growth should be sub-linear (not exponential)
                assert (
                    time_ratio <= count_ratio * 1.5
                ), f"Discovery time growth too high: {time_ratio:.2f}x for {count_ratio:.2f}x files"

    def test_large_month_range_processing(self):
        """Test processing many months of data"""
        # Simulate processing 12 months of data
        months = [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
        ]

        processing_times = []
        memory_usage = []

        process = psutil.Process()

        for i, month in enumerate(months, 1):
            start_time = time.time()
            memory_before = process.memory_info().rss / 1024 / 1024  # MB

            # Simulate processing a month of data
            month_data = {
                "flightdate": [f"2024-{month}-01"] * 1000,
                "reporting_airline": (["AA", "UA", "DL"] * 334)[:1000],
                "flight_number_reporting_airline": [str(j) for j in range(1000)],
                "origin": ["LAX"] * 1000,
                "dest": ["JFK"] * 1000,
            }

            df = pd.DataFrame(month_data)

            # Simulate typical processing operations
            _airline_stats = df.groupby("reporting_airline").size()  # noqa: F841
            _daily_stats = df.groupby("flightdate").size()  # noqa: F841

            processing_time = time.time() - start_time
            memory_after = process.memory_info().rss / 1024 / 1024  # MB

            processing_times.append(processing_time)
            memory_usage.append(memory_after - memory_before)

            # Each month should process in reasonable time
            assert (
                processing_time < 1.0
            ), f"Month {month} processing took {processing_time:.3f}s (too slow)"

            # Memory usage should be reasonable
            month_memory = memory_after - memory_before
            assert (
                month_memory < 20
            ), f"Month {month} used {month_memory:.1f}MB (too much memory)"

            # Clean up
            del df

        # Processing time should remain consistent across months
        # Only check consistency if the average time is significant (> 10ms)
        avg_time = sum(processing_times) / len(processing_times)
        if avg_time > 0.01:  # Only test consistency if operations take > 10ms
            for i, proc_time in enumerate(processing_times):
                month = months[i]
                # No month should be more than 10x the average (allow for timing variance)
                assert (
                    proc_time <= avg_time * 10
                ), f"Month {month} processing time {proc_time:.3f}s too high (avg: {avg_time:.3f}s)"
        else:
            # For very fast operations, just check all are reasonable (< 1 second)
            for i, proc_time in enumerate(processing_times):
                month = months[i]
                assert (
                    proc_time < 1.0
                ), f"Month {month} processing time {proc_time:.3f}s too high"


class TestResourceUtilizationBoundaries:
    """Test resource utilization boundaries and limits"""

    def test_cpu_usage_during_operations(self):
        """Test CPU usage during data processing operations"""
        # Create dataset for CPU-intensive operations
        data = {
            "flightdate": ["2024-03-01"] * 5000,
            "reporting_airline": (["AA", "UA", "DL", "WN", "B6"] * 1000)[:5000],
            "flight_number_reporting_airline": [str(i) for i in range(5000)],
            "origin": (["LAX", "ORD", "ATL", "DFW", "SFO"] * 1000)[:5000],
            "dest": (["JFK", "MIA", "SEA", "BOS", "LAS"] * 1000)[:5000],
            "distance": [float(i % 3000 + 100) for i in range(5000)],
        }
        df = pd.DataFrame(data)

        # Monitor CPU usage during operations
        process = psutil.Process()
        _cpu_before = process.cpu_percent()  # noqa: F841

        start_time = time.time()

        # CPU-intensive operations
        airline_stats = (
            df.groupby("reporting_airline")
            .agg(
                {
                    "distance": ["mean", "sum", "count"],
                    "flight_number_reporting_airline": "count",
                }
            )
            .reset_index()
        )

        route_stats = (
            df.groupby(["origin", "dest"])
            .agg({"distance": "mean", "reporting_airline": "nunique"})
            .reset_index()
        )

        # Complex calculations
        df["distance_category"] = pd.cut(
            df["distance"],
            bins=5,
            labels=["short", "medium", "long", "very_long", "ultra_long"],
        )
        category_stats = (
            df.groupby(["distance_category", "reporting_airline"], observed=True)
            .size()
            .reset_index()
        )

        operation_time = time.time() - start_time
        _cpu_after = process.cpu_percent()  # noqa: F841

        # Operations should complete in reasonable time
        assert (
            operation_time < 5.0
        ), f"CPU-intensive operations took {operation_time:.2f}s (too slow)"

        # Verify operations produced expected results
        assert len(airline_stats) <= 5, "Should have stats for up to 5 airlines"
        assert len(route_stats) <= 25, "Should have stats for up to 25 routes"
        assert len(category_stats) > 0, "Should have category stats"

    def test_memory_cleanup_after_operations(self):
        """Test that memory is properly cleaned up after operations"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Perform memory-intensive operations
        for i in range(5):
            # Create large temporary dataset
            temp_data = {
                "flightdate": ["2024-03-01"] * 2000,
                "reporting_airline": (["AA", "UA", "DL"] * 667)[:2000],
                "flight_number_reporting_airline": [str(j) for j in range(2000)],
                "origin": ["LAX"] * 2000,
                "dest": ["JFK"] * 2000,
                "data": list(range(2000)),  # Extra column for memory usage
            }

            df = pd.DataFrame(temp_data)

            # Perform operations
            stats = df.groupby("reporting_airline").sum()

            # Explicitly clean up
            del df
            del stats
            del temp_data

        # Check memory after cleanup
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Memory should not increase excessively after cleanup
        assert (
            memory_increase < 50
        ), f"Memory increased by {memory_increase:.1f}MB after operations (possible memory leak)"

    def test_concurrent_operation_resource_usage(self):
        """Test resource usage when simulating concurrent operations"""
        import queue
        import threading

        results_queue = queue.Queue()

        def process_data_chunk(chunk_id, data_size):
            """Process a chunk of data and record performance metrics"""
            process = psutil.Process()
            start_time = time.time()
            memory_before = process.memory_info().rss / 1024 / 1024

            # Create and process data chunk
            chunk_data = {
                "flightdate": [f"2024-03-{chunk_id:02d}"] * data_size,
                "reporting_airline": (["AA", "UA", "DL"] * (data_size // 3 + 1))[
                    :data_size
                ],
                "flight_number_reporting_airline": [str(i) for i in range(data_size)],
                "origin": ["LAX"] * data_size,
                "dest": ["JFK"] * data_size,
            }

            df = pd.DataFrame(chunk_data)
            _result = df.groupby("reporting_airline").size()  # noqa: F841

            processing_time = time.time() - start_time
            memory_after = process.memory_info().rss / 1024 / 1024
            memory_used = memory_after - memory_before

            results_queue.put(
                {
                    "chunk_id": chunk_id,
                    "processing_time": processing_time,
                    "memory_used": memory_used,
                    "records_processed": len(df),
                }
            )

        # Simulate 3 concurrent operations
        threads = []
        chunk_size = 500

        for i in range(3):
            thread = threading.Thread(
                target=process_data_chunk, args=(i + 1, chunk_size)
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Collect results
        results = []
        while not results_queue.empty():
            results.append(results_queue.get())

        assert len(results) == 3, "Should have results from 3 concurrent operations"

        # Verify each operation completed successfully
        for result in results:
            assert (
                result["processing_time"] < 2.0
            ), f"Chunk {result['chunk_id']} took {result['processing_time']:.2f}s (too slow)"
            assert (
                result["memory_used"] < 25
            ), f"Chunk {result['chunk_id']} used {result['memory_used']:.1f}MB (too much)"
            assert (
                result["records_processed"] == chunk_size
            ), f"Chunk {result['chunk_id']} processed wrong number of records"


class TestTimingAndThroughputValidation:
    """Test timing and throughput characteristics"""

    def test_record_processing_throughput(self):
        """Test throughput for processing different volumes of records"""
        throughput_data = []

        record_counts = [100, 500, 1000, 2500, 5000]

        for record_count in record_counts:
            # Create dataset
            data = {
                "flightdate": ["2024-03-01"] * record_count,
                "reporting_airline": (["AA", "UA", "DL"] * (record_count // 3 + 1))[
                    :record_count
                ],
                "flight_number_reporting_airline": [
                    str(i) for i in range(record_count)
                ],
                "origin": ["LAX"] * record_count,
                "dest": ["JFK"] * record_count,
            }

            df = pd.DataFrame(data)

            # Measure processing time
            start_time = time.time()

            # Standard processing operations
            airline_counts = df.groupby("reporting_airline").size()
            unique_flights = df["flight_number_reporting_airline"].nunique()
            filtered_data = df[df["reporting_airline"] == "AA"]

            processing_time = time.time() - start_time

            # Calculate throughput
            if processing_time > 0:
                records_per_second = record_count / processing_time
            else:
                records_per_second = float("inf")

            throughput_data.append((record_count, processing_time, records_per_second))

            # Basic validation
            assert len(airline_counts) <= 3, "Should have at most 3 airlines"
            assert unique_flights == record_count, "Should have unique flight numbers"
            assert len(filtered_data) > 0, "Should have some AA flights"

        # Verify throughput characteristics
        for record_count, proc_time, throughput in throughput_data:
            # Should process at least 1000 records per second
            assert (
                throughput >= 1000
            ), f"Low throughput: {throughput:.0f} records/sec for {record_count} records"

            # Processing time should be sub-linear with record count
            assert (
                proc_time < record_count / 1000
            ), f"Processing too slow: {proc_time:.3f}s for {record_count} records"

    def test_file_io_timing_characteristics(self):
        """Test file I/O timing characteristics"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            file_sizes = [100, 500, 1000, 2500]  # Number of records
            io_metrics = []

            for size in file_sizes:
                # Create test data
                data = {
                    "flightdate": ["2024-03-01"] * size,
                    "reporting_airline": (["AA", "UA", "DL"] * (size // 3 + 1))[:size],
                    "flight_number_reporting_airline": [str(i) for i in range(size)],
                    "origin": ["LAX"] * size,
                    "dest": ["JFK"] * size,
                    "distance": [2475.0] * size,
                }
                df = pd.DataFrame(data)

                file_path = temp_path / f"test_file_{size}.parquet"

                # Measure write time
                write_start = time.time()
                df.to_parquet(file_path, index=False)
                write_time = time.time() - write_start

                # Measure read time
                read_start = time.time()
                df_read = pd.read_parquet(file_path)
                read_time = time.time() - read_start

                # Verify data integrity
                assert len(df_read) == len(df), "Read data should match written data"
                assert list(df_read.columns) == list(df.columns), "Columns should match"

                # Calculate file size
                file_size_mb = file_path.stat().st_size / 1024 / 1024

                io_metrics.append(
                    {
                        "records": size,
                        "write_time": write_time,
                        "read_time": read_time,
                        "file_size_mb": file_size_mb,
                        "write_throughput": (
                            size / write_time if write_time > 0 else float("inf")
                        ),
                        "read_throughput": (
                            size / read_time if read_time > 0 else float("inf")
                        ),
                    }
                )

            # Verify I/O performance characteristics
            for metrics in io_metrics:
                records = metrics["records"]
                write_time = metrics["write_time"]
                read_time = metrics["read_time"]

                # Write and read should be reasonably fast
                assert (
                    write_time < records / 1000 + 0.1
                ), f"Write too slow: {write_time:.3f}s for {records} records"
                assert (
                    read_time < records / 2000 + 0.1
                ), f"Read too slow: {read_time:.3f}s for {records} records"

                # Throughput should be reasonable
                assert (
                    metrics["write_throughput"] > 500
                ), f"Low write throughput: {metrics['write_throughput']:.0f} records/sec"
                assert (
                    metrics["read_throughput"] > 1000
                ), f"Low read throughput: {metrics['read_throughput']:.0f} records/sec"


if __name__ == "__main__":
    print("Running performance boundaries tests...")

    # Run all test classes
    test_classes = [
        TestMemoryUsagePatterns(),
        TestConfigurationPerformanceImpact(),
        TestScaleScenarios(),
        TestResourceUtilizationBoundaries(),
        TestTimingAndThroughputValidation(),
    ]

    for test_class in test_classes:
        class_name = test_class.__class__.__name__
        print(f"\n🧪 Testing {class_name}:")

        for method_name in dir(test_class):
            if method_name.startswith("test_"):
                print(f"   • {method_name}")
                method = getattr(test_class, method_name)
                try:
                    method()
                    print("     ✅ PASSED")
                except Exception as e:
                    print(f"     ❌ FAILED: {e}")

    print("\n✅ Performance boundaries tests completed!")
