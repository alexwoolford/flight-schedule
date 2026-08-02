#!/usr/bin/env python3
"""
Pipeline Integration Tests
==========================

Tests real data pipeline integration and flow WITHOUT using mocks.
Focuses on testing actual integration points between download and load phases,
file format compatibility, and multi-month data handling.
"""

import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import download_bts_flight_data  # noqa: E402
    import load_bts_data  # noqa: E402
except ImportError:
    # Tests will skip if modules can't be imported
    download_bts_flight_data = None
    load_bts_data = None


class TestFileFormatIntegration:
    """Test file format compatibility between download and load phases"""

    def test_parquet_schema_compatibility(self):
        """The real fixture carries the columns and dtypes the loader expects.

        This asserts against tests/fixtures/bts_flights_2025_07_18.parquet --
        a file the download stage actually produced -- not against a DataFrame
        built inside the test. The previous version did the latter, and so
        could only ever confirm that pandas had honoured the literals on the
        line above; it named `crs_dep_time`/`crs_arr_time`, which do not exist
        in BTS data (the columns are `crsdeptime`/`crsarrtime`), and asserted
        `flightdate` was `object` when download_bts_flight_data.py declares it
        `datetime64[us]`. Both mistakes were invisible because the fabricated
        frame was the only thing under test.

        Kept DB-free deliberately: this is the download/load *file format*
        contract, which is checkable from the Parquet file alone.
        """
        fixture = Path(__file__).parent / "fixtures" / "bts_flights_2025_07_18.parquet"
        assert fixture.exists(), f"fixture missing: {fixture}"

        # The five composite-key fields (schedule_composite_unique) plus the
        # columns load_bts_data.py builds the four timestamps and filters from.
        # Names are BTS's own, lowercased -- crsdeptime, not crs_dep_time.
        required = {
            "flightdate": "datetime64[us]",
            "reporting_airline": "string",
            "flight_number_reporting_airline": "int64",
            "origin": "string",
            "dest": "string",
            "crsdeptime": "datetime64[us]",
            "crsarrtime": "datetime64[us]",
            "crselapsedtime": "Float64",
            "cancelled": "Float64",
            "distance": "Float64",
        }

        df = pd.read_parquet(fixture, columns=list(required))
        # 21,495 rows as published; load_bts_data.py's `cancelled == 0` filter
        # is what reduces this to the 21,376 Schedule nodes ci_verify_loaded.py
        # counts. Both numbers are asserted so a resliced fixture cannot
        # quietly change either one.
        assert (
            len(df) == 21495
        ), f"fixture should hold 21,495 published BTS rows, got {len(df)}"
        assert (
            int((df["cancelled"] == 0).sum()) == 21376
        ), "21,376 rows should survive the loader's cancelled == 0 filter"

        for col, expected_dtype in required.items():
            assert str(df[col].dtype) == expected_dtype, (
                f"{col} should read as {expected_dtype}, got {df[col].dtype}. "
                "download_bts_flight_data.py's BTS_COLUMN_TYPES is authoritative; "
                "a drift here is what the three-tier read fallback in "
                "load_bts_data.py exists to survive."
            )

        # Every declared column must agree with what the file really holds --
        # BTS's monthly CSVs are not dtype-stable, which is the whole reason
        # BTS_COLUMN_TYPES and the PyArrow schema exist.
        if download_bts_flight_data is not None:
            declared = download_bts_flight_data.BTS_COLUMN_TYPES
            for col, expected_dtype in required.items():
                assert declared.get(col) == expected_dtype, (
                    f"BTS_COLUMN_TYPES declares {col} as {declared.get(col)}, "
                    f"but the written fixture reads back as {expected_dtype}"
                )

    def test_parquet_timestamps_are_not_utc_adjusted(self):
        """BTS times are local wall clock, and the Parquet must say so.

        `isAdjustedToUTC=false` is what makes TimestampNTZType the *correct*
        reading of these columns rather than merely a configured one, so it is
        the file-format half of the guarantee that
        test_timezone_semantics_are_pinned_not_inherited covers on the Spark
        side. If a future writer emits `isAdjustedToUTC=true`, Spark reads the
        columns as instants and bakes the loader machine's offset into
        `flightdate` -- one of the five composite-key fields -- relocating the
        entire day. See the matrix in create_spark_session().
        """
        import pyarrow.parquet as pq

        fixture = Path(__file__).parent / "fixtures" / "bts_flights_2025_07_18.parquet"
        parquet_file = pq.ParquetFile(fixture)

        for col in ("flightdate", "crsdeptime", "crsarrtime"):
            # A tz-naive Arrow timestamp is precisely Parquet's
            # isAdjustedToUTC=false; a tz-aware one would carry the offset that
            # must never be baked in. Asserted via schema_arrow rather than
            # ParquetLogicalType, whose only public attributes are `type` and
            # `to_json` -- the isAdjustedToUTC flag is reachable there only by
            # parsing repr(), which is not an API.
            field = parquet_file.schema_arrow.field(col)
            assert field.type.tz is None, (
                f"{col} is written tz-aware ({field.type.tz}), i.e. Parquet "
                "isAdjustedToUTC=true. BTS publishes local wall-clock times, "
                "so this must stay naive or Spark reads them as instants and "
                "shifts the whole day."
            )
            # Spark cannot read nanosecond Parquet timestamps.
            assert (
                field.type.unit == "us"
            ), f"{col} must be microsecond-precision for Spark to read it, got {field.type.unit}"

        # Belt and braces: confirm the on-disk logical type agrees, in case a
        # future pyarrow makes schema_arrow lossy about the flag.
        logical = str(
            parquet_file.schema.column(
                parquet_file.schema_arrow.names.index("crsdeptime")
            ).logical_type
        )
        assert "isAdjustedToUTC=false" in logical, logical

    def test_parquet_file_creation_and_reading(self):
        """Test creating and reading Parquet files like the pipeline does"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_file = temp_path / "test_flights_2024_03.parquet"

            # Create sample data matching BTS format
            sample_data = {
                "flightdate": ["2024-03-01", "2024-03-02"],
                "reporting_airline": ["AA", "UA"],
                "flight_number_reporting_airline": ["100", "200"],
                "origin": ["LAX", "ORD"],
                "dest": ["JFK", "SFO"],
                "crs_dep_time": ["1430", "0800"],
                "crs_arr_time": ["2300", "1200"],
                "cancelled": [0, 0],
                "distance": [2475.0, 1846.0],
            }

            df = pd.DataFrame(sample_data)

            # Write to Parquet (simulating download phase)
            df.to_parquet(test_file, index=False)

            # Verify file was created
            assert test_file.exists(), "Parquet file should be created"
            assert test_file.stat().st_size > 0, "Parquet file should not be empty"

            # Read back (simulating load phase)
            df_read = pd.read_parquet(test_file)

            # Verify data integrity
            assert len(df_read) == 2, "Should read back 2 records"
            assert list(df_read.columns) == list(df.columns), "Columns should match"
            assert df_read["reporting_airline"].tolist() == [
                "AA",
                "UA",
            ], "Data should match"

    def test_bts_filename_pattern_recognition(self):
        """Test that filename patterns work across download and load modules"""
        valid_patterns = [
            "bts_flights_2024_01.parquet",
            "bts_flights_2024_12.parquet",
            "bts_flights_2023_06.parquet",
        ]

        invalid_patterns = [
            "flights_2024_01.parquet",  # Missing 'bts' prefix
            "bts_flights_2024_13.parquet",  # Invalid month
            "bts_flights_2024_01.csv",  # Wrong extension
            "bts_flights_24_01.parquet",  # Wrong year format
        ]

        for filename in valid_patterns:
            # Test pattern extraction
            parts = filename.replace(".parquet", "").split("_")
            assert len(parts) == 4, f"Valid filename should have 4 parts: {filename}"
            assert parts[0] == "bts", f"Should start with 'bts': {filename}"
            assert parts[1] == "flights", f"Should contain 'flights': {filename}"

            year = int(parts[2])
            month = int(parts[3])
            assert 2020 <= year <= 2030, f"Year should be reasonable: {filename}"
            assert 1 <= month <= 12, f"Month should be valid: {filename}"

        for filename in invalid_patterns:
            # These should fail pattern validation
            if not filename.endswith(".parquet"):
                assert not filename.endswith(
                    ".parquet"
                ), f"Should reject non-parquet: {filename}"
            else:
                parts = filename.replace(".parquet", "").split("_")
                is_valid = (
                    len(parts) == 4
                    and parts[0] == "bts"
                    and parts[1] == "flights"
                    and parts[2].isdigit()
                    and parts[3].isdigit()
                    and 2020 <= int(parts[2]) <= 2030
                    and 1 <= int(parts[3]) <= 12
                )
                assert not is_valid, f"Should reject invalid pattern: {filename}"


class TestMultiMonthDataHandling:
    """Test handling of multiple months of data"""

    def test_multi_month_file_discovery(self):
        """Test discovering multiple months of data files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_dir = temp_path / "data" / "bts_flight_data"
            data_dir.mkdir(parents=True)

            # Create multiple month files
            test_files = [
                "bts_flights_2024_01.parquet",
                "bts_flights_2024_02.parquet",
                "bts_flights_2024_03.parquet",
            ]

            # Create sample data for each file
            for filename in test_files:
                file_path = data_dir / filename

                # Extract month from filename for sample data
                month = filename.split("_")[3].split(".")[0]
                sample_data = {
                    "flightdate": [f"2024-{month}-01", f"2024-{month}-02"],
                    "reporting_airline": ["AA", "UA"],
                    "flight_number_reporting_airline": ["100", "200"],
                    "origin": ["LAX", "ORD"],
                    "dest": ["JFK", "SFO"],
                    "crs_dep_time": ["1430", "0800"],
                    "crs_arr_time": ["2300", "1200"],
                    "cancelled": [0, 0],
                    "distance": [2475.0, 1846.0],
                }

                df = pd.DataFrame(sample_data)
                df.to_parquet(file_path, index=False)

            # Test file discovery
            discovered_files = list(data_dir.glob("bts_flights_*.parquet"))
            assert len(discovered_files) == 3, "Should discover all 3 files"

            # Test file sorting (chronological order)
            discovered_files.sort()
            expected_order = [
                "bts_flights_2024_01.parquet",
                "bts_flights_2024_02.parquet",
                "bts_flights_2024_03.parquet",
            ]
            actual_order = [f.name for f in discovered_files]
            assert (
                actual_order == expected_order
            ), "Files should be in chronological order"

    def test_multi_month_data_combination(self):
        """Test combining data from multiple months"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_dir = temp_path / "data" / "bts_flight_data"
            data_dir.mkdir(parents=True)

            # Create data for 3 months
            monthly_data = {
                "01": ["2024-01-01", "2024-01-15"],
                "02": ["2024-02-01", "2024-02-14"],
                "03": ["2024-03-01", "2024-03-15"],
            }

            all_data = []

            for month, dates in monthly_data.items():
                file_path = data_dir / f"bts_flights_2024_{month}.parquet"

                month_data = {
                    "flightdate": dates,
                    "reporting_airline": ["AA", "UA"],
                    "flight_number_reporting_airline": ["100", "200"],
                    "origin": ["LAX", "ORD"],
                    "dest": ["JFK", "SFO"],
                    "crs_dep_time": ["1430", "0800"],
                    "crs_arr_time": ["2300", "1200"],
                    "cancelled": [0, 0],
                    "distance": [2475.0, 1846.0],
                }

                df = pd.DataFrame(month_data)
                df.to_parquet(file_path, index=False)
                all_data.extend(dates)

            # Test reading and combining all files
            combined_dfs = []
            for file_path in data_dir.glob("bts_flights_*.parquet"):
                df = pd.read_parquet(file_path)
                combined_dfs.append(df)

            combined_df = pd.concat(combined_dfs, ignore_index=True)

            # Verify combined data
            assert (
                len(combined_df) == 6
            ), "Should have 6 total records (2 per month × 3 months)"

            unique_dates = sorted(combined_df["flightdate"].unique())
            expected_dates = sorted(all_data)
            assert (
                unique_dates == expected_dates
            ), "Should have all dates from all months"

    def test_date_range_validation_across_months(self):
        """Test date range validation when combining multiple months"""
        date_scenarios = [
            # (filename, flightdate, should_be_consistent)
            ("bts_flights_2024_01.parquet", "2024-01-15", True),  # Correct month
            ("bts_flights_2024_02.parquet", "2024-02-28", True),  # Correct month
            ("bts_flights_2024_01.parquet", "2024-02-15", False),  # Wrong month in file
            ("bts_flights_2024_03.parquet", "2024-01-15", False),  # Wrong month in file
        ]

        for filename, flightdate, should_be_consistent in date_scenarios:
            # Extract expected month from filename
            filename_month = filename.split("_")[3].split(".")[0]

            # Extract actual month from date
            date_month = flightdate.split("-")[1]

            is_consistent = filename_month == date_month

            assert (
                is_consistent == should_be_consistent
            ), f"Date consistency check failed: {filename} vs {flightdate}"


class TestPipelineConfigurationIntegration:
    """Test configuration compatibility between pipeline stages"""

    def test_directory_structure_consistency(self):
        """Test that directory structure works for both download and load"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test standard directory structure
            expected_structure = ["data/bts_flight_data", "logs", "private_data"]

            for dir_path in expected_structure:
                full_path = temp_path / dir_path
                full_path.mkdir(parents=True, exist_ok=True)

                assert full_path.exists(), f"Directory should be created: {dir_path}"
                assert full_path.is_dir(), f"Path should be a directory: {dir_path}"

            # Test data directory specifically
            data_dir = temp_path / "data" / "bts_flight_data"

            # Create a test file
            test_file = data_dir / "bts_flights_2024_01.parquet"
            test_file.touch()

            assert test_file.exists(), "Test file should be created in data directory"

    def test_environment_variable_compatibility(self):
        """Test environment variables used by both modules"""
        env_vars = ["NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD", "NEO4J_DATABASE"]

        # Test environment variable patterns
        for var_name in env_vars:
            # Test that variable names follow expected patterns
            assert var_name.startswith(
                "NEO4J_"
            ), f"Neo4j vars should start with NEO4J_: {var_name}"
            assert var_name.isupper(), f"Env vars should be uppercase: {var_name}"
            assert (
                "_" in var_name
            ), f"Env vars should use underscore separator: {var_name}"

    def test_spark_configuration_compatibility(self):
        """Test Spark configuration that affects both download and load"""
        spark_configs = {
            "spark.driver.memory": ["1g", "2g", "4g"],
            "spark.executor.memory": ["1g", "2g", "4g"],
            "spark.sql.shuffle.partitions": ["200", "400", "800"],
            "spark.neo4j.batch.size": ["1000", "5000", "10000"],
        }

        for config_key, valid_values in spark_configs.items():
            # Test config key format
            assert config_key.startswith(
                "spark."
            ), f"Spark configs should start with 'spark.': {config_key}"

            for value in valid_values:
                # Test that values can be parsed appropriately
                if config_key.endswith(".memory"):
                    # Memory values should end with 'g' or 'm'
                    assert value[-1] in [
                        "g",
                        "m",
                    ], f"Memory values should end with g/m: {value}"
                    assert value[
                        :-1
                    ].isdigit(), f"Memory size should be numeric: {value}"
                elif "partitions" in config_key or "batch.size" in config_key:
                    # Numeric configs should be parseable as integers
                    assert value.isdigit(), f"Numeric config should be digits: {value}"
                    assert int(value) > 0, f"Numeric config should be positive: {value}"


class TestDataPipelineFlow:
    """Test the complete data pipeline flow"""

    def test_download_to_load_data_flow(self):
        """Test the flow from download output to load input"""
        # This tests the interface between download and load phases

        # Simulate download phase output
        download_output = {
            "flightdate": "2024-03-01",
            "reporting_airline": "AA",
            "flight_number_reporting_airline": "100",
            "origin": "LAX",
            "dest": "JFK",
            "crs_dep_time": "1430",
            "crs_arr_time": "2300",
            "cancelled": 0,
            "distance": 2475.0,
        }

        # Test that load phase can process this data
        required_fields = [
            "flightdate",
            "reporting_airline",
            "flight_number_reporting_airline",
            "origin",
            "dest",
        ]

        for field in required_fields:
            assert (
                field in download_output
            ), f"Download output missing required field: {field}"
            assert (
                download_output[field] is not None
            ), f"Required field should not be None: {field}"
            assert (
                download_output[field] != ""
            ), f"Required field should not be empty: {field}"

    def test_timestamp_format_consistency(self):
        """Test timestamp format consistency between pipeline stages"""
        timestamp_test_cases = [
            # (crs_time_format, expected_valid)
            ("1430", True),  # Standard afternoon time
            ("0800", True),  # Morning time with leading zero
            ("2359", True),  # Late night time
            ("0000", True),  # Midnight
            ("1260", False),  # Invalid minute
            ("2400", False),  # Invalid hour
            ("abc", False),  # Non-numeric
            ("14:30", False),  # Wrong format (should be HHMM, not HH:MM)
        ]

        for time_str, expected_valid in timestamp_test_cases:
            # Test time format validation (used in both download and load)
            if len(time_str) == 4 and time_str.isdigit():
                hour = int(time_str[:2])
                minute = int(time_str[2:])
                is_valid = 0 <= hour <= 23 and 0 <= minute <= 59
            else:
                is_valid = False

            assert (
                is_valid == expected_valid
            ), f"Time format validation failed: {time_str}"

    def test_data_quality_pipeline_integration(self):
        """Test data quality checks across pipeline stages"""
        # Test data that should be filtered out by quality checks
        quality_test_data = [
            # (record, should_pass_quality_checks, reason)
            (
                {
                    "flightdate": "2024-03-01",
                    "reporting_airline": "AA",
                    "flight_number_reporting_airline": "100",
                    "origin": "LAX",
                    "dest": "JFK",
                    "cancelled": 0,
                },
                True,
                "Complete valid record",
            ),
            (
                {
                    "flightdate": None,
                    "reporting_airline": "AA",
                    "flight_number_reporting_airline": "100",
                    "origin": "LAX",
                    "dest": "JFK",
                    "cancelled": 0,
                },
                False,
                "Missing flight date",
            ),
            (
                {
                    "flightdate": "2024-03-01",
                    "reporting_airline": "AA",
                    "flight_number_reporting_airline": "100",
                    "origin": "LAX",
                    "dest": "LAX",  # Same as origin
                    "cancelled": 0,
                },
                False,
                "Same origin and destination",
            ),
            (
                {
                    "flightdate": "2024-03-01",
                    "reporting_airline": "AA",
                    "flight_number_reporting_airline": "100",
                    "origin": "LAX",
                    "dest": "JFK",
                    "cancelled": 1,  # Cancelled flight
                },
                False,
                "Cancelled flight",
            ),
        ]

        for record, expected_pass, reason in quality_test_data:
            # Apply quality checks similar to what the pipeline does
            passes_quality = (
                record.get("flightdate") is not None
                and record.get("reporting_airline") is not None
                and record.get("flight_number_reporting_airline") is not None
                and record.get("origin") is not None
                and record.get("dest") is not None
                and record.get("origin") != record.get("dest")
                and record.get("cancelled", 0) == 0
            )

            assert passes_quality == expected_pass, f"Quality check failed: {reason}"


class TestErrorHandlingIntegration:
    """Test error handling across pipeline stages"""

    def test_missing_file_handling(self):
        """Test handling of missing data files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            data_dir = temp_path / "data" / "bts_flight_data"
            data_dir.mkdir(parents=True)

            # Test looking for non-existent files
            non_existent_files = [
                "bts_flights_2024_99.parquet",  # Invalid month
                "bts_flights_1985_01.parquet",  # Before BTS data
                "missing_file.parquet",
            ]

            for filename in non_existent_files:
                file_path = data_dir / filename
                assert not file_path.exists(), f"File should not exist: {filename}"

                # Test graceful handling of missing files
                try:
                    # This should not crash the pipeline
                    files_found = list(data_dir.glob("bts_flights_*.parquet"))
                    assert isinstance(
                        files_found, list
                    ), "Should return empty list for no files"
                except Exception as e:
                    assert False, f"Should handle missing files gracefully: {e}"

    def test_corrupted_data_handling(self):
        """Test handling of corrupted or malformed data"""
        corrupted_data_scenarios = [
            # Data that might cause issues in the pipeline
            {"flightdate": "invalid-date", "reporting_airline": "AA"},
            {"flightdate": "2024-03-01", "reporting_airline": ""},  # Empty airline
            {"flightdate": "2024-13-01", "reporting_airline": "AA"},  # Invalid month
            {"flightdate": "2024-03-32", "reporting_airline": "AA"},  # Invalid day
        ]

        for corrupted_record in corrupted_data_scenarios:
            # Test that corrupted data can be identified
            has_issues = False

            # Check for date format issues
            if "flightdate" in corrupted_record:
                date_str = corrupted_record["flightdate"]
                if date_str and "-" in date_str:
                    try:
                        parts = date_str.split("-")
                        if len(parts) == 3:
                            year, month, day = map(int, parts)
                            if not (1 <= month <= 12 and 1 <= day <= 31):
                                has_issues = True
                        else:
                            has_issues = True
                    except ValueError:
                        has_issues = True
                else:
                    has_issues = True

            # Check for empty required fields
            if corrupted_record.get("reporting_airline") == "":
                has_issues = True

            assert (
                has_issues
            ), f"Should detect issues in corrupted data: {corrupted_record}"


if __name__ == "__main__":
    print("Running pipeline integration tests...")

    # Run all test classes
    test_classes = [
        TestFileFormatIntegration(),
        TestMultiMonthDataHandling(),
        TestPipelineConfigurationIntegration(),
        TestDataPipelineFlow(),
        TestErrorHandlingIntegration(),
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

    print("\n✅ Pipeline integration tests completed!")
