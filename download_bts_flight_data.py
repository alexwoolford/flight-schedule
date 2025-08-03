#!/usr/bin/env python3
"""
Download official US government flight data from Bureau of Transportation
Statistics (BTS).
This downloads 100% factual, real flight performance data that airlines are required
to report.

Source: https://transtats.bts.gov/
Data: Airline On-Time Performance Data (1987-2025)
"""

import argparse
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# ============================================================================
# BTS DATA SCHEMA ENFORCEMENT
# ============================================================================
# Comprehensive schema definition to ensure consistent data types across
# all months, preventing ClassCastException and timestamp compatibility issues

BTS_COLUMN_TYPES = {
    # ==== CORE IDENTIFIERS (always integers) ====
    "year": "int32",
    "quarter": "int32",
    "month": "int32",
    "dayofmonth": "int32",
    "dayofweek": "int32",
    "flight_number_reporting_airline": "int64",  # Non-nullable integer (filter NULLs)
    "dot_id_reporting_airline": "int64",
    # ==== AIRLINE IDENTIFIERS (strings) ====
    "reporting_airline": "string",
    "iata_code_reporting_airline": "string",
    "tail_number": "string",
    # ==== AIRPORT IDENTIFIERS ====
    "origin": "string",  # Airport codes (LGA, JFK, etc.)
    "dest": "string",
    "originairportid": "Int64",
    "originairportseqid": "Int64",
    "destairportid": "Int64",
    "destairportseqid": "Int64",
    # ==== GEOGRAPHIC DATA ====
    "origincitymarketid": "Int64",
    "destcitymarketid": "Int64",
    "origincityname": "string",
    "destcityname": "string",
    "originstate": "string",
    "deststate": "string",
    "originstatename": "string",
    "deststatename": "string",
    "originstatefips": "Int64",
    "deststatefips": "Int64",
    "originwac": "Int64",
    "destwac": "Int64",
    # ==== TIME/DATE COLUMNS (datetime with microsecond precision) ====
    "flightdate": "datetime64[us]",
    "crsdeptime": "datetime64[us]",
    "deptime": "datetime64[us]",
    "crsarrtime": "datetime64[us]",
    "arrtime": "datetime64[us]",
    "wheelsoff": "datetime64[us]",
    "wheelson": "datetime64[us]",
    "firstdeptime": "datetime64[us]",
    # ==== TIME BLOCKS (strings) ====
    "deptimeblk": "string",
    "arrtimeblk": "string",
    # ==== DELAY AND PERFORMANCE (nullable floats) ====
    "crselapsedtime": "Float64",
    "actualelapsedtime": "Float64",
    "airtime": "Float64",
    "flights": "Float64",
    "distance": "Float64",
    "distancegroup": "Int64",
    "depdelay": "Float64",
    "depdelayminutes": "Float64",
    "depdel15": "Float64",
    "departuredelaygroups": "Float64",
    "arrdelay": "Float64",
    "arrdelayminutes": "Float64",
    "arrdel15": "Float64",
    "arrivaldelaygroups": "Float64",
    "carrierdelay": "Float64",
    "weatherdelay": "Float64",
    "nasdelay": "Float64",
    "securitydelay": "Float64",
    "lateaircraftdelay": "Float64",
    "taxiout": "Float64",
    "taxiin": "Float64",
    # ==== CANCELLATION/DIVERSION ====
    "cancelled": "Float64",  # 0.0/1.0 indicator
    "cancellationcode": "string",
    "diverted": "Float64",  # 0.0/1.0 indicator
    "divreacheddest": "Float64",
    "divactualelapsedtime": "Float64",
    "divarrdelay": "Float64",
    "divdistance": "Float64",
    "divairportlandings": "Int64",
    # ==== DIVERSION DETAILS (all nullable) ====
    "div1airport": "string",
    "div1airportid": "Float64",
    "div1airportseqid": "Float64",
    "div1wheelson": "Float64",
    "div1wheelsoff": "Float64",
    "div1totalgtime": "Float64",
    "div1longestgtime": "Float64",
    "div1tailnum": "string",
    "div2airport": "string",
    "div2airportid": "Float64",
    "div2airportseqid": "Float64",
    "div2wheelson": "Float64",
    "div2wheelsoff": "Float64",
    "div2totalgtime": "Float64",
    "div2longestgtime": "Float64",
    "div2tailnum": "string",
    "div3airport": "string",
    "div3airportid": "Float64",
    "div3airportseqid": "Float64",
    "div3wheelson": "Float64",
    "div3wheelsoff": "Float64",
    "div3totalgtime": "Float64",
    "div3longestgtime": "Float64",
    "div3tailnum": "string",
    "div4airport": "string",
    "div4airportid": "Float64",
    "div4airportseqid": "Float64",
    "div4wheelson": "Float64",
    "div4wheelsoff": "Float64",
    "div4totalgtime": "Float64",
    "div4longestgtime": "Float64",
    "div4tailnum": "string",
    "div5airport": "string",
    "div5airportid": "Float64",
    "div5airportseqid": "Float64",
    "div5wheelson": "Float64",
    "div5wheelsoff": "Float64",
    "div5totalgtime": "Float64",
    "div5longestgtime": "Float64",
    "div5tailnum": "string",
    "totaladdgtime": "Float64",
    "longestaddgtime": "Float64",
    "unnamed:_109": "Float64",  # Often empty column
}


def enforce_bts_schema(csv_path: str) -> pd.DataFrame:
    """Read CSV with enforced BTS schema for consistency across all months."""
    print("   📋 Reading CSV with enforced schema...")

    # Read CSV with object dtype first to avoid inference issues
    df = pd.read_csv(csv_path, dtype="object", low_memory=False)

    # Clean column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    print(f"   🔧 Enforcing data types for {len(df.columns)} columns...")

    # Apply schema enforcement
    for col in df.columns:
        if col in BTS_COLUMN_TYPES:
            target_type = BTS_COLUMN_TYPES[col]
            try:
                df[col] = _convert_column_type(df[col], target_type, col)
            except Exception as e:
                print(
                    f"      ⚠️  Warning: Failed to convert {col} to {target_type}: {e}"
                )

    # Handle NULL values in critical non-nullable columns
    critical_cols = ["flight_number_reporting_airline", "dot_id_reporting_airline"]
    for col in critical_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                print(f"   🧹 Dropping {null_count} rows with NULL {col}")
                df = df.dropna(subset=[col])
                # Re-convert to int64 after dropping NULLs
                df[col] = df[col].astype("int64")

    print(f"   ✅ Schema enforcement complete - final shape: {df.shape}")
    return df


def _convert_column_type(
    series: pd.Series, target_type: str, col_name: str
) -> pd.Series:
    """Convert a pandas Series to the target data type with proper null handling"""

    if target_type.startswith("datetime64"):
        if "time" in col_name and col_name != "flightdate":
            # Handle HHMM time format
            return pd.to_datetime(series, format="%H%M", errors="coerce").dt.floor("us")
        else:
            # Handle date columns
            return pd.to_datetime(series, errors="coerce").dt.floor("us")
    elif target_type == "string":
        return series.astype("string")
    elif target_type == "int64":
        numeric_series = pd.to_numeric(series, errors="coerce")
        return numeric_series.astype("int64")
    elif target_type == "Float64":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    elif target_type == "Int64":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    elif target_type == "int32":
        return pd.to_numeric(series, errors="coerce").astype("int32")
    else:
        return series.astype(target_type)


def get_bts_pyarrow_schema() -> pa.Schema:
    """Generate PyArrow schema for consistent Parquet writing"""
    fields = []
    for col_name, dtype in BTS_COLUMN_TYPES.items():
        if dtype.startswith("datetime64"):
            fields.append(pa.field(col_name, pa.timestamp("us")))
        elif dtype == "string":
            fields.append(pa.field(col_name, pa.string()))
        elif dtype in ["int32"]:
            fields.append(pa.field(col_name, pa.int32()))
        elif dtype in ["int64", "Int64"]:
            fields.append(pa.field(col_name, pa.int64()))
        elif dtype == "Float64":
            fields.append(pa.field(col_name, pa.float64()))
        else:
            fields.append(pa.field(col_name, pa.string()))
    return pa.schema(fields)


def validate_schema_consistency(data_dir: Path) -> dict:
    """
    Built-in schema validation to test consistency across all months.
    Returns validation results for critical columns.
    """
    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        return {"error": "No parquet files found"}

    print(f"\n🔍 SCHEMA VALIDATION: Testing {len(parquet_files)} files")
    print("=" * 60)

    results = {
        "files_tested": len(parquet_files),
        "consistent_schemas": True,
        "critical_columns": {},
        "issues": [],
    }

    # Test critical columns that caused the original ClassCastException
    critical_columns = ["flight_number_reporting_airline", "div2tailnum"]

    reference_types = {}
    for file_path in sorted(parquet_files):
        print(f"📁 {file_path.name}")
        try:
            df = pd.read_parquet(file_path)

            for col in critical_columns:
                if col in df.columns:
                    dtype = str(df[col].dtype)
                    null_count = df[col].isnull().sum()

                    if col not in reference_types:
                        reference_types[col] = dtype
                        results["critical_columns"][col] = {
                            "reference_type": dtype,
                            "consistent": True,
                            "files": {},
                        }

                    results["critical_columns"][col]["files"][file_path.name] = {
                        "dtype": dtype,
                        "null_count": null_count,
                    }

                    if dtype != reference_types[col]:
                        results["consistent_schemas"] = False
                        results["critical_columns"][col]["consistent"] = False
                        issue = (
                            f"{col}: {file_path.name} has {dtype}, "
                            f"expected {reference_types[col]}"
                        )
                        results["issues"].append(issue)
                        print(f"   ❌ {issue}")
                    else:
                        print(f"   ✅ {col}: {dtype} (NULLs: {null_count})")

        except Exception as e:
            issue = f"Failed to read {file_path.name}: {e}"
            results["issues"].append(issue)
            print(f"   ❌ {issue}")

    status = "✅ PASS" if results["consistent_schemas"] else "❌ FAIL"
    print(f"\n{status}: Schema consistency validation")

    return results


class BTSFlightDataDownloader:
    """Download real flight data from Bureau of Transportation Statistics."""

    def __init__(self):
        self.base_url = "https://transtats.bts.gov/PREZIP"
        self.data_dir = Path("data/bts_flight_data")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = 3
        self.timeout = 120  # 2 minutes timeout

    def check_requirements(self) -> bool:
        """Check if required packages are installed."""
        required_packages = ["pandas", "pyarrow", "requests"]
        optional_packages = ["tqdm"]
        missing = []
        missing_optional = []

        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                missing.append(package)

        for package in optional_packages:
            try:
                __import__(package)
            except ImportError:
                missing_optional.append(package)

        if missing:
            print(f"❌ Missing required packages: {', '.join(missing)}")
            print(f"   Install with: pip install {' '.join(missing)}")
            return False

        if missing_optional:
            print(f"⚠️  Optional packages not found: {', '.join(missing_optional)}")
            print(
                f"   For better progress bars: pip install {' '.join(missing_optional)}"
            )

        return True

    def download_monthly_data(self, year: int, month: int) -> Path:
        """
        Download official BTS flight performance data for a specific month.

        Args:
            year: Year (1987-2025)
            month: Month (1-12)

        Returns:
            Path to downloaded parquet file
        """
        # BTS filename format:
        # On_Time_Reporting_Carrier_On_Time_Performance_1987_present_YYYY_M.zip
        filename = (
            f"On_Time_Reporting_Carrier_On_Time_Performance_"
            f"1987_present_{year}_{month}.zip"
        )
        url = f"{self.base_url}/{filename}"

        zip_path = self.data_dir / filename
        # The CSV inside has parentheses:
        # On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_YYYY_M.csv
        csv_filename = (
            f"On_Time_Reporting_Carrier_On_Time_Performance_"
            f"(1987_present)_{year}_{month}.csv"
        )
        csv_path = self.data_dir / csv_filename
        parquet_path = self.data_dir / f"bts_flights_{year}_{month:02d}.parquet"

        # Skip if already processed
        if parquet_path.exists():
            print(f"✅ Already have {parquet_path.name}")
            return parquet_path

        print(f"📥 Downloading {year}-{month:02d} flight data from BTS...")
        print(f"   URL: {url}")

        for attempt in range(self.max_retries):
            try:
                # Download the ZIP file with progress bar
                print(
                    f"   💾 Downloading... (attempt {attempt + 1}/{self.max_retries})"
                )
                response = requests.get(url, stream=True, timeout=self.timeout)
                response.raise_for_status()

                total_size = int(response.headers.get("content-length", 0))

                if HAS_TQDM and total_size > 0:
                    with open(zip_path, "wb") as f, tqdm(
                        desc="Download",
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    # Simple download without progress bar
                    with open(zip_path, "wb") as f:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(
                                    f"\r   Progress: {percent:.1f}%", end="", flush=True
                                )
                    if total_size > 0:
                        print()  # New line after progress
                break

            except (
                requests.exceptions.RequestException,
                requests.exceptions.Timeout,
            ) as e:
                print(f"   ⚠️  Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    print(f"   ⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ All {self.max_retries} attempts failed")
                    return None

        try:
            # Extract CSV from ZIP
            print("   📂 Extracting CSV...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.data_dir)

            # Convert CSV to Parquet with ENFORCED SCHEMA for consistency
            print("   🔄 Converting to Parquet with enforced schema...")

            # Use schema enforcer to ensure consistent data types across all months
            df = enforce_bts_schema(csv_path)

            # Save as Spark-compatible Parquet with enforced schema
            self.save_spark_compatible_parquet_with_schema(df, parquet_path)

            print(f"   ✅ Saved {len(df):,} flight records to {parquet_path.name}")

            # Clean up temporary files
            os.remove(zip_path)
            os.remove(csv_path)

            return parquet_path

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Failed to download {year}-{month}: {e}")
            return None
        except Exception as e:
            print(f"   ❌ Error processing {year}-{month}: {e}")
            return None

    def save_spark_compatible_parquet_with_schema(
        self, df: pd.DataFrame, output_file: Path
    ):
        """Save DataFrame to Parquet with enforced BTS schema for perfect consistency."""

        print("   💾 Saving with enforced schema...")

        # Get the predefined PyArrow schema
        schema = get_bts_pyarrow_schema()

        # Only include columns that exist in both the dataframe and our schema
        available_columns = []
        for field in schema:
            if field.name in df.columns:
                available_columns.append(field.name)
            else:
                print(f"      ⚠️  Column {field.name} not found in data")

        # Create subset schema for available columns only
        available_fields = [
            field for field in schema if field.name in available_columns
        ]
        subset_schema = pa.schema(available_fields)

        # Select only the columns we have schema definitions for
        df_subset = df[available_columns]

        print(
            f"   📊 Using {len(available_columns)}/{len(BTS_COLUMN_TYPES)} defined columns"
        )

        try:
            # Convert DataFrame to PyArrow table with explicit enforced schema
            table = pa.Table.from_pandas(
                df_subset, schema=subset_schema, preserve_index=False
            )

            # Save as Parquet with enforced schema
            pq.write_table(table, output_file, compression="snappy")

            print("   ✅ Schema-enforced Parquet saved successfully")

        except Exception as e:
            print(f"   ❌ Schema enforcement failed: {e}")
            print("   🔄 Falling back to dynamic schema...")

            # Fallback to old method if schema enforcement fails
            self.save_spark_compatible_parquet_fallback(df, output_file)

    def save_spark_compatible_parquet_fallback(
        self, df: pd.DataFrame, output_file: Path
    ):
        """Fallback method using dynamic schema (original implementation)"""
        # Ensure datetime columns are properly typed with microsecond precision
        datetime_columns = [
            "flightdate",
            "crsdeptime",
            "deptime",
            "crsarrtime",
            "arrtime",
            "wheelsoff",
            "wheelson",
        ]
        for col in datetime_columns:
            if col in df.columns:
                # Convert to microsecond precision (remove nanoseconds)
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.floor("us")

        # Create PyArrow schema with microsecond precision (Spark compatible)
        # Dynamically build schema based on DataFrame dtypes
        fields = []
        for col_name, dtype in df.dtypes.items():
            if pd.api.types.is_datetime64_any_dtype(dtype):
                fields.append(pa.field(col_name, pa.timestamp("us")))
            elif pd.api.types.is_integer_dtype(dtype):
                fields.append(pa.field(col_name, pa.int64()))
            elif pd.api.types.is_float_dtype(dtype):
                fields.append(pa.field(col_name, pa.float64()))
            else:  # object type, usually string
                fields.append(pa.field(col_name, pa.string()))

        schema = pa.schema(fields)

        # Convert DataFrame to PyArrow table with explicit schema
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Save as Parquet with microsecond precision
        pq.write_table(table, output_file, compression="snappy")

    def get_dataset_summary(self, year: int) -> dict:
        """Get summary statistics for downloaded data."""
        import glob

        files = sorted(glob.glob(str(self.data_dir / f"bts_flights_{year}_*.parquet")))

        if not files:
            return {"error": f"No data files found for {year}"}

        total_flights = 0
        months = []
        airlines = set()
        airports = set()

        for file in files:
            df = pd.read_parquet(file)
            total_flights += len(df)
            month = Path(file).stem.split("_")[-1]
            months.append((month, len(df)))

            # Sample to get unique values (faster than full scan)
            sample = df.sample(min(10000, len(df)))
            airlines.update(sample["reporting_airline"].unique())
            airports.update(sample["origin"].unique())
            airports.update(sample["dest"].unique())

        return {
            "year": year,
            "total_flights": total_flights,
            "months": len(months),
            "monthly_breakdown": months,
            "airlines": len(airlines),
            "airports": len(airports),
            "files": files,
        }

    def download_year_data(self, year: int) -> list:
        """Download all months for a given year."""
        downloaded_files = []

        print(f"\n🗓️  Downloading BTS flight data for {year}")
        print("=" * 60)

        for month in range(1, 13):
            # Don't try to download future months
            if year == datetime.now().year and month > datetime.now().month:
                break

            file_path = self.download_monthly_data(year, month)
            if file_path:
                downloaded_files.append(file_path)

            # Be nice to the server
            time.sleep(1)

        return downloaded_files

    def get_sample_data(self, file_path: Path, n_rows: int = 1000) -> pd.DataFrame:
        """Get a sample of the data for inspection."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        df = pd.read_parquet(file_path)
        return df.head(n_rows)


def main():
    parser = argparse.ArgumentParser(description="Download real BTS flight data")
    parser.add_argument(
        "--year", type=int, default=2024, help="Year to download (default: 2024)"
    )
    parser.add_argument("--month", type=int, help="Specific month to download (1-12)")
    parser.add_argument(
        "--sample", action="store_true", help="Show sample of downloaded data"
    )
    parser.add_argument(
        "--summary", action="store_true", help="Show dataset summary statistics"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate schema consistency across all files",
    )

    args = parser.parse_args()

    print("🇺🇸 BTS Real Flight Data Downloader")
    print("📊 Source: Bureau of Transportation Statistics")
    print("✅ 100% factual government data")
    print()

    downloader = BTSFlightDataDownloader()

    # Check requirements
    if not downloader.check_requirements():
        sys.exit(1)

    # Show summary if requested
    if args.summary:
        print(f"📈 Dataset Summary for {args.year}")
        print("=" * 50)
        summary = downloader.get_dataset_summary(args.year)

        if "error" in summary:
            print(f"❌ {summary['error']}")
            return

        print(f"📊 Total flights: {summary['total_flights']:,}")
        print(f"📅 Months available: {summary['months']}/12")
        print(f"✈️  Airlines: {summary['airlines']}")
        print(f"🏢 Airports: {summary['airports']}")
        print(f"📁 Files: {len(summary['files'])}")

        print("\n📆 Monthly breakdown:")
        for month, count in summary["monthly_breakdown"]:
            print(f"   {month}: {count:,} flights")
        return

    # Schema validation
    if args.validate:
        print(f"🔍 Schema Validation for {args.year}")
        print("=" * 50)
        downloader = BTSFlightDataDownloader()
        results = validate_schema_consistency(downloader.data_dir)

        if "error" in results:
            print(f"❌ {results['error']}")
            return

        print("\n📊 Validation Summary:")
        print(f"   Files tested: {results['files_tested']}")
        print(
            f"   Schema consistent: {'✅ YES' if results['consistent_schemas'] else '❌ NO'}"
        )

        if results["issues"]:
            print("\n⚠️  Issues found:")
            for issue in results["issues"]:
                print(f"   - {issue}")
        else:
            print("\n🎉 All schemas are perfectly consistent!")

        return

    if args.month:
        # Download specific month
        file_path = downloader.download_monthly_data(args.year, args.month)
        if file_path and args.sample:
            print(f"\n📋 Sample data from {file_path.name}:")
            sample_df = downloader.get_sample_data(file_path, 5)
            print(sample_df.head())
            print(f"\nColumns: {list(sample_df.columns)}")
            print(f"Total rows: {len(pd.read_parquet(file_path)):,}")
    else:
        # Download full year
        files = downloader.download_year_data(args.year)
        print(f"\n✅ Downloaded {len(files)} monthly files for {args.year}")

        if files and args.sample:
            print(f"\n📋 Sample data from {files[0].name}:")
            sample_df = downloader.get_sample_data(files[0], 5)
            print(sample_df.head())


if __name__ == "__main__":
    main()
