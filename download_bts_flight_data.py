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

            # Convert CSV to Parquet for better performance
            print("   🔄 Converting to Parquet...")
            df = pd.read_csv(csv_path)

            # Clean up column names
            df.columns = df.columns.str.lower().str.replace(" ", "_")

            # Convert to datetime types where appropriate with MICROSECOND precision
            # (Spark compatible)
            datetime_cols = [
                "flightdate",
                "crsdeptime",
                "deptime",
                "crsarrtime",
                "arrtime",
            ]
            for col in datetime_cols:
                if col in df.columns:
                    # Handle time columns (HHMM format)
                    if "time" in col and col != "flightdate":
                        # Convert HHMM to proper time
                        df[col] = pd.to_datetime(
                            df[col], format="%H%M", errors="coerce"
                        ).dt.floor("us")
                    elif col == "flightdate":
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.floor(
                            "us"
                        )

            # Save as Spark-compatible Parquet with microsecond precision
            self.save_spark_compatible_parquet(df, parquet_path)

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

    def save_spark_compatible_parquet(
        self, df: pd.DataFrame, output_file: Path
    ):
        """Save DataFrame to Parquet with microsecond precision for Spark
        compatibility."""
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
