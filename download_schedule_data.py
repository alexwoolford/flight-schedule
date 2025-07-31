#!/usr/bin/env python3
"""
Unified schedule data downloader - replaces both download_real and download_yearly
"""

import argparse
import json
import os
import time
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests


class UnifiedScheduleDownloader:
    """Single downloader that handles both real-time and yearly data"""

    def __init__(self, output_dir: str = "schedule_data"):
        self.base_url = "https://www.transtats.bts.gov/PREZIP/"
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Standard carrier mapping
        self.carrier_mapping = {
            "AA": "American Airlines",
            "DL": "Delta Air Lines",
            "UA": "United Airlines",
            "WN": "Southwest Airlines",
            "AS": "Alaska Airlines",
            "B6": "JetBlue Airways",
            "F9": "Frontier Airlines",
            "NK": "Spirit Airlines",
            "G4": "Allegiant Air",
            "SY": "Sun Country Airlines",
        }

    def download_month_data(
        self, year: int, month: int, data_type: str = "T_T100D_SEGMENT_ALL_CARRIER"
    ) -> Optional[str]:
        """Download data for a specific month"""
        filename = f"{data_type}_{year}_{month:02d}.zip"
        url = f"{self.base_url}{filename}"
        local_path = os.path.join(self.output_dir, filename)

        print(f"📥 Downloading {year}-{month:02d}...")

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"   ✅ Downloaded {os.path.getsize(local_path) / 1024**2:.1f} MB")
            return local_path

        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return None

    def process_downloaded_file(self, zip_path: str) -> Optional[pd.DataFrame]:
        """Process a downloaded zip file into schedule format"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # Extract first CSV file
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    print(f"   ❌ No CSV files in {zip_path}")
                    return None

                csv_file = csv_files[0]
                print(f"   📊 Processing {csv_file}...")

                with zip_ref.open(csv_file) as f:
                    df = pd.read_csv(f)

                print(f"   📈 Loaded {len(df):,} raw records")

                # Convert to flight schedule format
                schedule_df = self.convert_to_schedule_format(df)
                print(f"   ✅ Converted to {len(schedule_df):,} schedule records")

                return schedule_df

        except Exception as e:
            print(f"   ❌ Error processing {zip_path}: {e}")
            return None

    def convert_to_schedule_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DOT T-100 data to flight schedule format"""
        schedule_records = []

        # Group by unique route/carrier combinations
        grouped = df.groupby(["CARRIER_GROUP", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID"])

        for (carrier, origin_id, dest_id), group in grouped:
            # Generate schedule record
            schedule_record = {
                "schedule_id": f"SCHED_{carrier}_{origin_id}_{dest_id}_{len(schedule_records)}",
                "carrier": carrier,
                "departure_station": (
                    group["ORIGIN"].iloc[0]
                    if "ORIGIN" in group.columns
                    else f"APT{origin_id}"
                ),
                "arrival_station": (
                    group["DEST"].iloc[0]
                    if "DEST" in group.columns
                    else f"APT{dest_id}"
                ),
                "effective_date": "2024-01-01",  # Could derive from YEAR/MONTH
                "discontinued_date": "2024-12-31",
                "published_departure_time": "12:00",  # Could randomize or derive
                "published_arrival_time": "14:00",
                "service_days_bitmap": 127,  # All days (could derive from frequency)
                "cabin_bitmap": 15,  # All cabin classes
                # Add additional fields as metadata
                "passengers": (
                    int(group["PASSENGERS"].sum())
                    if "PASSENGERS" in group.columns
                    else 0
                ),
                "freight": (
                    float(group["FREIGHT"].sum()) if "FREIGHT" in group.columns else 0.0
                ),
                "distance": (
                    float(group["DISTANCE"].mean())
                    if "DISTANCE" in group.columns
                    else 0.0
                ),
            }
            schedule_records.append(schedule_record)

        return pd.DataFrame(schedule_records)

    def download_date_range(
        self, start_year: int, start_month: int, end_year: int, end_month: int
    ) -> List[pd.DataFrame]:
        """Download data for a date range"""
        dataframes = []

        current_year, current_month = start_year, start_month

        while (current_year < end_year) or (
            current_year == end_year and current_month <= end_month
        ):
            zip_path = self.download_month_data(current_year, current_month)

            if zip_path:
                df = self.process_downloaded_file(zip_path)
                if df is not None:
                    dataframes.append(df)

                # Clean up zip file to save space
                os.remove(zip_path)

            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

            # Rate limiting
            time.sleep(1)

        return dataframes

    def save_combined_data(self, dataframes: List[pd.DataFrame], filename_prefix: str):
        """Save combined data in multiple formats"""
        if not dataframes:
            print("❌ No data to save")
            return

        print("🔄 Combining data...")
        combined_df = pd.concat(dataframes, ignore_index=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save as Parquet (primary format)
        parquet_file = f"{filename_prefix}_{timestamp}.parquet"
        combined_df.to_parquet(parquet_file, index=False, compression="snappy")

        print(f"✅ Saved {len(combined_df):,} records:")
        print(
            f"   📊 {parquet_file} ({os.path.getsize(parquet_file) / 1024**2:.1f} MB)"
        )

        return parquet_file


def main():
    parser = argparse.ArgumentParser(description="Download flight schedule data")
    parser.add_argument("--year", type=int, default=2024, help="Year to download")
    parser.add_argument(
        "--month", type=int, help="Specific month (1-12), or omit for full year"
    )
    parser.add_argument(
        "--start-month", type=int, default=1, help="Start month for range"
    )
    parser.add_argument("--end-month", type=int, default=12, help="End month for range")
    parser.add_argument(
        "--output-prefix", default="flight_schedule_data", help="Output filename prefix"
    )

    args = parser.parse_args()

    downloader = UnifiedScheduleDownloader()

    print(f"🚀 FLIGHT SCHEDULE DATA DOWNLOADER")
    print(f"=" * 50)

    if args.month:
        # Single month
        print(f"📅 Downloading {args.year}-{args.month:02d}")
        dataframes = [
            downloader.process_downloaded_file(
                downloader.download_month_data(args.year, args.month)
            )
        ]
    else:
        # Date range
        print(
            f"📅 Downloading {args.year}-{args.start_month:02d} to {args.year}-{args.end_month:02d}"
        )
        dataframes = downloader.download_date_range(
            args.year, args.start_month, args.year, args.end_month
        )

    # Save combined data
    if dataframes and any(df is not None for df in dataframes):
        valid_dfs = [df for df in dataframes if df is not None]
        downloader.save_combined_data(valid_dfs, args.output_prefix)
    else:
        print("❌ No data downloaded successfully")


if __name__ == "__main__":
    main()
