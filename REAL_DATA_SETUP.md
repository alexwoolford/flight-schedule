# Real Flight Data Setup

This project uses **100% factual flight data** from the US Bureau of Transportation
Statistics (BTS). Zero synthetic data — see rule 1 in `CLAUDE.md`.

## Quick Start

```bash
conda env create -f environment.yml    # first time only
conda activate flight-schedule

# Download all 12 months of 2025 (the default year)
python download_bts_flight_data.py
```

For the full pipeline — including creating the database and loading the graph —
follow [README.md](README.md). This file covers the download step only.

## What You Get

All 12 months of 2025:

- **7,001,619 real flights**
- **14 reporting US airlines** (Southwest, Delta, American, United, etc.)
- **352 real airports**
- **Complete temporal data** (scheduled vs actual times)
- **~226 MB** of Parquet on disk

Counts for other years differ; the numbers above are measured from the 2025 files
this repo downloads by default.

## Data Source

- **Source**: Bureau of Transportation Statistics (BTS)
- **URL**: https://www.transtats.bts.gov/
- **Data**: Airline On-Time Performance Reports
- **Requirement**: Airlines with >0.5% domestic revenue must report
- **Coverage**: US domestic commercial flights

## Usage Examples

```bash
# Download a specific month (fast, good for iteration)
python download_bts_flight_data.py --year 2025 --month 6

# Download a different year
python download_bts_flight_data.py --year 2024

# Show per-file record counts
python download_bts_flight_data.py --summary

# Check dtype consistency across all downloaded files
python download_bts_flight_data.py --validate

# Print a sample of the downloaded data
python download_bts_flight_data.py --sample
```

Existing files are skipped, so re-running is cheap and safe.

## File Structure

```
data/bts_flight_data/
├── bts_flights_2025_01.parquet
├── bts_flights_2025_02.parquet
├── ...
└── bts_flights_2025_12.parquet
```

These are gitignored — regenerate them, never commit them.

## Why the Download Normalizes Types

BTS's monthly CSVs are not dtype-stable across months, which breaks Spark reads
downstream. The downloader declares an explicit ~110-column type map and a
matching PyArrow schema, and writes every month through it so all files are
byte-compatible. Timestamps are floored to microseconds because Spark cannot read
nanosecond-precision Parquet timestamps.

`--validate` verifies this held after the fact.

## Data Schema

Each flight record contains flight date, airline, flight number, origin and
destination airports, scheduled vs actual departure and arrival times, delays,
cancellation and diversion flags, aircraft tail number, distance, and ~100 other
attributes. See `BTS_SCHEMA_ANALYSIS.md` for a column-level breakdown.

**No synthetic data. No fake IDs. No generated timestamps. 100% real.**
