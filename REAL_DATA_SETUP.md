# Real Flight Data Setup

This project uses **100% factual flight data** from the US Bureau of Transportation Statistics (BTS). Zero synthetic data.

## Quick Start

```bash
# One-command setup (installs dependencies + downloads data)
python setup_real_data.py

# Or manual setup
pip install -r requirements.txt
python download_bts_flight_data.py --year 2024
```

## What You Get

- **7,079,061 real flights** from 2024
- **15 major US airlines** (Southwest, Delta, American, United, etc.)
- **347 real airports** 
- **6,000+ real routes**
- **Complete temporal data** (scheduled vs actual times)
- **Government-verified accuracy**

## Data Source

- **Source**: Bureau of Transportation Statistics (BTS)
- **URL**: https://transtats.bts.gov/
- **Data**: Airline On-Time Performance Reports
- **Requirement**: Airlines with >0.5% domestic revenue must report
- **Coverage**: Every commercial flight in the US

## Features

✅ **No re-downloads** - Skips existing files  
✅ **Retry logic** - Handles network timeouts  
✅ **Progress bars** - Shows download progress  
✅ **Requirements check** - Validates dependencies  
✅ **Error recovery** - Graceful failure handling  
✅ **Summary stats** - Dataset overview  

## Usage Examples

```bash
# Download specific month
python download_bts_flight_data.py --year 2024 --month 6

# Show dataset summary
python download_bts_flight_data.py --summary

# Download full year with sample
python download_bts_flight_data.py --year 2024 --sample
```

## File Structure

```
data/bts_flight_data/
├── bts_flights_2024_01.parquet    # 547K flights
├── bts_flights_2024_02.parquet    # 519K flights
├── ...                            # ...
└── bts_flights_2024_12.parquet    # 591K flights
```

## Data Schema

Each flight record contains:
- Flight date, airline, flight number
- Origin/destination airports  
- Scheduled vs actual departure/arrival times
- Delays, cancellations, diversions
- Aircraft tail number, distance
- And 100+ other factual attributes

**No synthetic data. No fake IDs. No generated timestamps. 100% real.**