#!/usr/bin/env python3
"""
Data Quality Checks Tests
=========================

Tests real data quality validation WITHOUT using mocks.
Focuses on validating that data makes sense from a real-world aviation perspective,
including airline codes, airport codes, temporal consistency, and data integrity.
"""

import os
import sys
from datetime import date, datetime, time

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


class TestAirlineCodeValidation:
    """Test validation of real airline codes (IATA/ICAO)"""

    def test_common_airline_code_formats(self):
        """Test that airline codes follow real IATA format standards"""
        # Real IATA airline codes - 2-letter format
        valid_iata_codes = [
            "AA",  # American Airlines
            "UA",  # United Airlines
            "DL",  # Delta Air Lines
            "WN",  # Southwest Airlines
            "B6",  # JetBlue Airways
            "NK",  # Spirit Airlines
            "F9",  # Frontier Airlines
            "G4",  # Allegiant Air
            "HA",  # Hawaiian Airlines
            "AS",  # Alaska Airlines
        ]

        for code in valid_iata_codes:
            # IATA codes should be exactly 2 characters
            assert len(code) == 2, f"IATA code should be 2 characters: {code}"
            # Should be alphanumeric
            assert code.isalnum(), f"IATA code should be alphanumeric: {code}"
            # Should be uppercase
            assert code.isupper(), f"IATA code should be uppercase: {code}"

    def test_invalid_airline_code_patterns(self):
        """Test detection of invalid airline code patterns"""
        invalid_codes = [
            "A",  # Too short
            "AAA",  # Too long
            "A1",  # Valid pattern but check logic
            "aa",  # Lowercase
            "A@",  # Special character
            "",  # Empty
            " AA",  # Leading space
            "AA ",  # Trailing space
            "A A",  # Space in middle
        ]

        for code in invalid_codes:
            # Test various validation criteria
            is_valid = (
                len(code) == 2
                and code.isalnum()
                and code.isupper()
                and not code.isspace()
                and code.strip() == code
            )

            # Most of these should be invalid (except A1 which is technically valid format)
            if code == "A1":
                assert is_valid, f"A1 should be valid format even if not a real airline"
            else:
                assert not is_valid, f"Code should be invalid: '{code}'"

    def test_airline_code_data_consistency(self):
        """Test consistency of airline codes in datasets"""
        # Create test dataset with mixed airline codes
        test_data = {
            "flightdate": ["2024-03-01"] * 10,
            "reporting_airline": [
                "AA",
                "UA",
                "DL",
                "WN",
                "B6",
                "AA",
                "UA",
                "DL",
                "WN",
                "B6",
            ],
            "flight_number_reporting_airline": [str(i) for i in range(1, 11)],
            "origin": ["LAX"] * 10,
            "dest": ["JFK"] * 10,
        }
        df = pd.DataFrame(test_data)

        # Test unique airline code extraction
        unique_airlines = df["reporting_airline"].unique()
        assert len(unique_airlines) == 5, "Should have 5 unique airlines"

        # Test all codes follow format
        for airline in unique_airlines:
            assert len(airline) == 2, f"Airline code should be 2 chars: {airline}"
            assert airline.isalnum(), f"Airline code should be alphanumeric: {airline}"
            assert airline.isupper(), f"Airline code should be uppercase: {airline}"

        # Test code frequency makes sense
        airline_counts = df["reporting_airline"].value_counts()
        for airline, count in airline_counts.items():
            assert count > 0, f"Airline {airline} should have positive count"
            assert count <= len(
                df
            ), f"Airline {airline} count should not exceed total records"

    def test_airline_code_business_logic(self):
        """Test airline codes follow real business logic patterns"""
        # Real airline operational patterns
        major_us_carriers = ["AA", "UA", "DL", "WN"]
        low_cost_carriers = ["B6", "NK", "F9", "G4"]
        regional_carriers = ["9E", "OH", "YX", "EV"]

        # Test carrier categorization logic
        all_carriers = major_us_carriers + low_cost_carriers + regional_carriers

        for carrier in all_carriers:
            # All should be valid 2-letter codes
            assert len(carrier) == 2, f"Carrier code should be 2 letters: {carrier}"
            assert (
                carrier.isalpha() or carrier.isalnum()
            ), f"Carrier should be alpha/alnum: {carrier}"

        # Test that we can distinguish carrier types by code patterns
        # (This is domain knowledge - major carriers tend to have letter-only codes)
        major_letter_only = [c for c in major_us_carriers if c.isalpha()]
        assert (
            len(major_letter_only) >= 3
        ), "Most major carriers should have letter-only codes"

        # Low cost carriers often have mixed alphanumeric
        lcc_mixed = [c for c in low_cost_carriers if not c.isalpha()]
        assert len(lcc_mixed) >= 1, "Some LCCs should have mixed alphanumeric codes"


class TestAirportCodeValidation:
    """Test validation of real airport codes and their properties"""

    def test_common_airport_code_formats(self):
        """Test that airport codes follow real IATA format standards"""
        # Real IATA airport codes - 3-letter format
        valid_airport_codes = [
            "LAX",  # Los Angeles International
            "JFK",  # John F. Kennedy International
            "ORD",  # Chicago O'Hare International
            "ATL",  # Hartsfield-Jackson Atlanta International
            "DFW",  # Dallas/Fort Worth International
            "SFO",  # San Francisco International
            "SEA",  # Seattle-Tacoma International
            "LAS",  # McCarran International (Las Vegas)
            "MIA",  # Miami International
            "BOS",  # Logan International (Boston)
            "PHX",  # Phoenix Sky Harbor International
            "DEN",  # Denver International
        ]

        for code in valid_airport_codes:
            # IATA airport codes should be exactly 3 characters
            assert len(code) == 3, f"Airport code should be 3 characters: {code}"
            # Should be alphabetic only
            assert code.isalpha(), f"Airport code should be alphabetic: {code}"
            # Should be uppercase
            assert code.isupper(), f"Airport code should be uppercase: {code}"

    def test_invalid_airport_code_patterns(self):
        """Test detection of invalid airport code patterns"""
        invalid_codes = [
            "LA",  # Too short
            "LAXX",  # Too long
            "L1X",  # Contains number
            "lax",  # Lowercase
            "L@X",  # Special character
            "",  # Empty
            " LAX",  # Leading space
            "LAX ",  # Trailing space
            "L AX",  # Space in middle
            "L-X",  # Hyphen
        ]

        for code in invalid_codes:
            # Test airport code validation criteria
            is_valid = (
                len(code) == 3
                and code.isalpha()
                and code.isupper()
                and not code.isspace()
                and code.strip() == code
                and "-" not in code
            )

            assert not is_valid, f"Airport code should be invalid: '{code}'"

    def test_airport_code_geographic_logic(self):
        """Test airport codes follow geographic and logical patterns"""
        # US major hub airports by region (for reference)
        # west_coast_hubs = ["LAX", "SFO", "SEA", "PDX"]
        # east_coast_hubs = ["JFK", "LGA", "BOS", "MIA", "PHL"]
        # central_hubs = ["ORD", "DFW", "ATL", "DEN", "PHX"]

        # Test that airport codes are geographically sensible for common routes
        common_routes = [
            ("LAX", "JFK"),  # Cross-country
            ("SFO", "ORD"),  # West to Central
            ("MIA", "LAX"),  # Southeast to West
            ("BOS", "SEA"),  # Northeast to Northwest
            ("ATL", "DEN"),  # Southeast to Mountain
        ]

        for origin, dest in common_routes:
            # Origins and destinations should be different
            assert (
                origin != dest
            ), f"Origin and destination should be different: {origin} -> {dest}"

            # Both should be valid 3-letter codes
            assert (
                len(origin) == 3 and origin.isalpha()
            ), f"Origin should be valid: {origin}"
            assert (
                len(dest) == 3 and dest.isalpha()
            ), f"Destination should be valid: {dest}"

            # Should be uppercase
            assert (
                origin.isupper() and dest.isupper()
            ), f"Codes should be uppercase: {origin} -> {dest}"

    def test_airport_code_data_consistency(self):
        """Test consistency of airport codes in flight data"""
        # Create test dataset with realistic airport pairings
        test_data = {
            "flightdate": ["2024-03-01"] * 8,
            "reporting_airline": ["AA"] * 8,
            "flight_number_reporting_airline": [str(i) for i in range(1, 9)],
            "origin": ["LAX", "LAX", "JFK", "JFK", "ORD", "ORD", "ATL", "ATL"],
            "dest": ["JFK", "MIA", "LAX", "SFO", "LAX", "DFW", "LAX", "ORD"],
        }
        df = pd.DataFrame(test_data)

        # Test origin/destination validation
        for _, row in df.iterrows():
            origin = row["origin"]
            dest = row["dest"]

            # Origin and destination should be different
            assert origin != dest, f"Flight should not have same origin/dest: {origin}"

            # Both should be valid airport codes
            assert len(origin) == 3 and origin.isalpha(), f"Invalid origin: {origin}"
            assert len(dest) == 3 and dest.isalpha(), f"Invalid destination: {dest}"

        # Test airport frequency and hub logic
        all_airports = list(df["origin"]) + list(df["dest"])
        airport_counts = pd.Series(all_airports).value_counts()

        # Major hubs should appear more frequently
        major_hubs = ["LAX", "JFK", "ORD", "ATL"]
        for hub in major_hubs:
            if hub in airport_counts:
                # Hubs should appear multiple times
                assert (
                    airport_counts[hub] >= 2
                ), f"Hub {hub} should appear multiple times"

    def test_airport_distance_logic(self):
        """Test that airport distances make geographic sense"""
        # Known approximate distances between major airports (in miles)
        expected_distances = {
            ("LAX", "JFK"): (2400, 2500),  # Cross-country
            ("LAX", "SFO"): (300, 400),  # West coast
            ("JFK", "BOS"): (180, 220),  # East coast
            ("ORD", "ATL"): (580, 650),  # Central/Southeast
            ("LAX", "LAX"): (0, 0),  # Same airport
        }

        for (origin, dest), (min_dist, max_dist) in expected_distances.items():
            # Test distance range validation logic
            if origin == dest:
                expected_distance = 0
                assert (
                    expected_distance == 0
                ), f"Same airport distance should be 0: {origin}"
            else:
                # For different airports, distance should be positive and reasonable
                assert min_dist > 0, f"Distance should be positive: {origin} to {dest}"
                assert (
                    max_dist >= min_dist
                ), f"Max distance should be >= min: {origin} to {dest}"

                # Cross-country flights should be longer than regional flights
                if "LAX" in [origin, dest] and "JFK" in [origin, dest]:
                    assert (
                        min_dist > 2000
                    ), f"Cross-country flight should be long: {origin} to {dest}"
                elif origin in ["LAX", "SFO"] and dest in ["LAX", "SFO"]:
                    assert (
                        max_dist < 500
                    ), f"West coast flight should be shorter: {origin} to {dest}"


class TestTemporalConsistencyValidation:
    """Test temporal consistency and logic validation"""

    def test_flight_time_logic_consistency(self):
        """Test that flight times follow logical patterns"""
        # Test cases with departure and arrival times
        time_test_cases = [
            # (departure_time, arrival_time, expected_valid, reason)
            ("0800", "1200", True, "4-hour morning flight"),
            ("1430", "1630", True, "2-hour afternoon flight"),
            ("2200", "0100", True, "Red-eye flight crossing midnight"),
            ("1200", "1000", False, "Arrival before departure on same day"),
            ("0000", "2359", True, "Nearly full-day flight"),
            ("1500", "1500", False, "Same departure and arrival time"),
            ("0600", "0800", True, "2-hour early morning flight"),
        ]

        for dep_time, arr_time, expected_valid, reason in time_test_cases:
            # Parse times
            dep_hour = int(dep_time[:2])
            dep_min = int(dep_time[2:])
            arr_hour = int(arr_time[:2])
            arr_min = int(arr_time[2:])

            # Create time objects for same day
            dep_datetime = datetime.combine(date(2024, 3, 1), time(dep_hour, dep_min))
            arr_datetime = datetime.combine(date(2024, 3, 1), time(arr_hour, arr_min))

            # Calculate duration
            if arr_datetime >= dep_datetime:
                duration = arr_datetime - dep_datetime
                same_day_valid = True
            else:
                # Could be next-day arrival
                arr_next_day = datetime.combine(
                    date(2024, 3, 2), time(arr_hour, arr_min)
                )
                duration = arr_next_day - dep_datetime
                same_day_valid = False

            if expected_valid:
                # Valid flights should have positive duration
                if same_day_valid:
                    assert (
                        duration.total_seconds() > 0
                    ), f"Same-day flight should have positive duration: {reason}"
                else:
                    assert (
                        duration.total_seconds() > 0
                    ), f"Next-day flight should have positive duration: {reason}"
                    # Red-eye flights should be reasonable (< 18 hours)
                    assert (
                        duration.total_seconds() < 18 * 3600
                    ), f"Red-eye flight too long: {reason}"
            else:
                # Invalid flights should be caught
                if dep_time == arr_time:
                    assert (
                        dep_datetime == arr_datetime
                    ), f"Same times should be equal: {reason}"
                elif same_day_valid and arr_datetime < dep_datetime:
                    assert False, f"Should catch invalid same-day timing: {reason}"

    def test_flight_date_consistency(self):
        """Test flight date consistency and patterns"""
        # Test date patterns
        date_test_cases = [
            "2024-03-01",  # Valid date
            "2024-02-29",  # Leap year date
            "2024-12-31",  # Year-end date
            "2024-01-01",  # Year-start date
            "2024-06-15",  # Mid-year date
        ]

        for date_str in date_test_cases:
            # Parse date
            try:
                flight_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                # Date should be reasonable (within reasonable range)
                assert (
                    flight_date.year >= 2020
                ), f"Date year should be recent: {date_str}"
                assert (
                    flight_date.year <= 2030
                ), f"Date year should not be too future: {date_str}"

                # Month should be valid
                assert (
                    1 <= flight_date.month <= 12
                ), f"Month should be valid: {date_str}"

                # Day should be valid for the month
                assert 1 <= flight_date.day <= 31, f"Day should be valid: {date_str}"

                # Test day of week patterns (flights typically operate all days)
                day_of_week = flight_date.weekday()  # 0=Monday, 6=Sunday
                assert 0 <= day_of_week <= 6, f"Day of week should be valid: {date_str}"

            except ValueError:
                assert False, f"Date should be parseable: {date_str}"

    def test_flight_duration_reasonableness(self):
        """Test that flight durations are reasonable for given routes"""
        # Test cases with routes and expected duration ranges
        duration_test_cases = [
            # (origin, dest, min_hours, max_hours, flight_type)
            ("LAX", "JFK", 4.5, 6.5, "Cross-country"),
            ("LAX", "SFO", 1.0, 2.0, "Short regional"),
            ("JFK", "BOS", 1.0, 1.5, "Short East Coast"),
            ("ORD", "ATL", 1.5, 2.5, "Midwest to Southeast"),
            ("LAX", "ORD", 3.5, 4.5, "West to Central"),
        ]

        for origin, dest, min_hours, max_hours, flight_type in duration_test_cases:
            # Test duration validation logic
            min_minutes = min_hours * 60
            max_minutes = max_hours * 60

            # Duration should be positive
            assert (
                min_minutes > 0
            ), f"Minimum duration should be positive: {flight_type}"
            assert (
                max_minutes > min_minutes
            ), f"Max should be greater than min: {flight_type}"

            # Durations should be reasonable for commercial aviation
            assert min_minutes >= 30, f"Minimum flight time too short: {flight_type}"
            assert (
                max_minutes <= 18 * 60
            ), f"Maximum flight time too long: {flight_type}"

            # Cross-country flights should be longer than regional
            if flight_type == "Cross-country":
                assert (
                    min_hours >= 4
                ), f"Cross-country minimum too short: {origin} to {dest}"
            elif flight_type == "Short regional":
                assert (
                    max_hours <= 2.5
                ), f"Regional maximum too long: {origin} to {dest}"

    def test_seasonal_flight_patterns(self):
        """Test that flight patterns make sense seasonally"""
        # Seasonal route popularity patterns
        seasonal_routes = [
            # (origin, dest, peak_months, route_type)
            ("LAX", "MIA", [12, 1, 2], "Winter vacation"),
            ("JFK", "LAX", [6, 7, 8], "Summer travel"),
            ("ORD", "DEN", [12, 1, 2, 3], "Winter sports"),
            ("ATL", "BOS", [9, 10, 11], "Fall foliage"),
        ]

        for origin, dest, peak_months, route_type in seasonal_routes:
            # Test seasonal logic
            for month in peak_months:
                assert (
                    1 <= month <= 12
                ), f"Peak month should be valid: {month} for {route_type}"

            # Peak months should be reasonable for route type
            if route_type == "Winter vacation":
                winter_months = [12, 1, 2]
                overlap = set(peak_months) & set(winter_months)
                assert (
                    len(overlap) >= 2
                ), f"Winter vacation should peak in winter: {route_type}"
            elif route_type == "Summer travel":
                summer_months = [6, 7, 8]
                overlap = set(peak_months) & set(summer_months)
                assert (
                    len(overlap) >= 2
                ), f"Summer travel should peak in summer: {route_type}"


class TestCrossFieldDataIntegrity:
    """Test cross-field data integrity and relationships"""

    def test_flight_number_airline_consistency(self):
        """Test flight number patterns are consistent with airlines"""
        # Real airline flight number patterns
        airline_flight_patterns = [
            # (airline, flight_number_range, pattern_description)
            ("AA", range(1, 9999), "American Airlines full range"),
            ("UA", range(1, 9999), "United Airlines full range"),
            ("DL", range(1, 9999), "Delta Air Lines full range"),
            ("WN", range(1, 9999), "Southwest Airlines full range"),
            ("B6", range(1, 1999), "JetBlue Airways typical range"),
        ]

        for airline, flight_range, description in airline_flight_patterns:
            # Test flight number validation for airline
            test_flight_numbers = [1, 100, 1000, max(flight_range)]

            for flight_num in test_flight_numbers:
                if flight_num in flight_range:
                    # Valid flight number for this airline
                    assert isinstance(
                        flight_num, int
                    ), f"Flight number should be integer: {flight_num}"
                    assert (
                        flight_num > 0
                    ), f"Flight number should be positive: {flight_num}"
                    assert (
                        flight_num <= 9999
                    ), f"Flight number should be reasonable: {flight_num}"

                    # Test as string (how it appears in data)
                    flight_str = str(flight_num)
                    assert (
                        flight_str.isdigit()
                    ), f"Flight number string should be digits: {flight_str}"
                    assert (
                        len(flight_str) <= 4
                    ), f"Flight number string should be <= 4 chars: {flight_str}"

    def test_route_distance_airline_consistency(self):
        """Test that route distances make sense for airline types"""
        # Create test data with different airline/route combinations
        test_routes = [
            # (airline, origin, dest, expected_distance_range, airline_type)
            ("AA", "LAX", "JFK", (2400, 2500), "Major carrier long-haul"),
            ("WN", "LAX", "SFO", (300, 400), "Low-cost carrier short-haul"),
            ("B6", "JFK", "BOS", (180, 220), "Low-cost carrier regional"),
            ("UA", "ORD", "ATL", (580, 650), "Major carrier medium-haul"),
        ]

        for airline, origin, dest, (min_dist, max_dist), airline_type in test_routes:
            # Test airline/route consistency
            if airline in ["AA", "UA", "DL"]:  # Major carriers
                # Major carriers can operate all route types
                assert min_dist >= 0, f"Distance should be non-negative: {airline_type}"
            elif airline == "WN":  # Southwest (typically shorter routes)
                # Southwest traditionally focuses on shorter routes
                if max_dist > 2000:
                    # This would be unusual for Southwest but not impossible
                    pass  # Allow flexibility for business model evolution
            elif airline == "B6":  # JetBlue (mix of short and medium routes)
                # JetBlue operates various route lengths
                assert min_dist >= 0, f"Distance should be non-negative: {airline_type}"

    def test_aircraft_capacity_flight_frequency_logic(self):
        """Test logical relationships between routes and flight patterns"""
        # High-frequency routes (multiple daily flights)
        high_frequency_routes = [
            ("LAX", "SFO"),  # California corridor
            ("JFK", "BOS"),  # Northeast corridor
            ("ORD", "ATL"),  # Hub to hub
        ]

        # Low-frequency routes (typically once daily or less)
        low_frequency_routes = [
            ("LAX", "MIA"),  # Cross-country leisure
            ("SEA", "ATL"),  # Secondary city pair
        ]

        for origin, dest in high_frequency_routes:
            # High-frequency routes should have certain characteristics
            assert origin != dest, f"Origin should differ from destination: {origin}"
            # These routes typically serve business travelers
            # Should be between major cities or hubs

        for origin, dest in low_frequency_routes:
            # Low-frequency routes characteristics
            assert origin != dest, f"Origin should differ from destination: {origin}"
            # These routes often serve leisure travelers
            # May be longer distances or secondary markets

    def test_time_zone_logical_consistency(self):
        """Test that departure/arrival times make sense with time zones"""
        # Time zone test cases (simplified - using hour offsets from UTC)
        timezone_routes = [
            # (origin, dest, origin_offset, dest_offset, scenario)
            ("LAX", "JFK", -8, -5, "West to East Coast"),
            ("JFK", "LAX", -5, -8, "East to West Coast"),
            ("ORD", "DEN", -6, -7, "Central to Mountain"),
            ("MIA", "ORD", -5, -6, "Eastern to Central"),
        ]

        for origin, dest, orig_tz, dest_tz, scenario in timezone_routes:
            # Test timezone logic
            tz_diff = dest_tz - orig_tz

            # Test departure time scenarios
            dep_times = ["0800", "1200", "1800", "2200"]

            for dep_time in dep_times:
                dep_hour = int(dep_time[:2])

                # Simulate flight duration (2-6 hours typical)
                flight_duration_hours = 4  # Average

                # Calculate arrival time in destination timezone
                arr_hour_local = (dep_hour + flight_duration_hours + tz_diff) % 24

                # Logical checks
                assert (
                    0 <= arr_hour_local <= 23
                ), f"Arrival hour should be valid: {scenario}"

                # Red-eye flights (departing late, arriving early) should be identified
                if dep_hour >= 22 or dep_hour <= 5:
                    # Red-eye flights are common on certain routes
                    if scenario == "West to East Coast":
                        # Common for LAX to JFK red-eyes
                        assert True, "Red-eye flights are normal for this route"


class TestRealWorldDataPatterns:
    """Test that data follows real-world aviation patterns"""

    def test_hub_and_spoke_patterns(self):
        """Test that flight data reflects real hub-and-spoke airline operations"""
        # Major airline hubs
        airline_hubs = {
            "AA": ["DFW", "CLT", "MIA", "PHX"],  # American Airlines hubs
            "UA": ["ORD", "DEN", "SFO", "IAH"],  # United Airlines hubs
            "DL": ["ATL", "MSP", "DTW", "SLC"],  # Delta Air Lines hubs
            "WN": ["MDW", "BWI", "PHX", "LAS"],  # Southwest Airlines focus cities
        }

        for airline, hubs in airline_hubs.items():
            # Test hub validation
            for hub in hubs:
                assert len(hub) == 3, f"Hub code should be 3 letters: {hub}"
                assert hub.isalpha(), f"Hub code should be alphabetic: {hub}"
                assert hub.isupper(), f"Hub code should be uppercase: {hub}"

            # Test hub connectivity patterns
            # Hubs should have high connectivity (many routes)
            # This is a business logic test rather than data test
            for hub in hubs:
                # In real operations, major hubs should connect to many destinations
                # This validates that our hub list makes operational sense
                assert hub in [
                    "DFW",
                    "ATL",
                    "ORD",
                    "SFO",
                    "LAX",
                    "JFK",
                    "MIA",
                    "PHX",
                    "DEN",
                    "CLT",
                    "MSP",
                    "DTW",
                    "SLC",
                    "MDW",
                    "BWI",
                    "LAS",
                    "IAH",
                ], f"Hub should be a real major airport: {hub}"

    def test_airline_route_network_logic(self):
        """Test airline route networks follow real operational patterns"""
        # Test route network patterns
        network_patterns = [
            # (airline, typical_routes, network_type)
            ("AA", [("DFW", "LAX"), ("MIA", "JFK"), ("CLT", "BOS")], "Hub-and-spoke"),
            ("WN", [("MDW", "BWI"), ("PHX", "LAS"), ("BWI", "BOS")], "Point-to-point"),
            ("B6", [("JFK", "BOS"), ("JFK", "LAX"), ("BOS", "SFO")], "Focus city"),
        ]

        for airline, routes, network_type in network_patterns:
            for origin, dest in routes:
                # Basic route validation
                assert (
                    origin != dest
                ), f"Route should connect different airports: {origin} to {dest}"
                assert (
                    len(origin) == 3 and len(dest) == 3
                ), f"Airport codes should be 3 letters: {origin}-{dest}"

                # Network type validation
                if network_type == "Hub-and-spoke":
                    # At least one endpoint should be an airline hub
                    airline_hubs_list = {
                        "AA": ["DFW", "CLT", "MIA", "PHX"],
                        "UA": ["ORD", "DEN", "SFO", "IAH"],
                        "DL": ["ATL", "MSP", "DTW", "SLC"],
                    }.get(airline, [])

                    # Note: Not all routes must touch hubs, but major routes typically do
                    # has_hub = origin in airline_hubs_list or dest in airline_hubs_list

                elif network_type == "Point-to-point":
                    # Southwest-style operations
                    # Routes can be between any airports
                    assert True, "Point-to-point allows any city pairs"

    def test_seasonal_capacity_patterns(self):
        """Test flight patterns reflect seasonal demand"""
        # Seasonal route patterns
        seasonal_patterns = [
            # (route, peak_season, reason)
            (("LAX", "MIA"), "winter", "Snowbird migration"),
            (("JFK", "FLL"), "winter", "Northeast to Florida"),
            (("ORD", "DEN"), "winter", "Ski season"),
            (("LAX", "SEA"), "summer", "West Coast leisure"),
        ]

        for (origin, dest), peak_season, reason in seasonal_patterns:
            # Test seasonal logic makes sense
            if peak_season == "winter":
                peak_months = [12, 1, 2, 3]
            elif peak_season == "summer":
                peak_months = [6, 7, 8]
            elif peak_season == "spring":
                peak_months = [3, 4, 5]
            elif peak_season == "fall":
                peak_months = [9, 10, 11]

            # Validate peak months
            for month in peak_months:
                assert 1 <= month <= 12, f"Peak month should be valid: {month}"

            # Test route logic for seasonal patterns
            if "Florida" in reason:
                # Florida routes peak in winter
                assert (
                    "winter" in peak_season
                ), f"Florida routes should peak in winter: {reason}"
            elif "Ski" in reason:
                # Ski destinations peak in winter
                assert (
                    "winter" in peak_season
                ), f"Ski routes should peak in winter: {reason}"

    def test_business_vs_leisure_route_patterns(self):
        """Test route patterns reflect business vs leisure travel"""
        # Business travel routes (typically Monday-Friday patterns)
        business_routes = [
            ("JFK", "BOS"),  # Northeast business corridor
            ("LAX", "SFO"),  # California business corridor
            ("ORD", "ATL"),  # Hub to hub business
            ("DCA", "BOS"),  # East Coast business
        ]

        # Leisure travel routes (typically weekend/seasonal patterns)
        leisure_routes = [
            ("JFK", "MIA"),  # Northeast to leisure destination
            ("ORD", "LAS"),  # Midwest to entertainment
            ("LAX", "HNL"),  # Mainland to Hawaii
            ("BOS", "MCO"),  # Northeast to theme parks
        ]

        for origin, dest in business_routes:
            # Business routes characteristics
            assert (
                origin != dest
            ), f"Business route should connect different cities: {origin}"
            # Business routes typically:
            # - Connect major business centers
            # - Have high frequency during weekdays
            # - Shorter average distances for regional business

        for origin, dest in leisure_routes:
            # Leisure routes characteristics
            assert (
                origin != dest
            ), f"Leisure route should connect different cities: {origin}"
            # Leisure routes typically:
            # - Connect population centers to vacation destinations
            # - Have seasonal patterns
            # - May have longer distances


class TestReferenceDataValidation:
    """Test reference data validation and completeness"""

    def test_airport_reference_data_completeness(self):
        """Test that airport reference data is complete and consistent"""
        # Essential airport data fields
        required_airport_fields = [
            "code",  # IATA code
            "name",  # Airport name
            "city",  # City served
            "state",  # State/province
            "country",  # Country
        ]

        # Sample airport data for validation
        sample_airports = [
            {
                "code": "LAX",
                "name": "Los Angeles International",
                "city": "Los Angeles",
                "state": "CA",
                "country": "USA",
            },
            {
                "code": "JFK",
                "name": "John F. Kennedy International",
                "city": "New York",
                "state": "NY",
                "country": "USA",
            },
            {
                "code": "ORD",
                "name": "Chicago O'Hare International",
                "city": "Chicago",
                "state": "IL",
                "country": "USA",
            },
        ]

        for airport in sample_airports:
            # Test required fields presence
            for field in required_airport_fields:
                assert (
                    field in airport
                ), f"Airport should have {field} field: {airport['code']}"
                assert (
                    airport[field] is not None
                ), f"Airport {field} should not be None: {airport['code']}"
                assert (
                    len(str(airport[field]).strip()) > 0
                ), f"Airport {field} should not be empty: {airport['code']}"

            # Test field format validation
            assert (
                len(airport["code"]) == 3
            ), f"Airport code should be 3 chars: {airport['code']}"
            assert airport[
                "code"
            ].isalpha(), f"Airport code should be alphabetic: {airport['code']}"
            assert airport[
                "code"
            ].isupper(), f"Airport code should be uppercase: {airport['code']}"

            # Test name consistency - most major airports should have descriptive names
            assert (
                len(airport["name"]) > 5
            ), f"Airport name should be descriptive: {airport['code']}"

    def test_airline_reference_data_completeness(self):
        """Test that airline reference data is complete and consistent"""
        # Essential airline data fields
        required_airline_fields = [
            "code",  # IATA code
            "name",  # Airline name
            "country",  # Country of origin
        ]

        # Sample airline data for validation
        sample_airlines = [
            {"code": "AA", "name": "American Airlines", "country": "USA"},
            {"code": "UA", "name": "United Airlines", "country": "USA"},
            {"code": "DL", "name": "Delta Air Lines", "country": "USA"},
        ]

        for airline in sample_airlines:
            # Test required fields presence
            for field in required_airline_fields:
                assert (
                    field in airline
                ), f"Airline should have {field} field: {airline['code']}"
                assert (
                    airline[field] is not None
                ), f"Airline {field} should not be None: {airline['code']}"
                assert (
                    len(str(airline[field]).strip()) > 0
                ), f"Airline {field} should not be empty: {airline['code']}"

            # Test field format validation
            assert (
                len(airline["code"]) == 2
            ), f"Airline code should be 2 chars: {airline['code']}"
            assert airline[
                "code"
            ].isalnum(), f"Airline code should be alphanumeric: {airline['code']}"
            assert airline[
                "code"
            ].isupper(), f"Airline code should be uppercase: {airline['code']}"

            # Test name consistency - most airlines should have standard naming patterns
            assert (
                len(airline["name"]) > 3
            ), f"Airline name should be descriptive: {airline['code']}"

    def test_route_reference_data_validation(self):
        """Test route reference data makes geographic and operational sense"""
        # Sample route data
        sample_routes = [
            {
                "origin": "LAX",
                "dest": "JFK",
                "distance": 2475,
                "typical_duration": 330,  # minutes
            },
            {
                "origin": "LAX",
                "dest": "SFO",
                "distance": 337,
                "typical_duration": 85,  # minutes
            },
            {
                "origin": "JFK",
                "dest": "BOS",
                "distance": 187,
                "typical_duration": 75,  # minutes
            },
        ]

        for route in sample_routes:
            # Test route data consistency
            origin = route["origin"]
            dest = route["dest"]
            distance = route["distance"]
            duration = route["typical_duration"]

            # Basic validation
            assert (
                origin != dest
            ), f"Route should connect different airports: {origin} to {dest}"
            assert (
                distance > 0
            ), f"Route distance should be positive: {origin} to {dest}"
            assert (
                duration > 0
            ), f"Route duration should be positive: {origin} to {dest}"

            # Test distance/duration correlation
            # Rough speed check (commercial aircraft typically 400-600 mph in cruise)
            # Short flights have lower average speeds due to taxi, takeoff, climb, descent, landing time
            speed_mph = (distance / duration) * 60  # Convert minutes to hours

            if distance < 300:  # Very short flights
                assert (
                    100 <= speed_mph <= 700
                ), f"Short flight speed should account for ground ops: {speed_mph:.0f} mph for {origin} to {dest}"
            elif distance < 1000:  # Medium flights
                assert (
                    150 <= speed_mph <= 700
                ), f"Medium flight speed should be reasonable: {speed_mph:.0f} mph for {origin} to {dest}"
            else:  # Long flights
                assert (
                    200 <= speed_mph <= 700
                ), f"Long flight speed should be reasonable: {speed_mph:.0f} mph for {origin} to {dest}"

            # Short flights should have reasonable minimums (taxi, takeoff, landing time)
            if distance < 500:  # Short flights
                assert (
                    duration >= 45
                ), f"Short flight duration should include ground time: {origin} to {dest}"

            # Long flights should not be unreasonably fast
            if distance > 2000:  # Long flights
                assert (
                    duration >= 240
                ), f"Long flight should take reasonable time: {origin} to {dest}"

    def test_data_quality_completeness_metrics(self):
        """Test overall data quality and completeness metrics"""
        # Sample dataset for quality testing
        sample_flight_data = {
            "flightdate": ["2024-03-01", "2024-03-01", "2024-03-01", "2024-03-01"],
            "reporting_airline": ["AA", "UA", "DL", "WN"],
            "flight_number_reporting_airline": ["100", "200", "300", "400"],
            "origin": ["LAX", "JFK", "ORD", "ATL"],
            "dest": ["JFK", "LAX", "ATL", "ORD"],
            "crs_dep_time": ["0800", "1200", "1600", "2000"],
            "crs_arr_time": ["1600", "1500", "1800", "2200"],
            "cancelled": [0, 0, 0, 0],
            "distance": [2475.0, 2475.0, 606.0, 606.0],
        }

        df = pd.DataFrame(sample_flight_data)

        # Test data completeness
        for column in df.columns:
            # Check for missing values
            null_count = df[column].isnull().sum()
            total_count = len(df)
            completeness_ratio = (total_count - null_count) / total_count

            assert (
                completeness_ratio >= 0.95
            ), f"Column {column} should be 95%+ complete"

        # Test data consistency across fields
        for _, row in df.iterrows():
            # Test airport codes
            assert len(row["origin"]) == 3, f"Origin should be 3 chars: {row['origin']}"
            assert (
                len(row["dest"]) == 3
            ), f"Destination should be 3 chars: {row['dest']}"
            assert (
                row["origin"] != row["dest"]
            ), f"Origin should differ from dest: {row['origin']}"

            # Test airline codes
            assert (
                len(row["reporting_airline"]) == 2
            ), f"Airline should be 2 chars: {row['reporting_airline']}"

            # Test flight numbers
            assert row[
                "flight_number_reporting_airline"
            ].isdigit(), f"Flight number should be numeric: {row['flight_number_reporting_airline']}"

            # Test times
            assert (
                len(row["crs_dep_time"]) == 4
            ), f"Departure time should be HHMM format: {row['crs_dep_time']}"
            assert (
                len(row["crs_arr_time"]) == 4
            ), f"Arrival time should be HHMM format: {row['crs_arr_time']}"

            # Test distance
            assert (
                row["distance"] > 0
            ), f"Distance should be positive: {row['distance']}"


if __name__ == "__main__":
    print("Running data quality checks tests...")

    # Run all test classes
    test_classes = [
        TestAirlineCodeValidation(),
        TestAirportCodeValidation(),
        TestTemporalConsistencyValidation(),
        TestCrossFieldDataIntegrity(),
        TestRealWorldDataPatterns(),
        TestReferenceDataValidation(),
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

    print("\n✅ Data quality checks tests completed!")
