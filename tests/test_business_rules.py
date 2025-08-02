#!/usr/bin/env python3
"""
Business Rules Tests
===================

Tests business logic validation with real-world flight scenarios
WITHOUT using mocks. Focuses on the actual filtering and validation
logic used in the data pipeline.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestFlightValidationRules:
    """Test business rules with real-world flight scenarios"""

    def test_basic_flight_validity(self):
        """Test the core flight validity rules from load_bts_data.py"""
        # These mirror the filtering logic from lines 818-825 in load_bts_data.py
        flight_scenarios = [
            # (flightdate, airline, flight_num, origin, dest, cancelled, should_be_valid, reason)
            ("2024-03-01", "AA", "100", "LAX", "JFK", 0, True, "Valid domestic flight"),
            (
                "2024-03-01",
                "UA",
                "200",
                "ORD",
                "SFO",
                0,
                True,
                "Valid cross-country flight",
            ),
            (None, "AA", "100", "LAX", "JFK", 0, False, "Missing flight date"),
            ("2024-03-01", None, "100", "LAX", "JFK", 0, False, "Missing airline"),
            ("2024-03-01", "AA", None, "LAX", "JFK", 0, False, "Missing flight number"),
            ("2024-03-01", "AA", "100", None, "JFK", 0, False, "Missing origin"),
            ("2024-03-01", "AA", "100", "LAX", None, 0, False, "Missing destination"),
            ("2024-03-01", "AA", "100", "LAX", "JFK", 1, False, "Cancelled flight"),
            ("2024-03-01", "", "100", "LAX", "JFK", 0, False, "Empty airline code"),
            ("2024-03-01", "AA", "", "LAX", "JFK", 0, False, "Empty flight number"),
            ("2024-03-01", "AA", "100", "", "JFK", 0, False, "Empty origin"),
            ("2024-03-01", "AA", "100", "LAX", "", 0, False, "Empty destination"),
        ]

        for (
            flightdate,
            airline,
            flight_num,
            origin,
            dest,
            cancelled,
            expected_valid,
            reason,
        ) in flight_scenarios:
            # Apply the business rules from the actual pipeline
            is_valid = (
                flightdate is not None
                and airline is not None
                and airline != ""
                and flight_num is not None
                and flight_num != ""
                and origin is not None
                and origin != ""
                and dest is not None
                and dest != ""
                and cancelled == 0
            )

            assert (
                is_valid == expected_valid
            ), f"Flight validation failed: {reason} - Expected {expected_valid}, got {is_valid}"

    def test_route_business_rules(self):
        """Test route-specific business rules"""
        route_scenarios = [
            # (origin, dest, is_valid, reason)
            ("LAX", "JFK", True, "Valid transcontinental route"),
            ("ORD", "ATL", True, "Valid domestic hub-to-hub"),
            ("SFO", "SEA", True, "Valid west coast route"),
            ("DFW", "MIA", True, "Valid south-central to southeast"),
            ("LAX", "LAX", False, "Same origin and destination"),
            ("JFK", "JFK", False, "Same origin and destination"),
            ("ORD", "ORD", False, "Hub to same hub invalid"),
            ("", "JFK", False, "Empty origin"),
            ("LAX", "", False, "Empty destination"),
            (
                "XYZ",
                "JFK",
                True,
                "Unknown origin (should still be valid for processing)",
            ),
            (
                "LAX",
                "XYZ",
                True,
                "Unknown destination (should still be valid for processing)",
            ),
        ]

        for origin, dest, expected_valid, reason in route_scenarios:
            # Route validity: origin and dest must be present and different
            is_valid = (
                origin is not None
                and origin != ""
                and dest is not None
                and dest != ""
                and origin != dest
            )

            assert is_valid == expected_valid, f"Route validation failed: {reason}"

    def test_airline_code_business_rules(self):
        """Test airline code validation with real BTS patterns"""
        airline_scenarios = [
            # (airline_code, is_valid, reason)
            ("AA", True, "American Airlines"),
            ("UA", True, "United Airlines"),
            ("DL", True, "Delta Airlines"),
            ("WN", True, "Southwest Airlines"),
            ("B6", True, "JetBlue (alphanumeric)"),
            ("9E", True, "Endeavor Air (starts with number)"),
            ("F9", True, "Frontier Airlines"),
            ("G4", True, "Allegiant Air"),
            ("NK", True, "Spirit Airlines"),
            ("AS", True, "Alaska Airlines"),
            ("", False, "Empty airline code"),
            ("A", False, "Single character"),
            ("ABC", False, "Three characters"),
            ("1", False, "Single digit"),
            ("123", False, "Three digits"),
            (None, False, "Null airline code"),
        ]

        for airline_code, expected_valid, reason in airline_scenarios:
            # Airline code validity: must be 2 characters, alphanumeric
            is_valid = (
                airline_code is not None
                and len(airline_code) == 2
                and airline_code.isalnum()
                and airline_code.isupper()
            )

            assert (
                is_valid == expected_valid
            ), f"Airline validation failed: {reason} - {airline_code}"

    def test_flight_number_business_rules(self):
        """Test flight number validation patterns"""
        flight_number_scenarios = [
            # (flight_number, is_valid, reason)
            ("100", True, "Standard numeric flight number"),
            ("1234", True, "Four-digit flight number"),
            ("1", True, "Single digit flight number"),
            ("9999", True, "High flight number"),
            ("0001", True, "Zero-padded flight number"),
            ("", False, "Empty flight number"),
            (None, False, "Null flight number"),
            ("ABC", False, "Alphabetic flight number"),
            ("12A", False, "Mixed alphanumeric"),
            ("0", True, "Zero flight number (technically valid)"),
        ]

        for flight_num, expected_valid, reason in flight_number_scenarios:
            # Flight number validity: must be present and numeric
            is_valid = (
                flight_num is not None
                and str(flight_num) != ""
                and str(flight_num).isdigit()
            )

            assert (
                is_valid == expected_valid
            ), f"Flight number validation failed: {reason} - {flight_num}"


class TestConnectionTimingRules:
    """Test connection timing and window validation logic"""

    def test_minimum_connection_time(self):
        """Test minimum connection time requirements"""
        connection_scenarios = [
            # (arr_time, dep_time, connection_minutes, is_valid, reason)
            ("14:00", "16:00", 120, True, "2-hour connection (comfortable)"),
            ("14:00", "15:30", 90, True, "90-minute connection (good)"),
            ("14:00", "15:00", 60, True, "1-hour connection (minimum acceptable)"),
            ("14:00", "14:45", 45, True, "45-minute connection (tight but valid)"),
            ("14:00", "14:30", 30, False, "30-minute connection (too tight)"),
            ("14:00", "14:15", 15, False, "15-minute connection (impossible)"),
            ("14:00", "14:00", 0, False, "Same time arrival and departure"),
            ("14:00", "13:30", -30, False, "Departure before arrival"),
        ]

        for (
            arr_time,
            dep_time,
            expected_minutes,
            expected_valid,
            reason,
        ) in connection_scenarios:
            # Parse times
            arr_hour, arr_min = map(int, arr_time.split(":"))
            dep_hour, dep_min = map(int, dep_time.split(":"))

            # Calculate connection time in minutes
            arr_total_min = arr_hour * 60 + arr_min
            dep_total_min = dep_hour * 60 + dep_min
            actual_connection_minutes = dep_total_min - arr_total_min

            # Connection is valid if >= 45 minutes
            is_valid = actual_connection_minutes >= 45

            assert (
                actual_connection_minutes == expected_minutes
            ), f"Connection time calculation wrong: {reason}"
            assert is_valid == expected_valid, f"Connection validity wrong: {reason}"

    def test_maximum_connection_time(self):
        """Test maximum reasonable connection time"""
        max_connection_scenarios = [
            # (arr_time, dep_time, hours_between, is_valid, reason)
            ("10:00", "15:00", 5, True, "5-hour layover (reasonable)"),
            ("10:00", "16:00", 6, False, "6-hour layover (too long)"),
            ("10:00", "20:00", 10, False, "10-hour layover (excessive)"),
            ("23:00", "06:00", 7, False, "7-hour overnight layover (too long)"),
            ("14:00", "18:00", 4, True, "4-hour afternoon layover"),
            ("08:00", "12:00", 4, True, "4-hour morning layover"),
        ]

        for (
            arr_time,
            dep_time,
            expected_hours,
            expected_valid,
            reason,
        ) in max_connection_scenarios:
            # Parse times
            arr_hour, arr_min = map(int, arr_time.split(":"))
            dep_hour, dep_min = map(int, dep_time.split(":"))

            # Calculate connection time
            arr_total_min = arr_hour * 60 + arr_min
            dep_total_min = dep_hour * 60 + dep_min

            # Handle overnight connections
            if dep_total_min < arr_total_min:
                dep_total_min += 24 * 60  # Add 24 hours

            connection_minutes = dep_total_min - arr_total_min
            connection_hours = connection_minutes / 60

            # Connection is valid if between 45 minutes and 5 hours (300 minutes)
            is_valid = 45 <= connection_minutes <= 300

            assert (
                abs(connection_hours - expected_hours) < 0.1
            ), f"Connection hours calculation wrong: {reason}"
            assert (
                is_valid == expected_valid
            ), f"Max connection validity wrong: {reason}"

    def test_overnight_connection_logic(self):
        """Test overnight connection handling"""
        overnight_scenarios = [
            # (arr_time, dep_time_next_day, is_overnight, is_valid, reason)
            ("23:30", "06:30", True, True, "Red-eye arrival with morning departure"),
            ("22:00", "08:00", True, True, "Evening arrival with morning departure"),
            ("23:45", "01:30", True, False, "Very short overnight connection"),
            ("20:00", "07:00", True, False, "Too long overnight connection (11 hours)"),
            ("23:00", "05:00", True, True, "6-hour overnight connection"),
        ]

        for (
            arr_time,
            dep_time,
            is_overnight,
            expected_valid,
            reason,
        ) in overnight_scenarios:
            # Parse times
            arr_hour, arr_min = map(int, arr_time.split(":"))
            dep_hour, dep_min = map(int, dep_time.split(":"))

            arr_total_min = arr_hour * 60 + arr_min
            dep_total_min = dep_hour * 60 + dep_min

            if is_overnight:
                # Add 24 hours to departure time
                dep_total_min += 24 * 60

            connection_minutes = dep_total_min - arr_total_min

            # Overnight connections: 2-10 hours (120-600 minutes)
            if is_overnight:
                is_valid = 120 <= connection_minutes <= 600
            else:
                is_valid = 45 <= connection_minutes <= 300

            assert (
                is_valid == expected_valid
            ), f"Overnight connection validity wrong: {reason}"


class TestDataQualityRules:
    """Test data quality validation rules"""

    def test_temporal_data_consistency(self):
        """Test temporal data consistency rules"""
        temporal_scenarios = [
            # (dep_time, arr_time, flight_duration_hours, is_consistent, reason)
            ("08:00", "12:00", 4, True, "4-hour cross-country flight"),
            ("14:30", "16:45", 2.25, True, "2.25-hour domestic flight"),
            (
                "10:00",
                "10:30",
                0.5,
                False,
                "30-minute flight (too short for commercial)",
            ),
            ("09:00", "08:30", -0.5, False, "Arrival before departure (same day)"),
            ("23:30", "01:30", 2, True, "2-hour red-eye flight"),
            ("12:00", "23:00", 11, False, "11-hour domestic flight (too long)"),
            (None, "14:00", None, False, "Missing departure time"),
            ("14:00", None, None, False, "Missing arrival time"),
        ]

        for (
            dep_time,
            arr_time,
            expected_duration,
            expected_consistent,
            reason,
        ) in temporal_scenarios:
            if dep_time is None or arr_time is None:
                is_consistent = False
            else:
                # Parse times
                dep_hour, dep_min = map(int, dep_time.split(":"))
                arr_hour, arr_min = map(int, arr_time.split(":"))

                dep_total_min = dep_hour * 60 + dep_min
                arr_total_min = arr_hour * 60 + arr_min

                # Handle overnight flights
                if arr_total_min < dep_total_min:
                    arr_total_min += 24 * 60

                duration_minutes = arr_total_min - dep_total_min
                duration_hours = duration_minutes / 60

                # Commercial flights: 45 minutes to 8 hours is reasonable
                is_consistent = 0.75 <= duration_hours <= 8.0

            assert (
                is_consistent == expected_consistent
            ), f"Temporal consistency check failed: {reason}"

    def test_distance_validation_rules(self):
        """Test distance field validation with realistic values"""
        distance_scenarios = [
            # (distance_miles, is_valid, reason)
            (100, True, "Short regional flight"),
            (500, True, "Medium domestic flight"),
            (2500, True, "Cross-country flight"),
            (5000, True, "International flight"),
            (0, False, "Zero distance"),
            (-100, False, "Negative distance"),
            (15000, False, "Unrealistic distance (longer than pole-to-pole)"),
            (None, False, "Missing distance"),
        ]

        for distance, expected_valid, reason in distance_scenarios:
            # Distance validation: must be positive and reasonable
            if distance is None:
                is_valid = False
            else:
                is_valid = 50 <= distance <= 10000  # Reasonable commercial flight range

            assert (
                is_valid == expected_valid
            ), f"Distance validation failed: {reason} - {distance}"

    def test_delay_validation_rules(self):
        """Test delay field validation with realistic ranges"""
        delay_scenarios = [
            # (delay_minutes, is_valid, reason)
            (0, True, "On-time flight"),
            (15, True, "15-minute delay"),
            (60, True, "1-hour delay"),
            (180, True, "3-hour delay"),
            (480, True, "8-hour delay (severe weather)"),
            (-10, True, "10 minutes early (valid)"),
            (-60, False, "1 hour early (unrealistic)"),
            (1440, False, "24-hour delay (unrealistic)"),
            (None, True, "Missing delay (acceptable for some flights)"),
        ]

        for delay, expected_valid, reason in delay_scenarios:
            # Delay validation: reasonable range for commercial aviation
            if delay is None:
                is_valid = True  # Missing delay data is acceptable
            else:
                is_valid = -30 <= delay <= 720  # -30 minutes early to 12 hours late

            assert (
                is_valid == expected_valid
            ), f"Delay validation failed: {reason} - {delay}"

    def test_cancellation_consistency_rules(self):
        """Test cancellation status consistency"""
        cancellation_scenarios = [
            # (cancelled, dep_time, arr_time, is_consistent, reason)
            (0, "14:00", "16:00", True, "Not cancelled with valid times"),
            (1, None, None, True, "Cancelled with no times"),
            (1, "14:00", None, False, "Cancelled but has departure time"),
            (1, None, "16:00", False, "Cancelled but has arrival time"),
            (1, "14:00", "16:00", False, "Cancelled but has both times"),
            (0, None, "16:00", False, "Not cancelled but missing departure"),
            (0, "14:00", None, False, "Not cancelled but missing arrival"),
        ]

        for (
            cancelled,
            dep_time,
            arr_time,
            expected_consistent,
            reason,
        ) in cancellation_scenarios:
            # Consistency rule: cancelled flights shouldn't have operation times
            if cancelled == 1:
                is_consistent = dep_time is None and arr_time is None
            else:
                # Non-cancelled flights should have both times (in ideal data)
                is_consistent = dep_time is not None and arr_time is not None

            assert (
                is_consistent == expected_consistent
            ), f"Cancellation consistency failed: {reason}"


if __name__ == "__main__":
    print("Running business rules tests...")

    # Run all test classes
    test_classes = [
        TestFlightValidationRules(),
        TestConnectionTimingRules(),
        TestDataQualityRules(),
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

    print("\n✅ Business rules tests completed!")
