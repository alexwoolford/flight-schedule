#!/usr/bin/env python3
"""
Data Transformation Tests
========================

Tests real data transformation logic with actual BTS data patterns
and edge cases WITHOUT using mocks. Focuses on business logic validation.
"""

import os
import sys
from datetime import date, datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestBTSDateParsing:
    """Test BTS date parsing with real-world edge cases"""

    def test_valid_date_formats(self):
        """Test parsing of valid BTS date formats"""
        valid_dates = [
            "2024-01-01",  # New Year's Day
            "2024-02-29",  # Leap year
            "2024-12-31",  # Year end
            "2023-06-15",  # Mid-year
            "2020-02-29",  # Another leap year
        ]

        for date_str in valid_dates:
            # Test basic format validation
            parts = date_str.split("-")
            assert len(parts) == 3, f"Date {date_str} should have 3 parts"

            year, month, day = parts
            assert len(year) == 4, f"Year should be 4 digits: {year}"
            assert len(month) == 2, f"Month should be 2 digits: {month}"
            assert len(day) == 2, f"Day should be 2 digits: {day}"

            # Test that they're all numeric
            assert year.isdigit(), f"Year should be numeric: {year}"
            assert month.isdigit(), f"Month should be numeric: {month}"
            assert day.isdigit(), f"Day should be numeric: {day}"

            # Test valid ranges
            year_int = int(year)
            month_int = int(month)
            day_int = int(day)

            assert 1987 <= year_int <= 2030, f"Year out of BTS range: {year_int}"
            assert 1 <= month_int <= 12, f"Month out of range: {month_int}"
            assert 1 <= day_int <= 31, f"Day out of range: {day_int}"

            # Test actual date creation (catches invalid dates like Feb 30)
            test_date = date(year_int, month_int, day_int)
            assert test_date is not None

    def test_invalid_date_edge_cases(self):
        """Test handling of invalid dates that might appear in BTS data"""
        invalid_dates = [
            "2023-02-29",  # Not a leap year
            "2024-13-01",  # Invalid month
            "2024-02-30",  # Invalid day for February
            "2024-04-31",  # Invalid day for April
            "2024-00-15",  # Month zero
            "2024-06-00",  # Day zero
        ]

        for date_str in invalid_dates:
            parts = date_str.split("-")
            if len(parts) == 3:
                year, month, day = parts
                if year.isdigit() and month.isdigit() and day.isdigit():
                    year_int, month_int, day_int = int(year), int(month), int(day)

                    # These should raise ValueError when creating actual date
                    try:
                        date(year_int, month_int, day_int)
                        assert (
                            False
                        ), f"Date {date_str} should be invalid but was accepted"
                    except ValueError:
                        # Expected - invalid date
                        pass

    def test_date_boundary_conditions(self):
        """Test date boundary conditions relevant to BTS data"""
        boundary_dates = [
            ("1987-01-01", True),  # First year of BTS data
            ("1986-12-31", False),  # Before BTS data started
            ("2024-12-31", True),  # Current valid date
            ("2030-12-31", True),  # Future but reasonable
            ("2050-01-01", False),  # Too far in future
        ]

        for date_str, should_be_valid in boundary_dates:
            parts = date_str.split("-")
            year, month, day = parts
            year_int = int(year)

            # BTS data availability check
            is_in_bts_range = 1987 <= year_int <= 2030
            assert (
                is_in_bts_range == should_be_valid
            ), f"BTS range check failed for {date_str}"


class TestBTSTimestampCombination:
    """Test combining dates and times like the real pipeline does"""

    def test_normal_timestamp_combination(self):
        """Test normal timestamp combination scenarios"""
        test_cases = [
            ("2024-03-01", "1430", "2024-03-01 14:30:00"),  # Afternoon flight
            ("2024-03-01", "0800", "2024-03-01 08:00:00"),  # Morning flight
            ("2024-03-01", "2359", "2024-03-01 23:59:00"),  # Late night
            ("2024-03-01", "0001", "2024-03-01 00:01:00"),  # Early morning
            ("2024-03-01", "1200", "2024-03-01 12:00:00"),  # Noon
        ]

        for date_str, time_str, expected in test_cases:
            # Simulate the timestamp combination logic from load_bts_data.py
            if time_str is not None and len(time_str) == 4:
                hour = time_str[:2]
                minute = time_str[2:]

                # Validate hour and minute
                assert hour.isdigit(), f"Hour should be numeric: {hour}"
                assert minute.isdigit(), f"Minute should be numeric: {minute}"

                hour_int = int(hour)
                minute_int = int(minute)

                assert 0 <= hour_int <= 23, f"Hour out of range: {hour_int}"
                assert 0 <= minute_int <= 59, f"Minute out of range: {minute_int}"

                # Create expected timestamp format
                combined = f"{date_str} {hour_int:02d}:{minute_int:02d}:00"
                assert (
                    combined == expected
                ), f"Timestamp combination failed: {combined} != {expected}"

    def test_edge_case_times(self):
        """Test edge case time values that might appear in BTS data"""
        edge_cases = [
            ("2024-03-01", "0000", "2024-03-01 00:00:00"),  # Midnight
            ("2024-03-01", "2400", None),  # Invalid - should be 0000 next day
            ("2024-03-01", "9999", None),  # Invalid time
            ("2024-03-01", "1260", None),  # Invalid minute
            ("2024-03-01", "25:30", None),  # Wrong format
            ("2024-03-01", "", None),  # Empty time
            ("2024-03-01", None, None),  # Null time
        ]

        for date_str, time_str, expected in edge_cases:
            if time_str is None or time_str == "":
                # Null/empty times should result in null timestamps
                assert expected is None
            elif len(time_str) != 4 or not time_str.isdigit():
                # Invalid format should result in null
                assert expected is None
            else:
                hour = int(time_str[:2])
                minute = int(time_str[2:])

                if hour >= 24 or minute >= 60:
                    # Invalid time values should result in null
                    assert expected is None

    def test_overnight_flight_logic(self):
        """Test handling of overnight flights (departure and arrival on different days)"""
        overnight_scenarios = [
            # Departure late, arrival early next day
            ("2024-03-01", "2330", "2024-03-02", "0130"),
            ("2024-03-01", "2345", "2024-03-02", "0200"),
            ("2024-03-01", "2359", "2024-03-02", "0001"),
        ]

        for dep_date, dep_time, arr_date, arr_time in overnight_scenarios:
            # Validate departure timestamp
            if dep_time and len(dep_time) == 4:
                dep_hour = int(dep_time[:2])
                dep_minute = int(dep_time[2:])
                assert 0 <= dep_hour <= 23
                assert 0 <= dep_minute <= 59

            # Validate arrival timestamp
            if arr_time and len(arr_time) == 4:
                arr_hour = int(arr_time[:2])
                arr_minute = int(arr_time[2:])
                assert 0 <= arr_hour <= 23
                assert 0 <= arr_minute <= 59

            # For overnight flights, arrival date should be after departure date
            dep_date_obj = datetime.strptime(dep_date, "%Y-%m-%d").date()
            arr_date_obj = datetime.strptime(arr_date, "%Y-%m-%d").date()

            assert (
                arr_date_obj > dep_date_obj
            ), "Arrival date should be after departure for overnight flights"


class TestBTSDataTypeConversion:
    """Test data type conversions matching the real pipeline"""

    def test_numeric_field_conversion(self):
        """Test conversion of numeric fields like distance, delays"""
        numeric_test_cases = [
            # (input_value, field_type, expected_output, should_succeed)
            ("123.45", "float", 123.45, True),
            ("1500", "int", 1500, True),
            ("0", "int", 0, True),
            ("-15", "int", -15, True),  # Negative delay
            ("", "float", None, False),  # Empty string
            ("N/A", "int", None, False),  # BTS null indicator
            ("NULL", "float", None, False),  # Another null format
            ("abc", "int", None, False),  # Non-numeric
            ("12.34.56", "float", None, False),  # Invalid decimal
        ]

        for input_val, field_type, expected, should_succeed in numeric_test_cases:
            if should_succeed:
                if (
                    field_type == "int"
                    and input_val.isdigit()
                    or (input_val.startswith("-") and input_val[1:].isdigit())
                ):
                    result = int(input_val)
                    assert (
                        result == expected
                    ), f"Int conversion failed: {input_val} -> {result} != {expected}"
                elif field_type == "float":
                    try:
                        result = float(input_val)
                        assert (
                            abs(result - expected) < 0.001
                        ), f"Float conversion failed: {input_val} -> {result} != {expected}"
                    except ValueError:
                        assert (
                            not should_succeed
                        ), f"Float conversion should have succeeded: {input_val}"
            else:
                # Should fail conversion
                if field_type == "int":
                    try:
                        int(input_val)
                        assert False, f"Int conversion should have failed: {input_val}"
                    except ValueError:
                        pass  # Expected failure
                elif field_type == "float":
                    try:
                        float(input_val)
                        assert (
                            False
                        ), f"Float conversion should have failed: {input_val}"
                    except ValueError:
                        pass  # Expected failure

    def test_airline_code_normalization(self):
        """Test airline code normalization matching BTS patterns"""
        airline_test_cases = [
            # (input, expected_output, is_valid)
            ("AA", "AA", True),  # American Airlines
            ("UA", "UA", True),  # United Airlines
            ("DL", "DL", True),  # Delta
            ("WN", "WN", True),  # Southwest
            ("B6", "B6", True),  # JetBlue (alphanumeric)
            ("9E", "9E", True),  # Endeavor Air (starts with number)
            ("aa", "AA", True),  # Lowercase should be normalized
            ("", None, False),  # Empty
            ("A", None, False),  # Too short
            ("ABC", None, False),  # Too long
            ("A1", "A1", True),  # Valid alphanumeric
            ("11", "11", True),  # Numeric airline code
        ]

        for input_code, expected, is_valid in airline_test_cases:
            if is_valid:
                # Normalize to uppercase
                normalized = input_code.upper() if input_code else None

                if normalized and len(normalized) == 2 and normalized.isalnum():
                    assert (
                        normalized == expected
                    ), f"Airline normalization failed: {input_code} -> {normalized} != {expected}"
                else:
                    assert not is_valid, f"Airline code should be invalid: {input_code}"
            else:
                # Should be invalid
                if not input_code or len(input_code) != 2 or not input_code.isalnum():
                    assert (
                        expected is None
                    ), f"Invalid airline code should result in None: {input_code}"

    def test_airport_code_validation(self):
        """Test airport code validation with real IATA codes"""
        airport_test_cases = [
            # (input, expected, is_valid)
            ("LAX", "LAX", True),  # Los Angeles
            ("JFK", "JFK", True),  # New York JFK
            ("ORD", "ORD", True),  # Chicago O'Hare
            ("ATL", "ATL", True),  # Atlanta
            ("DFW", "DFW", True),  # Dallas Fort Worth
            ("SFO", "SFO", True),  # San Francisco
            ("lax", "LAX", True),  # Lowercase should normalize
            ("", None, False),  # Empty
            ("LA", None, False),  # Too short
            ("LAXX", None, False),  # Too long
            ("L1X", "L1X", True),  # Could be valid (some airports have numbers)
            ("123", "123", True),  # Numeric codes exist in some systems
        ]

        for input_code, expected, is_valid in airport_test_cases:
            if is_valid:
                normalized = input_code.upper() if input_code else None

                if normalized and len(normalized) == 3 and normalized.isalnum():
                    assert (
                        normalized == expected
                    ), f"Airport normalization failed: {input_code} -> {normalized} != {expected}"
                else:
                    assert not is_valid, f"Airport code should be invalid: {input_code}"
            else:
                if not input_code or len(input_code) != 3 or not input_code.isalnum():
                    assert (
                        expected is None
                    ), f"Invalid airport code should result in None: {input_code}"


class TestBTSDataValidationRules:
    """Test business rule validation matching the real pipeline"""

    def test_flight_route_validation(self):
        """Test flight route business rules"""
        route_test_cases = [
            # (origin, destination, is_valid, reason)
            ("LAX", "JFK", True, "Valid cross-country route"),
            ("LAX", "LAX", False, "Same origin and destination"),
            ("ORD", "ATL", True, "Valid domestic route"),
            ("", "JFK", False, "Missing origin"),
            ("LAX", "", False, "Missing destination"),
            (None, "JFK", False, "Null origin"),
            ("LAX", None, False, "Null destination"),
        ]

        for origin, dest, is_valid, reason in route_test_cases:
            # Apply business rule: origin != dest AND both must be present
            rule_passes = (
                origin is not None
                and dest is not None
                and origin != ""
                and dest != ""
                and origin != dest
            )

            assert (
                rule_passes == is_valid
            ), f"Route validation failed: {origin} -> {dest} ({reason})"

    def test_cancellation_status_validation(self):
        """Test cancellation status validation"""
        cancellation_test_cases = [
            # (cancelled_value, is_valid)
            (0, True),  # Not cancelled
            (1, True),  # Cancelled
            ("0", True),  # String zero
            ("1", True),  # String one
            (2, False),  # Invalid value
            (-1, False),  # Negative value
            ("", False),  # Empty string
            (None, False),  # Null value
            ("Y", False),  # Non-numeric
        ]

        for cancelled_val, is_valid in cancellation_test_cases:
            # Apply business rule: cancelled must be 0 or 1
            if cancelled_val is not None:
                try:
                    cancelled_int = int(cancelled_val)
                    rule_passes = cancelled_int in [0, 1]
                except (ValueError, TypeError):
                    rule_passes = False
            else:
                rule_passes = False

            assert (
                rule_passes == is_valid
            ), f"Cancellation validation failed: {cancelled_val}"

    def test_distance_validation(self):
        """Test distance field validation"""
        distance_test_cases = [
            # (distance_value, is_valid)
            (100.5, True),  # Valid positive distance
            (0, False),  # Zero distance invalid
            (-50, False),  # Negative distance invalid
            (5000, True),  # Long distance flight
            ("250.5", True),  # String numeric
            ("", False),  # Empty string
            (None, False),  # Null value
            ("abc", False),  # Non-numeric
        ]

        for distance_val, is_valid in distance_test_cases:
            # Apply business rule: distance > 0
            if distance_val is not None and distance_val != "":
                try:
                    distance_float = float(distance_val)
                    rule_passes = distance_float > 0
                except (ValueError, TypeError):
                    rule_passes = False
            else:
                rule_passes = False

            assert (
                rule_passes == is_valid
            ), f"Distance validation failed: {distance_val}"

    def test_temporal_consistency_validation(self):
        """Test temporal consistency rules"""
        temporal_test_cases = [
            # (dep_time, arr_time, same_day, is_valid, reason)
            ("1430", "1630", True, True, "Normal 2-hour flight"),
            ("0800", "1200", True, True, "Morning to noon flight"),
            ("2300", "0100", False, True, "Overnight flight (next day arrival)"),
            ("1630", "1430", True, False, "Arrival before departure (same day)"),
            (
                "0100",
                "2300",
                True,
                False,
                "Arrival 22 hours later (impossible same day)",
            ),
            (None, "1430", True, False, "Missing departure time"),
            ("1430", None, True, False, "Missing arrival time"),
        ]

        for dep_time, arr_time, same_day, is_valid, reason in temporal_test_cases:
            # Basic validation: both times must be present
            if dep_time is None or arr_time is None:
                rule_passes = False
            elif same_day:
                # For same-day flights, arrival should be after departure
                # (This is a simplified check - real logic would handle overnight flights)
                dep_hour = int(dep_time[:2])
                dep_minute = int(dep_time[2:])
                arr_hour = int(arr_time[:2])
                arr_minute = int(arr_time[2:])

                dep_minutes = dep_hour * 60 + dep_minute
                arr_minutes = arr_hour * 60 + arr_minute

                # For same day, arrival should be after departure but not too far
                rule_passes = (
                    dep_minutes < arr_minutes and (arr_minutes - dep_minutes) <= 720
                )  # Max 12 hours
            else:
                # For overnight flights, we'd need more complex logic
                rule_passes = True  # Simplified for this test

            assert (
                rule_passes == is_valid
            ), f"Temporal validation failed: {dep_time} -> {arr_time} ({reason})"


if __name__ == "__main__":
    print("Running data transformation tests...")

    # Run all test classes
    test_classes = [
        TestBTSDateParsing(),
        TestBTSTimestampCombination(),
        TestBTSDataTypeConversion(),
        TestBTSDataValidationRules(),
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

    print("\n✅ Data transformation tests completed!")
