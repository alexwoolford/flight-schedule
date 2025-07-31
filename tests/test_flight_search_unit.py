#!/usr/bin/env python3
"""
Unit Tests for Flight Search Logic
=================================

Pure unit tests that don't require Neo4j database connection.
Tests basic Python functionality and imports.
"""

import os
import sys
from datetime import datetime

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestBasicFunctionality:
    """Basic unit tests that verify core functionality"""

    def test_imports_work(self):
        """Test that core modules can be imported"""
        try:
            from flight_search_demo import FlightSearchDemo

            assert FlightSearchDemo is not None
        except ImportError as e:
            pytest.fail(f"Could not import FlightSearchDemo: {e}")

    def test_datetime_operations(self):
        """Test basic datetime operations used in flight search"""
        # Test date string parsing
        date_str = "2024-06-18"
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        assert parsed_date.year == 2024
        assert parsed_date.month == 6
        assert parsed_date.day == 18

        # Test time range validation
        start_hour = 9
        end_hour = 13
        assert start_hour < end_hour
        assert 0 <= start_hour <= 23
        assert 0 <= end_hour <= 23

    def test_string_operations(self):
        """Test string operations used in flight search"""
        # Test airport code normalization
        code = "eddf"
        normalized = code.upper().strip()
        assert normalized == "EDDF"

        # Test city name cleaning
        city = " Nice "
        cleaned = city.strip().lower()
        assert cleaned == "nice"

    def test_environment_setup(self):
        """Test that required environment setup works"""
        # Test that we can import required packages
        import os
        import sys

        # Test basic path operations
        current_dir = os.path.dirname(__file__)
        parent_dir = os.path.dirname(current_dir)
        assert os.path.exists(current_dir)
        assert os.path.exists(parent_dir)

        # Test Python version compatibility
        assert sys.version_info >= (3, 9)
