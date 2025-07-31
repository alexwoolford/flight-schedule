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

    def test_datetime_types_in_graph(self):
        """CRITICAL: Test that datetime properties are stored as native DateTime objects, not integers"""
        try:
            from neo4j import GraphDatabase
            from dotenv import load_dotenv
            import os
            
            load_dotenv(override=True)
            uri = os.getenv('NEO4J_URI')
            username = os.getenv('NEO4J_USERNAME') 
            password = os.getenv('NEO4J_PASSWORD')
            database = os.getenv('NEO4J_DATABASE', 'flights')
            
            # Skip test if Neo4j not available (CI without loaded data)
            if not all([uri, username, password]):
                pytest.skip("Neo4j credentials not available - skipping datetime type test")
                
            driver = GraphDatabase.driver(uri, auth=(username, password))
            
            query = '''
            MATCH (s:Schedule)
            RETURN 
              s.date_of_operation,
              s.first_seen_time,
              s.last_seen_time
            LIMIT 1
            '''
            
            with driver.session(database=database) as session:
                result = session.run(query)
                record = result.single()
                
                if record:
                    date_val = record["s.date_of_operation"]
                    first_val = record["s.first_seen_time"] 
                    last_val = record["s.last_seen_time"]
                    
                    # CRITICAL: These should be Neo4j DateTime objects, NOT integers
                    assert not isinstance(date_val, int), f"date_of_operation should be DateTime, not int: {date_val}"
                    assert not isinstance(first_val, int), f"first_seen_time should be DateTime, not int: {first_val}"
                    assert not isinstance(last_val, int), f"last_seen_time should be DateTime, not int: {last_val}"
                    
                    # They should be Neo4j DateTime or Python datetime objects
                    from neo4j.time import DateTime
                    from datetime import datetime
                    valid_types = (DateTime, datetime)
                    
                    assert isinstance(date_val, valid_types), f"date_of_operation type: {type(date_val)}"
                    assert isinstance(first_val, valid_types), f"first_seen_time type: {type(first_val)}"
                    assert isinstance(last_val, valid_types), f"last_seen_time type: {type(last_val)}"
                else:
                    pytest.skip("No Schedule nodes found - skipping datetime type test")
                    
            driver.close()
            
        except Exception as e:
            # If Neo4j is not available, skip the test
            if "Cannot resolve address" in str(e) or "Connection refused" in str(e):
                pytest.skip(f"Neo4j not available - skipping datetime type test: {e}")
            else:
                pytest.fail(f"DateTime type test failed: {e}")
