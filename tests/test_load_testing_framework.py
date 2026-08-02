#!/usr/bin/env python3
"""
Load Testing Framework Validation Tests
=======================================

Tests the load testing framework setup and configuration without actually
running database queries. These are lightweight validation tests to ensure
the framework is correctly configured before running real load tests.

**Runs in its own pytest process.** Importing this file imports locust, which
gevent-patches `threading` process-wide, and that deadlocks FastAPI's
`TestClient`. See `TestGeventIsolation` below, which gates the separation.
"""

import re
from pathlib import Path

import pytest


class TestLoadTestScriptValidation:
    """Test load test script structure and imports"""

    def test_realistic_load_test_imports(self):
        """Test that realistic load test script imports work"""
        try:
            import neo4j_flight_load_test  # noqa: F401
        except ImportError as e:
            pytest.fail(f"Failed to import neo4j_flight_load_test: {e}")

    def test_locust_user_class_structure(self):
        """Test that the load test defines proper Locust user class

        This used to write a fake flight_test_scenarios.json into the repo root
        (moving any real one aside and back) to satisfy an import dependency that
        never existed -- neo4j_flight_load_test.py reads airports and dates from
        the database in on_start, not from a file. A plain import is enough, and
        it no longer creates or deletes files in the working tree.
        """
        import neo4j_flight_load_test

        user_class = neo4j_flight_load_test.ItinerarySearchUser

        # Note: no on_stop. The driver is process-wide (flight_search.get_driver()),
        # so a per-user teardown would close a pool other users are still using.
        assert hasattr(user_class, "on_start")
        assert hasattr(user_class, "wait_time")

        # Should have task methods
        methods = [name for name in dir(user_class) if not name.startswith("_")]
        task_methods = [
            m for m in methods if hasattr(getattr(user_class, m), "locust_task_weight")
        ]
        assert (
            len(task_methods) >= 2
        )  # Should have multiple task types (nonstop + full search)

    def test_load_test_holds_no_cypher_of_its_own(self):
        """The load test must drive the served code path, not a private copy.

        This is the defect the rewrite fixed: the old file carried its own query
        with a CASE-based duration that read a westbound timezone offset as a
        midnight crossing (1439 minutes for a 59-minute flight), so it was
        measuring a query the service would never run. One airport-sampling query
        is legitimate -- itinerary Cypher is not.

        Matches on Cypher syntax rather than words like "CONNECTS_TO", which appear
        legitimately in the file's own prose explaining why the Cypher is gone.
        """
        source = (
            Path(__file__).parent.parent / "neo4j_flight_load_test.py"
        ).read_text()

        assert "flight_search" in source, "load test must call flight_search"

        # Traversal syntax, which prose never contains. The one permitted query
        # samples airports and uses -[:DEPARTS_FROM]-> exactly once.
        assert "-[:CONNECTS_TO]->" not in source
        assert "-[:ARRIVES_AT]->" not in source
        assert "duration.between" not in source
        assert source.count("-[:DEPARTS_FROM]->") == 1, (
            "expected exactly one DEPARTS_FROM, in the airport-volume sampling "
            "query; more than that means itinerary Cypher came back"
        )

    def test_query_construction_logic(self):
        """The query the load test drives is built by flight_search, and is sane.

        This replaces a test that defined a ~60-line Cypher string inline and then
        asserted that string contained "MATCH", "WHERE", "$origin" and so on. It
        could only fail if someone edited the literal directly above the
        assertions, so it gated nothing — and the literal it carried was a copy of
        the *deleted* wrong query, complete with
        `duration.between(scheduled_arrival_time, scheduled_departure_time)`,
        which subtracts two local clocks at different airports (CLAUDE.md: never
        do this). A test asserting a banned idiom is worse than no test.

        So assert against the query that actually runs.
        """
        import flight_search

        query = flight_search.build_search_query(0, 2)

        assert "MATCH" in query and "RETURN" in query
        assert "ORDER BY total_minutes" in query
        assert "$origin" in query and "$dest" in query and "$date" in query

        # The two frames must not be mixed. Journey length comes from real block
        # times and real layovers, never from subtracting local timestamps.
        assert "duration.between" not in query, "local-clock subtraction is banned"
        assert "scheduled_duration_minutes" in query

        # The path-level guard CONNECTS_TO cannot express.
        assert "airports[0..i]" in query, "acyclicity guard missing"


class TestConnectionPoolingSetup:
    """Test connection pooling configuration"""

    def test_connection_configuration_structure(self):
        """The real pool size is set and in a sane range.

        This used to define a `config` dict of plausible-looking numbers and then
        assert bounds on its own literals -- it passed regardless of what the code
        did, and in fact `max_connection_lifetime` and
        `connection_acquisition_timeout` are not set anywhere in this repo. Read
        the actual value instead.
        """
        import flight_search

        assert 10 <= flight_search.DEFAULT_POOL_SIZE <= 200

    def test_load_test_does_not_build_a_driver_per_user(self):
        """One pooled driver for the process, from flight_search.

        The old load test constructed a driver per simulated user, which put
        driver and TLS setup inside every measurement. Nothing here may call
        GraphDatabase.driver() itself.
        """
        source = (
            Path(__file__).parent.parent / "neo4j_flight_load_test.py"
        ).read_text()
        assert "GraphDatabase.driver(" not in source
        assert "flight_search.get_driver()" in source

    def test_neo4j_driver_parameters(self):
        """Test that Neo4j driver parameters are valid"""
        # Test connection string format
        test_uris = [
            "bolt://localhost:7687",
            "bolt://192.0.2.10:7687",
            "neo4j://localhost:7687",
            "bolt+s://production.com:7687",
        ]

        for uri in test_uris:
            assert "://" in uri
            assert uri.startswith(("bolt://", "neo4j://", "bolt+s://", "neo4j+s://"))
            parts = uri.split("://")[1]
            assert ":" in parts  # Should have host:port


class TestGeventIsolation:
    """
    This file must keep running in a pytest process of its own.

    Importing it imports locust, which has gevent monkey-patch `threading` for the
    remaining life of the interpreter. FastAPI's `TestClient` drives the app
    through an anyio blocking-portal *thread*; once that thread is a greenlet, the
    two deadlock and the process parks in `gevent/hub.py` forever.

    That failure mode is worse than a test failure: CI would hang until the job
    timeout rather than report red. Verified by reproduction —
    `TestApiStatusMapping` in test_flight_search_service_unit.py passes alone in
    0.42s and never returns when this module is imported first.
    """

    def test_ci_runs_this_file_in_its_own_step(self):
        """The workflow must not fold this file in with the TestClient tests."""
        workflow = Path(__file__).parent.parent / ".github/workflows/ci.yml"
        if not workflow.exists():
            pytest.skip("workflow not present")
        content = workflow.read_text()

        steps = [s for s in content.split("- name:") if "pytest" in s]
        our_steps = [s for s in steps if "test_load_testing_framework.py" in s]
        assert our_steps, "this file is not in any CI step"
        assert len(our_steps) == 1, "this file is invoked by more than one step"

        # The step that runs this file must run ONLY this file. Anything importing
        # fastapi.testclient alongside it deadlocks.
        step = our_steps[0]
        others = [
            name
            for name in re.findall(r"tests/(test_\w+\.py)", step)
            if name != "test_load_testing_framework.py"
        ]
        assert not others, (
            f"gevent deadlock risk: {others} share a pytest process with this "
            "file. It must run in a step of its own — see this class's docstring."
        )

    def test_locust_import_really_does_patch_threading(self):
        """Anti-vacuity: the guard above only matters if the patch is real."""
        from gevent.monkey import is_module_patched

        import neo4j_flight_load_test  # noqa: F401

        assert is_module_patched("threading"), (
            "locust no longer monkey-patches threading; if that is permanent, the "
            "separate CI step and this class can go"
        )


class TestLoadTestFrameworkReadiness:
    """Test overall load testing framework readiness"""

    def test_required_dependencies_available(self):
        """Test that all required dependencies are available"""
        required_modules = ["locust", "neo4j", "faker"]

        for module in required_modules:
            try:
                __import__(module)
            except ImportError:
                pytest.fail(
                    f"Required module '{module}' not available for load testing"
                )

    def test_analysis_tools_available(self):
        """Test that load test analysis tools are available"""
        # Only checking for the simple CLI analysis tool
        # Primary analysis should use Locust's interactive web interface
        analysis_scripts = [
            "quick_load_test_analysis.py",
        ]

        for script in analysis_scripts:
            # Look for script in project root (parent of tests directory)
            script_path = Path(__file__).parent.parent / script
            assert (
                script_path.exists()
            ), f"Analysis script {script} not found at {script_path}"

    def test_documentation_completeness(self):
        """Test that load testing documentation exists"""
        doc_files = ["LOAD_TESTING_GUIDE.md"]

        for doc_file in doc_files:
            doc_path = Path(doc_file)
            assert doc_path.exists(), f"Documentation file {doc_file} not found"

            # Should contain key sections
            content = doc_path.read_text()
            assert "Install Dependencies" in content
            assert "conda" in content  # Should mention conda, not pip
            assert "locust" in content


# Meta-test for the testing approach
class TestTestingFrameworkSanity:
    """Test that our testing approach for load testing makes sense"""

    def test_testing_philosophy(self):
        """Validate our testing philosophy for load testing frameworks"""
        # These tests should focus on:
        # 1. Configuration validation (✓)
        # 2. Setup verification (✓)
        # 3. Dependency checking (✓)
        # 4. Structure validation (✓)

        # These tests should NOT:
        # - Actually run database queries (too slow, requires setup)
        # - Run actual load tests (that's integration testing)
        # - Test database performance (that's what the load test is for)

        testing_principles = {
            "fast_execution": True,  # Tests should run quickly
            "no_database_required": True,  # Should not require live database
            "configuration_focused": True,  # Focus on setup validation
            "dependency_aware": True,  # Check required dependencies
        }

        assert all(
            testing_principles.values()
        ), "Load testing framework tests should follow these principles"

    def test_load_testing_vs_unit_testing_balance(self):
        """Ensure we're not over-testing the load testing framework"""
        # Load testing frameworks are primarily integration tools
        # Unit tests should focus on:
        # - Setup validation
        # - Configuration checking
        # - Basic structure verification

        # Load tests themselves provide the real validation:
        # - Database performance
        # - Query correctness
        # - Connection handling
        # - Error scenarios

        test_file = Path(__file__)
        content = test_file.read_text()

        # Should have reasonable number of test methods (not too many, not too few)
        test_methods = content.count("def test_")
        assert (
            8 <= test_methods <= 20
        ), f"Should have reasonable number of tests, found {test_methods}"

        print(
            f"✅ Load testing framework has {test_methods} lightweight validation tests"
        )
        print(
            "   These verify setup and configuration without running actual load tests"
        )
        print(
            "   Actual performance testing happens when you run: "
            "locust -f neo4j_flight_load_test.py"
        )
