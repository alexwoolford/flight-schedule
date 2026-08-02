#!/usr/bin/env python3
"""
Unit tests for flight_search.py and api.py — no database.
=========================================================

What can be checked without Neo4j is input normalisation, the rendered Cypher,
and HTTP status mapping. Those are also where the silent failures live: a bad
`localdatetime`/`datetime` choice or a dropped acyclicity guard produces wrong
*results* rather than an error, so the query text is asserted directly.

Correctness against real data is `tests/test_flight_search_integration.py`.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import flight_search  # noqa: E402
from flight_search import SearchError, build_search_query  # noqa: E402


class FakeResult(list):
    """A neo4j Result is iterable and has .single(); that is all this needs."""

    def single(self):
        return self[0] if self else None


class FakeSession:
    """Captures the query and params a call would have sent."""

    def __init__(self, records=None):
        self.records = records or []
        self.calls = []

    def run(self, query, **params):
        self.calls.append((query, params))
        return FakeResult(self.records)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeDriver:
    def __init__(self, records=None):
        self.session_obj = FakeSession(records)
        self.sessions = 0

    def session(self, **kwargs):
        self.sessions += 1
        return self.session_obj


class TestAirportNormalisation:
    def test_lowercase_and_whitespace_are_accepted(self):
        driver = FakeDriver()
        flight_search.search_itineraries(
            " lga ", "boi", "2025-07-18", driver=driver, database="neo4j"
        )
        _, params = driver.session_obj.calls[0]
        assert params["origin"] == "LGA"
        assert params["dest"] == "BOI"

    @pytest.mark.parametrize("bad", ["", "  ", "L", "LGAX", "LG1", "12", None, 5])
    def test_non_iata_codes_are_rejected(self, bad):
        with pytest.raises(SearchError):
            flight_search.search_itineraries(
                bad, "BOI", "2025-07-18", driver=FakeDriver(), database="neo4j"
            )

    def test_same_origin_and_dest_is_rejected(self):
        # Not merely pointless: {0,2} from LGA back to LGA is a nonsense query the
        # acyclicity guard would filter to nothing after doing the traversal.
        with pytest.raises(SearchError, match="both LGA"):
            flight_search.search_itineraries(
                "LGA", "lga", "2025-07-18", driver=FakeDriver(), database="neo4j"
            )


class TestDateAndTimeNormalisation:
    def test_date_accepts_iso_string_and_date_objects(self):
        import datetime as dt

        for value in ("2025-07-18", dt.date(2025, 7, 18), dt.datetime(2025, 7, 18, 9)):
            driver = FakeDriver()
            flight_search.search_itineraries(
                "LGA", "BOI", value, driver=driver, database="neo4j"
            )
            assert driver.session_obj.calls[-1][1]["date"] == "2025-07-18"

    @pytest.mark.parametrize(
        "bad", ["18/07/2025", "2025-13-01", "July 18", "", 20250718]
    )
    def test_bad_dates_are_rejected(self, bad):
        with pytest.raises(SearchError):
            flight_search.search_itineraries(
                "LGA", "BOI", bad, driver=FakeDriver(), database="neo4j"
            )

    def test_bare_time_is_resolved_against_the_search_date(self):
        driver = FakeDriver()
        flight_search.search_itineraries(
            "LGA",
            "BOI",
            "2025-07-18",
            depart_after="09:00",
            driver=driver,
            database="neo4j",
        )
        assert driver.session_obj.calls[0][1]["depart_after"] == "2025-07-18T09:00:00"

    def test_full_timestamp_is_passed_through(self):
        # The escape hatch for "allow landing after midnight": a caller who wants
        # a deadline past midnight has to say so explicitly, because silently
        # rolling it would return itineraries the caller meant to exclude.
        driver = FakeDriver()
        flight_search.search_itineraries(
            "LGA",
            "BOI",
            "2025-07-18",
            arrive_before="2025-07-19T02:00",
            driver=driver,
            database="neo4j",
        )
        assert driver.session_obj.calls[0][1]["arrive_before"] == "2025-07-19T02:00:00"

    @pytest.mark.parametrize("bad", ["9am", "25:00", "noon", "9", ""])
    def test_bad_times_are_rejected(self, bad):
        with pytest.raises(SearchError):
            flight_search.search_itineraries(
                "LGA",
                "BOI",
                "2025-07-18",
                depart_after=bad,
                driver=FakeDriver(),
                database="neo4j",
            )


class TestStopBounds:
    def test_zero_stops_uses_a_single_node_path(self):
        # Cypher rejects both {0,0} and {0} as quantifiers, so the nonstop case
        # must not be rendered as one. This test exists because that is a syntax
        # error at query time, not import time — it would only surface in
        # production on a max_stops=0 request.
        query = build_search_query(0, 0)
        assert "{0,0}" not in query and "->{0}" not in query
        assert "MATCH p = (first)" in query
        assert "WITH p, first AS last" in query

    def test_positive_bounds_render_a_quantifier(self):
        assert "-[:CONNECTS_TO]->{0,2}" in build_search_query(0, 2)
        assert "-[:CONNECTS_TO]->{1,1}" in build_search_query(1, 1)

    @pytest.mark.parametrize("lo,hi", [(-1, 2), (2, 1), (1, 0)])
    def test_invalid_bounds_are_rejected(self, lo, hi):
        with pytest.raises(SearchError):
            build_search_query(lo, hi)
        with pytest.raises(SearchError):
            flight_search.search_itineraries(
                "LGA",
                "BOI",
                "2025-07-18",
                min_stops=lo,
                max_stops=hi,
                driver=FakeDriver(),
                database="neo4j",
            )

    def test_quantifier_cannot_be_injected(self):
        # The quantifier is string-substituted because Cypher will not take it as
        # a parameter, so the bounds must be ints by the time they reach the
        # template. A string that looks like Cypher must not survive.
        with pytest.raises((SearchError, ValueError)):
            build_search_query(0, "2}(x)-[:CONNECTS_TO]->{0,9")

    def test_limit_must_be_positive(self):
        with pytest.raises(SearchError):
            flight_search.search_itineraries(
                "LGA",
                "BOI",
                "2025-07-18",
                limit=0,
                driver=FakeDriver(),
                database="neo4j",
            )


class TestRenderedQuery:
    """
    The query text carries three things that fail *silently* if wrong. Asserting
    on the text is the only DB-free way to catch a regression in them.
    """

    def test_acyclicity_guard_is_always_present(self):
        # Without it, 18.41% of LGA->DFW itineraries at 3 stops revisit an airport
        # and 385 of 11,488 fly back to the origin. Nothing errors; the results
        # are just wrong.
        for lo, hi in [(0, 0), (0, 2), (1, 3)]:
            query = build_search_query(lo, hi)
            assert "airports[i] IN airports[0..i]" in query

    def test_deadline_uses_localdatetime_not_datetime(self):
        # scheduled_arrival_time is a LOCAL DATETIME. Comparing it to a zoned
        # datetime() yields NULL, not false, so WHERE drops every row and a route
        # with 40 valid itineraries returns zero with no error.
        query = build_search_query(0, 2, arrive_before="2025-07-18T15:00:00")
        assert "localdatetime($arrive_before)" in query
        assert "datetime($arrive_before)" not in query.replace(
            "localdatetime($arrive_before)", ""
        )

    def test_depart_filter_uses_localdatetime(self):
        query = build_search_query(0, 2, depart_after="2025-07-18T09:00:00")
        assert "localdatetime($depart_after)" in query

    def test_filters_are_absent_when_not_requested(self):
        query = build_search_query(0, 2)
        assert "$depart_after" not in query
        assert "$arrive_before" not in query

    def test_duration_is_never_a_local_subtraction(self):
        # The defect this whole model exists to avoid: subtracting two local
        # timestamps that belong to different airports. Totals must come from
        # scheduled_duration_minutes plus layover_minutes.
        query = build_search_query(0, 2)
        assert "scheduled_duration_minutes" in query
        assert "layover_minutes" in query
        assert "duration.between" not in query
        assert "duration.inSeconds" not in query

    def test_no_overnight_guard_is_reintroduced(self):
        # Every query-side overnight guard tried here was wrong: a +/-180-minute
        # block tolerance left 11,975 impossible edges, and comparing
        # date(arrival_utc) to date(departure_utc) tests UTC midnight, excluding
        # 3,135 ordinary evening flights. The loader fixes this; the query must
        # not try to.
        query = build_search_query(0, 2, arrive_before="2025-07-18T15:00:00")
        assert "date(" not in query.replace("date($date)", "")


class TestSearchExecution:
    def test_one_query_is_issued_not_one_per_depth(self):
        # Iterative deepening was measured far slower at the tail (p95 1,323ms vs
        # 63ms with a morning departure filter) and gives up global ranking. If
        # someone reintroduces the loop, this fails.
        driver = FakeDriver()
        flight_search.search_itineraries(
            "LGA", "BOI", "2025-07-18", max_stops=2, driver=driver, database="neo4j"
        )
        assert len(driver.session_obj.calls) == 1

    def test_records_become_itineraries(self):
        record = {
            "stops": 1,
            "flights": ["DL2510", "DL2304"],
            "route": ["LGA", "MSP", "BOI"],
            "carriers": ["DL", "DL"],
            "departs": "2025-07-18T06:05:00",
            "arrives": "2025-07-18T10:50:00",
            "departs_utc": "2025-07-18T10:05:00",
            "arrives_utc": "2025-07-18T16:50:00",
            "air_minutes": 354,
            "layover_minutes": [51],
            "total_minutes": 405,
        }
        driver = FakeDriver([record])
        results = flight_search.search_itineraries(
            "LGA", "BOI", "2025-07-18", driver=driver, database="neo4j"
        )
        assert len(results) == 1
        assert results[0].flights == ["DL2510", "DL2304"]
        assert results[0].total_minutes == 405
        assert results[0].as_dict()["route"] == ["LGA", "MSP", "BOI"]

    def test_as_dict_is_json_serialisable(self):
        import json

        import neo4j.time

        record = {
            "stops": 0,
            "flights": ["DL878"],
            "route": ["LGA", "DFW"],
            "carriers": ["DL"],
            # Real driver types, not strings: as_dict has to convert these or the
            # endpoint 500s on serialisation.
            "departs": neo4j.time.DateTime(2025, 7, 18, 6, 0),
            "arrives": neo4j.time.DateTime(2025, 7, 18, 8, 40),
            "departs_utc": neo4j.time.DateTime(2025, 7, 18, 10, 0),
            "arrives_utc": neo4j.time.DateTime(2025, 7, 18, 13, 40),
            "air_minutes": 220,
            "layover_minutes": [],
            "total_minutes": 220,
        }
        driver = FakeDriver([record])
        result = flight_search.search_itineraries(
            "LGA", "DFW", "2025-07-18", driver=driver, database="neo4j"
        )[0]
        payload = json.dumps(result.as_dict())
        assert "2025-07-18T06:00:00" in payload


class TestDriverPooling:
    def test_driver_is_created_once_and_shared(self, monkeypatch):
        # One driver per process. The old load test built one per simulated user,
        # which put driver and TLS setup inside every measurement.
        created = []

        class Sentinel:
            def close(self):
                created.append("closed")

        def fake_driver(uri, **kwargs):
            created.append(kwargs)
            return Sentinel()

        monkeypatch.setattr(flight_search, "_driver", None)
        monkeypatch.setattr(flight_search.GraphDatabase, "driver", fake_driver)
        monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")

        first = flight_search.get_driver()
        second = flight_search.get_driver()
        assert first is second
        assert len(created) == 1
        assert created[0]["max_connection_pool_size"] == flight_search.DEFAULT_POOL_SIZE
        flight_search.close_driver()

    def test_missing_uri_raises_rather_than_connecting_to_a_default(self, monkeypatch):
        # Rule 5: no hard-coded connection details. A missing URI must fail
        # loudly, not silently try localhost.
        monkeypatch.setattr(flight_search, "_driver", None)
        monkeypatch.setattr(flight_search, "load_dotenv", lambda **kw: None)
        monkeypatch.delenv("NEO4J_URI", raising=False)
        with pytest.raises(RuntimeError, match="NEO4J_URI"):
            flight_search.get_driver()


class TestApiStatusMapping:
    """
    The API's own job is status codes: bad input is the caller's problem (400/422),
    an unreachable graph is the service's (503). Getting these backwards makes a
    dashboard lie about which is broken.
    """

    @pytest.fixture
    def client(self, monkeypatch):
        from fastapi.testclient import TestClient

        import api

        # Never touch a real database from a DB-free test.
        monkeypatch.setattr(api.flight_search, "get_driver", lambda: FakeDriver())
        monkeypatch.setattr(api.flight_search, "close_driver", lambda: None)
        with TestClient(api.app) as client:
            yield client

    def test_bad_airport_is_400(self, client, monkeypatch):
        import api

        def boom(**kwargs):
            raise SearchError("origin must be a 3-letter IATA code")

        monkeypatch.setattr(api.flight_search, "search_itineraries", boom)
        response = client.get(
            "/itineraries", params={"origin": "XX", "dest": "BOI", "date": "2025-07-18"}
        )
        assert response.status_code == 400
        assert "IATA" in response.json()["detail"]

    def test_out_of_range_stops_is_422(self, client):
        response = client.get(
            "/itineraries",
            params={
                "origin": "LGA",
                "dest": "BOI",
                "date": "2025-07-18",
                "max_stops": 9,
            },
        )
        assert response.status_code == 422

    def test_unreachable_graph_is_503_not_500(self, client, monkeypatch):
        from neo4j.exceptions import ServiceUnavailable

        import api

        def boom(**kwargs):
            raise ServiceUnavailable("cannot resolve address")

        monkeypatch.setattr(api.flight_search, "search_itineraries", boom)
        response = client.get(
            "/itineraries",
            params={"origin": "LGA", "dest": "BOI", "date": "2025-07-18"},
        )
        assert response.status_code == 503
        assert "unavailable" in response.json()["detail"]

    def test_empty_result_reports_whether_the_date_was_built(self, client, monkeypatch):
        import api

        monkeypatch.setattr(api.flight_search, "search_itineraries", lambda **kw: [])
        monkeypatch.setattr(api.flight_search, "is_searchable", lambda d: False)
        body = client.get(
            "/itineraries",
            params={"origin": "LGA", "dest": "BOI", "date": "2025-01-05"},
        ).json()
        assert body["count"] == 0
        # Without this the caller cannot tell "no routes" from "date not built".
        assert body["date_is_searchable"] is False

    def test_diagnostic_failure_does_not_break_a_valid_empty_answer(
        self, client, monkeypatch
    ):
        from neo4j.exceptions import ServiceUnavailable

        import api

        def boom(date):
            raise ServiceUnavailable("dropped")

        monkeypatch.setattr(api.flight_search, "search_itineraries", lambda **kw: [])
        monkeypatch.setattr(api.flight_search, "is_searchable", boom)
        response = client.get(
            "/itineraries",
            params={"origin": "LGA", "dest": "BOI", "date": "2025-07-18"},
        )
        assert response.status_code == 200
        assert "date_is_searchable" not in response.json()

    def test_health_is_503_when_no_connections_are_built(self, client, monkeypatch):
        # A static {"ok": true} would hide exactly this: a healthy service in
        # front of a database nothing has been loaded into.
        import api

        monkeypatch.setattr(
            api.flight_search, "get_driver", lambda: FakeDriver([{"edges": 0}])
        )
        response = client.get("/health")
        assert response.status_code == 503
        assert "build-connections" in response.json()["detail"]

    def test_health_reports_edge_count_when_loaded(self, client, monkeypatch):
        import api

        monkeypatch.setattr(
            api.flight_search, "get_driver", lambda: FakeDriver([{"edges": 623508}])
        )
        body = client.get("/health").json()
        assert body == {"status": "ok", "connects_to_edges": 623508}
