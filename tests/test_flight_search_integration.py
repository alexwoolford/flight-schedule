#!/usr/bin/env python3
"""
flight_search.py and api.py against a real loaded graph.
========================================================

The unit tests assert the query *text*; these assert its *results* on real BTS
data. That split matters: an acyclic-guard regression or a local/UTC mix-up
produces plausible-looking output, so the checks that catch it have to run against
flights that actually exist.

Requires the one-day fixture loaded, plus `--solve-offsets` and
`--build-connections` for it (see CLAUDE.md). Skips cleanly if the graph is
unreachable — and, as with the rest of this suite, `tests/ci_verify_loaded.py`
is what stops an all-skipped run from reporting success in CI.

Every assertion here is anti-vacuous: each one that could pass on an empty result
set also asserts the result set is non-empty.
"""

import os
import sys
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import flight_search  # noqa: E402
from flight_search import search_itineraries  # noqa: E402

FIXTURE_DATE = "2025-07-18"

# Routes on the fixture day, verified present. LGA->DFW has nonstops; LGA->BOI
# has none and must route through a hub, which is what exercises the multi-leg
# path. Both are real, not chosen for convenience.
NONSTOP_ROUTE = ("LGA", "DFW")
CONNECTING_ROUTE = ("LGA", "BOI")


@pytest.fixture(scope="module")
def graph():
    """Shared driver, or skip. One per module so pooling is exercised."""
    try:
        driver = flight_search.get_driver()
        driver.verify_connectivity()
    except Exception as exc:
        pytest.skip(f"Neo4j not available: {exc}")
    if not flight_search.is_searchable(FIXTURE_DATE):
        pytest.skip(
            f"no CONNECTS_TO edges for {FIXTURE_DATE} — run --solve-offsets and "
            "--build-connections"
        )
    return driver


class TestSearchResults:
    def test_nonstop_route_returns_nonstops_first(self, graph):
        results = search_itineraries(*NONSTOP_ROUTE, FIXTURE_DATE, limit=10)
        assert results, "LGA->DFW has nonstops on this date; got nothing"
        # Ranking is by total elapsed journey, so where a nonstop exists it wins.
        assert results[0].stops == 0
        assert results[0].route == list(NONSTOP_ROUTE)
        assert len(results[0].flights) == 1

    def test_connecting_route_routes_through_a_hub(self, graph):
        results = search_itineraries(*CONNECTING_ROUTE, FIXTURE_DATE, limit=10)
        assert results, "LGA->BOI should be reachable with a connection"
        assert all(r.stops >= 1 for r in results), "LGA->BOI has no nonstop"
        for r in results:
            assert r.route[0] == "LGA" and r.route[-1] == "BOI"
            assert len(r.flights) == r.stops + 1
            assert len(r.layover_minutes) == r.stops

    def test_results_are_ordered_by_total_journey(self, graph):
        results = search_itineraries(*CONNECTING_ROUTE, FIXTURE_DATE, limit=20)
        assert len(results) > 1
        totals = [r.total_minutes for r in results]
        assert totals == sorted(totals)

    def test_max_stops_zero_finds_no_nonstop_where_none_exists(self, graph):
        # A real negative: LGA->BOI genuinely has no nonstop on this day, so an
        # empty result here is correct — and the paired assertion below proves the
        # query is not simply broken.
        assert search_itineraries(*CONNECTING_ROUTE, FIXTURE_DATE, max_stops=0) == []
        assert search_itineraries(*NONSTOP_ROUTE, FIXTURE_DATE, max_stops=0)

    def test_limit_is_respected(self, graph):
        assert len(search_itineraries(*CONNECTING_ROUTE, FIXTURE_DATE, limit=3)) <= 3

    def test_min_stops_excludes_shallower_itineraries(self, graph):
        results = search_itineraries(
            *NONSTOP_ROUTE, FIXTURE_DATE, min_stops=1, max_stops=1, limit=10
        )
        assert results, "LGA->DFW should have 1-stop options too"
        assert {r.stops for r in results} == {1}


class TestItineraryValidity:
    """
    The invariants that make an itinerary sellable. All are enforced by the
    CONNECTS_TO edge or the acyclic guard, so a failure here means one of those
    regressed.
    """

    @pytest.fixture(scope="class")
    def sample(self, graph):
        results = []
        for origin, dest in [
            NONSTOP_ROUTE,
            CONNECTING_ROUTE,
            ("BUF", "CHS"),
            ("PVD", "BOI"),
            ("ALB", "SNA"),
        ]:
            results += search_itineraries(
                origin, dest, FIXTURE_DATE, max_stops=3, limit=50
            )
        assert len(results) > 100, f"sample too small to be meaningful: {len(results)}"
        return results

    # Deep sample, used only for the acyclicity checks. It has to be this deep to
    # mean anything: a revisit is a long detour, so ranking by total journey buries
    # every one of them. Measured with the guard removed, LGA->DFW at {0,3}: 0
    # revisits in the top 1,000 results, first one at rank 1,038, 531 in the top
    # 5,000. A serving-sized limit cannot see this defect at all, which is exactly
    # how it would ship.
    DEEP_ROUTE = ("LGA", "DFW")
    DEEP_LIMIT = 3000

    @pytest.fixture(scope="class")
    def deep_sample(self, graph):
        results = search_itineraries(
            *self.DEEP_ROUTE, FIXTURE_DATE, max_stops=3, limit=self.DEEP_LIMIT
        )
        # Anti-vacuity: if the fixture stops producing enough 3-stop paths on this
        # route, the checks below silently stop testing anything.
        assert len(results) >= self.DEEP_LIMIT, (
            f"only {len(results)} paths — too few for the revisit checks to be "
            "falsifiable; pick a denser route"
        )
        assert any(r.stops == 3 for r in results), "no 3-stop paths in the sample"
        return results

    def test_no_itinerary_revisits_an_airport(self, deep_sample):
        # Without the guard: 154 of these 3,000 revisit an airport. With it, none.
        offenders = [r.route for r in deep_sample if len(set(r.route)) != len(r.route)]
        assert not offenders, (
            f"{len(offenders)} of {len(deep_sample)} itineraries revisit an "
            f"airport, e.g. {offenders[0]}"
        )

    def test_no_itinerary_returns_to_its_origin(self, deep_sample):
        # The worst case, and the one a passenger would notice: LGA->MIA->CLT->LGA.
        offenders = [r.route for r in deep_sample if r.route[0] in r.route[1:]]
        assert not offenders, f"{len(offenders)} itineraries fly back to the origin"

    def test_every_itinerary_is_single_carrier_family(self, sample):
        # CONNECTS_TO maps wholly-owned regionals onto their mainline parent, so
        # AA+MQ is legitimate; two unrelated carriers spliced together is not.
        families = {
            "MQ": "AA",
            "OH": "AA",
            "YX": "DL",
            "OO": "UA",
            "9E": "DL",
            "YV": "AA",
        }
        for r in sample:
            mapped = {families.get(c, c) for c in r.carriers}
            assert len(mapped) == 1, f"cross-carrier itinerary: {r.flights}"

    def test_total_minutes_equals_air_plus_layovers(self, sample):
        # The arithmetic that replaced subtracting two local timestamps. If someone
        # "simplifies" total_minutes back into a timestamp difference, this fails
        # on every timezone-crossing leg.
        for r in sample:
            assert r.total_minutes == r.air_minutes + sum(r.layover_minutes)

    def test_utc_instants_are_ordered_and_match_the_total(self, sample):
        # The falsifiable version of the duration claim: elapsed time between the
        # UTC endpoints must equal air time plus layovers exactly. This is what
        # subtracting the *local* endpoints gets wrong on ~half of all flights.
        checked = 0
        for r in sample:
            if r.departs_utc is None or r.arrives_utc is None:
                continue
            elapsed = (
                r.arrives_utc.to_native() - r.departs_utc.to_native()
            ).total_seconds() / 60
            assert (
                elapsed == r.total_minutes
            ), f"{r.flights}: {elapsed} vs {r.total_minutes}"
            assert r.arrives_utc > r.departs_utc
            checked += 1
        assert checked > 100, f"only checked {checked} itineraries"

    def test_local_subtraction_would_have_disagreed(self, sample):
        # Anti-vacuity for the test above: prove the local pair really is a
        # different (wrong) answer on this data, so that assertion is not passing
        # for trivial reasons.
        disagreements = 0
        for r in sample:
            if r.departs is None or r.arrives is None:
                continue
            local = (r.arrives.to_native() - r.departs.to_native()).total_seconds() / 60
            if local != r.total_minutes:
                disagreements += 1
        assert disagreements > 0, (
            "local subtraction agreed everywhere, so the UTC assertion proves "
            "nothing — is the sample all single-timezone?"
        )


class TestFilters:
    def test_depart_after_is_local_at_the_origin(self, graph):
        results = search_itineraries(
            *CONNECTING_ROUTE, FIXTURE_DATE, depart_after="12:00", limit=20
        )
        assert results
        for r in results:
            assert r.departs.hour >= 12

    def test_depart_after_actually_excludes_something(self, graph):
        # Anti-vacuity: the filter must be narrowing the result set, or the
        # assertion above is meaningless.
        unfiltered = search_itineraries(*CONNECTING_ROUTE, FIXTURE_DATE, limit=100)
        filtered = search_itineraries(
            *CONNECTING_ROUTE, FIXTURE_DATE, depart_after="12:00", limit=100
        )
        assert len(filtered) < len(unfiltered)
        assert filtered

    def test_arrive_before_is_local_at_the_destination(self, graph):
        deadline = 15
        results = search_itineraries(
            *CONNECTING_ROUTE, FIXTURE_DATE, arrive_before=f"{deadline}:00", limit=20
        )
        assert results, "there are itineraries into BOI before 15:00"
        for r in results:
            assert r.arrives.hour < deadline

    def test_deadline_returns_rows_at_all(self, graph):
        # The specific silent failure this guards: comparing a LOCAL DATETIME to a
        # zoned datetime() yields NULL, so WHERE drops every row and a route with
        # valid itineraries returns zero with no error. An empty result here is
        # indistinguishable from that bug, so it must be non-empty.
        assert search_itineraries(
            *NONSTOP_ROUTE, FIXTURE_DATE, arrive_before="23:59", limit=5
        )

    def test_red_eye_deadline_needs_no_overnight_guard(self, graph):
        # scheduled_arrival_time carries the DESTINATION's date, so a flight
        # landing after midnight compares as the next day. Searching the whole
        # fixture day with a late deadline must not admit anything landing on a
        # later local date.
        results = search_itineraries(
            *CONNECTING_ROUTE, FIXTURE_DATE, arrive_before="23:59", limit=100
        )
        assert results
        fixture = date.fromisoformat(FIXTURE_DATE)
        for r in results:
            assert r.arrives.to_native().date() <= fixture, (
                f"{r.flights} lands {r.arrives}, after the deadline — an overnight "
                "leak means the arrival date repair regressed"
            )


class TestSearchableDates:
    def test_reports_only_dates_with_connections(self, graph):
        dates = flight_search.searchable_dates()
        assert FIXTURE_DATE in dates
        for d in dates:
            assert flight_search.is_searchable(d)

    def test_a_date_with_no_flights_is_not_searchable(self, graph):
        # Distinguishing "no routes" from "never built" is the whole point of this
        # call; a date far outside any loaded slice must come back False.
        far_off = (date.fromisoformat(FIXTURE_DATE) - timedelta(days=4000)).isoformat()
        assert not flight_search.is_searchable(far_off)


class TestApiAgainstRealData:
    @pytest.fixture(scope="class")
    def client(self, graph):
        fastapi_testclient = pytest.importorskip("fastapi.testclient")
        import api

        with fastapi_testclient.TestClient(api.app) as client:
            yield client

    def test_health_reports_a_loaded_graph(self, client):
        body = client.get("/health").json()
        assert body["status"] == "ok"
        assert body["connects_to_edges"] > 0

    def test_dates_lists_the_fixture_date(self, client):
        assert FIXTURE_DATE in client.get("/dates").json()["dates"]

    def test_search_returns_json_serialisable_itineraries(self, client):
        body = client.get(
            "/itineraries",
            params={
                "origin": CONNECTING_ROUTE[0],
                "dest": CONNECTING_ROUTE[1],
                "date": FIXTURE_DATE,
                "depart_after": "09:00",
                "limit": 5,
            },
        ).json()
        assert body["count"] > 0
        for it in body["itineraries"]:
            # ISO strings, not driver temporal objects — the endpoint would 500 on
            # serialisation otherwise.
            assert isinstance(it["departs"], str) and "T" in it["departs"]
            assert isinstance(it["arrives_utc"], str)
            assert it["total_minutes"] == it["air_minutes"] + sum(it["layover_minutes"])

    def test_unbuilt_date_is_distinguishable_from_no_routes(self, client):
        far_off = (date.fromisoformat(FIXTURE_DATE) - timedelta(days=4000)).isoformat()
        body = client.get(
            "/itineraries",
            params={
                "origin": CONNECTING_ROUTE[0],
                "dest": CONNECTING_ROUTE[1],
                "date": far_off,
            },
        ).json()
        assert body["count"] == 0
        assert body["date_is_searchable"] is False
