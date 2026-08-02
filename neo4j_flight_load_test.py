#!/usr/bin/env python3
"""
Load test for itinerary search.
===============================

Drives concurrent traffic at the same code path the service serves —
`flight_search.search_itineraries()` — rather than holding its own copy of the
Cypher. That matters for more than tidiness: the previous version of this file
carried a *different, wrong* query (a `CASE`-based duration that read a westbound
timezone offset as a midnight crossing, returning 1439 minutes for a 59-minute
flight), so it was load-testing something the service would never run.

    locust -f neo4j_flight_load_test.py                      # web UI at :8089
    locust -f neo4j_flight_load_test.py --headless \
           --users 50 --spawn-rate 5 --run-time 300s --csv locust

Then: `python quick_load_test_analysis.py locust_stats.csv`.

Two tasks, weighted 70/30 — a nonstop lookup and a full multi-stop search, which
is roughly how a booking front end behaves (most searches are on routes with
nonstops). The weights are stated here rather than in prose elsewhere; there is no
other distribution.

What this file gets right that its predecessor did not, all of which changed the
numbers rather than just the code:

* **Sampling is by flight volume**, not `ORDER BY a.code` then `[:100]`. That slice
  was alphabetical — ABE…ELM — which excluded 19 of the 30 busiest airports (ORD,
  LAX, JFK, LGA, SFO, EWR, …) and left ~95% of the weighted task returning zero
  rows. A load test where the hot path is empty measures nothing.
* **One driver for the whole process**, from `flight_search.get_driver()`, created
  on first use and shared by every simulated user. The old code built one per
  user, so driver construction and TLS setup landed inside every measurement.
* **Constant Locust stat names.** Per-pair names produced up to 29,700
  single-sample rows, making every reported percentile a percentile of one or two
  observations.
* **Dates come from `CONNECTS_TO` coverage**, so sampled dates are searchable.
  `--build-connections` is per-date, and searching an unbuilt date returns empty
  instantly — fast, and meaningless. The old unbounded `DISTINCT` over every
  Schedule node also ran once per simulated user.

Requires a loaded graph with `--solve-offsets` and `--build-connections` run for at
least one date; it fails at startup with that instruction rather than reporting
zeros.
"""

import random
import threading
import time

from locust import User, between, task

import flight_search

# Sampling universe. 60 origins is enough to spread load across real hubs without
# the sample degenerating to a handful of supernodes.
TOP_ORIGINS = 60

# Depth served by default. See the latency table in flight_search.py: {0,3} exceeds
# 200 ms on 25-27 of 40 pairs unfiltered, so load-testing it would measure a
# configuration the service does not serve.
MAX_STOPS = flight_search.DEFAULT_MAX_STOPS

# Locust stat names. Constant on purpose — these are what percentiles aggregate
# over, and they are the strings quick_load_test_analysis.py matches on.
NONSTOP_TASK = "nonstop lookup"
SEARCH_TASK = f"itinerary search {{0,{MAX_STOPS}}}"


def _busiest_origins(driver, database, date):
    """The `TOP_ORIGINS` airports with the most departures on `date`."""
    query = """
    MATCH (s:Schedule {flightdate: date($date)})-[:DEPARTS_FROM]->(a:Airport)
    RETURN a.code AS code, count(*) AS flights
    ORDER BY flights DESC, code
    LIMIT $limit
    """
    with driver.session(database=database) as session:
        return [
            record["code"]
            for record in session.run(query, date=date, limit=TOP_ORIGINS)
        ]


class ItinerarySearchUser(User):
    """
    One simulated traveller issuing searches against the graph.

    The airport and date universe is loaded once per process (class attributes),
    not per user: it is identical for every user and the queries behind it are not
    what this test is measuring.
    """

    wait_time = between(1, 3)

    airports = None
    dates = None

    # Locust starts each user in its own greenlet, and every one of them calls
    # on_start. An unlocked `if cls.airports is None` check therefore races: a
    # measured 10-user run ran the setup queries 5 times, because each greenlet
    # yielded inside session.run() before any of them had assigned the result.
    # Same double-checked pattern as flight_search.get_driver().
    _setup_lock = threading.Lock()

    @classmethod
    def _prepare(cls):
        if cls.airports is not None:
            return
        with cls._setup_lock:
            if cls.airports is not None:
                return
            cls._load_universe()

    @classmethod
    def _load_universe(cls):
        driver = flight_search.get_driver()
        database = flight_search.get_database()

        dates = flight_search.searchable_dates(driver, database)
        if not dates:
            raise RuntimeError(
                "No dates have CONNECTS_TO edges, so every search would return "
                "empty instantly and measure nothing. Run:\n"
                "  python load_bts_data.py --solve-offsets YYYY-MM-DD\n"
                "  python load_bts_data.py --build-connections YYYY-MM-DD"
            )

        # Sample airports from a date that is actually searchable, so volume rank
        # reflects the day being queried.
        airports = _busiest_origins(driver, database, dates[0])
        if len(airports) < 2:
            raise RuntimeError(
                f"Only {len(airports)} airports found — load data first: "
                "python load_bts_data.py"
            )

        # Assigned last, and `airports` last of the two: it is the sentinel
        # _prepare() checks, so setting it before validation would cache a
        # rejected universe and let later users skip the error entirely.
        cls.dates = dates
        cls.airports = airports
        print(
            f"✅ {len(airports)} origins by volume, "
            f"{len(dates)} searchable dates ({dates[0]}…{dates[-1]})"
        )

    def on_start(self):
        self._prepare()

    def _route(self):
        """A random real origin/destination pair. `random` selects among values
        read out of the database; it never invents one (CLAUDE.md rule 1)."""
        origin, dest = random.sample(self.airports, 2)  # nosec B311
        return origin, dest, random.choice(self.dates)  # nosec B311

    def _timed(self, name, call):
        """Run `call`, reporting wall clock and result count to Locust."""
        started = time.perf_counter()
        try:
            results = call()
        except Exception as exc:
            self.environment.events.request.fire(
                request_type="Neo4j",
                name=name,
                response_time=int((time.perf_counter() - started) * 1000),
                response_length=0,
                exception=exc,
            )
            return []
        self.environment.events.request.fire(
            request_type="Neo4j",
            name=name,
            response_time=int((time.perf_counter() - started) * 1000),
            response_length=len(results),
        )
        return results

    @task(70)
    def nonstop_lookup(self):
        """ "Is there a nonstop?" — the cheapest and commonest search."""
        origin, dest, date = self._route()
        self._timed(
            NONSTOP_TASK,
            lambda: flight_search.search_itineraries(
                origin, dest, date, max_stops=0, limit=10
            ),
        )

    @task(30)
    def itinerary_search(self):
        """Full search across all depths at once, ranked by total journey."""
        origin, dest, date = self._route()
        self._timed(
            SEARCH_TASK,
            lambda: flight_search.search_itineraries(
                origin, dest, date, max_stops=MAX_STOPS, limit=20
            ),
        )


if __name__ == "__main__":
    print(__doc__)
