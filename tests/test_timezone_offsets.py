#!/usr/bin/env python3
"""
Gate the UTC-offset solve and the timestamps it produces.
=========================================================

The defect this exists to prevent: `Airport` carries no timezone, and the loader
composes both the local departure and the local arrival onto the *origin's*
flightdate. Subtracting them therefore gives a wrong answer for about half of all
flights, silently. Measured on 2025-07-18 before the fix: arrival-minus-departure
matched the BTS block time for only 10,453 of 21,376 flights (48.9%), and 940
flights appeared to arrive before they departed.

`solve_airport_offsets()` recovers every airport's UTC offset from the loaded data
alone -- no timezone database -- and `write_utc_times()` stores absolute instants.

A note on what is worth asserting. The obvious check,
`arrival_utc - departure_utc == scheduled_duration_minutes`, is a TAUTOLOGY:
write_utc_times() *defines* arrival as departure + block, so it reduces to
"addition works" and passes against arbitrarily wrong offsets. It is kept below
only as a cheap guard that the write actually touched every row. The assertions
that can genuinely fail are:

  * the round-trip (TestUtcTimestamps::test_local_arrival_round_trips), which
    reconstructs local arrival from UTC and compares against the stored value --
    this fails if any single airport's offset is wrong; and
  * the spot check against real-world offsets (TestSolvedOffsets), which is the
    only thing that catches a *uniform* error, since the round-trip is invariant
    to a constant shift and to whole multiples of 24h.

Verified by mutation: shifting every offset by 1h leaves the round-trip at 100%
and is caught only by the spot check; skipping the dateline normalisation likewise
survives the round-trip and is caught only by the GUM assertion; putting one
airport off by 1h drops the round-trip to 19,309/21,376. All three assertions are
load-bearing for different failure classes.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from load_bts_data import (  # noqa: E402
    OFFSET_ANCHOR,
    solve_airport_offsets,
)

# Real July (northern-summer DST) UTC offsets in minutes, from public knowledge --
# deliberately NOT derived from the code under test, which is the whole point of a
# spot check. A uniform error in the solve is invisible to every internal
# consistency check, so these literals are the only external reference point.
#
# Chosen to span every US offset band and both special cases: Arizona (PHX, no
# DST), Hawaii (HNL, no DST), Alaska (ANC), the Caribbean (SJU/STT, no DST), and
# Guam (GUM), which sits west of the dateline and is the one value that comes out
# +600 rather than -840 only because of the normalisation step.
KNOWN_JULY_OFFSETS = {
    "JFK": -240,
    "LGA": -240,
    "BOS": -240,
    "ATL": -240,
    "MIA": -240,
    "DTW": -240,
    "ORD": -300,
    "MDW": -300,
    "DFW": -300,
    "MSP": -300,
    "IAH": -300,
    "DEN": -360,
    "SLC": -360,
    "PHX": -420,
    "LAX": -420,
    "SFO": -420,
    "SEA": -420,
    "ANC": -480,
    "HNL": -600,
    "GUM": 600,
    "SJU": -240,
    "STT": -240,
}


@pytest.fixture(scope="module")
def offsets(neo4j_driver, neo4j_database, search_date, loaded_graph):
    """Solve offsets for the busiest loaded date."""
    with neo4j_driver.session(database=neo4j_database) as session:
        return solve_airport_offsets(session, search_date)


def _is_july(search_date):
    """The spot-check table is DST-specific; offsets differ by season."""
    return 4 <= int(search_date.split("-")[1]) <= 10


class TestSolvedOffsets:
    """The solve itself, checked against reality rather than against itself."""

    def test_solves_every_airport_in_the_data(
        self, neo4j_driver, neo4j_database, search_date, offsets
    ):
        """No airport is left without an offset

        A partial solve is the dangerous outcome: it leaves a subset of flights
        with no UTC time at all, and any query that filters on those properties
        then silently omits them. An earlier single-tier version of the solve
        skipped 116 low-frequency stations (STT, BET, BRW, SCC) and so left 386
        of 21,376 flights unwritten.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            airports = {
                r["code"]
                for r in session.run(
                    """
                    MATCH (s:Schedule) WHERE s.flightdate = date($date)
                    UNWIND [s.origin, s.dest] AS code
                    RETURN DISTINCT code
                    """,
                    date=search_date,
                )
            }

        missing = sorted(airports - set(offsets))
        assert not missing, (
            f"{len(missing)} airport(s) on {search_date} have no solved offset: "
            f"{missing[:20]}. Every flight touching them will be left without a "
            "UTC timestamp, which filters them out of results silently rather "
            "than failing."
        )

    def test_offsets_are_whole_hours(self, offsets):
        """Every US airport offset is a whole number of hours"""
        fractional = {c: o for c, o in offsets.items() if o % 60 != 0}
        assert not fractional, (
            f"Non-whole-hour offsets: {fractional}. This means the clock times "
            "and the block times disagree by a non-timezone amount, so the "
            "solve is fitting noise."
        )

    def test_offsets_are_in_range(self, offsets):
        """Offsets fall within the real span of US airports"""
        out_of_range = {c: o for c, o in offsets.items() if not -720 < o <= 720}
        assert not out_of_range, (
            f"Offsets outside (-720, 720]: {out_of_range}. A value near -840 is "
            "the dateline case (GUM/SPN) and means the normalisation in "
            "solve_airport_offsets() did not run."
        )

    def test_matches_known_real_world_offsets(self, offsets, search_date):
        """Solved offsets equal the actual UTC offsets of known airports

        The only assertion here that can catch a *uniform* error. Every internal
        consistency check -- conflicts, the round-trip, the block-time identity --
        is invariant to adding a constant to every airport, because they all
        depend on differences. Verified by mutation: +60 on every offset leaves
        the round-trip at 21,376/21,376 and is caught only here.
        """
        if not _is_july(search_date):
            pytest.skip(
                f"{search_date} is outside northern-summer DST; the spot-check "
                "table is July-specific (ORD is -300 in July, -360 in January)"
            )

        checked = {c: o for c, o in KNOWN_JULY_OFFSETS.items() if c in offsets}
        assert len(checked) >= 10, (
            f"Only {len(checked)} of the {len(KNOWN_JULY_OFFSETS)} spot-check "
            "airports are present, which is too few to detect a uniform shift. "
            "Is the loaded fixture a full day of US domestic BTS?"
        )

        wrong = {
            c: (offsets[c], expected)
            for c, expected in checked.items()
            if offsets[c] != expected
        }
        assert not wrong, (
            f"Solved offsets disagree with real UTC offsets (got, expected): "
            f"{wrong}. Anchored on {OFFSET_ANCHOR}. If ALL of them are off by "
            "the same amount the anchor is wrong; if only GUM is off by 1440 the "
            "dateline normalisation is broken."
        )

    def test_dateline_airports_are_normalised(self, offsets, search_date):
        """Guam resolves to +10, not -14

        Separated from the bulk spot check because it exercises a specific branch
        that nothing else reaches, and because it is the one case where the raw
        BFS result is 24h away from the truth.
        """
        if not _is_july(search_date):
            pytest.skip("Spot-check table is July-specific")
        if "GUM" not in offsets:
            pytest.skip("GUM has no flights on this date")

        assert offsets["GUM"] == 600, (
            f"GUM solved to {offsets['GUM']}, expected +600 (UTC+10). A value of "
            "-840 means the dateline normalisation in solve_airport_offsets() "
            "was skipped -- it is exactly 1440 minutes away."
        )

    def test_anchor_is_present_and_exact(self, offsets):
        """The anchor airport carries precisely its declared offset"""
        code, expected = OFFSET_ANCHOR
        assert code in offsets, (
            f"Anchor {code} is absent from the solve, so nothing pins the "
            "relative solution to the UTC scale."
        )
        assert offsets[code] == expected, (
            f"Anchor {code} solved to {offsets[code]} but is declared as "
            f"{expected}. The shift step is not doing what it claims."
        )


class TestUtcTimestamps:
    """The stored properties, and the defect they exist to fix."""

    def test_every_flight_has_utc_timestamps(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """No flight is left without UTC times

        Skips rather than fails when the properties are absent everywhere, since
        that means --solve-offsets simply has not been run; fails when they are
        present on some rows but not others, which is a partial write.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            record = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                RETURN count(s) AS total,
                       count(s.scheduled_departure_utc) AS with_dep,
                       count(s.scheduled_arrival_utc) AS with_arr
                """,
                date=search_date,
            ).single()

        if record["with_dep"] == 0:
            pytest.skip(
                f"No UTC timestamps on {search_date} — run "
                f"`python load_bts_data.py --solve-offsets {search_date}`"
            )

        assert record["with_dep"] == record["total"], (
            f"{record['total'] - record['with_dep']:,} of {record['total']:,} "
            "flights have no scheduled_departure_utc. A partial write is worse "
            "than none: those flights vanish from any UTC-filtered query."
        )
        assert record["with_arr"] == record["total"], (
            f"{record['total'] - record['with_arr']:,} of {record['total']:,} "
            "flights have no scheduled_arrival_utc."
        )

    def test_utc_duration_equals_block_time(
        self, neo4j_driver, neo4j_database, search_date, utc_flight_count
    ):
        """UTC arrival minus UTC departure is the BTS block time

        Deliberately weak: this is TAUTOLOGICAL given how write_utc_times()
        derives arrival, and it would pass against completely wrong offsets. Its
        only job is to catch a row the write missed or a NULL that poisoned the
        arithmetic. The real correctness check is the round-trip below.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            matching = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                  AND s.scheduled_departure_utc IS NOT NULL
                  AND duration.inSeconds(s.scheduled_departure_utc,
                        s.scheduled_arrival_utc).seconds / 60
                      = s.scheduled_duration_minutes
                RETURN count(s) AS count
                """,
                date=search_date,
            ).single()["count"]

        assert matching == utc_flight_count, (
            f"{utc_flight_count - matching:,} of {utc_flight_count:,} flights "
            "have a UTC span that is not the BTS block time. Since arrival is "
            "computed as departure + block, this can only be a missed row or a "
            "NULL."
        )

    def test_local_arrival_round_trips(
        self, neo4j_driver, neo4j_database, search_date, offsets, utc_flight_count
    ):
        """Converting UTC arrival back to destination-local time reproduces BTS

        THE correctness assertion. Unlike the block-time identity it is not
        circular: the stored local arrival came from the BTS feed, while the UTC
        arrival was derived from the departure, the block time, and the *origin's*
        offset. Recovering the former from the latter requires the
        *destination's* offset to be right too, so a single wrong airport shows up
        here (verified: ORD off by 1h gives 19,309/21,376).

        Compares the FULL timestamp, date included. An earlier revision compared
        only the time-of-day, because the stored arrival's date was the origin's
        and so wrong for every overnight leg. `--solve-offsets` now rewrites that
        date, which makes the strict form the correct assertion -- and it is
        strictly stronger: it fails if the date repair is dropped or applied with
        the wrong sign, neither of which a time-of-day comparison can see.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            matching = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                  AND s.scheduled_arrival_utc IS NOT NULL
                WITH s, $off[s.dest] AS dest_offset
                WHERE dest_offset IS NOT NULL
                WITH s, s.scheduled_arrival_utc
                        + duration({minutes: dest_offset}) AS recomputed
                WHERE recomputed = s.scheduled_arrival_time
                RETURN count(s) AS count
                """,
                date=search_date,
                off=offsets,
            ).single()["count"]

        assert matching == utc_flight_count, (
            f"Only {matching:,} of {utc_flight_count:,} flights round-trip: "
            "converting scheduled_arrival_utc back to local time at the "
            "destination did not reproduce the BTS arrival time. At least one "
            "airport's offset is wrong."
        )

    def test_no_flight_arrives_before_it_departs(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """The headline symptom is gone

        On 2025-07-18 the local timestamps put 940 flights on the ground before
        they took off. In UTC that must be zero, with no tolerance.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            bad = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                  AND s.scheduled_arrival_utc IS NOT NULL
                  AND s.scheduled_arrival_utc <= s.scheduled_departure_utc
                RETURN count(s) AS count
                """,
                date=search_date,
            ).single()["count"]

        assert bad == 0, (
            f"{bad:,} flights arrive at or before their departure in UTC. These "
            "are real instants, so this is not a timezone artefact."
        )

    def test_overnight_legs_cross_into_the_next_utc_day(
        self, neo4j_driver, neo4j_database, search_date, utc_flight_count
    ):
        """Cross-midnight flights are sequenced onto the following day

        The anti-vacuity half matters as much as the assertion: if a future
        fixture contained no overnight legs this test would pass while checking
        nothing, and the cross-midnight bug it guards would be undetectable. On
        2025-07-18, 3,152 of 21,376 flights cross a UTC day boundary.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            crossing = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                  AND s.scheduled_arrival_utc IS NOT NULL
                  AND date(s.scheduled_arrival_utc)
                      > date(s.scheduled_departure_utc)
                RETURN count(s) AS count
                """,
                date=search_date,
            ).single()["count"]

        assert crossing > 0, (
            f"No flight on {search_date} crosses a UTC day boundary out of "
            f"{utc_flight_count:,}. A real day of US domestic flying always has "
            "some (3,152 on 2025-07-18), so this fixture can no longer detect "
            "the cross-midnight defect and the assertion above is vacuous."
        )
        assert crossing < utc_flight_count, (
            f"All {utc_flight_count:,} flights cross a UTC day boundary, which "
            "cannot be right — the offsets are likely shifted wholesale."
        )

    def test_local_arrival_date_is_the_destinations(
        self, neo4j_driver, neo4j_database, search_date, utc_flight_count
    ):
        """An unguarded deadline filter is correct because the date is repaired

        This is the property that lets every deadline query in the docs drop its
        overnight guard. The loader composes both timestamps onto the ORIGIN's
        flightdate; --solve-offsets rewrites the local arrival off the UTC instant
        so its date belongs to the destination.

        Anti-vacuity matters here: if no leg crossed local midnight, a deadline
        query with no guard would pass this suite and still be wrong in
        production. Assert the hazard is present before asserting it is handled.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            record = session.run(
                """
                MATCH (s:Schedule) WHERE s.flightdate = date($date)
                  AND s.scheduled_arrival_utc IS NOT NULL
                WITH s, date(s.scheduled_arrival_time) AS arr_date
                RETURN count(*) AS total,
                       sum(CASE WHEN arr_date > s.flightdate THEN 1 ELSE 0 END)
                           AS next_day,
                       // Wrongly accepted by a guardless "< 15:00" filter: a leg
                       // that lands the next local day but still compares early.
                       sum(CASE WHEN arr_date > s.flightdate
                                 AND s.scheduled_arrival_time
                                     < localdatetime(toString(s.flightdate)
                                                     + 'T15:00:00')
                                THEN 1 ELSE 0 END) AS false_accepts
                """,
                date=search_date,
            ).single()

        assert record["next_day"] > 0, (
            f"No leg on {search_date} lands on the following local day out of "
            f"{utc_flight_count:,}. A real day of US domestic flying always has "
            "some (915 on 2025-07-18), so this fixture cannot detect a deadline "
            "filter that ignores overnight arrivals."
        )
        assert record["false_accepts"] == 0, (
            f"{record['false_accepts']:,} legs land the next local day yet still "
            "satisfy a same-day 15:00 deadline. The local arrival date is not "
            "the destination's, so --solve-offsets did not repair it and every "
            "deadline query in the docs is silently wrong."
        )


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def data(self):
        return self._rows


class _FakeSession:
    """Feeds solve_airport_offsets() a chosen set of directed-pair deltas.

    The guard clauses in solve_airport_offsets() are unreachable through the
    database: real BTS data is internally consistent, so no query against it ever
    produces a conflict, a disconnected graph, or a missing anchor. Verified by
    mutation -- deleting the conflict `raise` entirely leaves every other test in
    this file passing. Driving the pure function directly is the only way to gate
    those branches.

    This constructs relationships between airports, not flight records: no
    timestamps, carriers, flight numbers or schedules are invented, and nothing
    here reaches the graph. CLAUDE.md rule 1 forbids fabricating flight data,
    which is why these tests assert on error handling rather than on any value
    that could be mistaken for a real measurement.
    """

    def __init__(self, rows):
        self._rows = rows

    def run(self, *args, **kwargs):
        return _FakeResult(self._rows)


def _pair(origin, dest, delta, flights=10):
    return {"origin": origin, "dest": dest, "delta": delta, "flights": flights}


class TestSolveGuards:
    """The error paths, which real data cannot reach."""

    def test_contradictory_deltas_raise(self):
        """Two pairs implying different offsets for one airport is fatal

        Not a case to absorb with a tolerance: the offsets are exact whole hours
        on real data, so disagreement means the underlying times are inconsistent
        and every derived UTC timestamp would be silently wrong.
        """
        anchor, _ = OFFSET_ANCHOR
        rows = [
            _pair(anchor, "AAA", 60),
            _pair(anchor, "BBB", 120),
            # BBB->AAA implies AAA is 60 min ahead of BBB, i.e. AAA = +180 from
            # the anchor, contradicting the +60 above.
            _pair("BBB", "AAA", 60),
        ]
        with pytest.raises(RuntimeError, match="contradictory"):
            solve_airport_offsets(_FakeSession(rows), "2025-07-18")

    def test_disconnected_graph_raises(self):
        """An airport unreachable from the root is fatal, not skipped

        Returning a partial map would leave those flights with no UTC time, and
        every UTC-filtered query would omit them without error.
        """
        anchor, _ = OFFSET_ANCHOR
        # The anchor sits in the larger component so this reaches the
        # disconnection guard rather than tripping the missing-anchor one first.
        rows = [
            _pair(anchor, "AAA", 60),
            _pair("AAA", anchor, -60),
            _pair(anchor, "BBB", 120),
            # A second component with no path back to the anchor.
            _pair("YYY", "ZZZ", 60),
            _pair("ZZZ", "YYY", -60),
        ]
        with pytest.raises(RuntimeError, match="disconnected"):
            solve_airport_offsets(_FakeSession(rows), "2025-07-18")

    def test_missing_anchor_raises(self):
        """Without the anchor the solution is only relative

        BFS recovers differences; one known absolute value is what places them on
        the UTC scale. Silently returning relative offsets would look plausible
        and be uniformly wrong -- the exact failure the spot check exists to
        catch, caught earlier and more clearly.
        """
        rows = [_pair("AAA", "BBB", 60), _pair("BBB", "AAA", -60)]
        with pytest.raises(RuntimeError, match="anchor"):
            solve_airport_offsets(_FakeSession(rows), "2025-07-18")

    def test_no_pairs_raises(self):
        """An unloaded date is reported rather than yielding an empty map"""
        with pytest.raises(RuntimeError, match="No usable airport pairs"):
            solve_airport_offsets(_FakeSession([]), "1999-01-01")

    def test_fractional_offset_raises(self):
        """A non-whole-hour result means the solve is fitting noise"""
        anchor, _ = OFFSET_ANCHOR
        rows = [_pair(anchor, "AAA", 37), _pair("AAA", anchor, -37)]
        with pytest.raises(RuntimeError, match="whole hour"):
            solve_airport_offsets(_FakeSession(rows), "2025-07-18")

    def test_dateline_offset_is_normalised_not_rejected(self):
        """A +10 airport survives the whole-hour check

        Guards against a normalisation that runs in the wrong order: the wrap has
        to happen before the whole-hour assertion, or GUM's raw -840 would be
        rejected outright. Both are whole hours here, so only the returned value
        distinguishes correct ordering.
        """
        anchor, anchor_offset = OFFSET_ANCHOR
        # GUM sits 17 hours ahead of Phoenix in July (-7 -> +10).
        rows = [_pair(anchor, "GUM", 17 * 60), _pair("GUM", anchor, -17 * 60)]
        offsets = solve_airport_offsets(_FakeSession(rows), "2025-07-18")
        assert offsets[anchor] == anchor_offset
        assert offsets["GUM"] == 600, (
            f"GUM came out {offsets['GUM']}, expected +600. The dateline wrap "
            "must be applied before the whole-hour check."
        )


@pytest.fixture(scope="module")
def utc_flight_count(neo4j_driver, neo4j_database, search_date):
    """Flights carrying UTC timestamps, skipping if --solve-offsets never ran."""
    with neo4j_driver.session(database=neo4j_database) as session:
        count = session.run(
            """
            MATCH (s:Schedule) WHERE s.flightdate = date($date)
              AND s.scheduled_departure_utc IS NOT NULL
            RETURN count(s) AS count
            """,
            date=search_date,
        ).single()["count"]
    if count == 0:
        pytest.skip(
            f"No UTC timestamps on {search_date} — run "
            f"`python load_bts_data.py --solve-offsets {search_date}`"
        )
    return count


class TestConnectsToUsesUtc:
    """CONNECTS_TO sequencing, now checkable exactly rather than heuristically."""

    def test_no_edge_departs_before_its_inbound_lands(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """Every connection is chronologically possible in absolute time

        This is the assertion the UTC work was for. The previous local-time build
        passed the equivalent check trivially (both timestamps shared the origin's
        date) while shipping 11,975 impossible edges, e.g. AA6 HNL->DFW arriving
        06:02 the next morning spliced to an 08:10 DFW departure the day before.
        """
        with neo4j_driver.session(database=neo4j_database) as session:
            if (
                session.run(
                    "MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) AS c"
                ).single()["c"]
                == 0
            ):
                pytest.skip("No CONNECTS_TO edges — see --build-connections")

            bad = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.flightdate = date($date)
                  AND s1.scheduled_arrival_utc IS NOT NULL
                  AND s2.scheduled_departure_utc IS NOT NULL
                  AND s2.scheduled_departure_utc <= s1.scheduled_arrival_utc
                RETURN count(r) AS count
                """,
                date=search_date,
            ).single()["count"]

        assert bad == 0, (
            f"{bad:,} CONNECTS_TO edges have the onward flight departing at or "
            "before the inbound one lands, in absolute time. Rebuild with "
            "--rebuild-connections after --solve-offsets."
        )

    def test_stored_layover_is_the_utc_layover(
        self, neo4j_driver, neo4j_database, search_date
    ):
        """r.layover_minutes agrees with the UTC instants it was derived from"""
        with neo4j_driver.session(database=neo4j_database) as session:
            record = session.run(
                """
                MATCH (s1:Schedule)-[r:CONNECTS_TO]->(s2:Schedule)
                WHERE s1.flightdate = date($date)
                  AND s1.scheduled_arrival_utc IS NOT NULL
                  AND s2.scheduled_departure_utc IS NOT NULL
                RETURN count(r) AS total,
                       sum(CASE WHEN duration.inSeconds(
                                       s1.scheduled_arrival_utc,
                                       s2.scheduled_departure_utc).seconds / 60
                                     = r.layover_minutes
                                THEN 1 ELSE 0 END) AS matching
                """,
                date=search_date,
            ).single()

        if record["total"] == 0:
            pytest.skip("No CONNECTS_TO edges with UTC times on this date")

        assert record["matching"] == record["total"], (
            f"{record['total'] - record['matching']:,} of {record['total']:,} "
            "edges store a layover that is not the UTC gap between the two "
            "flights. The edges predate the UTC switch; rebuild them."
        )
