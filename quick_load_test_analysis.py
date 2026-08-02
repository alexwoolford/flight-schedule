#!/usr/bin/env python3
"""
Quick Load Test Analysis
========================

Quick analysis of Locust load test results from CSV files.
Run this after downloading CSV data from Locust web UI.

Usage:
    python quick_load_test_analysis.py stats_file.csv
"""

import sys

import pandas as pd


def _grade(label, value, good, ok, unit="ms"):
    """Print `value` against two thresholds. Lower is better."""
    mark = "✅" if value < good else "⚠️ " if value < ok else "❌"
    verdict = "EXCELLENT" if value < good else "ACCEPTABLE" if value < ok else "POOR"
    print(f"   {mark} {label}: {verdict} ({value:.1f}{unit})")


def _print_overall(detailed):
    """Aggregate metrics, and the two that can be graded meaningfully."""
    total_requests = detailed["Request Count"].sum()
    total_failures = detailed["Failure Count"].sum()
    failure_rate = (total_failures / total_requests * 100) if total_requests > 0 else 0
    avg_response_time = detailed["Average Response Time"].mean()
    total_rps = detailed["Requests/s"].sum()

    print("📊 OVERALL PERFORMANCE:")
    print(f"   • Total Requests: {total_requests:,}")
    print(f"   • Failure Rate: {failure_rate:.2f}%")
    print(f"   • Average Response Time: {avg_response_time:.1f}ms")
    print(f"   • Total RPS: {total_rps:.1f}")

    print("\n🎯 PERFORMANCE EVALUATION:")
    _grade("Failure Rate", failure_rate, 1, 5, unit="%")
    _grade("Response Time", avg_response_time, 200, 500)

    # Throughput is deliberately NOT graded. It is set by how many users were
    # simulated and their think time (wait_time = between(1, 3)), not by what the
    # graph can serve: 8 users averaging 2s of think time cannot exceed ~4 req/s
    # no matter how fast Neo4j answers. Grading it "POOR" blamed the database for
    # the load generator's own configuration. To say anything about capacity,
    # raise --users until response time degrades.
    print(f"   ℹ️  Throughput: {total_rps:.1f} req/s — bounded by user count")
    print("      and think time, not by the database. Not a capacity number.")

    return failure_rate, avg_response_time


def _print_per_task(detailed):
    """Per-task breakdown, using Locust's own `Name` column.

    Printed as-is rather than mapped onto a guessed category: the previous version
    matched substrings ("direct_flight", "multi_hop", "analytics") that appear in
    no name this repo emits, so every row was labelled "Other" and the breakdown
    said nothing. Names come from NONSTOP_TASK / SEARCH_TASK in
    neo4j_flight_load_test.py.
    """
    print("\n📋 BY QUERY TYPE:")
    for _, row in detailed.iterrows():
        req_count = row["Request Count"]
        fail_rate = (row["Failure Count"] / req_count * 100) if req_count > 0 else 0

        print(f"   {row['Name']}:")
        print(f"     • Requests: {req_count:,.0f}")
        print(f"     • Avg Response Time: {row['Average Response Time']:.1f}ms")
        # Percentiles are what a serving budget is written against, and they are
        # only meaningful because the task names are constant. Per-pair names used
        # to produce ~30k single-sample rows, i.e. a "p95" over one observation.
        for col, label in (("95%", "p95"), ("99%", "p99")):
            if col in row.index:
                print(f"     • {label}: {row[col]:.0f}ms")
        print(f"     • Requests/sec: {row['Requests/s']:.1f}")
        print(f"     • Failure Rate: {fail_rate:.1f}%")


def analyze_results(csv_file):
    """Quick analysis of load test results"""
    try:
        df = pd.read_csv(csv_file)
        print("🚀 LOAD TEST RESULTS ANALYSIS")
        print("=" * 40)

        # Locust writes one row per task plus an "Aggregated" summary row.
        detailed = df[~df["Name"].isin(["Aggregated", "Total"])].copy()
        if len(detailed) == 0:
            print("❌ No detailed data found in CSV", file=sys.stderr)
            sys.exit(1)

        failure_rate, avg_response_time = _print_overall(detailed)
        _print_per_task(detailed)

        print("\n🔧 RECOMMENDATIONS:")
        slowest = detailed.loc[detailed["Average Response Time"].idxmax()]
        print(
            f"   • Slowest query type: {slowest['Name']} "
            f"({slowest['Average Response Time']:.1f}ms)"
        )
        if avg_response_time > 300:
            print("   • Consider query optimization or additional indexing")
        if failure_rate > 2:
            print("   • Investigate error patterns and connection handling")
        # No RPS-based recommendation: low throughput at low concurrency is the
        # load generator's think time, not a Neo4j misconfiguration.

    except Exception as e:
        # Exit non-zero. This used to print and return normally, so a malformed or
        # missing CSV looked like a successful analysis to any caller checking the
        # exit code.
        print(f"❌ Error analyzing results: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python quick_load_test_analysis.py <stats_file.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]
    analyze_results(csv_file)
