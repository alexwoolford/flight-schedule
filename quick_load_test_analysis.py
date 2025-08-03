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


def analyze_results(csv_file):
    """Quick analysis of load test results"""
    try:
        df = pd.read_csv(csv_file)
        print("🚀 LOAD TEST RESULTS ANALYSIS")
        print("=" * 40)

        # Filter out aggregated rows
        detailed = df[~df["Name"].isin(["Aggregated", "Total"])].copy()

        if len(detailed) == 0:
            print("❌ No detailed data found in CSV")
            return

        # Overall metrics
        total_requests = detailed["Request Count"].sum()
        total_failures = detailed["Failure Count"].sum()
        failure_rate = (
            (total_failures / total_requests * 100) if total_requests > 0 else 0
        )
        avg_response_time = detailed["Average Response Time"].mean()
        total_rps = detailed["Requests/s"].sum()

        print("📊 OVERALL PERFORMANCE:")
        print(f"   • Total Requests: {total_requests:,}")
        print(f"   • Failure Rate: {failure_rate:.2f}%")
        print(f"   • Average Response Time: {avg_response_time:.1f}ms")
        print(f"   • Total RPS: {total_rps:.1f}")

        # Performance evaluation
        print("\n🎯 PERFORMANCE EVALUATION:")
        if failure_rate < 1:
            print("   ✅ Failure Rate: EXCELLENT")
        elif failure_rate < 5:
            print("   ⚠️  Failure Rate: ACCEPTABLE")
        else:
            print("   ❌ Failure Rate: POOR")

        if avg_response_time < 200:
            print("   ✅ Response Time: EXCELLENT")
        elif avg_response_time < 500:
            print("   ⚠️  Response Time: ACCEPTABLE")
        else:
            print("   ❌ Response Time: POOR")

        if total_rps > 30:
            print("   ✅ Throughput: EXCELLENT")
        elif total_rps > 15:
            print("   ⚠️  Throughput: ACCEPTABLE")
        else:
            print("   ❌ Throughput: POOR")

        # Query type breakdown
        print("\n📋 BY QUERY TYPE:")
        for _, row in detailed.iterrows():
            name = row["Name"]
            if "direct_flight" in name:
                query_type = "Direct Flights"
            elif "connection" in name:
                query_type = "Connections"
            elif "multi_hop" in name:
                query_type = "Multi-hop"
            elif "analytics" in name:
                query_type = "Analytics"
            else:
                query_type = "Other"

            avg_time = row["Average Response Time"]
            rps = row["Requests/s"]
            req_count = row["Request Count"]
            failures = row["Failure Count"]
            fail_rate = (failures / req_count * 100) if req_count > 0 else 0

            print(f"   {query_type}:")
            print(f"     • Avg Response Time: {avg_time:.1f}ms")
            print(f"     • Requests/sec: {rps:.1f}")
            print(f"     • Failure Rate: {fail_rate:.1f}%")

        # Recommendations
        print("\n🔧 RECOMMENDATIONS:")
        slowest_query = detailed.loc[detailed["Average Response Time"].idxmax()]
        print(
            f"   • Slowest query type: {slowest_query['Name']} "
            f"({slowest_query['Average Response Time']:.1f}ms)"
        )

        if avg_response_time > 300:
            print("   • Consider query optimization or additional indexing")
        if failure_rate > 2:
            print("   • Investigate error patterns and connection handling")
        if total_rps < 20:
            print("   • Check Neo4j configuration and system resources")

    except Exception as e:
        print(f"❌ Error analyzing results: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python quick_load_test_analysis.py <stats_file.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]
    analyze_results(csv_file)
