#!/usr/bin/env python3
"""
Quick data verification script for setup-and-run.sh
Returns counts of Schedule and Airport nodes in Neo4j database.
"""

import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase


def main():
    # Load environment variables
    load_dotenv()

    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    username = os.getenv("NEO4J_USERNAME", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    if not password:
        print("0,0")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))

        with driver.session(database=database) as session:
            # Count Schedule nodes
            schedule_result = session.run("MATCH (s:Schedule) RETURN count(s) as count")
            schedule_count = schedule_result.single()["count"]

            # Count Airport nodes
            airport_result = session.run("MATCH (a:Airport) RETURN count(a) as count")
            airport_count = airport_result.single()["count"]

            # Output in CSV format for easy parsing by shell script
            print(f"{schedule_count},{airport_count}")

        driver.close()

    except Exception:
        # If any error occurs, return 0,0 to indicate failure
        print("0,0")
        sys.exit(1)


if __name__ == "__main__":
    main()
