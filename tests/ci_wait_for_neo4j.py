#!/usr/bin/env python3
"""
Block until Neo4j accepts an authenticated Bolt connection.
===========================================================

A service container's port is published — and so answers a TCP connect and an
HTTP GET on 7474 — before the server is actually ready to serve Bolt. Starting
the loader on the strength of a port check therefore races: measured against
`neo4j:2026.05.0-community`, both ports were open at t=0 while Bolt did not
complete a session until **t≈4.8s**. The healthcheck in
`.github/workflows/ci.yml` has the same limitation, so this runs as an explicit
step before the load.

**Auth failures are not retried.** Bad credentials are a configuration error,
not a readiness condition, and Neo4j locks an account that gets several wrong
passwords in a row ("The client has provided incorrect authentication details
too many times in a row") — so retrying turns a clear error into a 120-second
hang followed by a lockout that outlives the retry loop. Fail fast instead.

    python tests/ci_wait_for_neo4j.py [timeout_seconds]
"""

import os
import sys
import time

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

DEFAULT_TIMEOUT = 120.0
POLL_INTERVAL = 2.0


def wait_for_neo4j(timeout: float = DEFAULT_TIMEOUT) -> int:
    # override=True to match conftest.py and the rest of the repo: .env is
    # authoritative. Without it an exported NEO4J_PASSWORD left over from
    # another project silently wins and every connection attempt fails.
    load_dotenv(override=True)
    uri = os.getenv("NEO4J_URI")
    if not uri:
        print("❌ NEO4J_URI is not set", file=sys.stderr)
        return 1

    auth = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    deadline = time.monotonic() + timeout
    last_error = None
    attempts = 0
    while time.monotonic() < deadline:
        attempts += 1
        try:
            driver = GraphDatabase.driver(uri, auth=auth)
            try:
                driver.verify_connectivity()
                # verify_connectivity() can succeed before the target database
                # is available, so run a trivial query against it too.
                with driver.session(database=database) as session:
                    session.run("RETURN 1").consume()
            finally:
                driver.close()
        except AuthError as exc:
            print(
                f"❌ {uri} rejected the supplied credentials: {exc}\n"
                "   This is a configuration error, not a startup delay — not "
                "retrying, because repeated wrong passwords lock the account.\n"
                "   Check NEO4J_USERNAME / NEO4J_PASSWORD (note that an "
                "exported NEO4J_PASSWORD overrides .env unless override=True).",
                file=sys.stderr,
            )
            return 1
        except Exception as exc:
            last_error = exc
            time.sleep(POLL_INTERVAL)
            continue

        print(
            f"✅ {uri} is accepting authenticated Bolt connections to "
            f"'{database}' (after {attempts} attempt(s))"
        )
        return 0

    print(
        f"❌ {uri} did not accept a Bolt connection within {timeout:.0f}s "
        f"({attempts} attempts). Last error: {last_error}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_TIMEOUT
    sys.exit(wait_for_neo4j(seconds))
