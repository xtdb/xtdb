#!/usr/bin/env python3
"""
Minimal XTDB-via-ADBC hello world.

Requires a running XTDB node with the FlightSQL listener on localhost:9832.
The standalone Docker image enables this by default:

    docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly

Run:
    pip install adbc-driver-flightsql pyarrow
    python hello-world.py
"""

import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow as pa


def main() -> None:
    with flight_sql.connect("grpc://localhost:9832") as conn:
        with conn.cursor() as cur:
            # 1. Bulk-ingest an Arrow table: one round trip, no row shredding.
            users = pa.table({
                "_id":  ["alice", "bob", "carol"],
                "name": ["Alice", "Bob",   "Carol"],
                "age":  [30,      25,      28],
            })
            cur.adbc_ingest("users", users, mode="create_append")

            # 2. Query it back: results stream as Arrow batches.
            cur.execute("SELECT _id, name, age FROM users ORDER BY _id")
            result_table = cur.fetch_arrow_table()
            print("Users:")
            for row in result_table.to_pylist():
                print(f"  * {row['_id']}: {row['name']} ({row['age']})")

            # 3. Prepared statement with parameters.
            cur.execute("SELECT name FROM users WHERE age > ?", parameters=[26])
            print("\nOver 26:", cur.fetch_arrow_table().column("name").to_pylist())

            # 4. Session introspection: works for Go-driver-based ADBC clients.
            print(f"\nCurrent catalog: {conn.adbc_current_catalog}")
            print(f"Current schema:  {conn.adbc_current_db_schema}")

            # 5. Schema introspection without executing.
            schema = conn.adbc_get_table_schema(
                "users", db_schema_filter="public",
            )
            print(f"\nusers schema: {schema}")


if __name__ == "__main__":
    main()
