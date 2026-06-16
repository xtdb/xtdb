#!/usr/bin/env python3
"""
Bulk-ingest Parquet into XTDB via ADBC: a runnable example.

Writes a small Parquet fixture from Python (no binary committed), then
demonstrates the full happy path and the error cases from the how-to guide.

Requirements:
    pip install adbc-driver-flightsql pyarrow

Running XTDB (FlightSQL on 9832):
    docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly

Run:
    python main.py
"""

import os
import pathlib
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import adbc_driver_flightsql.dbapi as flight_sql
from adbc_driver_manager import DatabaseError

XTDB_URI = os.environ.get("XTDB_URI", "grpc://localhost:9832")


# ---------------------------------------------------------------------------
# 1. Write a small Parquet fixture: no binary committed to the repo.
# ---------------------------------------------------------------------------

def make_parquet(path: pathlib.Path) -> None:
    """Write a tiny orders table to a Parquet file."""
    orders = pa.table({
        "order_id":    ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
        "customer":    ["alice",   "bob",     "carol",   "alice",   "dave"],
        "product":     ["widget",  "gadget",  "widget",  "gizmo",   "gadget"],
        "quantity":    [2,         1,         5,         1,         3],
        "unit_price":  [9.99,      24.99,     9.99,      14.99,     24.99],
    })
    pq.write_table(orders, path)
    print(f"[fixture] Wrote {len(orders)} rows to {path}")


# ---------------------------------------------------------------------------
# 2. Minimal happy path: read_table → adbc_ingest.
# ---------------------------------------------------------------------------

def ingest_full_table(conn, parquet_path: pathlib.Path) -> None:
    print("\n[minimal path] Reading Parquet and ingesting as a single table …")

    table = pq.read_table(parquet_path)

    # Our Parquet doesn't have an _id column, so materialise one from order_id.
    table = table.rename_columns(
        ["_id" if c == "order_id" else c for c in table.schema.names]
    )
    print(f"  Schema after renaming: {table.schema}")

    with conn.cursor() as cur:
        cur.adbc_ingest("orders", table, mode="create_append")
    print(f"  Ingested {len(table)} rows into 'orders'.")


# ---------------------------------------------------------------------------
# 3. Chunked ingest: stream RecordBatches for large files.
# ---------------------------------------------------------------------------

def ingest_in_chunks(conn, parquet_path: pathlib.Path) -> None:
    print("\n[chunked ingest] Streaming row groups from Parquet …")

    with conn.cursor() as cur:
        pf = pq.ParquetFile(parquet_path)
        total = 0
        for batch in pf.iter_batches(batch_size=2):
            # Materialise _id from order_id for each batch individually.
            tbl = pa.Table.from_batches([batch])
            tbl = tbl.rename_columns(
                ["_id" if c == "order_id" else c for c in tbl.schema.names]
            )
            cur.adbc_ingest("orders_chunked", tbl, mode="create_append")
            total += len(tbl)
            print(f"  … ingested chunk of {len(tbl)} rows (running total: {total})")
    print(f"  Done: {total} rows total.")


# ---------------------------------------------------------------------------
# 4. Error cases: rejected modes surface as DatabaseError / INVALID_ARGUMENT.
# ---------------------------------------------------------------------------

def demonstrate_errors(conn) -> None:
    print("\n[error handling] Demonstrating rejected ingest modes …")

    sample = pa.table({"_id": ["x"], "v": [1]})

    with conn.cursor() as cur:
        # 'replace' is not supported: XTDB has no ERASE step.
        try:
            cur.adbc_ingest("orders", sample, mode="replace")
        except DatabaseError as e:
            print(f"  'replace' rejected (expected): {e}")

        # Missing _id: XTDB requires a primary key on every row.
        try:
            no_id = pa.table({"value": [42]})
            cur.adbc_ingest("orders", no_id, mode="create_append")
        except DatabaseError as e:
            print(f"  missing _id rejected (expected): {e}")


# ---------------------------------------------------------------------------
# 5. Verify: row count + schema introspection.
# ---------------------------------------------------------------------------

def verify(conn) -> None:
    print("\n[verify] Checking row counts and schema …")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM orders")
        n = cur.fetchone()[0]
        print(f"  orders row count: {n}")

        cur.execute("SELECT count(*) AS n FROM orders_chunked")
        n_chunked = cur.fetchone()[0]
        print(f"  orders_chunked row count: {n_chunked}")

    schema = conn.adbc_get_table_schema(
        "orders", db_schema_filter="public",
    )
    print(f"  orders schema: {schema}")


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = pathlib.Path(tmpdir) / "orders.parquet"
        make_parquet(parquet_path)

        print(f"\nConnecting to XTDB at {XTDB_URI} …")
        with flight_sql.connect(XTDB_URI) as conn:
            ingest_full_table(conn, parquet_path)
            ingest_in_chunks(conn, parquet_path)
            demonstrate_errors(conn)
            verify(conn)

    print("\nDone.")


if __name__ == "__main__":
    main()
