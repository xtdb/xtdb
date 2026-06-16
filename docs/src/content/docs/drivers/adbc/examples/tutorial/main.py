#!/usr/bin/env python3
"""
XTDB ADBC tutorial: FX trades end-to-end.

Companion script to docs/src/content/docs/drivers/adbc/tutorial.md.
Every section in the tutorial maps to a numbered function below.

Requirements:
    pip install adbc-driver-flightsql pyarrow pandas

Running against the Docker standalone image:
    docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
    python main.py

Override the FlightSQL URL:
    XTDB_ARROW_URL=grpc://localhost:9842 python main.py
"""

from __future__ import annotations

import os
import time
import warnings
from datetime import datetime, timezone

import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql

# XTDB advertises FlightSQL transaction support, so the dbapi defaults to
# manual-commit (PEP 249). This tutorial seeds exclusively via adbc_ingest,
# which commits atomically regardless of the connection's commit mode, so the
# reads below see their writes without an explicit commit. DML run via
# cursor.execute/executemany would instead need conn.commit() or autocommit=True.
warnings.filterwarnings("ignore", message="Cannot disable autocommit")

XTDB_URL = os.environ.get("XTDB_ARROW_URL", "grpc://localhost:9832")


# ── data ────────────────────────────────────────────────────────────────────

#: Ten historical FX trades covering three currency pairs.
INITIAL_TRADES: pa.Table = pa.table(
    {
        "_id": pa.array(
            ["t001", "t002", "t003", "t004", "t005",
             "t006", "t007", "t008", "t009", "t010"],
        ),
        "symbol": pa.array(
            ["EURUSD", "GBPUSD", "EURUSD", "USDJPY", "GBPUSD",
             "EURUSD", "USDJPY", "GBPUSD", "EURUSD", "USDJPY"],
        ),
        "qty": pa.array(
            [100_000, 250_000, 75_000, 500_000, 180_000,
             320_000, 90_000, 420_000, 110_000, 660_000],
            type=pa.int64(),
        ),
        "price": pa.array(
            [1.0921, 1.2734, 1.0918, 148.25, 1.2741,
             1.0935, 148.10, 1.2728, 1.0942, 148.30],
            type=pa.float64(),
        ),
        "traded_at": pa.array(
            [
                datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 9, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 9, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 9, 15, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 9, 20, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 10, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 10, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 10, 15, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 10, 20, tzinfo=timezone.utc),
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
    }
)


# ── sections ─────────────────────────────────────────────────────────────────

def section_2_connect() -> flight_sql.Connection:
    """Return an open connection (caller must close)."""
    print("\n── 2. Connecting ───────────────────────────────")
    conn = flight_sql.connect(XTDB_URL)
    print(f"Connected to {XTDB_URL}")
    return conn


def section_3_first_query(conn: flight_sql.Connection) -> None:
    print("\n── 3. First query ──────────────────────────────")
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetch_arrow_table()
    print("Result schema:", result.schema)
    print("Result:       ", result.to_pylist())


def section_4_bulk_ingest(conn: flight_sql.Connection) -> None:
    print("\n── 4. Bulk ingest ──────────────────────────────")

    with conn.cursor() as cur:
        cur.adbc_ingest("trades", INITIAL_TRADES, mode="create_append")
    print(f"Ingested {len(INITIAL_TRADES)} rows into 'trades'.")

    print("\nIngested schema:")
    for field in INITIAL_TRADES.schema:
        print(f"  {field.name}: {field.type}")

    # Read back: types survive the round-trip.
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM trades ORDER BY _id")
        back = cur.fetch_arrow_table()
    print(f"\nRead back {back.num_rows} rows.")
    print("Round-trip schema:", back.schema)


def section_5_parameterised_query(conn: flight_sql.Connection) -> None:
    print("\n── 5. Parameterised query ──────────────────────")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT _id, symbol, qty, price FROM trades WHERE symbol = ? ORDER BY _id",
            parameters=["EURUSD"],
        )
        result = cur.fetch_arrow_table()
    print(f"EURUSD trades ({result.num_rows} rows):")
    for row in result.to_pylist():
        print(f"  {row['_id']}  qty={row['qty']:>7,}  price={row['price']:.4f}")


def section_6_transactions(conn: flight_sql.Connection) -> None:
    """
    Demonstrates write visibility: each adbc_ingest is its own atomic commit,
    and reads on the same connection see the writes immediately, regardless of
    the connection's commit mode.

    Note: the dbapi defaults to manual-commit now that the server advertises
    transaction support, but adbc_ingest commits on its own. DML via
    cursor.execute/executemany would need conn.commit() or autocommit=True.
    Explicit multi-statement transactions (set_autocommit(False) → commit/
    rollback) are shown in the prepared-statements-and-transactions example.
    """
    print("\n── 6. Write visibility (adbc_ingest) ───────────")

    # First batch: confirmed committed immediately.
    batch_a = pa.table({
        "_id":    pa.array(["t011", "t012"]),
        "symbol": pa.array(["EURGBP", "EURGBP"]),
        "qty":    pa.array([60_000, 40_000], type=pa.int64()),
        "price":  pa.array([0.8571, 0.8569], type=pa.float64()),
    })
    with conn.cursor() as cur:
        cur.adbc_ingest("trades", batch_a, mode="create_append")

    # Same-connection read: sees the new rows without any explicit await.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trades WHERE symbol = 'EURGBP'")
        n = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
    print(f"  EURGBP rows visible immediately on same connection: {n}  (expected 2)")

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trades")
        total = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
    print(f"  Total trades: {total}  (expected 12)")


def section_7_introspection(conn: flight_sql.Connection) -> None:
    print("\n── 7. Introspection ────────────────────────────")

    # Full object tree, returned as an Arrow reader.
    reader = conn.adbc_get_objects(depth="all", db_schema_filter="public")
    tbl = reader.read_all()
    print(f"adbc_get_objects returned {tbl.num_rows} catalog row(s).")

    # Schema for a single table, direct pyarrow.Schema.
    schema = conn.adbc_get_table_schema("trades", db_schema_filter="public")
    print("adbc_get_table_schema('trades'):")
    for field in schema:
        print(f"  {field.name}: {field.type}")


def section_8_execute_schema(conn: flight_sql.Connection) -> None:
    """
    executeSchema: get the result Arrow schema without executing the query.

    XTDB includes the result schema (datasetSchema) in the PreparedStatement
    response so the client can read it back without a round-trip execution.
    If the server build pre-dates this feature the driver raises NOT_IMPLEMENTED.
    """
    print("\n── 8. executeSchema ────────────────────────────")
    try:
        with conn.cursor() as cur:
            schema = cur.adbc_execute_schema(
                "SELECT _id, symbol, qty, price FROM trades WHERE symbol = ?"
            )
        print("Result schema (without executing):")
        for field in schema:
            print(f"  {field.name}: {field.type}")
    except Exception as e:
        if "NOT_IMPLEMENTED" in str(e):
            print("  [server does not yet include datasetSchema in prepared-stmt response]")
        else:
            raise


def section_9_bitemporal(conn: flight_sql.Connection) -> None:
    print("\n── 9. Bitemporal, AS OF ────────────────────────")

    # Record system time before inserting the new trade.
    checkpoint = datetime.now(timezone.utc)
    time.sleep(0.1)  # ensure next write lands at a strictly later system time

    new_trade = pa.table({
        "_id":    pa.array(["t013"]),
        "symbol": pa.array(["USDCHF"]),
        "qty":    pa.array([200_000], type=pa.int64()),
        "price":  pa.array([0.9012], type=pa.float64()),
    })
    with conn.cursor() as cur:
        cur.adbc_ingest("trades", new_trade, mode="create_append")

    # Current view: t013 is present.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trades")
        count_now = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
    print(f"  Total trades now:                        {count_now}")

    # Historical view: AS OF the checkpoint, t013 had not yet been inserted.
    ts_str = checkpoint.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    as_of_sql = (
        f"SELECT count(*) FROM trades "
        f"FOR SYSTEM_TIME AS OF TIMESTAMP '{ts_str}+00:00'"
    )
    with conn.cursor() as cur:
        cur.execute(as_of_sql)
        count_before = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
    print(f"  Trades FOR SYSTEM_TIME AS OF checkpoint: {count_before}")
    print(f"  (checkpoint predates t013, count is {count_now - 1})")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("XTDB ADBC tutorial: FX trades")
    print(f"URL: {XTDB_URL}")

    conn = section_2_connect()
    try:
        section_3_first_query(conn)
        section_4_bulk_ingest(conn)
        section_5_parameterised_query(conn)
        section_6_transactions(conn)
        section_7_introspection(conn)
        section_8_execute_schema(conn)
        section_9_bitemporal(conn)
    finally:
        conn.close()

    print("\n── Done ────────────────────────────────────────")
    print("All sections completed successfully.")


if __name__ == "__main__":
    main()
