#!/usr/bin/env python3
"""
Prepared statements and transactions via ADBC.

Shows the full ADBC statement lifecycle over FlightSQL:
  1.  Simple execute with scalar parameter binding
  2.  Explicit prepare → bind → execute (amortise prepare over many calls)
  3.  Arrow batch parameter binding: multi-row RecordBatch as parameters
  4.  Multi-statement transactions: setAutoCommit / commit / rollback
  5.  ROLLBACK empties pending writes
  6.  Same-connection write-then-read visibility

Requirements:
    docker run --rm -d --name xtdb -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
    pip install adbc-driver-flightsql pyarrow

Run:
    python main.py [grpc://localhost:9832]
"""

import sys
import warnings

import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_flightsql
import adbc_driver_manager

warnings.filterwarnings("ignore")

URI = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:9832"


def section(title: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


# ── helpers ──────────────────────────────────────────────────────────────
def raw_conn(uri: str = URI) -> adbc_driver_manager.AdbcConnection:
    """Open a low-level AdbcConnection (bypasses dbapi)."""
    db = adbc_driver_flightsql.connect(uri)
    return adbc_driver_manager.AdbcConnection(db)


def read_all(handle) -> pa.Table:
    """Read a RecordBatchReader handle returned by execute_query."""
    return pa.RecordBatchReader._import_from_c(handle.address).read_all()


# ── 1. Seed data ──────────────────────────────────────────────────────────
def seed(cur: flight_sql.Cursor) -> None:
    cur.executemany(
        "INSERT INTO products (_id, name, price, stock)"
        " VALUES (?, ?, ?, ?)",
        [
            ["p1", "Widget",    9.99,  100],
            ["p2", "Gadget",   24.99,   50],
            ["p3", "Doohickey", 4.49,  200],
        ],
    )


# ── 2. Scalar parameter binding ──────────────────────────────────────────
def demo_scalar_binding(cur: flight_sql.Cursor) -> None:
    section("1. Scalar parameter binding")
    # Each call implicitly prepares and executes in one shot.
    cur.execute(
        "SELECT _id, name, price FROM products WHERE _id = ?",
        parameters=["p1"],
    )
    row = cur.fetchone()
    print(f"  p1 → {row[1]}, £{row[2]:.2f}")

    cur.execute(
        "SELECT _id, name, price FROM products WHERE price < ?",
        parameters=[10.0],
    )
    rows = cur.fetchall()
    print(f"  Under £10: {[r[1] for r in rows]}")


# ── 3. Prepare once / execute many ────────────────────────────────────────
def demo_prepare_bind_execute(conn: adbc_driver_manager.AdbcConnection) -> None:
    section("2. Prepare once / execute-many (low-level ADBC API)")
    # Use the raw ADBC connection to show the explicit lifecycle:
    #   set_sql_query → prepare → bind → execute_query.
    # In hot loops this avoids re-parsing the SQL on every call.
    stmt = adbc_driver_manager.AdbcStatement(conn)
    try:
        stmt.set_sql_query(
            "SELECT _id, name, price FROM products WHERE _id = ?"
        )
        stmt.prepare()          # parse + plan once
        print("  Prepared OK")

        for product_id in ["p1", "p2", "p3"]:
            # Bind a one-row Arrow batch; column names don't matter,
            # they map positionally to the ? placeholders.
            batch = pa.record_batch(
                {"v0": pa.array([product_id], type=pa.string())}
            )
            stmt.bind(batch)    # stays client-side, no round trip
            handle, _ = stmt.execute_query()
            row = read_all(handle).to_pylist()
            if row:
                print(f"  {product_id}: {row[0]['name']}, £{row[0]['price']:.2f}")
    finally:
        stmt.close()


# ── 4. Arrow batch parameter binding ─────────────────────────────────────
def demo_arrow_batch_binding(cur: flight_sql.Cursor) -> None:
    section("3. Arrow batch binding: multi-row RecordBatch as parameters")
    # Build a RecordBatch where each row is one INSERT execution.
    # Columns map positionally to the ? placeholders.
    # The whole batch travels as Arrow over the wire,
    # no row-shredding, no individual round trips per row.
    #
    # NOTE: use pa.string() (utf8) for string columns in bound batches.
    # pa.large_utf8() is not currently supported in bound parameters.
    param_batch = pa.record_batch({
        "v0": pa.array(["p4", "p5", "p6"],                   type=pa.string()),
        "v1": pa.array(["Thingamajig", "Whatsit", "Gizmo"],  type=pa.string()),
        "v2": pa.array([14.99, 7.49, 19.99],                 type=pa.float64()),
        "v3": pa.array([75, 120, 30],                        type=pa.int64()),
    })
    cur.executemany(
        "INSERT INTO products (_id, name, price, stock)"
        " VALUES (?, ?, ?, ?)",
        param_batch,
    )

    cur.execute("SELECT count(*) FROM products")
    total = cur.fetchone()[0]
    print(f"  Products after Arrow batch insert: {total}")

    # Results stream back as Arrow; fetch the whole table in one call.
    cur.execute("SELECT _id, name FROM products WHERE _id IN (?, ?, ?)",
                parameters=["p4", "p5", "p6"])
    arrow_tbl = cur.fetch_arrow_table()
    print(f"  Batch-inserted rows: {arrow_tbl.to_pydict()}")


# ── 5. Transactions ───────────────────────────────────────────────────────
def demo_transactions(uri: str) -> None:
    section("4. Autocommit off, COMMIT, ROLLBACK")
    # The ADBC connection-level transaction API:
    #
    #   conn.adbc_connection.set_autocommit(False)   ← opens a transaction
    #   conn.adbc_connection.commit()
    #   conn.adbc_connection.rollback()
    #
    # This uses the FlightSQL BeginTransaction / EndTransaction actions.
    # The server must advertise FLIGHT_SQL_SERVER_TRANSACTION support
    # (SqlInfo id 8 in GetSqlInfo).  Earlier builds may not; if set_autocommit
    # raises NotSupportedError the script falls back to a single-insert demo.

    with flight_sql.connect(uri) as conn:
        try:
            conn.adbc_connection.set_autocommit(False)
        except adbc_driver_manager.NotSupportedError:
            print("  ⚠  Server does not advertise transaction support.")
            print("     Update to a build that includes the FLIGHT_SQL_SERVER_TRANSACTION")
            print("     GetSqlInfo entry (fixed in main after 2026-05).")
            print("  Skipping transaction demo; falling back to autocommit demo.")
            _demo_autocommit_fallback(conn)
            return

        with conn.cursor() as cur:
            # ── 5a. ROLLBACK ────────────────────────────────────────
            cur.executemany(
                "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
                [["o1", "Widget", 10], ["o2", "Gadget", 5]],
            )

            # XTDB buffers DML in an open transaction and applies the whole
            # batch atomically on COMMIT, so reads on the same connection do
            # NOT see the pending writes until the transaction commits.
            cur.execute(
                "SELECT count(*) FROM orders WHERE _id IN (?, ?)",
                parameters=["o1", "o2"],
            )
            inside_txn = cur.fetchone()[0]
            print(f"  Rows visible inside open transaction (before commit): {inside_txn}  (expected 0)")

            conn.adbc_connection.rollback()
            print("  → ROLLBACK issued")

            conn.adbc_connection.set_autocommit(False)
            cur.execute(
                "SELECT count(*) FROM orders WHERE _id IN (?, ?)",
                parameters=["o1", "o2"],
            )
            after_rollback = cur.fetchone()[0]
            print(f"  Rows visible after ROLLBACK: {after_rollback}  (expected 0)")
            conn.adbc_connection.rollback()  # end the open txn

            # ── 5b. COMMIT ──────────────────────────────────────────
            conn.adbc_connection.set_autocommit(False)
            cur.executemany(
                "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
                [["o3", "Doohickey", 20]],
            )
            conn.adbc_connection.commit()
            print("  → COMMIT issued")

            cur.execute("SELECT item, qty FROM orders WHERE _id = ?",
                        parameters=["o3"])
            row = cur.fetchone()
            print(f"  After COMMIT: {row[0]}, qty={row[1]}")


def _demo_autocommit_fallback(conn: flight_sql.Connection) -> None:
    """Fallback for servers without transaction support: show autocommit behaviour."""
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
            [["o3", "Doohickey", 20]],
        )
        cur.execute("SELECT item, qty FROM orders WHERE _id = ?",
                    parameters=["o3"])
        row = cur.fetchone()
        print(f"  Autocommit insert persisted: {row}")


# ── 6. Same-connection write-then-read ───────────────────────────────────
def demo_write_then_read(cur: flight_sql.Cursor) -> None:
    section("5. Same-connection write-then-read visibility (no manual await)")
    # An insert in autocommit mode lands immediately.
    # The next read on the same connection sees the row without any
    # manual await: the write token is threaded through the planner.
    cur.executemany(
        "INSERT INTO products (_id, name, price, stock)"
        " VALUES (?, ?, ?, ?)",
        [["p9", "Overnight", 3.99, 500]],
    )
    cur.execute(
        "SELECT name, price FROM products WHERE _id = ?",
        parameters=["p9"],
    )
    row = cur.fetchone()
    print(f"  Immediately visible: name={row[0]}, price={row[1]}")


# ── main ─────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"Connecting to {URI} …")

    # Sections 1-3 and 5 aren't demonstrating manual transactions; they just
    # need to read their own writes, so they connect with autocommit=True.
    # Section 4 (demo_transactions) opens its own connection and drives
    # set_autocommit/commit/rollback explicitly.
    with flight_sql.connect(URI, autocommit=True) as conn:
        raw = conn.adbc_connection
        with conn.cursor() as cur:
            seed(cur)
            print("Seed data inserted.")

            demo_scalar_binding(cur)          # section 1
            demo_prepare_bind_execute(raw)    # section 2
            demo_arrow_batch_binding(cur)     # section 3

    demo_transactions(URI)                    # section 4

    with flight_sql.connect(URI, autocommit=True) as conn:
        with conn.cursor() as cur:
            demo_write_then_read(cur)         # section 5

    print("\nDone.")


if __name__ == "__main__":
    main()
