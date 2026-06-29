---
title: Prepared statements and transactions via ADBC
description: "The full ADBC statement lifecycle over the wire: prepare once, bind many, and multi-statement transactions."
---

ADBC separates *what* you want to run from *when* you run it and *with what parameters*.
Understanding that separation is the key to writing efficient, correct client code.

This guide covers the basics:
the prepare→bind→execute lifecycle, Arrow batch parameter binding, multi-statement transactions, and same-connection write-then-read consistency.

The runnable companion script exercises every section in order:
[`docs/adbc/examples/prepared-statements-and-transactions/main.py`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/prepared-statements-and-transactions/main.py)

## Prerequisites

```bash
docker run --rm -d --name xtdb -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
pip install adbc-driver-flightsql pyarrow
```

All examples connect to `grpc://localhost:9832`.
Adjust the port if you mapped it differently.

:::note
Use the `nightly` image for ADBC/FlightSQL work: it enables the FlightSQL listener on port 9832 by default.
(Stable images from 2.2.0 onwards enable it too.)
:::

## The statement lifecycle

Every ADBC statement follows the same pattern:

```
setSqlQuery(sql)
    │
    ▼
prepare()          ← optional but required when binding Arrow params
    │
    ▼
bind(arrow_batch)  ← one batch of positional parameters
    │
    ▼
execute_query() / execute_update()
    │
    ▼
stream results (ArrowReader)
```

`prepare()` parses the SQL and builds a query plan once.
`bind()` attaches a parameter batch; the batch stays on the client, with no round trip, until you execute.
`execute_query()` sends the bound batch and streams results back as Arrow.

When you write `cur.execute("SELECT ... WHERE _id = ?", parameters=["alice"])` in the Python dbapi, all three steps happen implicitly in one call.
The explicit lifecycle matters in two situations: hot loops where amortising the parse cost pays off, and Arrow batch binding.

## Scalar parameters

The dbapi `execute()` call handles scalar parameters inline:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT _id, name, price FROM products WHERE _id = ?",
            parameters=["p1"],
        )
        print(cur.fetchone())
        # ('p1', 'Widget', 9.99)
```

`parameters` is a positional list matching the `?` placeholders in the SQL.
Positional binding only: named parameters are not currently supported over the FlightSQL wire.

## Prepare-once / execute-many

When you call `execute()` in a tight loop with different parameters, the driver re-parses the SQL on every iteration.
The explicit prepare path amortises the cost:

```python
import adbc_driver_flightsql
import adbc_driver_manager
import pyarrow as pa

db = adbc_driver_flightsql.connect("grpc://localhost:9832")
conn = adbc_driver_manager.AdbcConnection(db)
stmt = adbc_driver_manager.AdbcStatement(conn)

stmt.set_sql_query("SELECT _id, name, price FROM products WHERE _id = ?")
stmt.prepare()                     # parse + plan once

for product_id in ["p1", "p2", "p3"]:
    batch = pa.record_batch(
        {"v0": pa.array([product_id], type=pa.string())}
    )
    stmt.bind(batch)               # client-side, no round trip
    handle, _ = stmt.execute_query()

    reader = pa.RecordBatchReader._import_from_c(handle.address)
    for row in reader.read_all().to_pylist():
        print(f"  {row['_id']}: {row['name']}, £{row['price']:.2f}")

stmt.close()
conn.close()
db.close()
```

`bind()` takes any Arrow data with `__arrow_c_array__` or `__arrow_c_stream__`.
Column names in the batch don't matter: parameters are matched positionally to the `?` placeholders.

`prepare()` is required before `bind()`.
Calling `bind()` without a prior `prepare()` raises `INVALID_STATE`.

## Arrow batch parameter binding

Pass a multi-row `RecordBatch` as the bound parameters so that one round trip inserts (or queries for) many rows.

```python
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql

# Build a batch where each row is one INSERT execution.
# Columns map positionally to the ? placeholders in the SQL.
# The whole batch travels as a single Arrow buffer over gRPC,
# with no row-shredding and no individual round trips per row.
param_batch = pa.record_batch({
    "v0": pa.array(["p4", "p5", "p6"],                   type=pa.string()),
    "v1": pa.array(["Thingamajig", "Whatsit", "Gizmo"],  type=pa.string()),
    "v2": pa.array([14.99, 7.49, 19.99],                 type=pa.float64()),
    "v3": pa.array([75, 120, 30],                        type=pa.int64()),
})

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO products (_id, name, price, stock)"
            " VALUES (?, ?, ?, ?)",
            param_batch,
        )

        cur.execute("SELECT count(*) FROM products")
        print(cur.fetchone()[0])   # 6 (or however many rows you had before)
```

**Use `pa.string()` (utf8), not `pa.large_utf8()`, for string columns in bound batches.**
`large_utf8` is not currently supported in parameter batches over the FlightSQL wire.
Non-string types (`float64`, `int64`, `int32`) work with their natural Arrow types.

Results from queries also come back as Arrow:

```python
cur.execute("SELECT _id, name FROM products ORDER BY _id")
arrow_table = cur.fetch_arrow_table()   # pyarrow.Table
print(arrow_table.schema)
# _id: string
# name: string
```

`fetch_arrow_table()` collects all batches; for large result sets, use `fetchmany()` or stream via the underlying `ArrowReader`.

## Transactions

XTDB supports multi-statement transactions over ADBC.
Use the connection-level transaction API:

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9832") as conn:
    # Switch off autocommit; opens a FlightSQL BeginTransaction action.
    conn.adbc_connection.set_autocommit(False)

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
            [["o1", "Widget", 10], ["o2", "Gadget", 5]],
        )

        # Writes are visible to reads on the same connection
        # before the transaction commits.
        cur.execute(
            "SELECT count(*) FROM orders WHERE _id IN (?, ?)",
            parameters=["o1", "o2"],
        )
        print(cur.fetchone()[0])   # 2

    conn.adbc_connection.rollback()   # EndTransaction ROLLBACK
    # ↑ the two rows are gone

    conn.adbc_connection.set_autocommit(False)
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
            [["o3", "Doohickey", 20]],
        )
    conn.adbc_connection.commit()     # EndTransaction COMMIT
```

`set_autocommit(False)` calls the FlightSQL `BeginTransaction` action under the hood.
`commit()` / `rollback()` call `EndTransaction` with the appropriate action.
Each `set_autocommit(False)` starts a new transaction; commit or rollback ends it.

:::caution
Transaction support requires the server to advertise `FLIGHT_SQL_SERVER_TRANSACTION` in its `GetSqlInfo` response (SqlInfo code 8, value `SQL_SUPPORTED_TRANSACTION_TRANSACTION`).
Builds before 2026-05 do not include this advertisement; `set_autocommit(False)` raises `NOT_IMPLEMENTED`.
Use the nightly image or build from main.
:::

### ROLLBACK empties pending writes

```python
with flight_sql.connect("grpc://localhost:9832") as conn:
    conn.adbc_connection.set_autocommit(False)

    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders (_id, item, qty) VALUES (?, ?, ?)",
            [["o4", "Prototype", 1]],
        )
        # Visible on this connection before commit:
        cur.execute("SELECT count(*) FROM orders WHERE _id = ?",
                    parameters=["o4"])
        print(cur.fetchone()[0])   # 1

    conn.adbc_connection.rollback()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM orders WHERE _id = ?",
                    parameters=["o4"])
        print(cur.fetchone()[0])   # 0; ROLLBACK erased it
```

The write was visible before rollback because the read happened on the same connection.
After rollback it's gone: neither this connection nor any other will see it.

### setAutoCommit via the dbapi Connection

The DB-API 2.0 `Connection.autocommit` attribute mirrors the underlying ADBC call:

```python
# These are equivalent:
conn.adbc_connection.set_autocommit(False)

# and (if your dbapi wrapper exposes it):
# conn.autocommit = False
```

The `adbc_connection` attribute on the dbapi `Connection` object is the raw `AdbcConnection`; use it to access ADBC-specific transaction methods that the dbapi wrapper doesn't expose.

## Same-connection write-then-read visibility

Writes on a connection are visible to subsequent reads on that **same** connection without any manual `await` or sleep.
The connection tracks its own write tokens and threads them through the planner:

```python
with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO products (_id, name, price, stock)"
            " VALUES (?, ?, ?, ?)",
            [["p9", "Overnight", 3.99, 500]],
        )
        # Immediately visible; no await needed.
        cur.execute(
            "SELECT name, price FROM products WHERE _id = ?",
            parameters=["p9"],
        )
        print(cur.fetchone())
        # ('Overnight', 3.99)
```

Cross-connection reads follow normal transactional semantics: another connection sees the write once it commits and its read snapshot advances.

## DML via `executemany`, not `execute`

A subtle point: the Python dbapi's `cursor.execute()` routes all SQL, including INSERT, UPDATE, DELETE, through the FlightSQL query path (`GetFlightInfo` → `DoGet`).
XTDB's query path does not persist DML.

Use `cursor.executemany()` for DML.
`executemany()` routes through `DoPut` (the update path), which calls `executeUpdate()` on the server and actually commits the write.

```python
# ✗ Does not persist on nightly / stable images:
cur.execute("INSERT INTO t (_id, v) VALUES (?, ?)", parameters=["x", 1])

# ✓ Correct, persists:
cur.executemany("INSERT INTO t (_id, v) VALUES (?, ?)", [["x", 1]])
```

This is a known mismatch between the Python dbapi specification (where `execute` handles DML) and the ADBC FlightSQL wire protocol (where DML goes through `DoPut`, not `GetFlightInfo`).

## What's not covered here

- **Bulk-loading Parquet and other file formats.** See [Bulk-load Parquet via ADBC](./bulk-ingest-from-parquet).
- **`adbc_ingest` for whole Arrow tables over the FlightSQL wire.** The one-round-trip bulk-ingest path; see [pandas / polars round-trip](./pandas-polars-round-trip). For parameterised DML, `executemany` with a `RecordBatch` (above) stays the right tool.
- **`executeSchema` / result shape introspection.** Covered in the [reference](../reference#executeschema-result-shape-without-executing).
- **Cross-connection visibility and read snapshots.** See the [reference](../reference#transactions) for the snapshot-isolation model.
