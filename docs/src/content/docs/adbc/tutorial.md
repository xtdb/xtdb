---
title: "ADBC tutorial: XTDB end-to-end in Python"
description: "An Arrow-native walkthrough: install the driver, connect, ingest, query, transact, and time-travel."
---

The tutorial is Python-first: `adbc_driver_flightsql` + `pyarrow` keep results in Arrow from XTDB to your process, with no intermediate decode.
A complete, runnable version of every snippet here lives in [`examples/tutorial/main.py`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/tutorial/main.py).

## What you'll build

A small worked example: load a `pyarrow.Table` of historical FX trades into XTDB, run analytical queries against it, add new trades, and travel back in time to the state before they arrived.
It covers bulk ingest, parameterised queries, introspection, and `executeSchema`.

## 1. Prerequisites

You need a Python 3.10+ environment and Docker.

```bash
pip install adbc-driver-flightsql pyarrow pandas
docker run --rm -d --name xtdb -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
```

The standalone Docker image starts both the PostgreSQL wire server (port 5432) and the FlightSQL gRPC server (port 9832).
The `nightly` image enables FlightSQL by default, as do stable images from 2.2.0 onwards.

## 2. Connecting

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect("grpc://localhost:9832")
```

The connection string is a standard gRPC URI.
`flight_sql.connect` returns a DB-API 2.0 `Connection` object and can also be used as a context manager.

For JVM readers: the in-process `node.connect()` path requires no network hop and no gRPC; see the [reference](/adbc/reference#in-process-kotlin--jvm) for the Kotlin API.

## 3. Your first query

```python
with conn.cursor() as cur:
    cur.execute("SELECT 1")
    result = cur.fetch_arrow_table()
    print(result.schema)
    print(result.to_pylist())
```

Expected output:

```
_column_1: int64 not null
[{'_column_1': 1}]
```

`fetch_arrow_table()` returns a `pyarrow.Table`, not Python rows or tuples.
Results arrive as Arrow batches and stay in Arrow form from XTDB's storage layer into your process, with no row-by-row decode.

## 4. Bulk ingest

Build the dataset of ten FX trades covering three currency pairs:

```python
import pyarrow as pa
from datetime import datetime, timezone

trades = pa.table({
    "_id": pa.array([
        "t001", "t002", "t003", "t004", "t005",
        "t006", "t007", "t008", "t009", "t010",
    ]),
    "symbol": pa.array([
        "EURUSD", "GBPUSD", "EURUSD", "USDJPY", "GBPUSD",
        "EURUSD", "USDJPY", "GBPUSD", "EURUSD", "USDJPY",
    ]),
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
})
```

Ingest in one round trip:

```python
with conn.cursor() as cur:
    cur.adbc_ingest("trades", trades, mode="create_append")
```

XTDB auto-creates the `trades` table.
The table name you pass becomes the target, and XTDB infers the schema from the data, so no `CREATE TABLE` is required first (you can still declare one explicitly if you want).
Every row must have an `_id` column: XTDB's document model requires it.

Verify the round-trip:

```python
with conn.cursor() as cur:
    cur.execute("SELECT * FROM trades ORDER BY _id")
    back = cur.fetch_arrow_table()
    print(back.num_rows, "rows")
    print(back.schema)
```

Expected output:

```
10 rows
_id: string
price: double
qty: int64
symbol: string
traded_at: timestamp[us, tz=UTC]
```

The types come back exactly as you put them in: no widening through a Postgres text protocol, no timestamp-to-string conversion.

## 5. Querying with parameters

Use `?` for positional parameters:

```python
with conn.cursor() as cur:
    cur.execute(
        "SELECT _id, symbol, qty, price FROM trades WHERE symbol = ? ORDER BY _id",
        parameters=["EURUSD"],
    )
    result = cur.fetch_arrow_table()

print(f"EURUSD trades: {result.num_rows} rows")
for row in result.to_pylist():
    print(f"  {row['_id']}  qty={row['qty']:>7,}  price={row['price']:.4f}")
```

Expected output:

```
EURUSD trades: 4 rows
  t001  qty=100,000  price=1.0921
  t003  qty= 75,000  price=1.0918
  t006  qty=320,000  price=1.0935
  t009  qty=110,000  price=1.0942
```

The `parameters` list maps positionally to the `?` placeholders.
The result is still a `pyarrow.Table`: the type system is unaffected by the parameter binding.

## 6. Write visibility

The dbapi defaults to manual-commit, so for ingest-and-query scripts connect with `autocommit=True` and each statement commits as it runs:

```python
conn = flight_sql.connect("grpc://localhost:9832", autocommit=True)
```

`adbc_ingest` commits atomically on its own regardless of commit mode, which is why the ingests in this tutorial are visible without any extra step.
For the full commit-mode, visibility, and multi-statement transaction rules see [Transactions](/adbc/reference#transactions) in the reference.

A committed write is visible to subsequent reads on the same connection, with no explicit synchronisation step:

```python
extra = pa.table({
    "_id":    pa.array(["t011", "t012"]),
    "symbol": pa.array(["EURGBP", "EURGBP"]),
    "qty":    pa.array([60_000, 40_000], type=pa.int64()),
    "price":  pa.array([0.8571, 0.8569], type=pa.float64()),
})

with conn.cursor() as cur:
    cur.adbc_ingest("trades", extra, mode="create_append")

# Same-connection read; sees the rows immediately.
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM trades WHERE symbol = 'EURGBP'")
    n = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
print(f"EURGBP rows: {n}")  # 2

with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM trades")
    total = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
print(f"Total trades: {total}")  # 12
```

Expected output:

```
EURGBP rows: 2
Total trades: 12
```

The `count(*)` immediately after the ingest sees the just-written rows.

## 7. Introspection: `getObjects` / `getTableSchema`

Discover what's in the database without running a query.

**`adbc_get_objects`** returns the full catalog → schema → table → column tree as an Arrow reader:

```python
reader = conn.adbc_get_objects(depth="all", db_schema_filter="public")
tbl = reader.read_all()
print(f"Catalog rows returned: {tbl.num_rows}")
```

Expected output:

```
Catalog rows returned: 1
```

The Arrow result encodes the entire schema tree in ADBC's standard nested-struct format.
Tooling that needs to walk the catalog (IDE autocomplete, dataframe pipeline builders) can consume this directly without issuing any SQL.

**`adbc_get_table_schema`** returns a `pyarrow.Schema` for a single table:

```python
schema = conn.adbc_get_table_schema("trades", db_schema_filter="public")
for field in schema:
    print(f"  {field.name}: {field.type}")
```

Expected output:

```
  _id: string
  price: double
  qty: int64
  symbol: string
  traded_at: timestamp[us, tz=UTC]
```

Column types reflect **live data**: freshly-ingested rows show up here immediately, the same way they show up via SQL `SELECT`.

## 8. `executeSchema`: result shape without running

`executeSchema` returns the Arrow schema of a query's result set without executing the query.
This lets pipeline builders and schema-on-read consumers learn the result columns before paying for a full execution.

```python
with conn.cursor() as cur:
    schema = cur.adbc_execute_schema(
        "SELECT _id, symbol, qty, price FROM trades WHERE symbol = ?"
    )
print("Result schema:")
for field in schema:
    print(f"  {field.name}: {field.type}")
```

Expected output:

```
Result schema:
  _id: string
  symbol: string
  qty: int64
  price: double
```

Note on parameter types: the schema is computed before any bind, so XTDB synthesises null-typed placeholder fields for `?` positions.
For queries where the projection does not depend on the parameter value, which covers the common `SELECT cols FROM t WHERE id = ?` shape, the returned schema is accurate.
For queries that project the parameter directly (`SELECT ?`, `SELECT ? + 1`), the result field types will be placeholder-typed rather than the type you would eventually bind.
Workaround: project through a column expression that fixes the type.

## 9. Bitemporality: `FOR SYSTEM_TIME AS OF`

XTDB records the wall-clock time at which every row was committed.
You can query the database as it stood at any point in the past using `FOR SYSTEM_TIME AS OF`.

Capture a checkpoint before inserting a new trade:

```python
import time
from datetime import datetime, timezone

checkpoint = datetime.now(timezone.utc)
time.sleep(0.1)  # ensure the next write has a strictly later system time

new_trade = pa.table({
    "_id":    pa.array(["t013"]),
    "symbol": pa.array(["USDCHF"]),
    "qty":    pa.array([200_000], type=pa.int64()),
    "price":  pa.array([0.9012], type=pa.float64()),
})
with conn.cursor() as cur:
    cur.adbc_ingest("trades", new_trade, mode="create_append")
```

Current view, with t013 present:

```python
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM trades")
    count_now = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
print(f"Total trades now: {count_now}")  # 13
```

Historical view: travel back to the checkpoint, before t013 existed:

```python
ts = checkpoint.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
with conn.cursor() as cur:
    cur.execute(f"SELECT count(*) FROM trades FOR SYSTEM_TIME AS OF TIMESTAMP '{ts}+00:00'")
    count_before = cur.fetch_arrow_table().to_pylist()[0]["_column_1"]
print(f"Trades AS OF checkpoint: {count_before}")  # 12
```

Expected output:

```
Total trades now: 13
Trades AS OF checkpoint: 12
```

The Arrow types of temporal columns, including the `_system_from` / `_system_to` hidden columns XTDB maintains, survive the FlightSQL round-trip without widening.
There is no Postgres-style text-format timestamp decode on the way back.

`FOR SYSTEM_TIME AS OF` is ordinary XTDB SQL and works equally well over the pgwire driver.
The ADBC path returns the result in Arrow form rather than decoded rows.

## 10. Where next

- **[Reference](/adbc/reference)**: the full supported ADBC surface in detail: which calls work, which don't, per-client caveats.
- **[How-to guides](/adbc/guides)**: task-shaped recipes:
  - [Bulk-load Parquet](/adbc/guides/bulk-ingest-from-parquet): load a Parquet file in one `adbc_ingest` call.
  - [pandas / polars round-trip](/adbc/guides/pandas-polars-round-trip): read query results directly into a DataFrame without an intermediate copy.
  - [Stream results into DuckDB](/adbc/guides/streaming-into-duckdb): feed XTDB query output into DuckDB via the Arrow C Data Interface.
  - [Point-in-time feature extraction](/adbc/guides/point-in-time-feature-extraction): use `FOR SYSTEM_TIME AS OF` inside a pipeline to recreate historical feature sets.
- **[Examples](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples)**: minimal hello-world programs in Python, Rust, and other languages.
