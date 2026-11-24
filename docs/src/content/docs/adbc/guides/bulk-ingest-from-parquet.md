---
title: Bulk-load Parquet into XTDB via ADBC
description: pyarrow.parquet.read_table → cur.adbc_ingest → done. Covers schema requirements, streaming large files, and error handling.
---

Loading a Parquet file into XTDB is one `adbc_ingest` call.
This guide shows that minimal path and the variations that come up in practice: keying the table on a column your Parquet already has, adding an `_id` when it has none, streaming large files in chunks, and handling the modes that XTDB rejects.

## Prerequisites

XTDB running with the FlightSQL listener on port `3000`, which the Docker standalone image does by default:

```bash
docker run --rm -p 5432:5432 -p 3000:3000 ghcr.io/xtdb/xtdb:nightly
```

Install the Python dependencies:

```bash
pip install adbc-driver-flightsql pyarrow
```

## The minimal path

```python
import pyarrow.parquet as pq
import adbc_driver_flightsql.dbapi as flight_sql

table = pq.read_table("orders.parquet")

with flight_sql.connect("grpc://localhost:3000") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("orders", table, mode="create_append")
```

`adbc_ingest` streams Arrow batches from the client into XTDB in a single round trip: no row shredding, no `executemany("INSERT INTO ...")`.

The one requirement: every row needs the table's [primary key](/reference/main/sql/schema#primary-keys-v221) column - `_id`, unless the table declares another.

## Keying the table on a column your Parquet already has

If a column already uniquely identifies each row (e.g. `order_id`), declare it as the table's primary key and ingest the Parquet as it is:

```python
import pyarrow.parquet as pq
import adbc_driver_flightsql.dbapi as flight_sql

table = pq.read_table("orders.parquet")

with flight_sql.connect("grpc://localhost:3000") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE orders (order_id PRIMARY KEY)")
    conn.commit()

    with conn.cursor() as cur:
        cur.adbc_ingest("orders", table, mode="create_append")
```

Commit the declaration before the first ingest.
The dbapi holds the `CREATE TABLE` in a transaction until `conn.commit()`, while each `adbc_ingest` call commits on its own:

- uncommitted declaration → the ingest reaches a table still keyed on `_id` → it's rejected with `INVALID_ARGUMENT: missing '_id'` and writes nothing.

Committing the declaration then and re-running the ingest keys the table as intended.
For when a table's key can be declared, see [Primary keys](/reference/main/sql/schema#primary-keys-v221).

## Adding `_id` when your Parquet has no key column

If no single column identifies a row, materialise an `_id` client-side before calling `adbc_ingest`.

**Option 1: build a composite key.**
If uniqueness comes from a combination of columns, hash them together:

```python
import pyarrow as pa
import pyarrow.compute as pc

# Concatenate two string columns to form a stable key.
composite = pc.binary_join_element_wise(
    table.column("customer_id").cast(pa.string()),
    table.column("order_date").cast(pa.string()),
    "-",
)
table = table.append_column("_id", composite)
```

**Option 2: generate a random UUID per row.**
When the rows have no natural key, a random UUID is the best default: it's unique by construction and works for both one-time loads and ongoing production ingest (unlike a positional index, which shifts if you re-ingest and silently overwrites).

```python
import uuid
import pyarrow as pa

ids = pa.array([str(uuid.uuid4()) for _ in range(len(table))], type=pa.string())
table = table.append_column("_id", ids)
```

XTDB upserts on the table's key: two rows with the same key in the same ingest call will produce one document in XTDB, with the later row's values winning.
If that's not what you want, make sure your key values are unique before ingesting.

## Streaming large files in chunks

`pq.read_table` loads the whole Parquet file into memory at once.
For files larger than available RAM, use `pq.ParquetFile` to iterate over row groups:

```python
import pyarrow.parquet as pq
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:3000") as conn:
    with conn.cursor() as cur:
        pf = pq.ParquetFile("large-orders.parquet")
        for batch in pf.iter_batches(batch_size=100_000):
            # Each batch is a pyarrow.RecordBatch.
            # adbc_ingest accepts RecordBatch as well as Table.
            cur.adbc_ingest("orders", batch, mode="create_append")
```

`adbc_ingest` accepts any Arrow-shaped data: `pyarrow.Table`, `pyarrow.RecordBatch`, or any object that exposes the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
Batches stream through without materialising the whole dataset in either the client or the server.

## Ingest modes

`create`, `append`, and `create_append` are all accepted and behave the same here, since XTDB auto-creates tables and upserts on the table's key.
`replace` and the fail-if-exists / fail-if-not-exist variants are rejected with a gRPC `INVALID_ARGUMENT`.
See [Bulk ingest](/adbc/reference#bulk-ingest) in the reference for the full mode table and the reasoning.

## Error handling

Catch `adbc_driver_flightsql.DatabaseError` (a `ProgrammingError` in the ADBC DBAPI sense) and read the gRPC status message from it directly.
Rejected modes and rows missing the table's key column return a descriptive `INVALID_ARGUMENT`.

```python
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql
from adbc_driver_manager import DatabaseError

with flight_sql.connect("grpc://localhost:3000") as conn:
    with conn.cursor() as cur:
        table = pa.table({
            "_id": ["alice"],
            "name": ["Alice"],
        })

        try:
            # "replace" is rejected: XTDB has no ERASE-then-reinsert step.
            cur.adbc_ingest("users", table, mode="replace")
        except DatabaseError as e:
            print(f"Ingest failed: {e}")
            # "Ingest failed: INVALID_ARGUMENT: ingest mode 'replace' is not supported …"

        try:
            # Missing _id column: INVALID_ARGUMENT.
            no_id = pa.table({"name": ["Bob"]})
            cur.adbc_ingest("users", no_id, mode="create_append")
        except DatabaseError as e:
            print(f"Ingest failed: {e}")
```

XTDB puts the full gRPC status message in `DatabaseError.args[0]`, so you can read it straight off the exception.

## Verifying the load

After ingesting, confirm the row count and schema look right:

```python
with conn.cursor() as cur:
    cur.execute("SELECT count(*) AS n FROM orders")
    print("Row count:", cur.fetchone()[0])

    schema = conn.adbc_get_table_schema(
        "orders", db_schema_filter="public",
    )
    print("Schema:", schema)
```

`adbc_get_table_schema` reflects live data: freshly ingested rows show up immediately, before any background flush.

## Runnable example

A self-contained script that writes a Parquet fixture and bulk-loads it is in the repository at
[`docs/adbc/examples/bulk-ingest-from-parquet/main.py`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/bulk-ingest-from-parquet/main.py).

```bash
docker run --rm -p 5432:5432 -p 3000:3000 ghcr.io/xtdb/xtdb:nightly
pip install adbc-driver-flightsql pyarrow
python docs/src/content/docs/adbc/examples/bulk-ingest-from-parquet/main.py
```
