---
title: Bulk-load Parquet into XTDB via ADBC
description: pyarrow.parquet.read_table → cur.adbc_ingest → done. Covers schema requirements, streaming large files, and error handling.
---

Loading a Parquet file into XTDB is one `adbc_ingest` call.
This guide shows that minimal path and the variations that come up in practice: adding an `_id` when your Parquet doesn't have one, streaming large files in chunks, and handling the modes that XTDB rejects.

## Prerequisites

XTDB running with the FlightSQL listener on port `9832`, which the Docker standalone image does by default:

```bash
docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
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

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("orders", table, mode="create_append")
```

`adbc_ingest` streams Arrow batches from the client into XTDB in a single round trip: no row shredding, no `executemany("INSERT INTO ...")`.

The one requirement: every row needs an `_id` column, XTDB's primary key.

## Adding `_id` when your Parquet doesn't have one

If your Parquet schema doesn't include an `_id` column, materialise one client-side before calling `adbc_ingest`.

**Option 1: promote an existing natural key.**
If a column already uniquely identifies each row (e.g. `order_id`), rename it:

```python
import pyarrow as pa
import pyarrow.parquet as pq

table = pq.read_table("orders.parquet")
# Rename order_id to _id, keep the original column too if you want.
table = table.rename_columns(
    ["_id" if c == "order_id" else c for c in table.schema.names]
)
```

**Option 2: build a composite key.**
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

**Option 3: generate a random UUID per row.**
When the rows have no natural key, a random UUID is the best default: it's unique by construction and works for both one-time loads and ongoing production ingest (unlike a positional index, which shifts if you re-ingest and silently overwrites).

```python
import uuid
import pyarrow as pa

ids = pa.array([str(uuid.uuid4()) for _ in range(len(table))], type=pa.string())
table = table.append_column("_id", ids)
```

XTDB upserts on `_id`: two rows with the same `_id` in the same ingest call will produce one document in XTDB, with the later row's values winning.
If that's not what you want, make sure your `_id` values are unique before ingesting.

## Streaming large files in chunks

`pq.read_table` loads the whole Parquet file into memory at once.
For files larger than available RAM, use `pq.ParquetFile` to iterate over row groups:

```python
import pyarrow.parquet as pq
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9832") as conn:
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

`create`, `append`, and `create_append` are all accepted and behave the same here, since XTDB auto-creates tables and upserts on `_id`.
`replace` and the fail-if-exists / fail-if-not-exist variants are rejected with a gRPC `INVALID_ARGUMENT`.
See [Bulk ingest](/adbc/reference#bulk-ingest) in the reference for the full mode table and the reasoning.

## Error handling

Catch `adbc_driver_flightsql.DatabaseError` (a `ProgrammingError` in the ADBC DBAPI sense) and read the gRPC status message from it directly.
Rejected modes return a descriptive `INVALID_ARGUMENT`; some other failures (a missing `_id`) are not yet mapped and surface as `INTERNAL`, with the cause still in the message.

```python
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql
from adbc_driver_manager import DatabaseError

with flight_sql.connect("grpc://localhost:9832") as conn:
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
            # Missing _id column. Currently surfaces as INTERNAL, not
            # INVALID_ARGUMENT (see the reference's error-mapping note).
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
docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
pip install adbc-driver-flightsql pyarrow
python docs/src/content/docs/adbc/examples/bulk-ingest-from-parquet/main.py
```
