---
title: Streaming XTDB results into DuckDB via Arrow
description: Combine XTDB's bitemporal data with DuckDB-resident data over Arrow, with no serialise/deserialise step.
---

When you already have data in DuckDB, ADBC lets you bring XTDB's bitemporal data alongside it without a copy: both are Arrow-native, so a query result streams from XTDB straight into DuckDB to be joined with what's already there.
The worked scenario: take a bitemporal trade snapshot from XTDB as of Q4 2024 and join it to a counterparty-tier lookup table held in DuckDB.

## Prerequisites

A running XTDB node with the FlightSQL listener on port 9832 and the pgwire server on port 5432.
The nightly Docker image enables both by default:

```sh
docker run --rm -d --name xtdb-duckdb-demo \
    -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
```

Install the Python dependencies:

```sh
pip install adbc-driver-flightsql pyarrow duckdb psycopg
```

`psycopg` seeds the trades over pgwire, so no external `psql` binary is required.

## The scenario

Seven trades are stored in XTDB with explicit valid-time ranges.
One of them, CommerzBank's T005, expired on `2024-06-30` and should be absent from the end-of-Q4 snapshot even though it was physically ingested.
The question: as of 2024-12-31, what's the total trade value per counterparty tier?

The counterparty-to-tier mapping lives in DuckDB as a local dimension table (Parquet, CSV, or in-memory; it does not need to be in XTDB).

## Step 1: Seed XTDB with bitemporal trades

Trades arrive via the normal write path.
Use `_valid_from` and `_valid_to` to record when each trade *was true*: the valid-time axis.

```python
import psycopg
from textwrap import dedent

SEED_SQL = dedent("""\
    INSERT INTO trades (_id, counterparty, notional, currency, _valid_from, _valid_to)
    VALUES
      ('T001','Barclays',    1500000.0,'GBP',TIMESTAMP '2024-01-15 00:00:00',TIMESTAMP '9999-12-31 00:00:00'),
      ('T002','Barclays',    2000000.0,'GBP',TIMESTAMP '2024-03-01 00:00:00',TIMESTAMP '9999-12-31 00:00:00'),
      ('T003','JPMorgan',    3200000.0,'USD',TIMESTAMP '2024-02-10 00:00:00',TIMESTAMP '9999-12-31 00:00:00'),
      ('T004','DeutscheBank', 800000.0,'EUR',TIMESTAMP '2024-04-01 00:00:00',TIMESTAMP '9999-12-31 00:00:00'),
      ('T005','CommerzBank', 1100000.0,'EUR',TIMESTAMP '2024-01-01 00:00:00',TIMESTAMP '2024-06-30 00:00:00'),
      ('T006','SocGen',       950000.0,'EUR',TIMESTAMP '2024-06-01 00:00:00',TIMESTAMP '9999-12-31 00:00:00'),
      ('T007','SocGen',      1750000.0,'EUR',TIMESTAMP '2024-09-15 00:00:00',TIMESTAMP '9999-12-31 00:00:00')
""")

PG_URI = "postgresql://localhost:5432/xtdb"
with psycopg.connect(PG_URI, user="xtdb", autocommit=True) as pg:
    with pg.cursor() as cur:
        cur.execute(SEED_SQL)
```

`_valid_from`/`_valid_to` in the INSERT body override the default (`now` → `end-of-time`) so XTDB records each trade's actual validity window.

## Step 2: Pull the Q4 snapshot via ADBC

`FOR VALID_TIME AS OF` pins the query to a single instant.
XTDB returns only the rows whose valid-time period *contains* that instant.
T005 (CommerzBank, valid to `2024-06-30`) is excluded.

```python
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql

SNAPSHOT_SQL = """\
    SELECT _id, counterparty, notional, currency
      FROM trades
           FOR VALID_TIME AS OF TIMESTAMP '2024-12-31 23:59:59'
     ORDER BY _id
"""

FSQL_URI = "grpc://localhost:9832"

with flight_sql.connect(FSQL_URI) as conn:
    with conn.cursor() as cur:
        cur.execute(SNAPSHOT_SQL)
        snapshot: pa.Table = cur.fetch_arrow_table()
```

`fetch_arrow_table()` returns a `pyarrow.Table`.
The data travels from XTDB's in-memory Arrow buffers across the FlightSQL gRPC stream and lands in the client as Arrow, so it isn't decoded row by row or widened through a text wire format.

`snapshot` now holds 6 rows: T001–T004, T006, T007.

## Step 3: Register the snapshot into DuckDB

```python
import duckdb

duck = duckdb.connect()          # in-memory DuckDB instance
duck.register("xtdb_trades", snapshot)
```

`duckdb.register()` accepts any object that exposes the Arrow C Data Interface: a `pyarrow.Table`, `RecordBatch`, polars `DataFrame`, etc.
DuckDB operates directly on the Arrow buffers XTDB produced.
No copy, no decode.

## Step 4: Create the counterparty-tier dimension in DuckDB

Enrichment data that doesn't belong in XTDB lives here.
In production this is typically a Parquet file on the local filesystem:

```python
duck.execute("""\
    CREATE TABLE counterparty_tiers AS
    SELECT * FROM (VALUES
        ('Barclays',     'Gold'),
        ('JPMorgan',     'Gold'),
        ('DeutscheBank', 'Silver'),
        ('SocGen',       'Silver'),
        ('CommerzBank',  'Bronze')
    ) AS t(counterparty, tier)
""")
```

Or from a Parquet file:

```python
duck.execute("CREATE TABLE counterparty_tiers AS SELECT * FROM 'tiers.parquet'")
```

## Step 5: Join and aggregate in DuckDB

The join runs inside DuckDB, over the Arrow buffers XTDB produced, combining the bitemporal snapshot with DuckDB's local lookup table.
No round-trip to XTDB, no re-serialisation.

```python
result: pa.Table = duck.execute("""\
    SELECT
        ct.tier,
        count(*)                  AS trade_count,
        sum(t.notional)           AS total_notional,
        round(avg(t.notional), 0) AS avg_notional
    FROM  xtdb_trades        AS t
    JOIN  counterparty_tiers AS ct ON t.counterparty = ct.counterparty
    GROUP BY ct.tier
    ORDER BY total_notional DESC
""").arrow().read_all()
```

Output:

```
Exposure by counterparty tier, as-of Q4 2024 end:
  Tier        Trades    Total Notional    Avg Notional
  ----------  ------  ----------------  --------------
  Gold             3         6,700,000       2,233,333
  Silver           3         3,500,000       1,166,667
```

CommerzBank (Bronze, T005) is absent because it did not exist at Q4 end.
The valid-time filter happened at the XTDB layer before any data crossed the wire.

## Streaming batches instead of materialising the whole snapshot

For result sets too large to fit in client memory, stream Arrow batches one at a time rather than calling `fetch_arrow_table()`:

```python
import adbc_driver_flightsql as flight_sql_raw

db = flight_sql_raw.connect(FSQL_URI)
conn = db.new_connection()
stmt = conn.new_statement()
stmt.set_sql_query(SNAPSHOT_SQL)
result, schema = stmt.execute_query()

# result is an ArrowReader; each iteration yields one RecordBatch.
for batch in result:
    duck.register("xtdb_batch", batch)
    duck.execute("INSERT INTO aggregated SELECT ..., FROM xtdb_batch")

result.close()
conn.close()
db.close()
```

The tradeoff:
- `fetch_arrow_table()` materialises the full snapshot in one shot: simple, and the right default for snapshots that fit in RAM.
- The `ArrowReader` path streams batch-by-batch: the right choice when the XTDB result set is large and you can incrementally fold each batch into an ongoing DuckDB aggregate without materialising the whole thing.

## Writing results back

DuckDB query results can go back to XTDB via bulk ingest, or out to Parquet:

**Back to XTDB:**

```python
# result is a pyarrow.Table; annotate with _id before ingest.
import pyarrow.compute as pc
result_with_id = result.append_column(
    "_id", pa.array([f"tier-summary-{r['tier']}" for r in result.to_pylist()])
)
with flight_sql.connect(FSQL_URI) as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("tier_summaries", result_with_id, mode="create_append")
```

**To Parquet:**

```python
import pyarrow.parquet as pq
pq.write_table(result, "tier_exposure_q4_2024.parquet")
```

## Runnable example

A self-contained script that runs the full scenario is in
[`examples/streaming-into-duckdb/main.py`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/streaming-into-duckdb/main.py).

```sh
docker run --rm -d --name xtdb-g5 -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
pip install adbc-driver-flightsql pyarrow duckdb psycopg
python docs/src/content/docs/adbc/examples/streaming-into-duckdb/main.py
docker rm -f xtdb-g5
```

## Why this works without a copy

XTDB's query engine produces Arrow `RecordBatch` objects internally.
The FlightSQL shim packages those batches into gRPC `DoGet` streams as-is.
The ADBC FlightSQL driver on the Python side decodes the gRPC framing and reconstructs the Arrow buffers in the client process.
`duckdb.register()` takes the Arrow C Data Interface pointer to those buffers: DuckDB's planner sees the Arrow schema and DuckDB's executor reads the data in-place.

The data moves as: XTDB in-memory Arrow → gRPC wire encoding → pyarrow Arrow buffer → DuckDB execution.
There is no intermediate JSON, CSV, Postgres text wire, or pickle.
The gRPC serialisation is the only encode/decode step, and it is Arrow's own IPC format.
