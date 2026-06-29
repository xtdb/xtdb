---
title: Round-tripping pandas / polars DataFrames via ADBC
description: Zero-copy dataframe ↔ XTDB through Arrow, with full type fidelity for timestamps, nulls, and categoricals.
---

ADBC keeps your data in Arrow from XTDB through to a DataFrame, with no row-by-row decode and no type widening through the Postgres text wire.
This guide shows both directions: reading query results straight into pandas / polars, and writing DataFrames back to XTDB.

Type fidelity is the main reason to use this path.
`TIMESTAMP WITH TIME ZONE`, nullable columns, and dictionary-encoded categoricals all survive intact.
The same round-trip through psycopg or JDBC widens timestamps, turns nullable Arrow arrays into `NaN`-polluted float columns, and discards the category encoding.

## Prerequisites

```bash
pip install adbc-driver-flightsql pyarrow pandas polars
docker run --rm -d --name xtdb -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
```

All examples below connect to `grpc://localhost:9832`.
The [runnable companion script](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/pandas-polars-round-trip/main.py) exercises every code block in order.

## The dataset

A small events table that stress-tests type fidelity:

- `_id`: required; XTDB's document key.
- `ts`: `TIMESTAMP WITH TIME ZONE`.
  psycopg hands this back as a tz-aware `datetime`; ADBC hands it back as `timestamp[us, tz=UTC]`, Arrow-native, no intermediate decode.
- `severity`: string, stored as a small-cardinality column.
  Ingested via pandas as `dictionary<int8, large_utf8>`; polars ingests as `dictionary<uint32, large_utf8>`.
  psycopg would widen to plain strings; ADBC returns it as `utf8`, which clients can re-encode as dictionary if needed (see the [dictionary callout](#dictionary-round-trip-caveat)).
- `value`: `float64`, always present.
- `label`: `utf8`, nullable.
  Some rows have `null` here.
  Through psycopg the column often surfaces as `object` dtype with `None` sprinkled in; Arrow models it as a validity bitmap alongside the data buffer.

## XTDB → Arrow (reading)

Connect and fetch a query result as an Arrow table:

```python
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT _id, ts, severity, value, label FROM events ORDER BY ts")
        arrow_table = cur.fetch_arrow_table()

# arrow_table is a pyarrow.Table; Arrow buffers, no copies yet.
print(arrow_table.schema)
# _id:      string
# ts:       timestamp[us, tz=UTC]
# severity: string
# value:    double
# label:    string   (nullable=True)
```

Query results stream from XTDB as Arrow batches over gRPC.
`fetch_arrow_table()` collects them into a single `pyarrow.Table`, memory-bound by the client; use the raw batch-by-batch path for very large result sets.

### Timezone normalisation

XTDB FlightSQL currently returns timestamps with timezone key `"Z"` (Zulu = UTC).
The `"Z"` key is valid Arrow but not a recognised IANA timezone string, so pandas will raise a `ZoneInfoNotFoundError` when printing or converting these columns.
Cast to `"UTC"` before handing the table to pandas or polars:

```python
def normalise_tz(table: pa.Table) -> pa.Table:
    """Cast timestamp[us, tz=Z] → timestamp[us, tz=UTC] for client compatibility."""
    for i, field in enumerate(table.schema):
        t = field.type
        if pa.types.is_timestamp(t) and t.tz == "Z":
            table = table.set_column(
                i, field.name,
                table.column(field.name).cast(pa.timestamp(t.unit, tz="UTC")),
            )
    return table

arrow_table = normalise_tz(arrow_table)
```

## Arrow → pandas

### With `types_mapper=pd.ArrowDtype` (recommended)

```python
import pandas as pd

df = arrow_table.to_pandas(types_mapper=pd.ArrowDtype)
print(df.dtypes)
# _id         string[pyarrow]
# ts          timestamp[us, tz=UTC][pyarrow]
# severity    string[pyarrow]
# value       double[pyarrow]
# label       string[pyarrow]
```

`types_mapper=pd.ArrowDtype` keeps all columns Arrow-backed:
- **Zero-copy** for all column types including strings: the pandas `ArrowDtype` column wraps the Arrow buffer directly, no allocation.
- Nulls surface as `pd.NA` (not `float('nan')`): `df["label"].isna().sum()` counts them correctly.
- Timestamps carry the UTC timezone as an `ArrowDtype`: no lossy intermediate.

### Without `types_mapper` (classic path)

```python
df_classic = arrow_table.to_pandas()
print(df_classic.dtypes)
# _id         object              ← strings decoded into Python objects
# ts          datetime64[us, UTC] ← zero-copy from the Arrow buffer
# severity    object              ← strings decoded, dictionary encoding gone
# value       float64             ← zero-copy from the Arrow buffer
# label       object              ← nulls decoded as Python None
```

Numeric and timestamp columns are zero-copy in the classic path too.
Strings allocate: pandas's `object` dtype is Python objects, not Arrow buffers.

## Arrow → polars

polars is Arrow-native, so `pl.from_arrow()` wraps the Arrow buffers without copying them:

```python
import polars as pl

df = pl.from_arrow(arrow_table)
print(df.schema)
# Schema({'_id': String, 'ts': Datetime(time_unit='us', time_zone='UTC'),
#         'severity': String, 'value': Float64, 'label': String})
```

The polars path is genuinely zero-copy for all column types including strings.
polars stores strings as Arrow `LargeUtf8` internally; it wraps the server-side `Utf8` buffer directly.

Nulls are modelled as Arrow nulls, not `NaN`:

```python
df["label"].null_count()   # 2; not NaN, not None-in-object
```

## DataFrame → XTDB (ingesting)

The clean bulk-ingest path (one gRPC round trip, no row shredding) uses `adbc_ingest`:

```python
import pyarrow as pa

# pandas → Arrow
arrow_from_pd = pa.Table.from_pandas(df, preserve_index=False)

# polars → Arrow (near-zero-copy)
arrow_from_pl = df_pl.to_arrow()

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("events", arrow_from_pd, mode="create_append")
        cur.adbc_ingest("events", arrow_from_pl, mode="create_append")
```

`adbc_ingest` maps to the FlightSQL `CommandStatementIngest` command: the whole table travels as one Arrow stream, no per-row round trips.
It's available on the `nightly` image and on stable images from 2.2.0 onwards.

## Building an ingest-ready Arrow table from pandas

Regardless of the ingest path, here is how to build a clean Arrow table from a pandas DataFrame:

```python
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({
    "_id": ["evt-001", "evt-002", "evt-003", "evt-004"],
    "ts": pd.to_datetime(
        ["2024-01-15T08:30:00Z", "2024-01-15T09:45:00Z",
         "2024-01-15T11:00:00Z", "2024-01-15T14:20:00Z"],
        utc=True,
    ),
    # Categorical → Arrow dictionary<int8, large_utf8>.
    "severity": pd.Categorical(
        ["INFO", "WARN", "ERROR", "INFO"],
        categories=["INFO", "WARN", "ERROR"],
    ),
    "value": [1.23, 4.56, 7.89, 2.34],
    # StringDtype with None → Arrow utf8 with validity bitmap (not NaN).
    "label": pd.array(["startup", None, "threshold", None],
                      dtype=pd.StringDtype()),
})

arrow_table = pa.Table.from_pandas(df, preserve_index=False)
# Schema:
#   _id:      large_string
#   ts:       timestamp[us, tz=UTC]
#   severity: dictionary<values=large_string, indices=int8, ordered=0>
#   value:    double
#   label:    large_string
```

Key points:

- Use `pd.StringDtype()` for nullable string columns so `None` maps to an Arrow null, not a `NaN`.
  Plain Python `str` in an `object` column would produce `None` or `float('nan')`, which `from_pandas` handles heuristically.
- Use `pd.Categorical` for low-cardinality string columns: `from_pandas` converts it to Arrow `dictionary<int8, large_utf8>` preserving the encoding.
- `preserve_index=False` drops the pandas index from the schema; the resulting table is clean Arrow with no pandas metadata bleeding in.

## Building an ingest-ready Arrow table from polars

polars `.to_arrow()` is near-zero-copy:

```python
import polars as pl
from datetime import datetime, timezone

df = pl.DataFrame({
    "_id": ["evt-005", "evt-006"],
    "ts": pl.Series(
        [datetime(2024, 1, 16, 8, 0, tzinfo=timezone.utc),
         datetime(2024, 1, 16, 10, 30, tzinfo=timezone.utc)],
        dtype=pl.Datetime("us", "UTC"),
    ),
    "severity": pl.Series(["WARN", "ERROR"], dtype=pl.Categorical),
    "value": [9.99, 0.01],
    "label": pl.Series([None, "manual"], dtype=pl.Utf8),
})

arrow_table = df.to_arrow()
# Schema:
#   _id:      large_string
#   ts:       timestamp[us, tz=UTC]
#   severity: dictionary<values=large_string, indices=uint32, ordered=0>
#   value:    double
#   label:    large_string
```

polars `pl.Categorical` maps to `dictionary<uint32, large_utf8>`, a wider index than pandas's `int8`, but both are valid Arrow dictionary types.

## Type fidelity: what survives, what doesn't

| Type | Via pgwire (psycopg) | Via ADBC |
|------|----------------------|----------|
| `TIMESTAMP WITH TIME ZONE` | Python `datetime` (tz-aware) | `timestamp[us, tz=UTC]`, Arrow native, zero-copy into polars / pandas with `ArrowDtype` |
| Nullable column | `object` dtype with `None` | Arrow validity bitmap, proper `null_count`, no NaN |
| `DOUBLE` / `FLOAT` | Python `float` | `float64` Arrow buffer, zero-copy into numpy / polars |
| `INTEGER` | Python `int` | `int32` / `int64` Arrow buffer, zero-copy |
| `VARCHAR` / `TEXT` | Python `str` | `utf8` / `large_utf8`, zero-copy into polars, copy into classic pandas |

### Dictionary round-trip caveat

XTDB's query engine returns `utf8` for string columns regardless of how they were ingested.
If you ingest a `dictionary<int8, utf8>` column and query it back, you'll get `utf8`: the data is intact but the encoding is gone.
Re-encode client-side if the downstream computation depends on it:

```python
col = arrow_table.column("severity")
if not pa.types.is_dictionary(col.type):
    arrow_table = arrow_table.set_column(
        arrow_table.schema.get_field_index("severity"),
        "severity",
        col.dictionary_encode(),
    )
```

### Null handling on the pandas ingest path

pandas uses `NaN` for missing float values and `None` for object dtype columns.
`pa.Table.from_pandas()` maps both to Arrow nulls, but only if the column uses a pandas nullable extension type.
Use `pd.StringDtype()`, `pd.Int64Dtype()`, etc. so `from_pandas` can distinguish a real `None` from a missing sentinel:

```python
# Correct: nullable Int64Dtype maps None → Arrow null.
df["count"] = pd.array([1, None, 3], dtype=pd.Int64Dtype())

# Risky: plain int list with None becomes float64 with NaN.
df["count"] = [1, None, 3]
```

## The pgwire comparison

Same round-trip via psycopg for contrast:

```python
import psycopg

with psycopg.connect("postgresql://xtdb@localhost:5432/xtdb") as pg:
    # Row-at-a-time INSERT; one round trip per row.
    pg.autocommit = True
    with pg.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(
                "INSERT INTO events (_id, ts, severity, value) VALUES (%s, %s, %s, %s)",
                (row["_id"], row["ts"], str(row["severity"]), row["value"]),
            )

        # SELECT returns rows decoded one at a time.
        cur.execute("SELECT _id, ts, severity, value, label FROM events ORDER BY ts")
        rows = cur.fetchall()
        # rows is a list of tuples; ts is a datetime, severity is a str,
        # nulls are None, no Arrow anywhere, no zero-copy.
```

The ADBC path:
- Ingests the whole table in a single gRPC call via `adbc_ingest` vs one `INSERT` per row.
- Returns Arrow batches vs decoded Python tuples.
- Preserves `timestamp[us, tz=UTC]` vs widening to `datetime`.
- Keeps nulls as Arrow null rather than Python `None` in an `object` column.

For analytical and bulk workloads, the Arrow path avoids a decode-and-re-encode step on every row.
For interactive queries and existing Postgres tooling, pgwire is still the right answer.

## Summary

- Use `cur.fetch_arrow_table()` to collect query results as a `pyarrow.Table`.
  Call `normalise_tz()` to convert `tz=Z` → `tz=UTC` before pandas/polars conversion.
- Use `.to_pandas(types_mapper=pd.ArrowDtype)` for zero-copy across all column types including strings.
- Use `pl.from_arrow(arrow_table)` for polars: genuinely zero-copy, nulls modelled correctly.
- Use `pa.Table.from_pandas(df, preserve_index=False)` to build an Arrow table for ingest.
  Use nullable extension types (`pd.StringDtype()`, `pd.Int64Dtype()`) to preserve nulls.
- Use `df.to_arrow()` for polars: near-zero-copy.
- Use `cur.adbc_ingest(table, arrow_table, mode="create_append")` for bulk Arrow ingest over FlightSQL: one round trip, no row shredding.
  Available on `nightly` and stable images from 2.2.0 onwards.
