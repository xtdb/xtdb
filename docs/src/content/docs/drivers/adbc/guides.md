---
title: ADBC how-to guides
description: Task-shaped recipes for working with XTDB via ADBC.
---

Each guide solves one specific task.
For a continuous walkthrough see the [tutorial](./tutorial); for the full supported surface see the [reference](./reference).

## Guides

### [Bulk-loading Parquet into XTDB via ADBC](./guides/bulk-ingest-from-parquet)

`pyarrow.parquet.read_table` → `cur.adbc_ingest`.
Schema requirements (the `_id` column), chunked ingest for tables larger than memory, error handling, and rejection modes.

### [Round-tripping pandas / polars DataFrames](./guides/pandas-polars-round-trip)

Two directions:

- Read XTDB into a `pyarrow.Table` and hand off to pandas / polars via `to_pandas()` / `pl.from_arrow()`, zero-copy where possible.
- Build a DataFrame in pandas / polars, convert to Arrow, `adbc_ingest` it.

Covers type-fidelity preservation (categoricals, optional fields, timestamps with timezone).

### [Point-in-time feature extraction for ML training sets](./guides/point-in-time-feature-extraction)

Extract training-set features as known at label time: a single SQL query against `FOR VALID_TIME AS OF` produces a `pyarrow.Table` joined as-of label time, ready for a feature pipeline.
It avoids training on information that wasn't yet available at the label time, the mistake feature stores are built to prevent.

### [Streaming results into DuckDB](./guides/streaming-into-duckdb)

`cur.fetch_arrow_table()` → DuckDB `register_arrow` → join with DuckDB-resident data.
XTDB holds the bitemporal source of truth, DuckDB runs the ad-hoc analytics, and Arrow moves between them with no serialise/deserialise step.

### [Rust ADBC + arrow-rs pipeline](./guides/rust-arrow-pipeline)

End-to-end in Rust with `adbc_core` + `arrow-rs`: connect, ingest a `RecordBatch`, query, hand off to DataFusion for analytical compute, write the result out to Parquet.
The Arrow types are checked at compile time through the whole pipeline.

### [Prepared statements and transactions](./guides/prepared-statements-and-transactions)

The full prepared-statement lifecycle over the wire: `prepare()` once, `bind()` + `executeQuery()` many times, parameter binding via Arrow batches.
Multi-statement transactions, autocommit on/off, rollback semantics, and how XTDB's same-connection write-then-read works in practice.
