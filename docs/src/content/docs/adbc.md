---
title: 'ADBC: Arrow-native client access'
description: Connect to XTDB via Arrow Database Connectivity, in-process or over FlightSQL.
---

XTDB exposes [ADBC](https://arrow.apache.org/adbc/), the Arrow Database Connectivity standard.
If your pipeline is Arrow-shaped throughout (pandas, polars, DuckDB, DataFusion, `arrow-rs`, pyarrow), ADBC fits straight in: query results land in your client as Arrow batches with no row-by-row decode, and bulk-ingesting an Arrow table happens in a single round trip.

XTDB's storage layer, query engine, and wire format are all Arrow-native, and ADBC carries that through to the client.

## Where to next

- **[Tutorial](/adbc/tutorial)**: an end-to-end walkthrough covering install, connect, ingest, query, transactions, and prepared statements.
- **[Reference](/adbc/reference)**: the supported ADBC surface in detail.
  Which calls work, which don't, and the per-client caveats (Python vs Rust vs Java).
- **[How-to guides](/adbc/guides)**: task-shaped recipes.
  "Bulk-load Parquet via ADBC", "round-trip a pandas DataFrame", "stream results into DuckDB".
- **[Examples](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples)**: minimal runnable hello-world programs in each supported language.

## ADBC vs pgwire

XTDB also exposes a [PostgreSQL wire-compatible server](/drivers).
Use whichever fits your stack:

| | pgwire | ADBC (FlightSQL) |
|---|---|---|
| Tooling | psql, JDBC, psycopg, every PG client | ADBC clients (Python, Rust, Go, C, R, Java) |
| Wire format | Postgres text/binary | Arrow |
| Bulk ingest | row-at-a-time `INSERT` / `executemany` | `cur.adbc_ingest(arrow_table)`, one round trip |
| Result format | rows decoded one at a time | Arrow batches streaming straight into your client |
| Type fidelity | Postgres-mapped | Arrow-native (preserves Arrow types) |

Both backends talk to the same XTDB node, so you can pick per workload and mix freely.
