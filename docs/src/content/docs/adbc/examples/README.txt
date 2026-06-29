XTDB ADBC examples
==================

Runnable companion code for the ADBC docs (/adbc).
One example per directory. Each one round-trips ingest + query against a
local XTDB node, and corresponds to a tutorial or how-to guide page.

This is a .txt (not .md) on purpose: Starlight's content loader globs every
.md/.mdx under src/content/docs/ and turns it into a routable page with a
required `title` frontmatter. A README.md here would break the docs build.

Prereqs
-------

A running XTDB node with the FlightSQL listener on localhost:9832.
The standalone Docker image enables this by default:

    docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly

The Python examples need:

    python -m pip install adbc-driver-flightsql pyarrow

plus extras called out per-example below (pandas, polars, duckdb).

Examples
--------

Hello-world (start here):

  python/                hello-world.py: minimal adbc_driver_flightsql + pyarrow
                         round-trip. The cleanest "Arrow stays Arrow" demo.
                           cd python && python hello-world.py

  rust/                  adbc_core + adbc_driver_manager loading the native
                         FlightSQL driver, + arrow. Compile-time-safe Arrow
                         round-trip. There is no pure-Rust FlightSQL driver, so
                         the first build downloads the native lib (~5 MB), so set
                         ADBC_FLIGHTSQL_VERSION on that build:
                           cd rust && ADBC_FLIGHTSQL_VERSION=1.11.0 cargo run

Tutorial companion:

  tutorial/              main.py: the FX-trades worked example from tutorial.md.
                         Every numbered section in the tutorial maps to a function here.

How-to guide companions (one per /adbc/guides/* page):

  bulk-ingest-from-parquet/        pyarrow.parquet.read_table -> cur.adbc_ingest;
                                   schema (_id) requirements, chunked ingest, rejection modes.

  pandas-polars-round-trip/        DataFrame in / DataFrame out; type fidelity for
                                   tz-timestamps, nullable fields, categoricals.
                                   Needs: pandas polars

  point-in-time-feature-extraction/  Bitemporal x Arrow: extract ML training features
                                   "as known at label time" via FOR VALID_TIME AS OF.

  streaming-into-duckdb/           cur.fetch_arrow_table() -> DuckDB register_arrow;
                                   XTDB as bitemporal source-of-truth, DuckDB as compute.
                                   Needs: duckdb

  prepared-statements-and-transactions/  prepare/bind/execute lifecycle, Arrow batch
                                   parameter binding, transactions and rollback.

  rust-arrow-pipeline/             Rust end-to-end with adbc_core + arrow-rs: ingest a
                                   RecordBatch, query, hand off to DataFusion, write Parquet.
                                   Compile-time guarantees through the whole pipeline.
                                     cd rust-arrow-pipeline && cargo run

Not yet shipped
---------------

Standalone C / R / Go / Kotlin-in-process examples are candidates for a future
round, one example per language, each focused enough to actually run-and-verify.
The in-process Kotlin/JVM path is already documented inline in reference.md.
