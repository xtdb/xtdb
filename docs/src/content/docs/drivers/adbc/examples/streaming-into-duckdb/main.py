#!/usr/bin/env python3
"""
XTDB as bitemporal source-of-truth + DuckDB as compute.

Scenario: as-of end-of-Q4 2024 trade snapshot from XTDB, joined against
a counterparty-tier lookup table in DuckDB, aggregated by tier.

Arrow is the substrate end-to-end: XTDB produces Arrow batches via ADBC
FlightSQL; DuckDB receives them directly with no serialise/deserialise step.

Requires:
    docker run --rm -d --name xtdb-duckdb-demo \\
        -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
    pip install adbc-driver-flightsql pyarrow duckdb psycopg

Run:
    python main.py
    python main.py grpc://localhost:9832 postgresql://localhost:5432/xtdb
"""

import sys
from textwrap import dedent

import duckdb
import pyarrow as pa
import psycopg
import adbc_driver_flightsql.dbapi as flight_sql

# ── connection defaults (override via argv) ───────────────────────────────────
FSQL_URI = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:9832"
PG_URI   = sys.argv[2] if len(sys.argv) > 2 else "postgresql://localhost:5432/xtdb"

# ── 1. Seed XTDB with bitemporal trade data ───────────────────────────────────
#
# Trades are written via the pgwire path, the normal production write path.
# _valid_from / _valid_to columns tell XTDB *when each record is true* on
# the valid-time axis.  CommerzBank's T005 expires 2024-06-30 and will
# therefore be absent from the Q4 snapshot even though it was ingested.

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

print(f"Seeding XTDB via pgwire ({PG_URI}) …")
with psycopg.connect(PG_URI, user="xtdb", autocommit=True) as pg:
    with pg.cursor() as cur:
        cur.execute(SEED_SQL)
print("  Seeded 7 trade records with explicit valid-time ranges.\n")

# ── 2. Pull the Q4 2024 end-of-quarter snapshot via ADBC ─────────────────────
#
# FOR VALID_TIME AS OF pins the query to a single instant in valid-time.
# XTDB returns only rows whose valid-time period contains that instant:
# T001 to T004 and T006 to T007 are active at 2024-12-31 23:59:59;
# T005 (CommerzBank, valid_to = 2024-06-30) is excluded.

SNAPSHOT_SQL = """\
    SELECT _id, counterparty, notional, currency
      FROM trades
           FOR VALID_TIME AS OF TIMESTAMP '2024-12-31 23:59:59'
     ORDER BY _id
"""

print(f"Querying XTDB via ADBC FlightSQL ({FSQL_URI}) …")
with flight_sql.connect(FSQL_URI) as conn:
    with conn.cursor() as cur:
        cur.execute(SNAPSHOT_SQL)
        snapshot: pa.Table = cur.fetch_arrow_table()   # Arrow, no row-by-row decode

print(f"Q4 2024 snapshot ({len(snapshot)} rows; T005/CommerzBank excluded by valid-time):")
for row in snapshot.to_pylist():
    print(f"  {row['_id']}  {row['counterparty']:<15}  {row['currency']}  {row['notional']:>12,.0f}")

# ── 3. Register the Arrow snapshot into DuckDB ────────────────────────────────
#
# duckdb.register() accepts any object that exposes the Arrow C Data Interface:
# a pyarrow.Table, RecordBatch, polars DataFrame, etc.
# DuckDB operates directly on the Arrow buffers XTDB produced; no copy, no decode.

duck = duckdb.connect()          # in-memory analytical engine
duck.register("xtdb_trades", snapshot)

# ── 4. Create the counterparty-tier dimension in DuckDB ──────────────────────
#
# Enrichment data lives in DuckDB: could be a Parquet file, a CSV,
# or (here) an inline relation.  The point is that it never touches XTDB.

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

# ── 5. Join + aggregate in DuckDB ─────────────────────────────────────────────
#
# DuckDB's vectorised engine runs the analytical query over the Arrow buffers
# that XTDB produced.  The bitemporal snapshot and the DuckDB dimension table
# join inside DuckDB's execution engine, with no round-trip to XTDB,
# no re-serialisation, no intermediate CSV.

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

print("\nExposure by counterparty tier, as-of Q4 2024 end:")
print(f"  {'Tier':<10}  {'Trades':>6}  {'Total Notional':>16}  {'Avg Notional':>14}")
print(f"  {'-'*10}  {'-'*6}  {'-'*16}  {'-'*14}")
for row in result.to_pylist():
    print(
        f"  {row['tier']:<10}  {row['trade_count']:>6}  "
        f"{row['total_notional']:>16,.0f}  {row['avg_notional']:>14,.0f}"
    )

print(
    "\nDone: XTDB produced Arrow batches via ADBC FlightSQL and DuckDB\n"
    "consumed them directly, with no serialise/deserialise step."
)
