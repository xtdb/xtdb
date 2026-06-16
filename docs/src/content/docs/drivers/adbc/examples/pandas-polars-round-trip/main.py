#!/usr/bin/env python3
"""
Round-trip pandas / polars DataFrames via XTDB ADBC.

Shows type fidelity for:
  - TIMESTAMP WITH TIME ZONE  (timestamp[us, tz=UTC])
  - Nullable / optional fields (Arrow validity bitmap, not NaN)
  - Dictionary / categorical columns (may need client-side re-encode after query)

Prerequisites:
    pip install adbc-driver-flightsql pyarrow pandas polars

XTDB nightly running with FlightSQL on localhost:9832 (adjust URIs as needed):
    docker run --rm -d --name xtdb-g2 \\
        -p 5444:5432 -p 9844:9832 \\
        ghcr.io/xtdb/xtdb:nightly

Then run:
    python main.py                          # defaults
    python main.py grpc://localhost:9844    # custom FlightSQL URI

The script seeds and round-trips entirely over FlightSQL via adbc_ingest
(CommandStatementIngest), available on the nightly image and stable 2.2.0+.
"""

import sys
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import polars as pl
import adbc_driver_flightsql.dbapi as flight_sql

FLIGHT_URI = sys.argv[1] if len(sys.argv) > 1 else "grpc://localhost:9832"

SEP = "─" * 60


def section(title: str) -> None:
    print(f"\n{SEP}\n{title}\n{SEP}")


# ---------------------------------------------------------------------------
# 1. Seed XTDB with typed events via adbc_ingest (bulk Arrow, one round trip)
# ---------------------------------------------------------------------------
section("1. Seeding XTDB with typed events")

EVENTS = [
    ("evt-001", "2024-01-15T08:30:00+00:00", "INFO",  1.23, "startup"),
    ("evt-002", "2024-01-15T09:45:00+00:00", "WARN",  4.56, None),
    ("evt-003", "2024-01-15T11:00:00+00:00", "ERROR", 7.89, "threshold"),
    ("evt-004", "2024-01-15T14:20:00+00:00", "INFO",  2.34, None),
    ("evt-005", "2024-01-16T08:00:00+00:00", "WARN",  9.99, "startup"),
    ("evt-006", "2024-01-16T10:30:00+00:00", "ERROR", 0.01, "manual"),
]

seed_table = pa.table(
    {
        "_id":      pa.array([e[0] for e in EVENTS], type=pa.string()),
        "ts":       pa.array([datetime.fromisoformat(e[1]) for e in EVENTS],
                             type=pa.timestamp("us", tz="UTC")),
        "severity": pa.array([e[2] for e in EVENTS], type=pa.string()),
        "value":    pa.array([e[3] for e in EVENTS], type=pa.float64()),
        "label":    pa.array([e[4] for e in EVENTS], type=pa.string()),
    }
)

with flight_sql.connect(FLIGHT_URI) as conn:
    with conn.cursor() as cur:
        # One gRPC round trip: the whole Arrow table goes over CommandStatementIngest.
        # Requires the nightly image or stable 2.2.0+; on older stable, seed via
        # row-by-row INSERT over pgwire instead.
        cur.adbc_ingest("events", seed_table, mode="create_append")
        print(f"Ingested {len(EVENTS)} rows via adbc_ingest.")

# ---------------------------------------------------------------------------
# 2. XTDB → Arrow via ADBC FlightSQL
# ---------------------------------------------------------------------------
section("2. XTDB → Arrow (ADBC FlightSQL)")

with flight_sql.connect(FLIGHT_URI) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT _id, ts, severity, value, label FROM events ORDER BY ts"
        )
        arrow_result = cur.fetch_arrow_table()

# XTDB FlightSQL returns timestamp[us, tz=Z] ("Z" = Zulu = UTC).
# Normalise to "UTC" so downstream pandas/polars handle it as a standard IANA key.
def normalise_tz(table: pa.Table) -> pa.Table:
    """Cast any timestamp[us, tz=Z] columns to timestamp[us, tz=UTC]."""
    for i, field in enumerate(table.schema):
        t = field.type
        if pa.types.is_timestamp(t) and t.tz == "Z":
            utc_type = pa.timestamp(t.unit, tz="UTC")
            table = table.set_column(i, field.name, table.column(field.name).cast(utc_type))
    return table

arrow_result = normalise_tz(arrow_result)

print(f"Arrow table: {arrow_result.num_rows} rows")
print("Schema:")
for field in arrow_result.schema:
    print(f"  {field.name:10s}  {field.type}  nullable={field.nullable}")

# ---------------------------------------------------------------------------
# 3. Arrow → pandas (two paths)
# ---------------------------------------------------------------------------
section("3. Arrow → pandas")

# Path A: types_mapper=pd.ArrowDtype
# Keeps Arrow-backed columns: zero-copy for numeric and strings both.
df_arrow = arrow_result.to_pandas(types_mapper=pd.ArrowDtype)
print("pandas with types_mapper=pd.ArrowDtype:")
print(df_arrow.dtypes.to_string())
print()
print(df_arrow.to_string())

# Verify timestamp has UTC timezone
ts_arrow_type = arrow_result.schema.field("ts").type
assert pa.types.is_timestamp(ts_arrow_type) and ts_arrow_type.tz is not None, (
    f"Expected timestamp with tz, got {ts_arrow_type}"
)
print(f"\nts Arrow type: {ts_arrow_type}  ✓ (timezone preserved)")

# Verify nulls are proper NA, not NaN
null_count_pd = df_arrow["label"].isna().sum()
print(f"label null count: {null_count_pd}  ✓ (expected 2)")
assert null_count_pd == 2, f"Expected 2 nulls, got {null_count_pd}"

# Path B: classic pandas (no types_mapper), shows widening
df_classic = arrow_result.to_pandas()
print("\npandas classic (no types_mapper), shows type widening:")
print(df_classic.dtypes.to_string())
print("(label becomes object with None; severity becomes object, dict encoding lost)")

# ---------------------------------------------------------------------------
# 4. Arrow → polars (zero-copy path)
# ---------------------------------------------------------------------------
section("4. Arrow → polars")

df_pl = pl.from_arrow(arrow_result)
print("polars schema:")
print(df_pl.schema)
print()
print(df_pl)

# Verify timestamp column has UTC timezone
ts_dtype = df_pl["ts"].dtype
assert isinstance(ts_dtype, pl.Datetime), f"Expected Datetime, got {ts_dtype}"
assert ts_dtype.time_zone == "UTC", f"Expected UTC tz, got {ts_dtype.time_zone}"
print(f"\nts polars type: {ts_dtype}  ✓ (UTC timezone preserved)")

# Verify nulls via polars null_count (not NaN, not None-in-object)
null_count_pl = df_pl["label"].null_count()
print(f"label null count: {null_count_pl}  ✓ (expected 2)")
assert null_count_pl == 2, f"Expected 2 polars nulls, got {null_count_pl}"

assert df_pl.height == 6, f"Expected 6 rows, got {df_pl.height}"
print(f"Total rows: {df_pl.height}  ✓")

# ---------------------------------------------------------------------------
# 5. DataFrame → Arrow (showing the conversion, not the ingest)
# ---------------------------------------------------------------------------
section("5. DataFrame → Arrow (ingest-ready)")

# pandas → Arrow
df_pd_source = pd.DataFrame({
    "_id":      ["evt-007", "evt-008"],
    "ts":       pd.to_datetime(["2024-01-17T08:00:00Z", "2024-01-17T10:00:00Z"],
                               utc=True),
    "severity": pd.Categorical(["INFO", "WARN"], categories=["INFO", "WARN", "ERROR"]),
    "value":    [5.55, 6.66],
    "label":    pd.array([None, "reprocess"], dtype=pd.StringDtype()),
})
arrow_from_pd = pa.Table.from_pandas(df_pd_source, preserve_index=False)
print("pandas → Arrow schema:")
print(arrow_from_pd.schema)
# Verify dictionary type preserved
sev_type = arrow_from_pd.schema.field("severity").type
assert pa.types.is_dictionary(sev_type), f"Expected dictionary, got {sev_type}"
print(f"severity type: {sev_type}  ✓ (categorical preserved as dictionary)")

# polars → Arrow
df_pl_source = pl.DataFrame({
    "_id":      ["evt-009", "evt-010"],
    "ts":       pl.Series(
                    [datetime(2024, 1, 18, 8, 0, tzinfo=timezone.utc),
                     datetime(2024, 1, 18, 10, 0, tzinfo=timezone.utc)],
                    dtype=pl.Datetime("us", "UTC"),
                ),
    "severity": pl.Series(["ERROR", "INFO"], dtype=pl.Categorical),
    "value":    [7.77, 8.88],
    "label":    pl.Series(["alert", None], dtype=pl.Utf8),
})
arrow_from_pl = df_pl_source.to_arrow()
print("\npolars → Arrow schema:")
print(arrow_from_pl.schema)
sev_pl_type = arrow_from_pl.schema.field("severity").type
assert pa.types.is_dictionary(sev_pl_type), f"Expected dictionary, got {sev_pl_type}"
print(f"severity type: {sev_pl_type}  ✓ (Categorical preserved as dictionary)")

# These Arrow tables ingest the same way as section 1's seed table:
#     cur.adbc_ingest("events", arrow_from_pd, mode="create_append")
#     cur.adbc_ingest("events", arrow_from_pl, mode="create_append")
# Not executed here: the dictionary-typed `severity` column is the subject of
# this section (type preservation), and dictionary ingest support over the wire
# is still firming up: re-encode to utf8 first if you want to round-trip it.
print("pandas + polars Arrow tables built (see comment above for the ingest call).")

# ---------------------------------------------------------------------------
# 6. Type fidelity summary
# ---------------------------------------------------------------------------
section("6. Type fidelity summary")

print("Arrow schema from XTDB query:")
for field in arrow_result.schema:
    print(f"  {field.name:10s}  {str(field.type):35s}  nullable={field.nullable}")

print("""
Round-trip fidelity via ADBC FlightSQL:
  TIMESTAMP WITH TIME ZONE  → timestamp[us, tz=UTC]      (Arrow native, zero-copy)
  Nullable column           → Arrow validity bitmap       (not NaN, not None-in-object)
  DOUBLE / INTEGER          → float64 / int32 Arrow       (zero-copy into numpy / polars)
  VARCHAR / TEXT            → utf8 / large_utf8            (zero-copy polars; copy classic pandas)
  Dictionary / Categorical  → client-side re-encode after query (server may return utf8)
""")

print("All assertions passed ✓")
