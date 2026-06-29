#!/usr/bin/env python3
"""
Point-in-time feature extraction for an ML training set, via XTDB + ADBC.

Scenario: loan-default prediction.
  - labels:   one row per (customer_id, observed_at, did_default)
  - features: customer attributes that drift over time
              (account_balance, credit_score, employment_status)

The job is to build a training matrix where each label sees the features
*as they were known at observed_at*, with no later updates leaking in.

Run:
    docker run --rm -d --name xtdb-pit -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
    pip install adbc-driver-flightsql pyarrow pandas
    python main.py                          # defaults to grpc://localhost:9832
    python main.py grpc://localhost:9842    # override the FlightSQL URL
    docker rm -f xtdb-pit

Override the FlightSQL URL via argv or the XTDB_URI env var.
"""

from __future__ import annotations

import datetime as dt
import os
import sys

import adbc_driver_flightsql.dbapi as flight_sql
import pyarrow as pa


XTDB_URI = (
    sys.argv[1] if len(sys.argv) > 1
    else os.environ.get("XTDB_URI", "grpc://localhost:9832")
)


def seed(cur) -> None:
    """Seed customer features with deliberately-shifting historical values.

    Each feature row carries its own `_valid_from`, so a customer's
    account_balance, credit_score and employment_status form a real timeline
    that evolves over months rather than over wall-clock.

    XTDB's insert-as-upsert keys on `_id`, so a later `_valid_from` for the
    same customer closes off the previous version's validity at that
    boundary: a per-customer step function over valid time.

    `_valid_from` is an ordinary column in the ingested Arrow table, so the
    whole timeline loads in a single `adbc_ingest` call per table.
    """

    # Three customers, three independent timelines.
    feature_rows = [
        # (customer_id, valid_from, balance, credit_score, employment)
        ("c1", "2024-01-01", 1_500.00, 680, "employed"),
        ("c1", "2024-04-01", 2_100.00, 700, "employed"),
        ("c1", "2024-07-01",   400.00, 640, "employed"),
        ("c1", "2024-10-01",    50.00, 580, "unemployed"),  # post-default drift

        ("c2", "2024-01-01", 8_000.00, 740, "employed"),
        ("c2", "2024-05-01", 9_500.00, 760, "employed"),
        ("c2", "2024-09-01",10_200.00, 770, "employed"),

        ("c3", "2024-01-01",   300.00, 610, "self-employed"),
        ("c3", "2024-03-01",   120.00, 590, "self-employed"),
        ("c3", "2024-06-01",    10.00, 540, "unemployed"),
        ("c3", "2024-11-01", 5_500.00, 700, "employed"),    # recovery, post-label
    ]

    # _valid_from must be a timestamp column (XTDB's valid-time axis is
    # microsecond timestamps, not bare dates).
    def ts(d: str) -> dt.datetime:
        return dt.datetime.fromisoformat(d).replace(tzinfo=dt.timezone.utc)

    features = pa.table({
        "_id":               pa.array([r[0] for r in feature_rows], type=pa.string()),
        "_valid_from":       pa.array([ts(r[1]) for r in feature_rows], type=pa.timestamp("us", tz="UTC")),
        "account_balance":   pa.array([r[2] for r in feature_rows], type=pa.float64()),
        "credit_score":      pa.array([r[3] for r in feature_rows], type=pa.int64()),
        "employment_status": pa.array([r[4] for r in feature_rows], type=pa.string()),
    })
    cur.adbc_ingest("customer_features", features, mode="create_append")

    # Labels: events we want to train on. A label is an *event* (observed at a
    # single instant), so we model its valid-time as the instantaneous chronon
    # [observed_at, observed_at + 1us). That makes observed_at the label's own
    # _valid_from, and the as-of join below a clean period-vs-period one.
    one_us = dt.timedelta(microseconds=1)
    label_rows = [
        # (label_id, customer_id, observed_at, did_default)
        ("L1", "c1", "2024-08-15", True),
        ("L2", "c2", "2024-08-15", False),
        ("L3", "c3", "2024-08-15", True),
    ]
    labels = pa.table({
        "_id":          pa.array([r[0] for r in label_rows], type=pa.string()),
        "customer_id":  pa.array([r[1] for r in label_rows], type=pa.string()),
        "_valid_from":  pa.array([ts(r[2]) for r in label_rows], type=pa.timestamp("us", tz="UTC")),
        "_valid_to":    pa.array([ts(r[2]) + one_us for r in label_rows], type=pa.timestamp("us", tz="UTC")),
        "did_default":  pa.array([r[3] for r in label_rows], type=pa.bool_()),
    })
    cur.adbc_ingest("loan_labels", labels, mode="create_append")


# observed_at is the label's _valid_from (its event chronon), so we project it
# from there.
NAIVE_SQL = """
SELECT l._id            AS label_id,
       l.customer_id,
       l._valid_from    AS observed_at,
       l.did_default,
       f.account_balance,
       f.credit_score,
       f.employment_status
FROM loan_labels AS l
JOIN customer_features AS f
  ON f._id = l.customer_id
ORDER BY l._id
"""

# Bitemporal AS-OF join: both sides are valid-time periods, so the join is a
# clean period-vs-period one. CONTAINS picks the customer_features version
# whose valid-time period contains the label's event chronon. One query
# covering every label, no Python loop.
ASOF_SQL = """
SELECT l._id            AS label_id,
       l.customer_id,
       l._valid_from    AS observed_at,
       l.did_default,
       f.account_balance,
       f.credit_score,
       f.employment_status
FROM loan_labels FOR ALL VALID_TIME AS l
LEFT JOIN customer_features FOR ALL VALID_TIME AS f
  ON f._id = l.customer_id
  AND f._valid_time CONTAINS l._valid_time
ORDER BY l._id
"""


def main() -> None:
    # autocommit=True so the joins read their own writes. adbc_ingest commits
    # atomically on its own, but the dbapi defaults to manual-commit per PEP
    # 249 (the server advertises transaction support, SqlInfo code 8), so
    # autocommit keeps any non-ingest statements visible too.
    with flight_sql.connect(XTDB_URI, autocommit=True) as conn:
        with conn.cursor() as cur:
            seed(cur)

            cur.execute(NAIVE_SQL)
            naive = cur.fetch_arrow_table()

            cur.execute(ASOF_SQL)
            asof = cur.fetch_arrow_table()

    naive_df = naive.to_pandas()
    asof_df  = asof.to_pandas()

    print("=" * 72)
    print("NAIVE join (latest features, regardless of label time): WRONG")
    print("=" * 72)
    print(naive_df.to_string(index=False))
    print()
    print(f"  rows returned: {len(naive_df)} (one per label, "
          f"but pulling each customer's *current* feature row)")
    print()

    print("=" * 72)
    print("BITEMPORAL AS-OF join (features as known at observed_at): CORRECT")
    print("=" * 72)
    print(asof_df.to_string(index=False))
    print()
    print(f"  rows returned: {len(asof_df)} (one per label)")
    print()

    print("=" * 72)
    print("Why these disagree")
    print("=" * 72)
    print(
        "c1 defaulted on 2024-08-15. The naive join uses c1's *current*\n"
        "feature row (balance=$50, score=580, employment=unemployed): a\n"
        "snapshot taken AFTER the default. The model would learn 'people\n"
        "with $50 balances and 580 scores default', which is true but\n"
        "unhelpful: those values are themselves consequences of the default.\n"
        "The AS-OF join pulls the row valid on 2024-08-15 (the July update:\n"
        "balance=$400, score=640, employment=employed), c1's state going\n"
        "into the default, the only thing a real-world model could ever see.\n"
        "Likewise c3's Nov recovery row is invisible to the AS-OF view.\n"
    )

    # Feed straight into sklearn: the dataframe is already model-shaped.
    feature_cols = ["account_balance", "credit_score", "employment_status"]
    X = asof_df[feature_cols]
    y = asof_df["did_default"].astype(int)
    print("Training matrix (X):")
    print(X.to_string(index=False))
    print()
    print("Labels (y):", y.tolist())


if __name__ == "__main__":
    main()
