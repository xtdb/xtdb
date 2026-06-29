---
title: Point-in-time feature extraction for ML training sets
description: Build leakage-free training matrices with one bitemporal SQL query.
---

Training models on data that changes over time runs into the as-of-join problem.
You label an event at time `T`, then join feature rows on the entity key.
A naive join pulls each entity's *current* feature values, including everything that happened after `T`.
The model trains on information from after the label time, scores well offline, and then degrades in production.

Much of what feature stores (Feast, Tecton, Hopsworks) do is to perform this join correctly.
In XTDB the join is one SQL query, because every row already carries its valid-time interval.
`adbc_driver_flightsql` returns the result as Arrow, and `pyarrow.Table.to_pandas()` gives you a model-ready dataframe.

The runnable example for this guide is in [`docs/.../examples/point-in-time-feature-extraction/main.py`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/point-in-time-feature-extraction/main.py).

## The scenario

Loan-default prediction.

**Labels.**
One row per event we want to train on: `(customer_id, observed_at, did_default)`.
A label is an *event*, observed at a single instant, not a state that holds over a span.
So we model its valid-time as the instantaneous chronon `[observed_at, observed_at + 1µs)`: `observed_at` becomes the label's own `_valid_from`, and the as-of join below is a clean period-vs-period one.

**Features.**
Per-customer attributes that drift: `account_balance`, `credit_score`, `employment_status`.
The truth about a customer on 2024-08-15 is different from the truth about that customer on 2024-11-01, and the model only ever gets to see the former.

We seed three customers with deliberately-shifting histories.
`c1` starts the year solvent, drifts down through summer, and bottoms out post-default in October.
`c3` defaults, then recovers in November.
A naive join would feed *both* of those post-default snapshots into training rows for a default that happened in August.

## Setup

Start a node with the FlightSQL listener:

```bash
docker run --rm -d --name xtdb -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
pip install adbc-driver-flightsql pyarrow pandas
```

Connect:

```python
import adbc_driver_flightsql.dbapi as flight_sql

# autocommit=True so each seed INSERT commits and the joins below read their
# own writes. The dbapi defaults to manual-commit (PEP 249) now that the
# server advertises transaction support, so without this the uncommitted
# seed rows would be invisible to subsequent queries on the same connection.
conn = flight_sql.connect("grpc://localhost:9832", autocommit=True)
```

## Seeding features as a temporal timeline

Every XTDB row carries a valid-time interval, exposed as `_valid_from` / `_valid_to`.
Setting `_valid_from` on `INSERT` records *when each version became true*, not when the database happened to learn it.

```python
cur.execute("""
    INSERT INTO customer_features
      (_id, _valid_from, account_balance, credit_score, employment_status)
    VALUES (?, ?, ?, ?, ?)
""", parameters=["c1", date(2024, 1, 1), 1500.00, 680, "employed"])
```

Re-inserting `_id = "c1"` with a later `_valid_from` doesn't overwrite the prior version: it closes off the previous interval and starts a new one.
After ingesting the seed, `c1`'s history is a step function over valid time:

```
c1: [2024-01-01 .. 2024-04-01)  balance=1500  score=680  employed
    [2024-04-01 .. 2024-07-01)  balance=2100  score=700  employed
    [2024-07-01 .. 2024-10-01)  balance=400   score=640  employed
    [2024-10-01 .. ∞         )  balance=50    score=580  unemployed
```

No audit table, no `effective_from` column, no triggers, no manual interval bookkeeping.

## The naive join: wrong

This is what most pipelines start with:

```sql
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
```

Without a temporal qualifier, XTDB returns each customer's *current* row, so this query gives every label the customer's feature values *as they are today*, regardless of when the label was observed.

For our seed:

```
label_id customer_id observed_at  did_default  account_balance  credit_score employment_status
      L1          c1  2024-08-15         True             50.0           580        unemployed
      L2          c2  2024-08-15        False          10200.0           770          employed
      L3          c3  2024-08-15         True           5500.0           700          employed
```

Look at `L1`.
The label says c1 defaulted on 2024-08-15.
The feature row says c1 has $50 in the bank and is unemployed.
Those numbers describe c1 *after* the default has already destroyed their finances.
The model "learns" that low balance + unemployed predicts default: true but tautological.
Look at `L3`.
The features for c3 show $5,500 and a 700 credit score: c3's *recovery* a quarter after the default we're trying to predict.
Train on this and the model learns the aftermath of a default, not the signals that precede one, so it has nothing useful to go on in production.

## The bitemporal AS-OF join: right

The fix is one SQL clause: `FOR ALL VALID_TIME` on the features table, joined against the label's valid-time period:

```sql
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
```

`FOR ALL VALID_TIME` opens up every historical version of `customer_features`.
Both sides are valid-time periods, so the join is period-against-period: `f._valid_time CONTAINS l._valid_time` picks the one feature version per customer whose period contains the label's event chronon, without spelling out the interval bounds by hand.
(`observed_at` is the label's `_valid_from`, so we project it from there.)

Same data, run again:

```
label_id customer_id observed_at  did_default  account_balance  credit_score employment_status
      L1          c1  2024-08-15         True            400.0           640          employed
      L2          c2  2024-08-15        False           9500.0           760          employed
      L3          c3  2024-08-15         True             10.0           540        unemployed
```

c1's August features are now $400 / 640 / employed: the state the customer was actually in before the default.
c3's features are $10 / 540 / unemployed: the dire state preceding the default, not the post-recovery snapshot.

This is one query covering every label, with no Python loop or per-row round trip.
XTDB applies the `FOR VALID_TIME AS OF` bound during the table scan, so each row is read at its as-of-label-time version directly.

### Why this is hard without bitemporal storage

The standard alternative is event sourcing: store every change to a customer as an immutable event with a timestamp, then for each label scan the event log up to `observed_at` and fold the latest state per attribute.
You'll either write that fold in application code (slow, hard to vectorise), bake it into a Spark/Flink job (complex, brittle, your own join-engine to debug), or rent a feature store (Feast/Tecton/Hopsworks) whose primary value-add is doing exactly this.

Each of those routes rebuilds part of what a bitemporal database already does.
XTDB is one, with the temporal logic built in and queried through standard SQL:2011 syntax.

## Handing the result to sklearn

`fetch_arrow_table()` gives a `pyarrow.Table`; `to_pandas()` gives the dataframe sklearn wants:

```python
cur.execute(ASOF_SQL)
features_df = cur.fetch_arrow_table().to_pandas()

X = features_df[["account_balance", "credit_score", "employment_status"]]
y = features_df["did_default"].astype(int)
```

The Arrow path is zero-copy for numeric columns and one allocation for strings.
For a one-million-label training set the conversion stays in seconds, not minutes.

If you'd rather skip pandas:

```python
import polars as pl
features_df = pl.from_arrow(cur.fetch_arrow_table())
```

polars consumes the Arrow batches directly with no intermediate copy.

## Variations

**Per-label valid-time.** Labels themselves don't have to live in XTDB.
Land them as an in-memory `pyarrow.Table` and ingest, or `INSERT` them directly; the AS-OF join works the same.

**Multi-table join.** Multiple feature tables, each with their own timeline:

```sql
SELECT l._id, l.customer_id, l._valid_from AS observed_at, l.did_default,
       cf.credit_score,
       acc.account_balance,
       emp.status AS employment_status
FROM loan_labels FOR ALL VALID_TIME AS l
LEFT JOIN credit_scores      FOR ALL VALID_TIME AS cf  ON cf._id  = l.customer_id AND cf._valid_time  CONTAINS l._valid_time
LEFT JOIN accounts           FOR ALL VALID_TIME AS acc ON acc._id = l.customer_id AND acc._valid_time CONTAINS l._valid_time
LEFT JOIN employment_history FOR ALL VALID_TIME AS emp ON emp._id = l.customer_id AND emp._valid_time CONTAINS l._valid_time
```

Each feature table is joined at its own correct point in time.
You can collapse the repetition with a SQL view; the optimiser still pushes each temporal predicate into its respective scan.

**Reproducible re-runs.** Pin the whole query to a system-time basis:

```sql
SETTING DEFAULT SYSTEM_TIME AS OF TIMESTAMP '2024-12-01T00:00:00Z'
SELECT ...
```

Re-run the training-set extraction six months later, get bit-identical output even if features were corrected in the meantime.
The `FOR VALID_TIME` clause picks the right version *in the timeline*; `SETTING DEFAULT SYSTEM_TIME` picks the right *timeline*.
This is the bitemporal pair; see [Time in XTDB](/about/time-in-xtdb) for the underlying model.

## Caveats

The example script seeds each table in a single `adbc_ingest` call, including a `_valid_from` column so every row sets its own valid-time and the customers build a real timeline.
`_valid_from` must be a `timestamp` column in the Arrow table, not a `date`.
For bulk loads of labels or feature timelines from Parquet or pandas, see [Bulk-load Parquet into XTDB via ADBC](/adbc/guides/bulk-ingest-from-parquet).

Training-set extraction is a single read query, so transactions don't enter into it.
But if you combine ingestion and querying on the same connection, see [Transactions](/adbc/reference#transactions) in the reference.
