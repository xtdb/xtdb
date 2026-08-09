#!/usr/bin/env python3
"""Produces the food-prices dataset for S3: the WFP Turkey retail food-price series
(as carried by jr200-labs/polars-hist-db) fanned out across synthetic regions and
written as Arrow IPC, one record batch per monthly partition.

Run occasionally, when refreshing the dataset. `mirror.py` fetches the upstream CSV
and writes `<size>/{places,products,units,prices}.arrow` into an out-dir; `upload.sh`
then syncs that dir to s3://xtdb-datasets/food-prices/. The benchmark pulls it back
down itself (see xtdb.bench.food-prices) — no CSV parsing, no price arithmetic and
no currency conversion on the load path, just Arrow batches straight into ADBC.

Python is deliberate and one-directional: it exists to bake the Arrow, and nothing
downstream depends on it. It needs pyarrow, which the JVM side does not.

The upstream series is small — 4 places, 52 products, 9 units, 75 months from 2013-05
to 2019-12 — so `--size` fans it out over synthetic regions to reach benchmark scale.
Region 0-3 carry the real prices verbatim; the rest are deterministic perturbations of
the national average, and each repeats the previous month's price with probability
`--repeat-rate` so that delta detection has genuine unchanged rows to discard.
"""

import argparse
import csv
import hashlib
import io
import urllib.request
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

UPSTREAM_CSV = (
    "https://raw.githubusercontent.com/jr200-labs/polars-hist-db/master/"
    "tests/_testdata_dataset_data/turkey_food_prices.csv"
)

# yearly USD/TRY, as used by polars-hist-db's `try_to_usd` transform
FX_USDTRY = {
    2010: 1.507, 2011: 1.674, 2012: 1.802, 2013: 1.915, 2014: 2.188,
    2015: 2.724, 2016: 3.020, 2017: 3.646, 2018: 4.830, 2019: 5.680,
    2020: 7.004, 2021: 8.886, 2022: 16.566, 2023: 23.085,
}

SIZES = {"tiny": 4, "small": 100, "med": 1000, "big": 5000}

PRICES_SCHEMA = pa.schema([
    pa.field("place_id", pa.int32(), nullable=False),
    pa.field("product_id", pa.int32(), nullable=False),
    pa.field("um_id", pa.int32(), nullable=False),
    pa.field("month", pa.date32(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("price_usd", pa.float64(), nullable=False),
])


def read_csv(source):
    """Reads the upstream CSV into dict rows. Product names embed commas
    (`"Bulgur (wheat, dry) - Retail"`), so the quoting is load-bearing."""
    if source.startswith("http"):
        with urllib.request.urlopen(source) as resp:
            text = resp.read().decode("utf-8")
    else:
        text = Path(source).read_text(encoding="utf-8")

    return list(csv.DictReader(io.StringIO(text)))


def jitter(seed_parts):
    """A stable multiplier in [0.75, 1.25) derived from the given parts.

    Seeded off a hash rather than `random` so a re-mirror produces byte-identical
    output, which is what makes benchmark runs comparable across refreshes."""
    digest = hashlib.sha256("|".join(str(p) for p in seed_parts).encode()).digest()
    return 0.75 + (int.from_bytes(digest[:8], "big") / 2**64) * 0.5


def curate(rows):
    """Splits the raw CSV rows into dimensions plus a month -> real-price series.

    Returns (places, products, units, series), where `series` maps
    (place_name, product_id, um_id, month) -> price."""
    places, products, units = {}, {}, {}
    series = {}

    for row in rows:
        product_id, um_id = int(row["ProductId"]), int(row["UmId"])
        products[product_id] = row["ProductName"]
        units[um_id] = row["UmName"]
        places.setdefault(row["Place"], len(places))
        month = date(int(row["Year"]), int(row["Month"]), 1)
        series[(row["Place"], product_id, um_id, month)] = float(row["Price"])

    return places, products, units, series


def write_table(out_dir, name, table):
    """Writes a dimension table as a one-batch IPC stream — same format as prices.arrow,
    so the benchmark reads every file through one code path."""
    path = out_dir / f"{name}.arrow"
    with ipc.new_stream(path, table.schema) as writer:
        writer.write_table(table)
    return path


def mirror(out_dir, size, rows, repeat_rate):
    place_count = SIZES[size]
    places, products, units, series = curate(rows)
    real_places = list(places)
    months = sorted({month for _, _, _, month in series})
    pairs = sorted({(pid, uid) for _, pid, uid, _ in series})

    out_dir = out_dir / size
    out_dir.mkdir(parents=True, exist_ok=True)

    place_names = [
        real_places[i] if i < len(real_places) else f"Region {i - len(real_places) + 1}"
        for i in range(place_count)
    ]
    write_table(out_dir, "places", pa.table(
        {"id": pa.array(range(place_count), pa.int32()),
         "name": pa.array(place_names, pa.string())},
        schema=pa.schema([pa.field("id", pa.int32(), nullable=False),
                          pa.field("name", pa.string(), nullable=False)])))

    write_table(out_dir, "products", pa.table(
        {"id": pa.array(sorted(products), pa.int32()),
         "name": pa.array([products[k] for k in sorted(products)], pa.string())},
        schema=pa.schema([pa.field("id", pa.int32(), nullable=False),
                          pa.field("name", pa.string(), nullable=False)])))

    write_table(out_dir, "units", pa.table(
        {"id": pa.array(sorted(units), pa.int32()),
         "name": pa.array([units[k] for k in sorted(units)], pa.string())},
        schema=pa.schema([pa.field("id", pa.int32(), nullable=False),
                          pa.field("name", pa.string(), nullable=False)])))

    # last emitted price per (place, product, unit), so a "repeat" month can restate it
    # verbatim and the benchmark's delta filter has something to discard
    previous = {}
    total = 0

    with ipc.new_stream(out_dir / "prices.arrow", PRICES_SCHEMA) as writer:
        for month in months:
            place_ids, product_ids, um_ids, prices, usd_prices = [], [], [], [], []
            fx = FX_USDTRY[month.year]

            for place_id in range(place_count):
                for product_id, um_id in pairs:
                    key = (place_id, product_id, um_id)
                    real = series.get((place_names[place_id], product_id, um_id, month))

                    synthetic = place_id >= len(real_places)

                    if synthetic:
                        national = series.get(("National Average", product_id, um_id, month))
                        price = national and national * jitter((place_id, product_id, um_id))
                    else:
                        price = real

                    if price is None:
                        continue

                    # real places stay verbatim, so `tiny` reproduces the upstream series
                    if synthetic and key in previous and jitter((key, month)) < 0.75 + repeat_rate * 0.5:
                        price = previous[key]

                    price = round(price, 4)
                    previous[key] = price

                    place_ids.append(place_id)
                    product_ids.append(product_id)
                    um_ids.append(um_id)
                    prices.append(price)
                    usd_prices.append(round(price / fx, 4))

            batch = pa.record_batch(
                [pa.array(place_ids, pa.int32()), pa.array(product_ids, pa.int32()),
                 pa.array(um_ids, pa.int32()), pa.array([month] * len(prices), pa.date32()),
                 pa.array(prices, pa.float64()), pa.array(usd_prices, pa.float64())],
                schema=PRICES_SCHEMA)
            writer.write_batch(batch)
            total += batch.num_rows

    print(f"{size}: {place_count} places x {len(pairs)} product/unit pairs "
          f"x {len(months)} months -> {total} rows in {out_dir}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out_dir", type=Path)
    parser.add_argument("--source", default=UPSTREAM_CSV,
                        help="upstream CSV URL or local path")
    parser.add_argument("--sizes", default=",".join(SIZES),
                        help=f"comma-separated subset of {','.join(SIZES)}")
    parser.add_argument("--repeat-rate", type=float, default=0.3,
                        help="fraction of restatements that repeat the previous price")
    args = parser.parse_args()

    rows = read_csv(args.source)
    print(f"read {len(rows)} rows from {args.source}")

    for size in args.sizes.split(","):
        mirror(args.out_dir, size.strip(), rows, args.repeat_rate)


if __name__ == "__main__":
    main()
