# food-prices

The dataset behind the `food-prices` benchmark: a monthly retail food-price series, fanned out over synthetic regions and baked to Arrow so the benchmark can hand batches straight to ADBC.

## Where it comes from

The source is the WFP Turkey retail food-price series as carried by [jr200-labs/polars-hist-db](https://github.com/jr200-labs/polars-hist-db), whose ingestion pipeline the benchmark ports.
It is checked in there rather than fetched, so `mirror.py` reads it over raw.githubusercontent by default; pass `--source` to point at a local copy instead.

Upstream is small — 4 places, 52 products, 9 units, 75 months from 2013-05 to 2019-12, 7,381 rows.
That is the right *shape* for the workload and nowhere near the right *size*, so `--size` fans it out across synthetic regions:

| Size | Places | Rows | On disk |
| --- | --- | --- | --- |
| `tiny` | 4 | 7,381 | 270 KB |
| `small` | 100 | 260,149 | 8 MB |
| `med` | 1,000 | ~2.6M | ~80 MB |
| `big` | 5,000 | ~13M | ~400 MB |

Regions 0-3 are the real places carrying their real prices, so `tiny` reproduces the upstream series exactly and is what the tests run against.
The rest are deterministic perturbations of the national average — seeded off a hash rather than a PRNG, so a re-mirror is byte-identical and runs stay comparable across refreshes.

Each synthetic region repeats the previous month's price a fraction of the time (`--repeat-rate`, default 0.3).
That is load-bearing: the benchmark's whole first act is deciding which rows actually changed, and a series where everything moves every month would never exercise it.

## Layout

Per size, four Arrow IPC streams:

`places.arrow`, `products.arrow`, `units.arrow`

: The dimension tables — `id`, `name`. One record batch each.

`prices.arrow`

: The fact table — `place_id`, `product_id`, `um_id`, `month`, `price`, `price_usd`.
  **One record batch per month**, in ascending order, which is what makes a batch a partition: the benchmark reads it with `ArrowStreamReader` and treats each `loadNextBatch` as one restatement to ingest.

`price_usd` is converted with the same hard-coded yearly USD/TRY table the library's `try_to_usd` transform uses, so the numbers line up with theirs.

## Running it

Python is deliberate and one-directional here: it exists to bake the Arrow, and nothing downstream depends on it.
It needs `pyarrow`, which the JVM side does not.

```bash
python -m venv .venv && .venv/bin/pip install pyarrow

.venv/bin/python mirror.py out            # all four sizes
.venv/bin/python mirror.py out --sizes tiny,small

./upload.sh out                           # -> s3://xtdb-datasets/food-prices/
```

Consumers don't run either script: `xtdb.bench.food-prices` fetches what it needs from S3 on its `:download` stage, into `modules/bench/dataset-downloads/food-prices/<size>/`.
