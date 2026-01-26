# Fusion Benchmark

A benchmark based on actual usage patterns to catch regressions and enable optimization.

## Workload Modes

### OLTP Mode (default)

Runs a mixed workload with interleaved reads and writes using a thread pool.
This catches concurrency-related issues that staged benchmarks miss.

Transaction weights:
- **Writes (30%)**: insert-readings (20%), update-system (10%)
- **Reads (70%)**: query-system-settings (25%), query-readings (20%),
  query-system-count (10%), query-range-bins (10%), query-registration (5%)

### Staged Mode (`--staged-only`)

Runs each phase sequentially: load data, then run each query type once.
Useful for isolated query performance measurement.

## Data Model (11 tables)

### Core tables

| Table | Description | Row count |
|-------|-------------|-----------|
| organisation | Manufacturers | 5 |
| device_series | Device series per org | 25 (5 per org) |
| device_model | Reference data | 50 (2 per series) |
| site | Location data | ~= system count |
| system | Main table, constantly updated | configurable (default 10k) |
| device | Links systems to device models | 2x system count |
| readings | High volume time-series (system_id, value, valid_time) | system_count x readings |

### Registration test tables

| Table | Description | Row count |
|-------|-------------|-----------|
| test_suite | Test suite definitions | 1 |
| test_case | Test cases within suites | 5 per suite |
| test_suite_run | Suite executions per system | 1 per system (80% pass rate) |
| test_case_run | Case execution results | 5 per suite run |

## Key Production Pathologies Captured

- Concurrent reads during writes (OLTP interleaving)
- Constant small-batch UPDATEs to frequently-queried table (system)
- Large-batch INSERT workload (readings)
- Temporal scatter: data arriving with variable system-time lag
- Complex multi-CTE queries with window functions
- range_bins temporal aggregation patterns
- Multi-table temporal joins with CONTAINS predicates

## Query Suite

See [fusion.sql](../resources/xtdb/bench/fusion.sql) for query definitions.

| Query | Description |
|-------|-------------|
| system-settings | Point-in-time system lookup |
| readings-for-system | Time-series scan with temporal join |
| system-count-over-time | Complex 4-table temporal aggregation |
| readings-range-bins | Hourly aggregation using range_bins() |
| cumulative-registration | Multi-CTE query with window functions + complex temporal joins |

## Usage

Default OLTP mode (30s duration, 4 threads):
```bash
./gradlew fusion
```

Custom OLTP settings:
```bash
./gradlew fusion -Pduration=PT2M -Pthreads=8
```

Staged mode (sequential, no interleaving):
```bash
./gradlew fusion -PstagedOnly
```

Custom scale:
```bash
./gradlew fusion -PdeviceCount=1000 -PreadingCount=1000
```
