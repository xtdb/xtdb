# Fusion Benchmark

A benchmark based on actual usage patterns to catch regressions and enable optimization.

## Data Model (11 tables)

### Core tables

| Table | Description | Row count |
|-------|-------------|-----------|
| organisation | Manufacturers | 5 |
| device_series | Device series per org | 25 (5 per org) |
| device_model | Reference data | 50 (2 per series) |
| site | Location data | ~= system count |
| system | Main table, constantly updated | configurable (default 10k) |
| device | Links systems to device models | 2× system count |
| readings | High volume time-series (system_id, value, valid_time) | system_count × readings |

### Registration test tables

| Table | Description | Row count |
|-------|-------------|-----------|
| test_suite | Test suite definitions | 1 |
| test_case | Test cases within suites | 5 per suite |
| test_suite_run | Suite executions per system | 1 per system (80% pass rate) |
| test_case_run | Case execution results | 5 per suite run |

## Workload Characteristics

### Readings ingestion

- Batch size: ~1000
- Insert frequency: every 5min of valid_time
- System-time lag:
  - 80%: 0-2ms delay (near real-time)
  - 20%: 0-50ms delay (delayed batch processing)

### System updates

- Small-batch UPDATEs: batch size 30
- Simulates constant trickle of system state changes (~every 5min)
- Update frequency: configurable rounds (default 10)

## Query Suite

| Query | Description |
|-------|-------------|
| system-settings | Point-in-time system lookup |
| readings-for-system | Time-series scan with temporal join |
| system-count-over-time | Complex 4-table temporal aggregation |
| readings-range-bins | Hourly aggregation using range_bins() |
| cumulative-registration | Multi-CTE query with window functions + complex temporal joins |

See [fusion.sql](../resources/xtdb/bench/fusion.sql) for query definitions.

## Key Production Pathologies Captured

- Constant small-batch UPDATEs to frequently-queried table (system)
- Large-batch INSERT workload (readings)
- Temporal scatter: data arriving with variable system-time lag
- Complex multi-CTE queries with window functions
- range_bins temporal aggregation patterns
- Multi-table temporal joins with CONTAINS predicates

## Usage

Default (10k systems × 1k readings):
```bash
./gradlew fusion
```

Custom scale:
```bash
./gradlew fusion -PdeviceCount=1000 -PreadingCount=1000
```
