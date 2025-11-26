# Benchmark Metrics (Azure) — Terraform

This Terraform module provisions Azure resources for benchmark metrics alerting and visualization.

## Resources

- **Logic App (alert relay)**: Relays Azure Monitor alerts to Slack via webhook
- **Action Group**: Routes Azure Monitor alerts to the Logic App
- **Logic App (anomaly detection)**: Scheduled anomaly detection for each benchmark using Logs Query API (managed identity, `for_each` over benchmarks)
- **Azure Portal Dashboard**: Time-series charts for recent benchmark runs (TPC-H, Yakbench, Readings, AuctionMark)
- **Role Assignments**: Log Analytics Reader for anomaly detection Logic Apps

## Supported Benchmarks

The module supports anomaly detection and dashboard visualization for:

| Benchmark | Filter Parameter | Metric | Default Value |
|-----------|------------------|--------|---------------|
| `tpch` | `scale-factor` | `time-taken-ms` | 1.0 |
| `yakbench` | `scale-factor` | `time-taken-ms` | 1.0 |
| `readings` | `devices` | `time-taken-ms` | 10000 |
| `auctionmark` | `duration` | `throughput` | PT30M (disabled) |

## Configuration

### Benchmark-specific variables

Each benchmark has its own set of variables:

**TPC-H:**
- `tpch_anomaly_logic_app_name` — Logic App name
- `tpch_anomaly_alert_enabled` — enable/disable
- `tpch_anomaly_scale_factor` — scale factor to filter on

**Yakbench:**
- `yakbench_anomaly_logic_app_name` — Logic App name
- `yakbench_anomaly_alert_enabled` — enable/disable
- `yakbench_anomaly_scale_factor` — scale factor to filter on

**Readings:**
- `readings_anomaly_logic_app_name` — Logic App name
- `readings_anomaly_alert_enabled` — enable/disable
- `readings_anomaly_devices` — device count to filter on

**AuctionMark:**
- `auctionmark_anomaly_logic_app_name` — Logic App name
- `auctionmark_anomaly_alert_enabled` — enable/disable (disabled by default)
- `auctionmark_anomaly_duration` — ISO 8601 duration to filter on (e.g., `PT30M`)

### Shared anomaly detection variables

- `anomaly_repo` — repository (owner/name) for run links, e.g. `xtdb/xtdb`
- `anomaly_schedule_frequency` — `Minute|Hour|Day|Week|Month`
- `anomaly_schedule_interval` — integer interval for the above frequency
- `anomaly_schedule_hour` — hour of day to run (0-23)
- `anomaly_schedule_timezone` — timezone for schedule
- `anomaly_timespan` — ISO8601 timespan for the Logs Query API (e.g., `P30D`)
- `anomaly_baseline_n` — number of prior runs to compute baseline
- `anomaly_sigma` — sigma multiplier for thresholding (e.g., `2` for ±2σ)
- `anomaly_new_normal_relative_threshold` — threshold for suppressing "new normal" alerts

### Notifications

- `action_group_name`, `action_group_short_name` — Action Group identity
- `slack_webhook_url` — Slack Incoming Webhook (provide via environment, not committed):
  ```bash
  export TF_VAR_slack_webhook_url="https://hooks.slack.com/services/XXX/YYY/ZZZ"
  ```
- `slack_subteam_id`, `slack_subteam_label` — Optional Slack user group mention

## Anomaly Detection Logic

The anomaly detection compares the latest benchmark run duration against a baseline computed from the previous N runs:

1. Query the last N+1 runs for the benchmark (filtered by name and parameter value)
2. Compute baseline: mean and standard deviation of the N runs before the latest
3. Check if latest run exceeds threshold: `mean ± (sigma × stddev)`
4. If threshold exceeded, post to Slack with run details

The "new normal" suppression (`anomaly_new_normal_relative_threshold`) prevents alerts when both the latest and previous runs are similar (within the threshold), indicating a sustained performance change rather than a spike.

## Local Development

### query.sh

A utility script for rendering and running KQL queries locally:

```bash
# Render a query (output KQL with variables substituted)
./query.sh render anomaly tpch
./query.sh render dashboard yakbench
./query.sh render anomaly readings
./query.sh render anomaly auctionmark

# Run a query against Azure Log Analytics (requires az cli login)
./query.sh run anomaly tpch
./query.sh run dashboard readings
```

Usage:
```
./query.sh <command> <query-type> <benchmark> [terraform.tfvars]
  command:    render | run
  query-type: anomaly | dashboard
  benchmark:  tpch | yakbench | readings | auctionmark
```

The script extracts queries from `main.tf` and substitutes Terraform variables from `terraform.tfvars`, so the queries stay in sync with the deployed infrastructure.
