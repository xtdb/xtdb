# Benchmark Metrics (Azure) — Terraform

This Terraform setup provisions Azure Monitor Logs ingestion for benchmark metrics.

Resources:
- Log Analytics Workspace (`var.workspace_name`)
- Custom Log Analytics Table (`var.table_name`, default `XTDBBenchmark_CL`)
- Data Collection Endpoint (DCE)
- Data Collection Rule (DCR) with transform KQL mapping input JSON to the table
- Role assignment: "Monitoring Metrics Publisher" on the DCR for the sender principal
- Logic App: Alert relay to Slack (Action Group posts here via webhook)
- Action Group: routes Azure Monitor alerts to the Logic App
- Logic App (azapi): Scheduled anomaly detection using Logs Query API (managed identity)
- Scheduled Query Alert: "missing ingestion" when no runs are observed within a window
- Azure Portal Dashboard: simple time-series of recent runs

## Caveats

- Log Analytics custom table lifecycle is managed manually. Terraform will reference and track the table (`azurerm_log_analytics_workspace_table`) but cannot author the schema end-to-end. Use Azure CLI to create/update/delete the table and its columns as needed; then import or let Terraform track its existence.

  Examples:
  ```bash
  # Create a custom table (name must end with _CL)
  az monitor log-analytics workspace table create \
    --resource-group "$RG" \
    --workspace-name "$LAW" \
    --name "$TABLE" \
    --plan Analytics \
      --columns \
        "TimeGenerated=datetime" \
        "run_id=string" \
        "git_sha=string" \
        "benchmark=string" \
        "repo=string" \
        "step=string" \
        "node_id=string" \
        "metric=string" \
        "value=real" \
        "unit=string" \
        "ts=datetime" \
        "params=dynamic"


  # Update retention
  az monitor log-analytics workspace table update \
    --resource-group "$RG" \
    --workspace-name "$LAW" \
    --name "$TABLE" \
    --retention-time 30 --total-retention-time 90

  # Delete table
  az monitor log-analytics workspace table delete \
    --resource-group "$RG" \
    --workspace-name "$LAW" \
    --name "$TABLE" -y
  ```

Outputs:
- `dce_ingest_endpoint` — e.g. https://<dce>.<region>.ingest.monitor.azure.com
- `dcr_immutable_id` — use in the Logs Ingestion API path
- `stream_name` — e.g. `Custom-XTDBBenchmark_CL`

## Configuring alerting (Terraform variables)

Alerting is controlled via variables in `variables.tf`, which you can override in `terraform.tfvars` or via environment variables (preferred for secrets). Highlights below reflect the current module:

- Anomaly detection (Logic App via Logs Query)
  - `anomaly_logic_app_name` — Logic App name
  - `anomaly_alert_enabled` — enable/disable the scheduled anomaly Logic App
  - `anomaly_repo` — repository (owner/name) to filter on, e.g. `xtdb/xtdb`
  - `anomaly_schedule_frequency` — `Minute|Hour|Day|Week|Month`
  - `anomaly_schedule_interval` — integer interval for the above frequency
  - `anomaly_timespan` — ISO8601 timespan for the Logs Query API (e.g., `P30D`)
  - `anomaly_baseline_n` — number of prior runs to compute baseline
  - `anomaly_sigma` — sigma multiplier for thresholding (e.g., `2` for ±2σ)
  - `anomaly_scale_factor` — TPC-H scale factor to scope queries

- Missing-ingestion scheduled alert (Azure Monitor)
  - `missing_alert_name` — Azure Monitor alert name
  - `missing_alert_severity` — 0 (most severe) to 4 (least)
  - `missing_alert_enabled` — enable/disable the scheduled alert
  - `missing_alert_evaluation_frequency` — ISO8601 cadence for evaluation (e.g., `PT1H`)
  - `missing_alert_window_duration` — ISO8601 lookback window (e.g., `P2D`)

- Notifications
  - `action_group_name`, `action_group_short_name` — Action Group identity
  - `slack_webhook_url` — Slack Incoming Webhook used by the Logic App relay
    - Provide this securely via environment, not committed tfvars: `export TF_VAR_slack_webhook_url="https://hooks.slack.com/services/XXX/YYY/ZZZ"`

Example overrides in `terraform.tfvars` (excerpt):
```hcl
# Core resources
location            = "westeurope"
resource_group_name = "xtdb-benchmark-metrics"
workspace_name      = "xtdb-benchmark-metrics"
dce_name            = "xtdb-benchmark-metrics"
dcr_name            = "xtdb-benchmark-metrics"
table_name          = "XTDBBenchmark_CL"
sender_principal_id = "<OBJECT_ID_ALLOWED_TO_SEND_METRICS>"

# Action Group (Slack)
action_group_name       = "xtdb-benchmark-alerts"
action_group_short_name = "xtdbbench"
# slack_webhook_url is recommended via environment (see below)

# Anomaly detection parameters (used by Logic App)
anomaly_logic_app_name     = "xtdb-benchmark-anomaly"
anomaly_alert_enabled      = false
anomaly_repo               = "xtdb/xtdb"
anomaly_schedule_frequency = "Hour"
anomaly_schedule_interval  = 1
anomaly_baseline_n         = 20
anomaly_sigma              = 0.01
anomaly_scale_factor       = 0.5
anomaly_timespan           = "P30D"

# Missing ingestion alert
missing_alert_evaluation_frequency = "P1D"
missing_alert_window_duration      = "P2D"
missing_alert_enabled              = false
missing_alert_severity             = 2

dashboard_name = "xtdb-benchmark-dashboard"
```

Provide Slack webhook at apply time (do not commit secrets):
```bash
export TF_VAR_slack_webhook_url="https://hooks.slack.com/services/XXX/YYY/ZZZ"
terraform -chdir=dev/cloud/azure/benchmark-metrics plan
terraform -chdir=dev/cloud/azure/benchmark-metrics apply
```

Anomaly logic in the Logic App compares the latest `overall` duration against a baseline of the last `anomaly_baseline_n` runs (mean ± `anomaly_sigma` × stdev), scoped by `benchmark == "tpch"`, `repo == var.anomaly_repo`, and `params.scaleFactor == var.anomaly_scale_factor`. If a violation occurs, a Slack message is posted that links to the configured repository’s run ID.

Input payload format (array of records):
```json
[
  {
    "run_id": "...",
    "git_sha": "...",
    "repo": "owner/repo",
    "benchmark": "tpch",
    "step": "overall",
    "node_id": "n1",
    "metric": "duration_ms",
    "value": 123.45,
    "unit": "ms",
    "ts": "2025-01-01T00:00:00Z",
    "params": {"scaleFactor": 1}
  }
]
```

Transform KQL in the DCR maps `ts` -> `TimeGenerated`, casts types, and projects all fields including `repo`.

## Usage

Use in GitHub Actions:
- DCE endpoint: `outputs.dce_ingest_endpoint`
- DCR immutable ID: `outputs.dcr_immutable_id`
- Stream: `outputs.stream_name`

Acquire token and post:
```bash
ACCESS_TOKEN=$(az account get-access-token --resource https://monitor.azure.com --query accessToken -o tsv)
URL="${dce_ingest_endpoint%/}/dataCollectionRules/${dcr_immutable_id}/streams/${stream_name}?api-version=2023-01-01"
curl -sS -X POST "$URL" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary @metrics.json
```
