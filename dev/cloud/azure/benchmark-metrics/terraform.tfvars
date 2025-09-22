# Core resources
location            = "westeurope"
resource_group_name = "xtdb-benchmark-metrics"
workspace_name      = "xtdb-benchmark-metrics"
dce_name            = "xtdb-benchmark-metrics"
dcr_name            = "xtdb-benchmark-metrics"

# Optional: custom table name (must end with _CL)
table_name = "XTDBBenchmark_CL"

# Principal allowed to post via Logs Ingestion API
# NOTE: set to the Object ID of the principal that will send metrics (e.g. your SP or workload identity)
sender_principal_id = "0995c238-9646-4014-be4d-487ff8300975"

# Action Group (Slack)
action_group_name       = "xtdb-benchmark-alerts"
action_group_short_name = "xtdbbench"
# set via TF_VAR_slack_webhook_url = "https://hooks.slack.com/services/XXX/YYY/ZZZ"
# slack_webhook_url       = "https://hooks.slack.com/services/XXX/YYY/ZZZ"
alert_email_receiver_name  = "tim"
alert_email_address        = "tim@juxt.pro"

# Alert configuration (slow run vs baseline)
slow_alert_name            = "xtdb-benchmark-slow-alert"
fast_alert_name            = "xtdb-benchmark-fast-alert"
alert_severity             = 3          # 0..4
alert_enabled              = true
alert_evaluation_frequency = "P1D"      # evaluate every 1 day
alert_window_duration      = "P30D"     # lookback window
alert_baseline_n           = 20         # previous N runs for baseline
alert_sigma                = 2          # 2σ slower than baseline
alert_scale_factor         = 0.5

missing_alert_evaluation_frequency = "P1D"
missing_alert_window_duration      = "P2D"
missing_alert_enabled              = true
missing_alert_severity             = 2

dashboard_name = "xtdb-benchmark-dashboard"