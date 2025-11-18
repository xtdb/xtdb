# Core resources - using existing cluster LAW
location            = "Central US"
resource_group_name = "cloud-benchmark-resources"
cluster_law_name    = "xtdb-bench-cluster-law"

# Action Group (Slack)
action_group_name       = "xtdb-benchmark-alerts"
action_group_short_name = "xtdbbench"
# set via TF_VAR_slack_webhook_url = "https://hooks.slack.com/services/XXX/YYY/ZZZ"
# slack_webhook_url       = "https://hooks.slack.com/services/XXX/YYY/ZZZ"
slack_subteam_id    = "SR1A6EW21"
slack_subteam_label = "@xtdb-team"

# Anomaly detection parameters (used by Logic App)
anomaly_logic_app_name     = "xtdb-benchmark-anomaly"
anomaly_alert_enabled      = true
anomaly_repo               = "xtdb/xtdb"
anomaly_schedule_frequency = "Day"
anomaly_schedule_interval  = 1
anomaly_baseline_n         = 30  # previous N runs for baseline
anomaly_sigma              = 2.0 # 2σ threshold
anomaly_scale_factor       = 1.0 # The scale factor to select on
anomaly_timespan           = "P30D"

missing_alert_evaluation_frequency = "P1D"
missing_alert_window_duration      = "P2D"
missing_alert_enabled              = true
missing_alert_severity             = 2

dashboard_name = "xtdb-benchmark-dashboard"
