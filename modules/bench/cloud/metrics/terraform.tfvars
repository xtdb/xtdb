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

# Shared anomaly detection parameters
anomaly_repo               = "xtdb/xtdb"
anomaly_schedule_frequency = "Day"
anomaly_schedule_interval  = 1
anomaly_baseline_n         = 30  # previous N runs for baseline
anomaly_sigma              = 2.0 # 2σ threshold
anomaly_timespan           = "P30D"

# TPC-H anomaly detection
tpch_anomaly_logic_app_name = "xtdb-benchmark-tpch-anomaly"
tpch_anomaly_alert_enabled  = true
tpch_anomaly_scale_factor   = 1.0

# Yakbench anomaly detection
yakbench_anomaly_logic_app_name = "xtdb-benchmark-yakbench-anomaly"
yakbench_anomaly_alert_enabled  = true
yakbench_anomaly_scale_factor   = 1.0

# Readings anomaly detection
readings_anomaly_logic_app_name = "xtdb-benchmark-readings-anomaly"
readings_anomaly_alert_enabled  = true
readings_anomaly_devices        = 10000

# AuctionMark anomaly detection (disabled - throughput metric not yet in data)
auctionmark_anomaly_logic_app_name = "xtdb-benchmark-auctionmark-anomaly"
auctionmark_anomaly_alert_enabled  = false
auctionmark_anomaly_duration       = "PT30M"

# TSBS IoT anomaly detection
tsbs_iot_anomaly_logic_app_name = "xtdb-benchmark-tsbs-iot-anomaly"
tsbs_iot_anomaly_alert_enabled  = true
tsbs_iot_anomaly_devices        = 2000

dashboard_name = "xtdb-benchmark-dashboard"
