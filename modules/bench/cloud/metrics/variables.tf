variable "location" {
  description = "Azure region, e.g., westeurope"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name (where cluster LAW resides)"
  type        = string
}

variable "cluster_law_name" {
  description = "Name of the existing cluster Log Analytics Workspace"
  type        = string
}

# Action Group (Slack) configuration
variable "action_group_name" {
  description = "Name for the Azure Monitor Action Group"
  type        = string
  default     = "xtdb-benchmark-alerts"
}

variable "action_group_short_name" {
  description = "Short name for Action Group"
  type        = string
  default     = "xtdbbench"
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for alerts (optional). If empty, no webhook receiver is created."
  type        = string
  default     = ""
  sensitive   = true
}

# Optional Slack subteam mention (e.g., <!subteam^S012345|@xtdb-team>)
variable "slack_subteam_id" {
  description = "Slack subteam (user group) ID to mention in alert messages (e.g., S012345). Leave empty to disable mention."
  type        = string
  default     = ""
}

variable "slack_subteam_label" {
  description = "Human-friendly label for the Slack subteam mention (used in fallback text), e.g., @xtdb-team"
  type        = string
  default     = "@xtdb-team"
}

# TPC-H anomaly detection configuration
variable "tpch_anomaly_logic_app_name" {
  description = "Logic App name for scheduled TPC-H anomaly detection"
  type        = string
  default     = "xtdb-bench-tpch-anomaly-schedule"
}

variable "tpch_anomaly_alert_enabled" {
  description = "Whether the TPC-H anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "tpch_anomaly_scale_factor" {
  description = "TPC-H scale factor to scope anomaly detection to (e.g., 0.5)"
  type        = number
  default     = 1.0
}

variable "anomaly_repo" {
  description = "Repository (owner/name) for run links in anomaly alerts"
  type        = string
  default     = "xtdb/xtdb"
}

variable "anomaly_schedule_frequency" {
  description = "Recurrence frequency for anomaly detection (Minute|Hour|Day|Week|Month)"
  type        = string
  default     = "Day"
}

variable "anomaly_schedule_interval" {
  description = "Recurrence interval for anomaly detection"
  type        = number
  default     = 1
}

variable "anomaly_schedule_hour" {
  description = "Hour of day to run anomaly detection (0-23, in specified timezone)"
  type        = number
  default     = 9
}

variable "anomaly_schedule_timezone" {
  description = "Timezone for anomaly detection schedule (e.g., UTC, Europe/London)"
  type        = string
  default     = "UTC"
}

variable "anomaly_timespan" {
  description = "Time span for Logs Query API (ISO8601, e.g., P30D)"
  type        = string
  default     = "P30D"
}

variable "anomaly_baseline_n" {
  description = "Number of previous runs to average for the baseline"
  type        = number
  default     = 30
}

variable "anomaly_sigma" {
  description = "Sigma multiplier for the stddev-based threshold (e.g., 2.0 for 2σ)"
  type        = number
  default     = 2
}

variable "anomaly_new_normal_relative_threshold" {
  description = "Relative threshold between the latest and previous run durations below which anomalies are suppressed as a new normal"
  type        = number
  default     = 0.05
}

variable "dashboard_name" {
  description = "Name of the Azure Portal dashboard to create"
  type        = string
  default     = "xtdb-benchmark-dashboard"
}

# Yakbench anomaly detection configuration
variable "yakbench_anomaly_logic_app_name" {
  description = "Logic App name for scheduled Yakbench anomaly detection"
  type        = string
  default     = "xtdb-bench-yakbench-anomaly-schedule"
}

variable "yakbench_anomaly_alert_enabled" {
  description = "Whether the Yakbench anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "yakbench_anomaly_scale_factor" {
  description = "Yakbench scale factor to scope anomaly detection to"
  type        = number
  default     = 1.0
}

# Readings anomaly detection configuration
variable "readings_anomaly_logic_app_name" {
  description = "Logic App name for scheduled Readings anomaly detection"
  type        = string
  default     = "xtdb-bench-readings-anomaly-schedule"
}

variable "readings_anomaly_alert_enabled" {
  description = "Whether the Readings anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "readings_anomaly_devices" {
  description = "Readings devices count to scope anomaly detection to"
  type        = number
  default     = 10000
}

# AuctionMark anomaly detection configuration
variable "auctionmark_anomaly_logic_app_name" {
  description = "Logic App name for scheduled AuctionMark anomaly detection"
  type        = string
  default     = "xtdb-bench-auctionmark-anomaly-schedule"
}

variable "auctionmark_anomaly_alert_enabled" {
  description = "Whether the AuctionMark anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "auctionmark_anomaly_duration" {
  description = "AuctionMark duration (ISO 8601, e.g. PT30M) to scope anomaly detection to"
  type        = string
  default     = "PT30M"
}

# TSBS IoT anomaly detection configuration
variable "tsbs_iot_anomaly_logic_app_name" {
  description = "Logic App name for scheduled TSBS IoT anomaly detection"
  type        = string
  default     = "xtdb-bench-tsbs-iot-anomaly-schedule"
}

variable "tsbs_iot_anomaly_alert_enabled" {
  description = "Whether the TSBS IoT anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "tsbs_iot_anomaly_devices" {
  description = "TSBS IoT devices count to scope anomaly detection to"
  type        = number
  default     = 2000
}

# Ingest TX Overhead anomaly detection configuration
variable "ingest_tx_overhead_anomaly_logic_app_name" {
  description = "Logic App name for scheduled Ingest TX Overhead anomaly detection"
  type        = string
  default     = "xtdb-bench-ingest-tx-overhead-anomaly-schedule"
}

variable "ingest_tx_overhead_anomaly_alert_enabled" {
  description = "Whether the Ingest TX Overhead anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "ingest_tx_overhead_anomaly_doc_count" {
  description = "Ingest TX Overhead doc count to scope anomaly detection to"
  type        = number
  default     = 100000
}

# Patch anomaly detection configuration
variable "patch_anomaly_logic_app_name" {
  description = "Logic App name for scheduled Patch anomaly detection"
  type        = string
  default     = "xtdb-bench-patch-anomaly-schedule"
}

variable "patch_anomaly_alert_enabled" {
  description = "Whether the Patch anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "patch_anomaly_doc_count" {
  description = "Patch doc count to scope anomaly detection to"
  type        = number
  default     = 500000
}

# Products anomaly detection configuration
variable "products_anomaly_logic_app_name" {
  description = "Logic App name for scheduled Products anomaly detection"
  type        = string
  default     = "xtdb-bench-products-anomaly-schedule"
}

variable "products_anomaly_alert_enabled" {
  description = "Whether the Products anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

# TS Devices anomaly detection configuration
variable "ts_devices_anomaly_logic_app_name" {
  description = "Logic App name for scheduled TS Devices anomaly detection"
  type        = string
  default     = "xtdb-bench-ts-devices-anomaly-schedule"
}

variable "ts_devices_anomaly_alert_enabled" {
  description = "Whether the TS Devices anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "ts_devices_anomaly_size" {
  description = "TS Devices size to scope anomaly detection to (small, med, big)"
  type        = string
  default     = "small"
}
