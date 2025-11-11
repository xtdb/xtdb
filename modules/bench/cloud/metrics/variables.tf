variable "location" {
  description = "Azure region, e.g., westeurope"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "workspace_name" {
  description = "Log Analytics workspace name"
  type        = string
}

variable "table_name" {
  description = "Log Analytics custom table name (must end with _CL)"
  type        = string
  default     = "XTDBBenchmark_CL"
}

variable "dce_name" {
  description = "Data Collection Endpoint name"
  type        = string
}

variable "dcr_name" {
  description = "Data Collection Rule name"
  type        = string
}

variable "sender_principal_id" {
  description = "Object ID of the principal to grant 'Monitoring Data Collection Rule Data Sender' on the DCR"
  type        = string
  default     = ""
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

# Logic App anomaly detection
variable "anomaly_logic_app_name" {
  description = "Logic App name for scheduled anomaly detection"
  type        = string
  default     = "xtdb-bench-anomaly-schedule"
}

variable "anomaly_alert_enabled" {
  description = "Whether the anomaly detection Logic App is enabled"
  type        = bool
  default     = true
}

variable "anomaly_repo" {
  description = "Repository (owner/name) to filter anomaly and missing-ingestion alerts on"
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

variable "anomaly_scale_factor" {
  description = "TPC-H scale factor to scope anomaly detection to (e.g., 0.5)"
  type        = number
  default     = 1.0
}

variable "anomaly_new_normal_relative_threshold" {
  description = "Relative threshold between the latest and previous run durations below which anomalies are suppressed as a new normal"
  type        = number
  default     = 0.05
}

# Missing ingestion alert configuration
variable "missing_alert_name" {
  description = "Name of the missing-ingestion scheduled query alert"
  type        = string
  default     = "xtdb-benchmark-missing-ingestion"
}

variable "missing_alert_severity" {
  description = "Alert severity (0=Sev0, 4=Sev4) for missing-ingestion"
  type        = number
  default     = 2
}

variable "missing_alert_enabled" {
  description = "Whether the missing-ingestion alert is enabled"
  type        = bool
  default     = true
}

variable "missing_alert_evaluation_frequency" {
  description = "How often to evaluate the missing-ingestion alert (ISO 8601 duration, e.g. PT1H)"
  type        = string
  default     = "PT1H"
}

variable "missing_alert_window_duration" {
  description = "Time window to check for missing-ingestion (ISO 8601 duration, e.g. P2D for 2 days)"
  type        = string
  default     = "P2D"
}

variable "dashboard_name" {
  description = "Name of the Azure Portal dashboard to create"
  type        = string
  default     = "xtdb-benchmark-dashboard"
}
