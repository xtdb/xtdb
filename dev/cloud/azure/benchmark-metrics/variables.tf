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

# Alert configuration
variable "slow_alert_name" {
  description = "Name of the scheduled query alert for slow runs"
  type        = string
  default     = "xtdb-benchmark-slow-alert"
}

variable "fast_alert_name" {
  description = "Name of the scheduled query alert for fast runs"
  type        = string
  default     = "xtdb-benchmark-fast-alert"
}

variable "alert_severity" {
  description = "Alert severity (0=Sev0, 4=Sev4)"
  type        = number
  default     = 3
}

variable "alert_enabled" {
  description = "Whether the scheduled query alert is enabled"
  type        = bool
  default     = true
}

variable "alert_evaluation_frequency" {
  description = "How often to evaluate the alert (ISO 8601 duration, e.g. PT1H)"
  type        = string
  default     = "PT1H"
}

variable "alert_window_duration" {
  description = "Time window for the alert query (ISO 8601 duration)"
  type        = string
  default     = "PT24H"
}

variable "alert_baseline_n" {
  description = "Number of previous runs to average for the baseline"
  type        = number
  default     = 20
}

variable "alert_sigma" {
  description = "Sigma multiplier for the stddev-based alert threshold (e.g., 2.0 for 2σ)"
  type        = number
  default     = 2
}

variable "alert_scale_factor" {
  description = "TPC-H scale factor to scope the alert to (e.g., 0.5)"
  type        = number
  default     = 0.5
}

# Action Group email receiver configuration
variable "alert_email_receiver_name" {
  description = "Name for the Action Group email receiver"
  type        = string
  default     = "tim"
}

variable "alert_email_address" {
  description = "Email address for the Action Group email receiver"
  type        = string
  default     = "tim@juxt.pro"
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
