variable "slack_webhook_url" {
  description = "The URL of the Slack webhook to send notifications to"
  type        = string
  sensitive   = true
}

variable "auctionmark_duration" {
  description = "The duration of the auctionmark test"
  type        = string
}

variable "auctionmark_scale_factor" {
  description = "The scale factor of the auctionmark test"
  type        = number
}

variable "eventhub_topic_suffix" {
  description = "String to suffix the eventhub topic with - update whenever you want to create a new topic"
  type        = string
}

variable "run_single_node" {
  description = "Whether to run a single node deployment"
  type        = bool
}
