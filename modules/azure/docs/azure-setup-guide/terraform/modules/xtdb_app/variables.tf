variable "resource_group_name" {
  description = "The name of the resource group."
  type        = string
}

variable "container_app_environment_id" {
  description = "The ID of the Container App Environment."
  type        = string
}

variable "user_assigned_identity_id" {
  description = "The ID of the user-assigned managed identity."
  type        = string
}

variable "user_assigned_identity_client_id" {
  description = "The client ID of the user-assigned managed identity."
  type        = string
}

variable "storage_account_primary_access_key" {
  description = "The primary access key for the storage account."
  type        = string
  sensitive   = true
}

variable "storage_account_name" {
  description = "The name of the storage account used by the XTDB node."
  type        = string
}

variable "storage_account_container_name" {
  description = "The name of the storage account container used by the XTDB node."
  type        = string
}

variable "service_bus_namespace" {
  description = "The name of the service bus namespace used by the XTDB node."
  type        = string
}

variable "service_bus_topic" {
  description = "The name of the service bus topic used by the XTDB node."
  type        = string
}

variable "local_disk_cache_max_size_gb" {
  description = "The size of the local disk cache in GB."
  type        = number
  default     = 50
}

variable "kafka_bootstrap_servers" {
  description = "The Kafka bootstrap server to connect to."
  type        = string
}

variable "xtdb_topic_name" {
  description = "The name of the Kafka topic to use as the XTDB Transaction Log."
  type        = string
}
