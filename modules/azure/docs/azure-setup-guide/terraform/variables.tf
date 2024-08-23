variable "resource_group_location" {
  description = "The location of the created resource group."
  type        = string
  default     = "West Europe"
}

variable "container_app_workload_profile_type" {
  description = "The workload profile type to run container apps on."
  type        = string
  default     = "D4"
}

variable "storage_account_tier" {
  description = "The tier of the storage account."
  type        = string
  default     = "Standard"
}

variable "storage_account_name" {
  description = "The unique name for the storage account across Azure."
  type        = string
  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24 && can(regex("^[-a-z0-9]*$", var.storage_account_name))
    error_message = "The storage account name must be 3 to 24 characters long and can contain only lowercase letters and numbers."
  }
}

variable "kafka_persisent_data_max_size_gb" {
  description = "The size in Gigabytes of the storage share to store Kafka data in."
  type        = number
  default     = 5120
}

variable "local_disk_cache_max_size_gb" {
  description = "The size of the local disk cache in GB."
  type        = number
  default     = 50
}
