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

variable "service_bus_namespace" {
  description = "The unique name for the service bus namespace across Azure."
  type        = string
  validation {
    condition     = length(var.service_bus_namespace) >= 6 && length(var.service_bus_namespace) <= 50 && can(regex("^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$", var.service_bus_namespace)) && !can(regex(".*(-sb|-mgmt)$", var.service_bus_namespace))
    error_message = "The service bus namespace name must be 6 to 50 characters long, contain only letters, numbers, and hyphens, start with a letter, end with a letter or number, and not end with '-sb' or '-mgmt'."
  }
}

variable "service_bus_sku" {
  description = "The SKU for the service bus."
  type        = string
  default     = "Standard"
}

variable "service_bus_message_ttl" {
  description = "The time-to-live for messages in the service bus."
  type        = string
  default     = "PT30M"
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
