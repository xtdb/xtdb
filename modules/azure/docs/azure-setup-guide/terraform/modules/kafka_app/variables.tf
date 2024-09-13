variable "resource_group_name" {
  description = "The name of the resource group."
  type        = string
}

variable "resource_group_location" {
  description = "The location of the resource group."
  type        = string
}

variable "container_app_environment_id" {
  description = "The ID of the Container App Environment."
  type        = string
}

variable "storage_account_name" {
  description = "The name of the storage account to setup the kafka file share on."
  type        = string
}

variable "storage_account_primary_access_key" {
  description = "The primary access key for the storage account."
  type        = string
  sensitive   = true
}

variable "kafka_persisent_data_max_size_gb" {
  description = "The size in Gigabytes of the storage share to store Kafka data in."
  type        = number
  default     = 100
}
