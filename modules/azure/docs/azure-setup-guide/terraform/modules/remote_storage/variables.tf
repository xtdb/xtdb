variable "resource_group_name" {
  description = "The name of the Azure Resource Group."
  type        = string
}

variable "resource_group_location" {
  description = "The location of the Azure Resource Group."
  type        = string
}

variable "user_assigned_identity_principal_id" {
  description = "The Principal ID of the user-assigned managed identity."
  type        = string
}

variable "storage_account_name" {
  description = "The unique name for the storage account across Azure."
  type        = string
  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24 && can(regex("^[-a-z0-9]*$", var.storage_account_name))
    error_message = "The storage account name must be 3 to 24 characters long and can contain only lowercase letters and numbers."
  }
}

variable "storage_account_tier" {
  description = "The tier of the storage account."
  type        = string
  default     = "Standard"
}

variable "storage_account_replication_type" {
  description = "The replication type for the storage account."
  type        = string
  default     = "LRS"
}

