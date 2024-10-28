variable "resource_group_location" {
  description = "The location of the created resource group."
  type        = string
  default     = "East US"
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

variable "kubernetes_namespace" {
  description = "The namespace to deploy the Kubernetes cluster into"
  type        = string
  default     = "xtdb-infra"
}

variable "kubernetes_service_account_name" {
  description = "The name of the service account to create for the Kubernetes cluster"
  type        = string
  default     = "xtdb-service-account"
}

variable "aks_system_pool_vm_size" {
  description = "The size of VM to use for the Kubernetes system node pool "
  type        = string
  default     = "Standard_D2_v2"
}

variable "aks_application_pool_vm_size" {
  description = "The size of VM to use for the Kubernetes application node pool"
  type        = string
  default     = "Standard_D8_v3"
}

