variable "resource_group_name" {
  description = "The name of the Azure Resource Group."
  type        = string
}

variable "resource_group_location" {
  description = "The location of the Azure Resource Group."
  type        = string
}

variable "user_assigned_identity_id" {
  description = "The ID of the user-assigned managed identity."
  type        = string
}

variable "kubernetes_namespace" {
  description = "The namespace to deploy the Kubernetes cluster into"
  type        = string
  default     = "xtdb-deployment"
}

variable "kubernetes_service_account_name" {
  description = "The name of the service account to create for the Kubernetes cluster"
  type        = string
  default     = "xtdb-service-account"
}

variable "aks_system_pool_vm_size" {
  description = "The VM size for the system pool in the AKS cluster."
  type        = string
}

variable "aks_application_pool_vm_size" {
  description = "The VM size for the application pool in the AKS cluster."
  type        = string
}
