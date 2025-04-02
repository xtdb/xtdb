variable "kubernetes_namespace" {
  description = "The namespace to deploy the Kubernetes cluster into"
  type        = string
  default     = "cloud-benchmark"
}

variable "kubernetes_service_account_name" {
  description = "The name of the service account to create for the Kubernetes cluster"
  type        = string
  default     = "xtdb-service-account"
}

variable "kubernetes_node_count" {
  description = "The number of Kubernetes nodes to deploy"
  type        = number
  default     = 3
}

variable "kubernetes_vm_size" {
  description = "The size of the Kubernetes VMs - update as necessary to scale"
  type        = string
  default     = "Standard_D2_v2"
}
