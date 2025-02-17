variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
}

variable "resource_group_location" {
  description = "Location of the Azure resource group"
  type        = string
}

variable "storage_account_name" {
  description = "Globally unique name for the storage account used by XTDB."
  type        = string
  validation {
    condition     = length(var.storage_account_name) >= 3 && length(var.storage_account_name) <= 24 && can(regex("^[-a-z0-9]*$", var.storage_account_name))
    error_message = "The storage account name must be 3 to 24 characters long and can contain only lowercase letters and numbers."
  }
}


variable "storage_account_tier" {
  description = "The performance tier of the storage account (e.g., Standard, Premium)"
  type        = string
  default     = "Standard"
}

variable "storage_account_replication_type" {
  description = "The replication strategy of the storage account (e.g., LRS, GRS, ZRS)"
  type        = string
  default     = "LRS"
}

variable "storage_account_container_name" {
  description = "Name of the storage container for XTDB"
  type        = string
}

variable "aks_cluster_name" {
  description = "Name of the XTDB AKS cluster"
  type        = string
}

variable "aks_system_pool_vm_size" {
  description = "VM size for the XTDB AKS system node pool"
  type        = string
  default     = "Standard_D2pds_v6"
}

variable "aks_system_pool_node_count" {
  description = "Number of nodes in the XTDB AKS system node pool"
  type        = number
  default     = 2
}

variable "aks_system_pool_availability_zones" {
  description = "Availability zones for the XTDB AKS system node pool"
  type        = list(string)
  default     = ["1", "2"]
}

variable "aks_application_pool_vm_size" {
  description = "VM size for the XTDB AKS application node pool"
  type        = string
  default     = "Standard_D4pds_v6"
}

variable "aks_application_pool_node_count" {
  description = "Number of nodes in the XTDB AKS application node pool"
  type        = number
  default     = 3
}

variable "aks_application_pool_availability_zones" {
  description = "Availability zones for the XTDB AKS application node pool"
  type        = list(string)
  default     = ["1", "2", "3"]
}

variable "aks_application_pool_os_disk_type" {
  description = "OS disk type for the XTDB AKS application node pool"
  type        = string
  default     = "Ephemeral"
}

variable "aks_application_pool_os_disk_size_gb" {
  description = "OS disk size in GB for the XTDB AKS application node pool"
  type        = number
  default     = 220
}
