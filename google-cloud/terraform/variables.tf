variable "project_id" {
  description = "Existing Google Cloud Project ID to deploy the XTDB infrastructure into."
  type        = string
  validation {
    condition     = length(var.project_id) >= 0
    error_message = "The Google cloud project ID must be set."
  }
}

variable "service_account_name" {
  description = "The name of the Google Cloud service account used for XTDB deployment."
  type        = string
  default     = "xtdb-service-account"
}

variable "kubernetes_namespace" {
  description = "Name of the Kubernetes namespace for XTDB"
  type        = string
  default     = "xtdb-deployment"
}

variable "kubernetes_service_account_name" {
  description = "Name of the Kubernetes service account for XTDB"
  type        = string
  default     = "xtdb-service-account"
}

variable "storage_bucket_name" {
  description = "The globally unique name of the Google Cloud Storage bucket for XTDB deployment."
  type        = string
  validation {
    condition     = length(var.storage_bucket_name) >= 0
    error_message = "The Google cloud storage bucket name must be set and globally unique."
  }
}

variable "storage_bucket_location" {
  description = "The geographical region where the Google Cloud Storage bucket will be created."
  type        = string
  default     = "us-central1"
}

variable "storage_bucket_storage_class" {
  description = "The storage class for the Google Cloud Storage bucket, defining performance and cost (e.g., STANDARD, NEARLINE, COLDLINE)."
  type        = string
  default     = "STANDARD"
}

variable "vpc_name" {
  description = "The name of the Google Cloud VPC network for XTDB deployment."
  type        = string
  default     = "xtdb-vpc"
}

variable "vpc_subnet_name" {
  description = "The name of the Google Cloud VPC subnet for XTDB deployment."
  type        = string
  default     = "xtdb-vpc-public-subnet"
}

variable "vpc_subnet_ip" {
  description = "The IP range for the Google Cloud VPC subnet."
  type        = string
  default     = "10.10.10.0/24"
}

variable "vpc_subnet_region" {
  description = "The region where the Google Cloud VPC subnet will be deployed."
  type        = string
  default     = "us-central1"
}

variable "kubernetes_cluster_name" {
  description = "The name of the Google Kubernetes Engine (GKE) cluster for XTDB deployment."
  type        = string
  default     = "xtdb-cluster"
}

variable "kubernetes_cluster_region" {
  description = "The region where the GKE cluster will be deployed."
  type        = string
  default     = "us-central1"
}

variable "kubernetes_cluster_zones" {
  description = "The list of zones within the selected region where the GKE cluster nodes will be distributed."
  type        = list(string)
  default     = ["us-central1-a"]
}

variable "default_node_pool_count" {
  description = "The initial number of nodes in the default node pool."
  type        = number
  default     = 1
}

variable "node_machine_type" {
  description = "The machine type used by nodes setup in GKE."
  type        = string
  default     = "n2-highmem-4"
}

variable "application_node_pool_locations" {
  description = "The specific zones where application node pool nodes will be deployed."
  type        = string
  default     = "us-central1-a"
}

variable "application_node_pool_min_nodes_per_location" {
  description = "The minimum number of nodes per zone to maintain in the application node pool."
  type        = number
  default     = 1
}

variable "application_node_pool_max_nodes_per_location" {
  description = "The maximum number of nodes per zone allowed in the application node pool."
  type        = number
  default     = 1
}

variable "application_node_pool_disk_size_gb" {
  description = "The disk size (in GB) allocated to each node in the application node pool."
  type        = number
  default     = 50
}

variable "application_node_pool_disk_type" {
  description = "The type of persistent disk used for application node pool nodes (e.g., pd-standard, pd-ssd)."
  type        = string
  default     = "pd-standard"
}

variable "application_node_pool_local_ephemeral_ssd_count" {
  description = "The number of local SSDs to attach to each node in the application node pool, used as scratch space (for XTDB local disk caches)."
  type        = number
  default     = 1
}
variable "deletion_protection" {
  description = "Whether to enable deletion protection on the GKE cluster."
  type        = bool
  default     = true
}
