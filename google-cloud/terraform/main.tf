resource "google_service_account" "xtdb_service_account" {
  project = var.project_id

  account_id   = var.service_account_name
  display_name = "XTDB Service Account"
}

module "xtdb_storage" {
  source  = "terraform-google-modules/cloud-storage/google"
  version = "~> 9.0"

  project_id = var.project_id

  # Storage bucket settings
  names         = [var.storage_bucket_name]
  location      = var.storage_bucket_location
  storage_class = var.storage_bucket_storage_class

  # Role assignments
  set_admin_roles = true
  admins          = [google_service_account.xtdb_service_account.member]
}

module "xtdb_vpc" {
  source  = "terraform-google-modules/network/google"
  version = "10.0.0"

  project_id   = var.project_id
  network_name = var.vpc_name

  subnets = [
    {
      subnet_name   = var.vpc_subnet_name
      subnet_ip     = var.vpc_subnet_ip
      subnet_region = var.vpc_subnet_region
    }
  ]
}

module "kubernetes_engine" {
  source  = "terraform-google-modules/kubernetes-engine/google"
  version = "35.0.1"

  project_id = var.project_id

  name   = var.kubernetes_cluster_name
  region = var.kubernetes_cluster_region
  zones  = var.kubernetes_cluster_zones

  network           = module.xtdb_vpc.network_name
  subnetwork        = module.xtdb_vpc.subnets_names[0]
  ip_range_pods     = ""
  ip_range_services = ""

  # Default node pool count - uses same machine type as application node pool
  initial_node_count = var.default_node_pool_count

  node_pools = [
    {
      name                              = "xtdb-pool"
      machine_type                      = var.node_machine_type
      node_locations                    = var.application_node_pool_locations
      min_count                         = var.application_node_pool_min_nodes_per_location
      max_count                         = var.application_node_pool_max_nodes_per_location
      disk_size_gb                      = var.application_node_pool_disk_size_gb
      disk_type                         = var.application_node_pool_disk_type
      auto_repair                       = true
      auto_upgrade                      = true
      local_ssd_ephemeral_storage_count = var.application_node_pool_local_ephemeral_ssd_count
      service_account                   = google_service_account.xtdb_service_account.email
    }
  ]

  node_pools_labels = {
    all = {}

    xtdb-pool = {
      environment = "dev"
    }
  }

  node_pools_taints = {
    all = []

    xtdb-pool = [
      {
        key    = "xtdb-pool"
        value  = true
        effect = "PREFER_NO_SCHEDULE"
      },
    ]
  }

  # We don't need to create a service account for the GKE cluster - we've already created one
  create_service_account = false
}

