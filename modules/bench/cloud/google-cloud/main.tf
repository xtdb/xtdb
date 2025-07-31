terraform {
  required_version = ">=1.3"

  backend "gcs" {
    bucket = "xtdb-bench-terraform-state"
    prefix = "bench"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.43.0, < 7"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0, < 4.0.0"
    }
  }
}


provider "google" {
  project = "xtdb-scratch"
  region  = "us-central1"
}

module "xtdb_gcp_bench" {
  source = "../../../../google-cloud/terraform"

  # Project & SA
  project_id            = "xtdb-scratch"
  service_account_name  = "xtdb-service-account"

  # Storage
  storage_bucket_name          = "xtdb-gcp-benchmark-bucket"
  storage_bucket_location      = "us-central1"
  storage_bucket_storage_class = "STANDARD"

  # VPC
  vpc_name          = "xtdb-vpc"
  vpc_subnet_name   = "xtdb-vpc-public-subnet"
  vpc_subnet_ip     = "10.10.10.0/24"
  vpc_subnet_region = "us-central1"

  # GKE
  kubernetes_cluster_name   = "xtdb-bench-cluster"
  kubernetes_cluster_region = "us-central1"
  kubernetes_cluster_zones  = ["us-central1-a", "us-central1-b", "us-central1-c"]

  default_node_pool_count = 1
  node_machine_type        = "n2-highmem-4"

  application_node_pool_locations              = "us-central1-a,us-central1-b,us-central1-c"
  application_node_pool_min_nodes_per_location = 1
  application_node_pool_max_nodes_per_location = 1
  application_node_pool_disk_size_gb           = 50
  application_node_pool_disk_type              = "pd-standard"
  application_node_pool_local_ephemeral_ssd_count = 1

  deletion_protection = false
}

resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = "projects/${module.xtdb_gcp_bench.project_id}/serviceAccounts/${module.xtdb_gcp_bench.service_account_email}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:xtdb-scratch.svc.id.goog[cloud-benchmark/xtdb-service-account]"
}

output "service_account_email" {
  value = module.xtdb_gcp_bench.service_account_email
}
