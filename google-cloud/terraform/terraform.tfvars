# Google Cloud Project
# project_id = "project-id"

# Google Cloud Service Account Name
service_account_name = "xtdb-service-account"

# Google Cloud Storage Settings
storage_bucket_name          = "xtdb-storage-bucket"
storage_bucket_location      = "us-central1"
storage_bucket_storage_class = "STANDARD"

# VPC/Network naming for XTDB VPC
vpc_name          = "xtdb-vpc"
vpc_subnet_name   = "xtdb-vpc-public-subnet"
vpc_subnet_ip     = "10.10.10.0/24"
vpc_subnet_region = "us-central1"

# Key GKE Config
kubernetes_cluster_name   = "xtdb-cluster"
kubernetes_cluster_region = "us-central1"
kubernetes_cluster_zones  = ["us-central1-a"]

## GKE Node Pool Settings
### Initial node count for the default node pool
default_node_pool_count = 1
### Node machine type shared by default and application node pools
node_machine_type           = "n2-highmem-2"


## GKE Application Node Pool Settings
application_node_pool_locations              = "us-central1-a,us-central1-b,us-central1-c"
application_node_pool_min_nodes_per_location = 1
application_node_pool_max_nodes_per_location = 1
application_node_pool_disk_size_gb           = 50
application_node_pool_disk_type              = "pd-standard"
### Ephemeral SSDs used as scratch storage for the XTDB nodes - ie, for their local disk cache
application_node_pool_local_ephemeral_ssd_count = 1
