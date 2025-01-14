# Resource group parameters
resource_group_name     = "xtdb-resource-group"
resource_group_location = "East US"

# Storage account parameters
storage_account_name             = ""
storage_account_tier             = "Standard"
storage_account_replication_type = "LRS"
storage_account_container_name   = "xtdbstorage"

# AKS cluster parameters
aks_cluster_name                        = "xtdb-aks-cluster"
aks_application_pool_vm_size            = "Standard_D8pds_v6"
aks_application_pool_node_count         = 1
aks_application_pool_availability_zones = ["1"]

# Kubernetes parameters
kubernetes_namespace = "xtdb-deployment"
kubernetes_service_account_name = "xtdb-service-account"
