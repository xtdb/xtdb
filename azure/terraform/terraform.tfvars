# Resource group parameters
resource_group_name     = "xtdb-resource-group"
resource_group_location = "East US"

# Storage account parameters
# storage_account_name             = "uniquestorageaccountname"
storage_account_tier             = "Standard"
storage_account_replication_type = "LRS"
storage_account_container_name   = "xtdbstorage"

# AKS cluster parameters
aks_cluster_name = "xtdb-aks-cluster"

## System Pool Config
aks_system_pool_vm_size            = "Standard_D2pds_v6"
aks_system_pool_node_max_count     = 2
aks_system_pool_node_min_count     = 2
aks_system_pool_availability_zones = ["1", "2"]

## Application Pool Config
aks_application_pool_vm_size            = "Standard_D4pds_v6"
aks_application_pool_node_max_count     = 3
aks_application_pool_node_min_count     = 3
aks_application_pool_availability_zones = ["1", "2", "3"]
aks_application_pool_os_disk_type       = "Ephemeral"
aks_application_pool_os_disk_size_gb    = 220
