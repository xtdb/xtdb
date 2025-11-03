terraform {
  required_version = ">=1.3"

  backend "azurerm" {
    resource_group_name  = "xtdb-bench-terraform-state"
    storage_account_name = "xtdbbenchstate"
    container_name      = "tfstate"
    key                 = "bench.terraform.tfstate"
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.51, < 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0, < 4.0.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = "91804669-c60b-4727-afa2-d7021fe5055b" # Long Run Reliability
}

module "xtdb_azure_bench" {
  source = "../../../../azure/terraform"

  resource_group_name     = "cloud-benchmark-resources"
  resource_group_location = "Central US"

  # Storage account parameters
  storage_account_name             = "xtdbazurebenchmark"
  storage_account_tier             = "Standard"
  storage_account_replication_type = "LRS"
  storage_account_container_name   = "xtdbazurebenchmarkcontainer"

  # AKS cluster parameters
  aks_cluster_name = "xtdb-bench-cluster"

  create_log_analytics_workspace          = true
  log_analytics_workspace_retention_days  = 30

  ## System Pool Config
  aks_system_pool_vm_size            = "Standard_D2pds_v6"
  aks_system_pool_node_max_count     = 2
  aks_system_pool_node_min_count     = 1
  aks_system_pool_availability_zones = ["1", "2"]

  ## Application Pool Config
  aks_application_pool_vm_size            = "Standard_D4pds_v6"
  aks_application_pool_node_max_count     = 3
  aks_application_pool_node_min_count     = 0
  aks_application_pool_availability_zones = ["1", "2", "3"]
  aks_application_pool_os_disk_type       = "Ephemeral"
  aks_application_pool_os_disk_size_gb    = 220
}

resource "azurerm_federated_identity_credential" "xtdb_identity_federation" {
  name                = "xtdb-federated-credential"
  resource_group_name = "cloud-benchmark-resources"
  parent_id           = module.xtdb_azure_bench.user_assigned_managed_identity_id
  issuer              = module.xtdb_azure_bench.oidc_issuer_url
  subject             = "system:serviceaccount:cloud-benchmark:xtdb-service-account"
  audience            = ["api://AzureADTokenExchange"]
}

output "user_assigned_identity_client_id" {
  value = module.xtdb_azure_bench.user_assigned_managed_identity_client_id
}
