# Resource group setup
resource "azurerm_resource_group" "xtdb_infra" {
  name     = var.resource_group_name
  location = var.resource_group_location
}

# User Assigned Identity to assign roles to resources
resource "azurerm_user_assigned_identity" "xtdb_infra" {
  location            = azurerm_resource_group.xtdb_infra.location
  name                = "xtdb-identity"
  resource_group_name = azurerm_resource_group.xtdb_infra.name
}

# Sets up storage account/storage container used by XTDB
# For more configuration options, see:
# https://registry.terraform.io/modules/Azure/avm-res-storage-storageaccount/azurerm/latest
module "xtdb_storage" {
  depends_on = [
    azurerm_resource_group.xtdb_infra,
    azurerm_user_assigned_identity.xtdb_infra
  ]
  source  = "Azure/avm-res-storage-storageaccount/azurerm"
  version = "0.4.0"

  # Resource group settings
  location            = azurerm_resource_group.xtdb_infra.location
  resource_group_name = azurerm_resource_group.xtdb_infra.name

  # Storage account settings
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
  name                     = var.storage_account_name

  containers = {
    xtdbstorage = {
      name                  = var.storage_account_container_name
      container_access_type = "private"
      role_assignments = {
        xtdbstorage = {
          principal_id               = azurerm_user_assigned_identity.xtdb_infra.principal_id
          role_definition_id_or_name = "Storage Blob Data Contributor"
        }
      }
    }
  }

  # Telemetry
  enable_telemetry = false

  # Public network access for storage account
  public_network_access_enabled = true
  network_rules = {
    default_action = "Allow"
  }
}

# Sets up an AKS cluster to be used by XTDB
# For more configuration options, see:
# https://registry.terraform.io/modules/Azure/aks/azurerm/latest
module "aks" {
  depends_on = [
    azurerm_resource_group.xtdb_infra,
    azurerm_user_assigned_identity.xtdb_infra
  ]
  source  = "Azure/aks/azurerm"
  version = "9.3.0"

  # Resource group settings
  location            = azurerm_resource_group.xtdb_infra.location
  resource_group_name = azurerm_resource_group.xtdb_infra.name

  # Cluster name and prefix
  cluster_name = var.aks_cluster_name
  prefix       = var.aks_cluster_name


  # Identity
  identity_type = "UserAssigned"
  identity_ids  = [azurerm_user_assigned_identity.xtdb_infra.id]

  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  # Node pools
  node_pools = {
    application = {
      name       = "xtdbpool"
      mode       = "User"
      vm_size    = var.aks_application_pool_vm_size
      node_count = var.aks_application_pool_node_count
      zones      = var.aks_application_pool_availability_zones
      labels = {
        "nodepool" = "xtdbpool"
      }
    }
  }

  # Application insights
  log_analytics_workspace_enabled = false

  # Network
  rbac_aad = false
}

# Federated Identity Credential for AKS 
resource "azurerm_federated_identity_credential" "xtdb_aks" {
  name                = "xtdb-aks-worker"
  resource_group_name = var.resource_group_name
  audience            = ["api://AzureADTokenExchange"]
  issuer              = module.aks.oidc_issuer_url
  parent_id           = azurerm_user_assigned_identity.xtdb_infra.id
  subject             = "system:serviceaccount:${var.kubernetes_namespace}:${var.kubernetes_service_account_name}"
}

