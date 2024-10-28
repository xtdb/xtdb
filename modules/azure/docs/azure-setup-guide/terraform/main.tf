# Resource group setup
resource "azurerm_resource_group" "xtdb_infra" {
  name     = "xtdb-resources"
  location = var.resource_group_location
}

# User Assigned Identity to assign roles to resources
resource "azurerm_user_assigned_identity" "xtdb_infra" {
  location            = azurerm_resource_group.xtdb_infra.location
  name                = "xtdb-identity"
  resource_group_name = azurerm_resource_group.xtdb_infra.name
}

# Setup for remote storage module
module "remote_storage" {
  source = "./modules/remote_storage"

  resource_group_name                 = azurerm_resource_group.xtdb_infra.name
  resource_group_location             = azurerm_resource_group.xtdb_infra.location
  user_assigned_identity_principal_id = azurerm_user_assigned_identity.xtdb_infra.principal_id

  # Config options for storage account
  # Storage account name be unique across azure.
  storage_account_name             = var.storage_account_name
  storage_account_tier             = var.storage_account_tier
  storage_account_replication_type = "LRS"
}

# AKS configuration
module "aks" {
  source = "./modules/aks"

  resource_group_name       = azurerm_resource_group.xtdb_infra.name
  resource_group_location   = azurerm_resource_group.xtdb_infra.location
  user_assigned_identity_id = azurerm_user_assigned_identity.xtdb_infra.id

  # Kubernetes namespace & service account names
  kubernetes_namespace            = var.kubernetes_namespace
  kubernetes_service_account_name = var.kubernetes_service_account_name

  # Config options for AKS cluster
  aks_system_pool_vm_size      = var.aks_system_pool_vm_size
  aks_application_pool_vm_size = var.aks_application_pool_vm_size
}
