
# Blob Storage Configuration
resource "azurerm_storage_account" "xtdb_storage" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.resource_group_location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_account_replication_type
}

resource "azurerm_storage_container" "xtdb_storage" {
  name                  = "xtdbstorage"
  storage_account_name  = azurerm_storage_account.xtdb_storage.name
  container_access_type = "private"
}

# Role Assignments
resource "azurerm_role_assignment" "xtdb_storage_blob_contributor" {
  principal_id         = var.user_assigned_identity_principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.xtdb_storage.id
}
