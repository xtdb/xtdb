output "storage_account_name" {
  value = azurerm_storage_account.xtdb_storage.name
}

output "storage_account_container_name" {
  value = azurerm_storage_container.xtdb_storage.name
}

output "storage_account_primary_access_key" {
  value = azurerm_storage_account.xtdb_storage.primary_access_key
}
