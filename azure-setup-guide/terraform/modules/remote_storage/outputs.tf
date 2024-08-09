output "storage_account_name" {
  value = azurerm_storage_account.xtdb_storage.name
}

output "storage_account_container_name" {
  value = azurerm_storage_container.xtdb_storage.name
}

output "storage_account_primary_access_key" {
  value = azurerm_storage_account.xtdb_storage.primary_access_key
}

output "service_bus_namespace" {
  value = azurerm_servicebus_namespace.xtdb_storage.name
}

output "service_bus_topic" {
  value = azurerm_servicebus_topic.xtdb_storage.name
}
