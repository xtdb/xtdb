output "storage_account_container" {
  value = keys(module.xtdb_storage.containers)[0]
}

output "storage_account_name" {
  value = module.xtdb_storage.name
}

output "user_managed_identity_client_id" {
  value = azurerm_user_assigned_identity.xtdb_infra.client_id
}
