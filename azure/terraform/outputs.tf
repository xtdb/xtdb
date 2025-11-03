output "resource_group_name" {
  value = var.resource_group_name
}

output "storage_account_container" {
  value = keys(module.xtdb_storage.containers)[0]
}

output "storage_account_name" {
  value = module.xtdb_storage.name
}

output "oidc_issuer_url" {
  value = module.aks.oidc_issuer_url
}

output "user_assigned_managed_identity_id" {
  value = azurerm_user_assigned_identity.xtdb_infra.id
}

output "user_assigned_managed_identity_name" {
  value = azurerm_user_assigned_identity.xtdb_infra.name
}

output "user_assigned_managed_identity_client_id" {
  value = azurerm_user_assigned_identity.xtdb_infra.client_id
}

output "log_analytics_workspace_id" {
  value = var.create_log_analytics_workspace ? azurerm_log_analytics_workspace.aks[0].id : (var.log_analytics_workspace != null ? var.log_analytics_workspace.id : null)
}
