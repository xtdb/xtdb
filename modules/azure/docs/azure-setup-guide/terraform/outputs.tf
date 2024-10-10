output "user_managed_identity_client_id" {
  value = azurerm_user_assigned_identity.xtdb_infra.client_id
}

output "storage_account_name" {
  value = module.remote_storage.storage_account_name
}

output "storage_account_container" {
  value = module.remote_storage.storage_account_container_name
}

output "kubernetes_namespace" {
  value = module.aks.kubernetes_namespace
}

output "kubernetes_service_account_name" {
  value = module.aks.kubernetes_service_account_name
}
