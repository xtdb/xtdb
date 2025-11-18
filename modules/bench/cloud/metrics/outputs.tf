output "workspace_id" {
  description = "Log Analytics Workspace resource ID (cluster LAW)"
  value       = data.azurerm_log_analytics_workspace.cluster_law.id
}

output "workspace_name" {
  description = "Log Analytics Workspace name"
  value       = data.azurerm_log_analytics_workspace.cluster_law.name
}

output "resource_group_name" {
  description = "Resource group name"
  value       = var.resource_group_name
}
