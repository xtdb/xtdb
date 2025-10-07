output "workspace_id" {
  description = "Log Analytics Workspace resource ID"
  value       = azurerm_log_analytics_workspace.law.id
}

output "table_name" {
  description = "Custom Log Analytics table name"
  value       = var.table_name
}

output "stream_name" {
  description = "DCR stream name to post to"
  value       = local.stream_name
}

output "dce_ingest_endpoint" {
  description = "Data Collection Endpoint ingestion endpoint"
  value       = azurerm_monitor_data_collection_endpoint.dce.logs_ingestion_endpoint
}

output "dcr_immutable_id" {
  description = "Immutable ID of the Data Collection Rule"
  value       = azurerm_monitor_data_collection_rule.dcr.immutable_id
}
