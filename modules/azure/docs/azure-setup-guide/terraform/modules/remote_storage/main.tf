
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

# Service Bus & Eventgrid Setup
resource "azurerm_eventgrid_system_topic" "xtdb_storage" {
  name                = "xtdbstorage-system-topic"
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name

  source_arm_resource_id = azurerm_storage_account.xtdb_storage.id
  topic_type             = "Microsoft.Storage.StorageAccounts"
}

resource "azurerm_servicebus_namespace" "xtdb_storage" {
  name                = var.service_bus_namespace
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
  sku                 = var.service_bus_sku
}

resource "azurerm_servicebus_topic" "xtdb_storage" {
  name                = "xtdbstorage-servicebus-topic"
  namespace_id        = azurerm_servicebus_namespace.xtdb_storage.id
  default_message_ttl = var.service_bus_message_ttl
}

resource "azurerm_eventgrid_system_topic_event_subscription" "xtdb_storage" {
  name                          = "xtdbstorage-system-topic-servicebus-topic-subscription"
  system_topic                  = azurerm_eventgrid_system_topic.xtdb_storage.name
  resource_group_name           = var.resource_group_name
  event_delivery_schema         = "EventGridSchema"
  service_bus_topic_endpoint_id = azurerm_servicebus_topic.xtdb_storage.id
}

# Role Assignments
resource "azurerm_role_assignment" "xtdb_storage_blob_contributor" {
  principal_id         = var.user_assigned_identity_principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.xtdb_storage.id
}

resource "azurerm_role_assignment" "xtdb_storage_eventgrid_contributor" {
  principal_id         = var.user_assigned_identity_principal_id
  role_definition_name = "EventGrid Contributor"
  scope                = azurerm_eventgrid_system_topic.xtdb_storage.id
}

resource "azurerm_role_assignment" "xtdb_storage_servicebus_contributor" {
  principal_id         = var.user_assigned_identity_principal_id
  role_definition_name = "Azure Service Bus Data Owner"
  scope                = azurerm_servicebus_namespace.xtdb_storage.id
}
