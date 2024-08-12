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

# App container environment setup
resource "azurerm_container_app_environment" "xtdb_infra" {
  name                = "xtdb-infra-app-environment"
  location            = azurerm_resource_group.xtdb_infra.location
  resource_group_name = azurerm_resource_group.xtdb_infra.name

  workload_profile {
    name                  = "xtdb-infra"
    workload_profile_type = var.container_app_workload_profile_type
    maximum_count         = 2
    minimum_count         = 1
  }
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

  # Config options for service bus
  # Service bus namespace name must be unique across azure.
  service_bus_namespace   = var.service_bus_namespace
  service_bus_sku         = var.service_bus_sku
  service_bus_message_ttl = var.service_bus_message_ttl
}

# Kafka Config
module "kafka_app" {
  source = "./modules/kafka_app"

  resource_group_name                = azurerm_resource_group.xtdb_infra.name
  resource_group_location            = azurerm_resource_group.xtdb_infra.location
  container_app_environment_id       = azurerm_container_app_environment.xtdb_infra.id
  storage_account_primary_access_key = module.remote_storage.storage_account_primary_access_key
  storage_account_name               = module.remote_storage.storage_account_name

  # Kafka options
  kafka_persisent_data_max_size_gb = var.kafka_persisent_data_max_size_gb
}

# XTDB Config

module "xtdb_app" {
  source = "./modules/xtdb_app"

  resource_group_name                = azurerm_resource_group.xtdb_infra.name
  container_app_environment_id       = azurerm_container_app_environment.xtdb_infra.id
  storage_account_primary_access_key = module.remote_storage.storage_account_primary_access_key
  user_assigned_identity_id          = azurerm_user_assigned_identity.xtdb_infra.id
  user_assigned_identity_client_id   = azurerm_user_assigned_identity.xtdb_infra.client_id

  # XTDB Config Options

  # Remote Storage Options - using our created remote storage module
  storage_account_name           = module.remote_storage.storage_account_name
  storage_account_container_name = module.remote_storage.storage_account_container_name
  service_bus_namespace          = module.remote_storage.service_bus_namespace
  service_bus_topic              = module.remote_storage.service_bus_topic
  local_disk_cache_max_size_gb   = var.local_disk_cache_max_size_gb

  # Kafka Transaction Log options - using our created kafka module
  kafka_bootstrap_servers = module.kafka_app.kafka_bootstrap_server
  xtdb_topic_name         = "xtdb-transaction-log"
}
