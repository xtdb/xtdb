# Resource group setup

resource "azurerm_resource_group" "cloud_benchmark" {
  name     = "cloud-benchmark-resources"
  location = "West Europe"
}

resource "azurerm_virtual_network" "cloud_benchmark" {
  name                = "cloud-benchmark-network"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  location            = azurerm_resource_group.cloud_benchmark.location
  address_space       = ["10.0.0.0/16"]
}

# Blob Storage Configuration

resource "azurerm_storage_account" "cloud_benchmark" {
  name                     = "xtdbazurebenchmark"
  resource_group_name      = azurerm_resource_group.cloud_benchmark.name
  location                 = azurerm_resource_group.cloud_benchmark.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "cloud_benchmark" {
  name                  = "xtdbazurebenchmarkcontainer"
  storage_account_name  = azurerm_storage_account.cloud_benchmark.name
  container_access_type = "private"
}

# Fileshare Configuration

resource "azurerm_storage_share" "cloud_benchmark" {
  name                 = "cloudbenchmarkshare"
  storage_account_name = azurerm_storage_account.cloud_benchmark.name
  quota                = 50
}

# Service Bus Setup
resource "azurerm_eventgrid_system_topic" "cloud_benchmark" {
  name                = "cloud-benchmark-system-topic"
  location            = azurerm_resource_group.cloud_benchmark.location
  resource_group_name = azurerm_resource_group.cloud_benchmark.name

  source_arm_resource_id = azurerm_storage_account.cloud_benchmark.id
  topic_type             = "Microsoft.Storage.StorageAccounts"
}

resource "azurerm_servicebus_namespace" "cloud_benchmark" {
  name                = "cloud-benchmark-eventbus"
  location            = azurerm_resource_group.cloud_benchmark.location
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  sku                 = "Standard"
}

resource "azurerm_servicebus_topic" "cloud_benchmark" {
  name                = "cloud-benchmark-servicebus-topic"
  namespace_id        = azurerm_servicebus_namespace.cloud_benchmark.id
  default_message_ttl = "PT5M"
}

resource "azurerm_eventgrid_system_topic_event_subscription" "cloud_benchmark" {
  name                          = "cloud-benchmark-system-topic-servicebus-topic-subscription"
  system_topic                  = azurerm_eventgrid_system_topic.cloud_benchmark.name
  resource_group_name           = azurerm_resource_group.cloud_benchmark.name
  event_delivery_schema         = "EventGridSchema"
  service_bus_topic_endpoint_id = azurerm_servicebus_topic.cloud_benchmark.id
}

# Event Hub Configuration

resource "azurerm_eventhub_namespace" "cloud_benchmark" {
  name                = "cloud-benchmark-eventhub-namespace"
  location            = azurerm_resource_group.cloud_benchmark.location
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "cloud_benchmark" {
  name                = "cloud-benchmark-eventhub-topic-${var.eventhub_topic_suffix}"
  namespace_name      = azurerm_eventhub_namespace.cloud_benchmark.name
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  partition_count     = 1
  message_retention   = 7
}

# Metrics Config
resource "azurerm_log_analytics_workspace" "cloud_benchmark" {
  name                = "cloud-benchmark-log-analytics-workspace"
  location            = azurerm_resource_group.cloud_benchmark.location
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# User Assigned Identity & Roles
resource "azurerm_user_assigned_identity" "cloud_benchmark" {
  location            = azurerm_resource_group.cloud_benchmark.location
  name                = "cloud-benchmark-identity"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
}

resource "azurerm_role_assignment" "cloud_benchmark" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "AcrPull"
  scope                = azurerm_resource_group.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_monitoring" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "Monitoring Metrics Publisher"
  scope                = azurerm_resource_group.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_blob_contributor" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_eventgrid_contributor" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "EventGrid Contributor"
  scope                = azurerm_eventgrid_system_topic.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_servicebus_contributor" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "Azure Service Bus Data Owner"
  scope                = azurerm_servicebus_namespace.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_eventhub_send" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "Azure Event Hubs Data Sender"
  scope                = azurerm_eventhub.cloud_benchmark.id
}

resource "azurerm_role_assignment" "cloud_benchmark_eventhub_receive" {
  principal_id         = azurerm_user_assigned_identity.cloud_benchmark.principal_id
  role_definition_name = "Azure Event Hubs Data Receiver"
  scope                = azurerm_eventhub.cloud_benchmark.id
}

# Container App Config
resource "azurerm_container_registry" "acr" {
  name                = "cloudbenchmarkregistry"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  location            = azurerm_resource_group.cloud_benchmark.location
  sku                 = "Basic"
  admin_enabled       = true
}

resource "azurerm_container_app_environment" "cloud_benchmark" {
  name                       = "cloud-benchmark-container-app-environment"
  location                   = azurerm_resource_group.cloud_benchmark.location
  resource_group_name        = azurerm_resource_group.cloud_benchmark.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.cloud_benchmark.id
}

## Fileshare Storage
resource "azurerm_container_app_environment_storage" "cloud_benchmark" {
  name                         = "app-persistent-storage"
  container_app_environment_id = azurerm_container_app_environment.cloud_benchmark.id
  account_name                 = azurerm_storage_account.cloud_benchmark.name
  share_name                   = azurerm_storage_share.cloud_benchmark.name
  access_key                   = azurerm_storage_account.cloud_benchmark.primary_access_key
  access_mode                  = "ReadWrite"
}

resource "azurerm_container_app" "cloud_benchmark_single_node" {
  count                        = var.run_single_node ? 1 : 0
  name                         = "cloud-benchmark-single-node"
  resource_group_name          = azurerm_resource_group.cloud_benchmark.name
  container_app_environment_id = azurerm_container_app_environment.cloud_benchmark.id
  revision_mode                = "Single"

  identity {
    type = "UserAssigned"
    identity_ids = [
      azurerm_user_assigned_identity.cloud_benchmark.id
    ]
  }

  registry {
    server   = "cloudbenchmarkregistry.azurecr.io"
    identity = azurerm_user_assigned_identity.cloud_benchmark.id
  }


  template {
    max_replicas = 1
    min_replicas = 1

    container {
      image = "cloudbenchmarkregistry.azurecr.io/xtdb-azure-bench:latest"
      name  = "cloud-benchmark"

      cpu    = 2
      memory = "4Gi"

      env {
        name  = "AUCTIONMARK_DURATION"
        value = var.auctionmark_duration
      }

      env {
        name  = "AUCTIONMARK_SCALE_FACTOR"
        value = var.auctionmark_scale_factor
      }

      env {
        name  = "AUCTIONMARK_LOAD_PHASE"
        value = true
      }

      env {
        name  = "AUCTIONMARK_LOAD_PHASE_ONLY"
        value = false
      }

      env {
        name  = "XTDB_LOCAL_DISK_CACHE"
        value = "/var/lib/xtdb/disk-cache/cache-1"
      }

      env {
        name  = "XTDB_AZURE_USER_MANAGED_IDENTITY_CLIENT_ID"
        value = azurerm_user_assigned_identity.cloud_benchmark.client_id
      }

      env {
        name  = "XTDB_AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.cloud_benchmark.name
      }

      env {
        name  = "XTDB_AZURE_STORAGE_CONTAINER"
        value = azurerm_storage_container.cloud_benchmark.name
      }

      env {
        name  = "XTDB_AZURE_SERVICE_BUS_NAMESPACE"
        value = azurerm_servicebus_namespace.cloud_benchmark.name
      }

      env {
        name  = "XTDB_AZURE_SERVICE_BUS_TOPIC_NAME"
        value = azurerm_servicebus_topic.cloud_benchmark.name
      }

      env {
        name  = "XTDB_AZURE_EVENTHUB_NAMESPACE"
        value = azurerm_eventhub_namespace.cloud_benchmark.name
      }

      env {
        name  = "XTDB_AZURE_EVENTHUB_NAME"
        value = azurerm_eventhub.cloud_benchmark.name
      }

      env {
        name  = "CLOUD_PLATFORM_NAME"
        value = "Azure"
      }

      # env {
      #   name = "SLACK_WEBHOOK_URL"
      #   value = var.slack_webhook_url
      # }

      volume_mounts {
        name = "app-persistent-storage"
        path = "/var/lib/xtdb"
      }
    }

    volume {
      name         = "app-persistent-storage"
      storage_name = azurerm_container_app_environment_storage.cloud_benchmark.name
      storage_type = "AzureFile"
    }
  }
}

# TODO: Multi node config could probably be added here - taking inspiration 
# from the above. Generally speaking, similar to how I'd handle in kubernetes:
# - InitContainer that runs first which ONLY runs load phase (ie, AUCTIONMARK_LOAD_PHASE_ONLY = true)
# - Can run a number of parallel containers that point to different local disk cache paths
