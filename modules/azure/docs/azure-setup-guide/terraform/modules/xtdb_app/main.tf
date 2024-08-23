resource "azurerm_storage_share" "xtdb_storage" {
  name                 = "xtdbdiskcache"
  storage_account_name = var.storage_account_name
  quota                = var.local_disk_cache_max_size_gb
}

resource "azurerm_container_app_environment_storage" "xtdb_storage_share" {
  name                         = "xtdb-persistent-storage"
  container_app_environment_id = var.container_app_environment_id
  account_name                 = var.storage_account_name
  share_name                   = azurerm_storage_share.xtdb_storage.name
  access_key                   = var.storage_account_primary_access_key
  access_mode                  = "ReadWrite"
}

resource "azurerm_container_app" "xtdb_app" {
  name                         = "xtdb-node"
  resource_group_name          = var.resource_group_name
  container_app_environment_id = var.container_app_environment_id
  revision_mode                = "Single"

  identity {
    type = "UserAssigned"
    identity_ids = [
      var.user_assigned_identity_id
    ]
  }

  ingress {
    external_enabled           = true
    target_port                = 3000
    allow_insecure_connections = true
    traffic_weight {
      percentage = 100
      latest_revision = true
    }
  }

  template {
    max_replicas = 1
    min_replicas = 1

    container {
      image = "ghcr.io/danmason/xtdb-azure-ea:test"
      name  = "xtdb-node"

      cpu    = 2
      memory = "4Gi"

      env {
        name  = "XTDB_LOCAL_DISK_CACHE"
        value = "/var/lib/xtdb/disk-cache/cache-1"
      }

      env {
        name  = "XTDB_AZURE_STORAGE_ACCOUNT"
        value = var.storage_account_name
      }

      env {
        name  = "XTDB_AZURE_STORAGE_CONTAINER"
        value = var.storage_account_container_name
      }

      env {
        name  = "XTDB_AZURE_USER_MANAGED_IDENTITY_CLIENT_ID"
        value = var.user_assigned_identity_client_id
      }

      env {
        name  = "KAFKA_BOOTSTRAP_SERVERS"
        value = var.kafka_bootstrap_servers
      }

      env {
        name  = "XTDB_TX_TOPIC"
        value = var.xtdb_tx_topic
      }

      env {
        name  = "XTDB_FILES_TOPIC"
        value = var.xtdb_files_topic
      }

      volume_mounts {
        name = "app-persistent-storage"
        path = "/var/lib/xtdb"
      }
    }

    volume {
      name         = "app-persistent-storage"
      storage_name = azurerm_container_app_environment_storage.xtdb_storage_share.name
      storage_type = "AzureFile"
    }
  }
}
