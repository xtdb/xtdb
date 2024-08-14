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

resource "azurerm_container_registry" "acr" {
  name                = "cloudbenchmarkregistry"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  location            = azurerm_resource_group.cloud_benchmark.location
  sku                 = "Basic"
  admin_enabled       = true
}

locals {
  namespace            = "cloud-benchmark"
  service_account_name = "cloud-benchmark-account"
}

## Kubernetes Cluster
resource "azurerm_kubernetes_cluster" "cloud_benchmark" {
  count               = var.run_single_node ? 1 : 0
  name                = "cloud-benchmark-cluster"
  location            = "East US"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  dns_prefix          = "cloud-benchmark-cluster"

  default_node_pool {
    name       = "default"
    node_count = 1
    vm_size    = "Standard_D2_v2"
    upgrade_settings {
      drain_timeout_in_minutes      = 0
      max_surge                     = "10%"
      node_soak_duration_in_minutes = 0
    }
  }

  identity {
    type = "UserAssigned"
    identity_ids = [
      azurerm_user_assigned_identity.cloud_benchmark.id
    ]
  }

  oidc_issuer_enabled       = true
  workload_identity_enabled = true
}

resource "azurerm_role_assignment" "cloud_benchmark_cluster_role" {
  count                            = var.run_single_node ? 1 : 0
  principal_id                     = azurerm_kubernetes_cluster.cloud_benchmark[0].kubelet_identity[0].object_id
  role_definition_name             = "AcrPull"
  scope                            = azurerm_container_registry.acr.id
  skip_service_principal_aad_check = true
}

resource "azurerm_federated_identity_credential" "cloud_benchmark" {
  count               = var.run_single_node ? 1 : 0
  name                = "cloud-benchmark-worker"
  resource_group_name = azurerm_resource_group.cloud_benchmark.name
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.cloud_benchmark[0].oidc_issuer_url
  parent_id           = azurerm_user_assigned_identity.cloud_benchmark.id
  subject             = "system:serviceaccount:${local.namespace}:${local.service_account_name}"
}

resource "local_file" "kubeconfig" {
  count      = var.run_single_node ? 1 : 0
  depends_on = [azurerm_kubernetes_cluster.cloud_benchmark]
  filename   = "kubeconfig"
  content    = azurerm_kubernetes_cluster.cloud_benchmark[0].kube_config_raw
}

provider "kubernetes" {
  config_path = local_file.kubeconfig[0].filename
}

resource "kubernetes_namespace" "cloud_benchmark" {
  count = var.run_single_node ? 1 : 0
  metadata {
    name = local.namespace
  }
}

resource "kubernetes_service_account" "cloud_benchmark" {
  count = var.run_single_node ? 1 : 0
  metadata {
    name      = local.service_account_name
    namespace = kubernetes_namespace.cloud_benchmark[0].metadata[0].name
  }
}

resource "kubernetes_persistent_volume_claim" "cloud_benchmark" {
  count = var.run_single_node ? 1 : 0
  metadata {
    name      = "cloud-benchmark-pvc"
    namespace = kubernetes_namespace.cloud_benchmark[0].metadata[0].name
  }

  spec {
    access_modes = ["ReadWriteOnce"]
    storage_class_name = "managed-csi"
    resources {
      requests = {
        storage = "50Gi"
      }
    }
  }

  wait_until_bound = false
}


resource "kubernetes_deployment" "cloud_benchmark" {
  count = var.run_single_node ? 1 : 0
  metadata {
    name      = "cloud-benchmark"
    namespace = kubernetes_namespace.cloud_benchmark[0].metadata[0].name
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "cloud-benchmark"
      }
    }
    template {
      metadata {
        labels = {
          app                           = "cloud-benchmark"
          "azure.workload.identity/use" = "true"
        }
      }
      spec {
        service_account_name = kubernetes_service_account.cloud_benchmark[0].metadata[0].name

        container {
          image = "cloudbenchmarkregistry.azurecr.io/xtdb-azure-bench:latest"
          name  = "cloud-benchmark"

          volume_mount {
            name = "volume"
            mount_path = "/var/lib/xtdb"
          }

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
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = "${azurerm_eventhub_namespace.cloud_benchmark.name}.servicebus.windows.net:9093"
          }

          env {
            name  = "XTDB_TOPIC_NAME"
            value = azurerm_eventhub.cloud_benchmark.name
          }

          env {
            name  = "CLOUD_PLATFORM_NAME"
            value = "Azure"
          }

          env {
            name  = "SLACK_WEBHOOK_URL"
            value = var.slack_webhook_url
          }
        }

        volume {
          name = "volume"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.cloud_benchmark[0].metadata[0].name
          }
        }
      }
    }
  }
}
