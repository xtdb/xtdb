resource "azurerm_storage_share" "kafka_data" {
  name                 = "kafkadata"
  storage_account_name = var.storage_account_name
  quota                = var.kafka_persisent_data_max_size_gb
}

resource "azurerm_container_app_environment_storage" "kafka_app" {
  name                         = "kafka-persistent-storage"
  container_app_environment_id = var.container_app_environment_id
  account_name                 = var.storage_account_name
  share_name                   = azurerm_storage_share.kafka_data.name
  access_key                   = var.storage_account_primary_access_key
  access_mode                  = "ReadWrite"
}

resource "azurerm_container_app" "kafka_app" {
  name                         = "kafka-app"
  revision_mode                = "Single"
  container_app_environment_id = var.container_app_environment_id
  resource_group_name          = var.resource_group_name

  ingress {
    external_enabled           = false
    target_port                = 9092
    exposed_port               = 9092
    transport                  = "tcp"
    traffic_weight {
      percentage = 100
      latest_revision = true 
    }
  }

  template {
    max_replicas = 1
    min_replicas = 1

    container {
      name   = "kafka"
      image  = "confluentinc/cp-kafka:latest"
      cpu    = 1.0
      memory = "2.0Gi"

      env {
        name  = "KAFKA_NODE_ID"
        value = 1
      }

      env {
        name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
        value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      }

      env {
        name  = "KAFKA_LISTENERS"
        value = "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092"
      }

      env {
        name  = "KAFKA_ADVERTISED_LISTENERS"
        value = "PLAINTEXT://kafka-app:29092,PLAINTEXT_HOST://kafka-app:9092"
      }

      env {
        name  = "KAFKA_JMX_PORT"
        value = "9101"
      }

      env {
        name  = "KAFKA_JMX_HOSTNAME"
        value = "localhost"
      }

      env {
        name  = "KAFKA_PROCESS_ROLES"
        value = "broker,controller"
      }

      env {
        name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
        value = "1"
      }

      env {
        name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
        value = "1@localhost:29093"
      }

      env {
        name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
        value = "PLAINTEXT"
      }

      env {
        name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
        value = "CONTROLLER"
      }

      env {
        name  = "CLUSTER_ID"
        value = "q1Sh-9_ISia_zwGINzRvyQ"
      }

      volume_mounts {
        name = "kafka-persistent-storage"
        path = "/var/lib/kafka/data"
      }

    }

    volume {
      name         = "kafka-persistent-storage"
      storage_name = azurerm_container_app_environment_storage.kafka_app.name
      storage_type = "AzureFile"
    }
  }
}
