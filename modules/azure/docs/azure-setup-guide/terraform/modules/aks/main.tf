## Kubernetes Cluster
resource "azurerm_kubernetes_cluster" "xtdb_aks" {
  name                = "xtdb-aks-cluster"
  location            = var.resource_group_location
  resource_group_name = var.resource_group_name
  dns_prefix          = "xtdb-aks-cluster"

  default_node_pool {
    name       = "system"
    node_count = 1
    vm_size    = var.aks_system_pool_vm_size
    upgrade_settings {
      drain_timeout_in_minutes      = 0
      max_surge                     = "10%"
      node_soak_duration_in_minutes = 0
    }
  }

  identity {
    type = "UserAssigned"
    identity_ids = [
      var.user_assigned_identity_id
    ]
  }

  oidc_issuer_enabled       = true
  workload_identity_enabled = true
}

resource "azurerm_kubernetes_cluster_node_pool" "xtdb_aks" {
  name                  = "xtdbpool"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.xtdb_aks.id
  vm_size               = var.aks_application_pool_vm_size
  node_count            = 1
  node_labels           = {
    "nodepool" = "xtdbpool"
  }
}

resource "azurerm_federated_identity_credential" "xtdb_aks" {
  name                = "xtdb-aks-worker"
  resource_group_name = var.resource_group_name
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.xtdb_aks.oidc_issuer_url
  parent_id           = var.user_assigned_identity_id
  subject             = "system:serviceaccount:${var.kubernetes_namespace}:${var.kubernetes_service_account_name}"
}
