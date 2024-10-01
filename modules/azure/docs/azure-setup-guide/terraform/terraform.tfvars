# Infra config & Tiers
resource_group_location      = "East US"
storage_account_tier         = "Standard"
aks_system_pool_vm_size      = "Standard_D2_v2"
aks_application_pool_vm_size = "Standard_D8_v3"

kubernetes_namespace            = "xtdb-deployment"
kubernetes_service_account_name = "xtdb-service-account"

# Must be set & globally unique
storage_account_name = ""
