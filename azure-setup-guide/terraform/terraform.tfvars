# Infra config & Tiers
resource_group_location             = "West Europe"
storage_account_tier                = "Standard"
service_bus_sku                     = "Standard"
container_app_workload_profile_type = "D4"

# Must be set & globally unique
storage_account_name  = ""
service_bus_namespace = ""

# How long to keep messages on the service bus watching for filechanges on the node
service_bus_message_ttl = "PT30M"

# Size of persistent disk backing the Kafka node
kafka_persisent_data_max_size_gb = 5120

# Size of the mounted disk cache volume for the XTDB node
local_disk_cache_max_size_gb = 50
