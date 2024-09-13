# Infra config & Tiers
resource_group_location             = "East US"
storage_account_tier                = "Standard"
container_app_workload_profile_type = "D4"

# Must be set & globally unique
storage_account_name  = ""

# Size of persistent disk backing the Kafka node
kafka_persisent_data_max_size_gb = 100

# Size of the mounted disk cache volume for the XTDB node
local_disk_cache_max_size_gb = 50
