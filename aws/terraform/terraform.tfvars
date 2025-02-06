aws_region = "us-east-1"

# S3 Storage
# s3_bucket_name = "unique-bucket-name"
s3_acl         = "private"

# VPC
vpc_name               = "xtdb-vpc"
vpc_cidr               = "10.0.0.0/16"
vpc_availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]
vpc_public_subnets     = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]

# EKS Cluster
eks_cluster_name                     = "xtdb-cluster"
eks_cluster_version                  = "1.32"
eks_public_access                    = true
eks_enable_creator_admin_permissions = true
eks_create_cloudwatch_log_group      = true

## EKS: Application Node Pool
application_node_pool_machine_type  = "i3.large"
application_node_pool_min_count     = 3
application_node_pool_max_count     = 3
application_node_pool_desired_count = 3
