terraform {
  required_version = ">=1.3"

  backend "s3" {
    bucket         = "xtdb-bench-terraform-state"
    key            = "bench/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "xtdb-bench-terraform-locks"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5.0, < 4.0.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  profile = "xtdb-bench" 
}

module "xtdb_aws" {
  source = "../../../../aws/terraform"
  
  aws_region = "us-east-1"

  # S3 Storage
  s3_bucket_name = "xtdb-bench-bucket"
  s3_acl         = "private"

  # VPC
  vpc_name               = "xtdb-vpc"
  vpc_cidr               = "10.0.0.0/16"
  vpc_availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]
  vpc_public_subnets     = ["10.0.4.0/24", "10.0.5.0/24", "10.0.6.0/24"]

  # EKS Cluster
  eks_cluster_name                     = "xtdb-bench-cluster"
  eks_cluster_version                  = "1.32"
  eks_public_access                    = true
  eks_enable_creator_admin_permissions = true
  eks_create_cloudwatch_log_group      = true

  ## EKS: Application Node Pool
  application_node_pool_machine_type  = "i3.large"
  application_node_pool_min_count     = 3
  application_node_pool_max_count     = 3
  application_node_pool_desired_count = 3
}

# Create IAM role for XTDB service account using IRSA (IAM Roles for Service Accounts)
module "xtdb_irsa_role" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "5.52.2"

  create_role                   = true
  role_name                     = "xtdb-service-role"
  provider_url                  = module.xtdb_aws.oidc_provider
  role_policy_arns              = [module.xtdb_aws.s3_access_policy_arn]
  oidc_fully_qualified_subjects = ["system:serviceaccount:cloud-benchmark:xtdb-service-account"]
}

output "service_account_role_arn" {
  value = module.xtdb_irsa_role.iam_role_arn
}
