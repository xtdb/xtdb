# Sets up storage bucket used by XTDB
# For more configuration options, see:
# https://registry.terraform.io/modules/terraform-aws-modules/s3-bucket/aws/latest
module "xtdb_storage" {
  source  = "terraform-aws-modules/s3-bucket/aws"
  version = "4.5.0"

  bucket = var.s3_bucket_name
  acl    = var.s3_acl

  control_object_ownership = true
  object_ownership         = "ObjectWriter"

  versioning = {
    enabled = false
  }

  tags = {
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}

# Creates an IAM Policy for S3 Access
# For more configuration options, see:
# https://registry.terraform.io/modules/terraform-aws-modules/iam/aws/latest/submodules/iam-policy
module "xtdb_s3_policy" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-policy"
  version = "5.52.2"

  name        = "xtdb-s3-access-policy"
  description = "Policy granting XTDB access to the specified S3 bucket"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:AbortMultipartUpload",
          "s3:ListBucketMultipartUploads"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
  tags = {
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}

# Sets up VPC used by EKS cluster
# For more configuration options, see:
# https://registry.terraform.io/modules/terraform-aws-modules/vpc/aws/latest
module "xtdb_vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.8.1"

  name                 = var.vpc_name
  cidr                 = var.vpc_cidr
  azs                  = var.vpc_availability_zones
  public_subnets       = var.vpc_public_subnets
  enable_dns_support   = true
  enable_dns_hostnames = true
  map_public_ip_on_launch = true
  enable_nat_gateway   = false
  enable_vpn_gateway   = false

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }

  tags = {
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}

# Sets up an EKS cluster to be used by XTDB
# For more configuration options, see:
# https://registry.terraform.io/modules/terraform-aws-modules/eks/aws/latest
module "xtdb_eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "20.33.1"

  cluster_name    = var.eks_cluster_name
  cluster_version = var.eks_cluster_version

  cluster_endpoint_public_access           = var.eks_public_access
  enable_cluster_creator_admin_permissions = var.eks_enable_creator_admin_permissions
  create_cloudwatch_log_group              = var.eks_create_cloudwatch_log_group

  vpc_id     = module.xtdb_vpc.vpc_id
  subnet_ids = module.xtdb_vpc.public_subnets

  # Optional
  cluster_compute_config = {
    enabled    = true
    node_pools = ["system"]
  }

  cluster_addons = {
    eks-pod-identity-agent = {
      most_recent       = true
      before_compute    = true
    }
    kube-proxy             = {
      most_recent       = true
      before_compute    = true
    }
    vpc-cni                = {
      most_recent       = true
      before_compute    = true
    }
  }

  eks_managed_node_groups = {
    application = {
      name           = "xtdbpool"
      instance_types = [var.application_node_pool_machine_type]
      min_size       = var.application_node_pool_min_count
      max_size       = var.application_node_pool_max_count
      desired_size   = var.application_node_pool_desired_count

      # EBS setup
      ebs_optimized = true
      block_device_mappings = [
        {
          device_name = "/dev/xvdf"
          ebs = {
            volume_size           = 100
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      ]
      labels = {
        "node_pool" = "xtdbpool"
      }
    }
  }

  tags = {
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}
