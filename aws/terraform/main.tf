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
  version = "5.19.0"

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
    coredns = {
      most_recent       = true
    }

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
    
    aws-ebs-csi-driver     = {
      most_recent       = true
      before_compute    = false
      service_account_role_arn = module.irsa_ebs_csi.iam_role_arn
    }
  }

  eks_managed_node_groups = {
    application = {
      name           = "xtdbpool"
      instance_types = [var.application_node_pool_machine_type]
      min_size       = var.application_node_pool_min_count
      max_size       = var.application_node_pool_max_count
      desired_size   = var.application_node_pool_desired_count

      # Mount instance store volumes in RAID-0 for kubelet and containerd
      # https://github.com/awslabs/amazon-eks-ami/blob/master/doc/USER_GUIDE.md#raid-0-for-kubelet-and-containerd-raid0
      # We recommend using IO optimized instances for this configuration, so want to ensure that the instance store is used for the RAID0
      pre_bootstrap_user_data = <<-EOT
          #!/usr/bin/env bash
          # Mount instance store volumes in RAID-0 for kubelet and containerd
          # https://github.com/awslabs/amazon-eks-ami/blob/master/doc/USER_GUIDE.md#raid-0-for-kubelet-and-containerd-raid0

          /bin/setup-local-disks raid0
        EOT
        
      cloudinit_pre_nodeadm = [
        {
          content_type = "application/node.eks.aws"
          content      = <<-EOT
            ---
            apiVersion: node.eks.aws/v1alpha1
            kind: NodeConfig
            spec:
              instance:
                localStorage:
                  strategy: RAID0
          EOT
        }
      ]
      
      labels = {
        "node_pool" = "xtdbpool"
      }

      iam_role_additional_policies = { AmazonEBSCSIDriverPolicy = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy" }
    }
  }

  tags = {
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}

# Required for using EBS CSI driver + volumes
module "irsa_ebs_csi" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "5.52.2"

  create_role                   = true
  role_name                     = "AmazonEKSTFEBSCSIRole-${module.xtdb_eks.cluster_name}"
  provider_url                  = module.xtdb_eks.oidc_provider
  role_policy_arns              = ["arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"]
  oidc_fully_qualified_subjects = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
}
