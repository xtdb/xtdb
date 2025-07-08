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
  private_subnets      = var.vpc_private_subnets

  enable_dns_support   = true
  enable_dns_hostnames = true
  map_public_ip_on_launch = false
  enable_nat_gateway   = true
  single_nat_gateway  = true
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

  cluster_endpoint_private_access          = true
  cluster_endpoint_public_access           = var.eks_public_access
  enable_cluster_creator_admin_permissions = var.eks_enable_creator_admin_permissions
  create_cloudwatch_log_group              = var.eks_create_cloudwatch_log_group

  cluster_security_group_additional_rules = {
    ingress_bastion = {
      description              = "Bastion access to EKS API"
      protocol                 = "tcp"
      from_port                = 443
      to_port                  = 443
      type                     = "ingress"
      source_security_group_id = aws_security_group.bastion.id
    }
  }

  # Cluster authentication configuration
  authentication_mode = "API"
  enable_irsa        = true

  access_entries = {
    # Grant cluster admin access to the bastion
    bastion = {
      principal_arn     = aws_iam_role.bastion.arn
      type             = "STANDARD"
      kubernetes_groups = ["system:masters"]
      kubernetes_username = "bastion-admin"
      policy_associations = {
        admin = {
          policy_arn = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
          access_scope = {
            type = "cluster"
          }
        }
      }
    }
  }

  vpc_id     = module.xtdb_vpc.vpc_id
  subnet_ids = module.xtdb_vpc.private_subnets

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

# Create security group for bastion host
resource "aws_security_group" "bastion" {
  name_prefix = "xtdb-bastion-"
  vpc_id      = module.xtdb_vpc.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Consider restricting to your IP
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name      = "xtdb-bastion"
    terraform = "true"
    managed_by = "XTDB Terraform"
  }
}

# Create IAM role for bastion host
data "aws_iam_policy_document" "bastion_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bastion" {
  name_prefix        = "xtdb-bastion-"
  assume_role_policy = data.aws_iam_policy_document.bastion_assume_role.json

  tags = {
    terraform  = "true"
    managed_by = "XTDB Terraform"
  }
}

resource "aws_iam_role_policy" "bastion_eks_access" {
  name = "eks-access"
  role = aws_iam_role.bastion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "eks:ListClusters",
          "eks:AccessKubernetesApi",
          "eks:ListNodegroups",
          "eks:DescribeNodegroup",
          "eks:ListUpdates",
          "eks:ListFargateProfiles"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "bastion" {
  name_prefix = "xtdb-bastion-"
  role        = aws_iam_role.bastion.name
}

# Create bastion host
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

resource "aws_instance" "bastion" {
  ami                         = data.aws_ami.amazon_linux_2.id
  instance_type               = "t3.micro"
  subnet_id                   = module.xtdb_vpc.public_subnets[0]
  vpc_security_group_ids      = [aws_security_group.bastion.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.bastion.name
  key_name                    = var.bastion_key_name

  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              yum install -y git jq
              
              # Install kubectl
              curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
              install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
              
              # Install AWS CLI
              curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
              yum install -y unzip
              unzip awscliv2.zip
              ./aws/install
              
              # Install Helm
              curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
              chmod 700 get_helm.sh
              ./get_helm.sh
              EOF

  tags = {
    Name      = "xtdb-bastion"
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
