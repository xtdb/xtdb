output "eks_cluster_name" {
  value = module.xtdb_eks.cluster_name
}

output "s3_bucket_name" {
  value = module.xtdb_storage.s3_bucket_id
}

output "s3_access_policy_arn" {
  value = module.xtdb_s3_policy.arn
}

output "oidc_provider" {
  value = module.xtdb_eks.oidc_provider
}

output "oidc_provider_arn" {
  value = module.xtdb_eks.oidc_provider_arn
}

output "aws_region" {
  value = var.aws_region
}

output "bastion_public_ip" {
  description = "Public IP address of the bastion host"
  value       = aws_instance.bastion.public_ip
}

output "bastion_public_dns" {
  description = "Public DNS name of the bastion host"
  value       = aws_instance.bastion.public_dns
}

output "bastion_role_arn" {
  description = "ARN of the bastion IAM role"
  value       = aws_iam_role.bastion.arn
}
