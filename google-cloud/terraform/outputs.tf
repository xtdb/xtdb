output "project_id" {
  value = var.project_id
}

output "bucket_name" {
  value = module.xtdb_storage.name
}

output "service_account_email" {
  value = google_service_account.xtdb_service_account.email
}
