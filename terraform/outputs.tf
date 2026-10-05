output "kafka_raw_bucket_name" {
  description = "Nom du bucket S3 pour les données Kafka"
  value       = module.aws.kafka_raw_bucket_name
}

output "kafka_iam_access_key_id" {
  description = "Access Key ID pour le connecteur Kafka (sensible)"
  value       = module.aws.kafka_iam_access_key_id
  sensitive   = true
}

output "kafka_iam_secret_access_key" {
  description = "Secret Access Key pour le connecteur Kafka (sensible)"
  value       = module.aws.kafka_iam_secret_access_key
  sensitive   = true
}

output "databricks_workspace_url" {
  description = "URL du workspace Databricks"
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "ID du workspace Databricks"
  value       = module.databricks.workspace_id
}

output "databricks_metastore_id" {
  description = "ID du metastore (existant)"
  value       = module.databricks.metastore_id
}

output "bronze_catalog_name" {
  description = "Nom du catalog bronze"
  value       = module.databricks.catalog_name
}

output "kafka_schema_name" {
  description = "Nom du schema kafka"
  value       = module.databricks.schema_name
}
output "ecommerce_catalog_name" {
  description = "Nom du catalog medallion (bronze / silver / gold)"
  value       = module.databricks.ecommerce_catalog_name
}

output "databricks_cluster_id" {
  description = "ID du cluster Databricks (pour le bundle)"
  value       = module.databricks.cluster_id
}
