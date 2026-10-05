output "kafka_raw_bucket_arn" {
  description = "ARN du bucket S3 pour les données brutes Kafka"
  value       = aws_s3_bucket.kafka_raw.arn
}

output "kafka_raw_bucket_name" {
  description = "Nom du bucket S3 pour les données brutes Kafka"
  value       = aws_s3_bucket.kafka_raw.bucket
}

output "databricks_artifacts_bucket" {
  description = "Nom du bucket S3 pour les artefacts Databricks"
  value       = aws_s3_bucket.databricks_artifacts.bucket
}

output "databricks_artifacts_bucket_arn" {
  description = "ARN du bucket S3 pour les artefacts Databricks"
  value       = aws_s3_bucket.databricks_artifacts.arn
}

output "databricks_cross_account_role_arn" {
  description = "ARN du rôle IAM pour Databricks"
  value       = aws_iam_role.databricks_cross_account.arn
}

output "databricks_instance_profile_arn" {
  description = "ARN du instance profile pour Databricks"
  value       = aws_iam_instance_profile.databricks_worker.arn
}

output "vpc_id" {
  description = "ID du VPC"
  value       = aws_vpc.main.id
}

output "private_subnet_ids" {
  description = "Liste des IDs des sous-réseaux privés"
  value       = aws_subnet.private[*].id
}

output "security_group_id" {
  description = "ID du security group pour Databricks"
  value       = aws_security_group.databricks.id
}

output "kafka_iam_access_key_id" {
  description = "Access Key ID pour le connecteur Kafka (sensible)"
  value       = aws_iam_access_key.kafka_s3.id
  sensitive   = true
}

output "kafka_iam_secret_access_key" {
  description = "Secret Access Key pour le connecteur Kafka (sensible)"
  value       = aws_iam_access_key.kafka_s3.secret
  sensitive   = true
}