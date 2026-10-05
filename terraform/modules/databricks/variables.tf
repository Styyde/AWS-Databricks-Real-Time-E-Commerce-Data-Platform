variable "aws_region" {
  description = "Région AWS"
  type        = string
}

variable "databricks_account_id" {
  description = "ID du compte Databricks (Account Console) – UUID"
  type        = string
  sensitive   = true
}

variable "databricks_client_id" {
  description = "Client ID du Service Principal Databricks (Account)"
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Client Secret du Service Principal Databricks (Account)"
  type        = string
  sensitive   = true
}

variable "databricks_workspace_client_id" {
  description = "Client ID du Service Principal Databricks (Workspace)"
  type        = string
  sensitive   = true
}

variable "databricks_workspace_client_secret" {
  description = "Client Secret du Service Principal Databricks (Workspace)"
  type        = string
  sensitive   = true
}

variable "workspace_name" {
  description = "Nom du workspace Databricks"
  type        = string
  default     = "my-data-workspace"
}

variable "bucket_artifacts" {
  description = "Nom du bucket S3 pour les artefacts"
  type        = string
}

variable "kafka_raw_bucket" {
  description = "Nom du bucket S3 contenant les données brutes (Kafka)"
  type        = string
}

variable "cross_account_role_arn" {
  description = "ARN du rôle IAM cross-account"
  type        = string
}

variable "vpc_id" {
  description = "ID du VPC"
  type        = string
}

variable "subnet_ids" {
  description = "Liste des IDs des sous-réseaux privés"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Liste des IDs des security groups"
  type        = list(string)
}
variable "instance_profile_arn" {
  description = "ARN de l'instance profile IAM pour les clusters Databricks"
  type        = string
}