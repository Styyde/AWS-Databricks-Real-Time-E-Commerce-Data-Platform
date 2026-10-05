variable "project_name" {
  description = "Nom du projet pour nommer les ressources"
  type        = string
}

variable "aws_region" {
  description = "Région AWS"
  type        = string
  default     = "us-east-1"
}

variable "databricks_account_id" {
  description = "ID du compte Databricks (utilisé comme External ID pour le rôle IAM)"
  type        = string
  sensitive   = true
}

variable "databricks_aws_account_id" {
  description = "ID du compte AWS appartenant à Databricks (pour le principal du rôle IAM)"
  type        = string
  default     = "414351767826"
}