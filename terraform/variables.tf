variable "project_name" {
  description = "Nom du projet utilisé pour nommer les ressources"
  type        = string
}

variable "aws_region" {
  description = "Région AWS"
  type        = string
  default     = "us-west-2"
}

variable "workspace_name" {
  description = "Nom du workspace Databricks"
  type        = string
  default     = "my-data-workspace"
}

variable "databricks_account_id" {
  description = "ID du compte Databricks (Account Console) – UUID"
  type        = string
  sensitive   = true
}

variable "databricks_aws_account_id" {
  description = "ID du compte AWS de Databricks (pour le principal du rôle IAM)"
  type        = string
  default     = "414351767826"
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