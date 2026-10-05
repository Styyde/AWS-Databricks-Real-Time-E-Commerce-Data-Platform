output "workspace_url" {
  description = "URL du workspace Databricks"
  value       = databricks_mws_workspaces.this.workspace_url
}

output "workspace_id" {
  description = "ID du workspace Databricks"
  value       = databricks_mws_workspaces.this.workspace_id
}

output "metastore_id" {
  description = "ID du metastore (data source)"
  value       = data.databricks_metastore.existing.id
}

output "catalog_name" {
  description = "Nom du catalog bronze"
  value       = databricks_catalog.bronze_catalog.name
}

output "schema_name" {
  description = "Nom du schema kafka"
  value       = databricks_schema.bronze_schema.name
}
output "ecommerce_catalog_name" {
  description = "Nom du catalog medallion"
  value       = databricks_catalog.ecommerce.name
}

output "cluster_id" {
  description = "ID du cluster standard"
  value       = databricks_cluster.standard.id
}
