# -----------------------------------------------------------------------------
# PROVIDER DATABRICKS - MWS (Account Console)
# -----------------------------------------------------------------------------
provider "databricks" {
  alias      = "mws"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id

  # Authentification via Service Principal Account
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}
resource "databricks_instance_profile" "this" {
  provider             = databricks.workspace
  instance_profile_arn = var.instance_profile_arn
}
# -----------------------------------------------------------------------------
# RESSOURCES MWS (Workspace)
# -----------------------------------------------------------------------------
resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${var.workspace_name}-storage"
  bucket_name                = var.bucket_artifacts
}

resource "databricks_mws_networks" "this" {
  provider           = databricks.mws
  account_id         = var.databricks_account_id
  network_name       = "${var.workspace_name}-network"
  vpc_id             = var.vpc_id
  subnet_ids         = var.subnet_ids
  security_group_ids = var.security_group_ids
}

resource "databricks_mws_credentials" "this" {
  provider         = databricks.mws
  credentials_name = "${var.workspace_name}-credentials"
  role_arn         = var.cross_account_role_arn
}

resource "databricks_mws_workspaces" "this" {
  provider        = databricks.mws
  account_id      = var.databricks_account_id
  workspace_name  = var.workspace_name
  aws_region      = var.aws_region

  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id
}

# -----------------------------------------------------------------------------
# PROVIDER DATABRICKS - Workspace (pour objets internes au Workspace)
# -----------------------------------------------------------------------------
provider "databricks" {
  alias         = "workspace"
  host          = databricks_mws_workspaces.this.workspace_url
  client_id     = var.databricks_workspace_client_id
  client_secret = var.databricks_workspace_client_secret
}

# -----------------------------------------------------------------------------
# UTILISER LE METASTORE EXISTANT (Niveau Account -> MWS)
# -----------------------------------------------------------------------------
data "databricks_metastore" "existing" {
  provider = databricks.mws
  name     = "metastore_aws_us_west_2"
}

# -----------------------------------------------------------------------------
# ASSIGNER LE METASTORE AU WORKSPACE (Niveau Account -> MWS)
# -----------------------------------------------------------------------------
resource "databricks_metastore_assignment" "this" {
  provider     = databricks.mws
  workspace_id = databricks_mws_workspaces.this.workspace_id
  metastore_id = data.databricks_metastore.existing.id
  
  # default_catalog_name a été retiré ici pour éviter le warning
}

# -----------------------------------------------------------------------------
# DÉFINIR LE CATALOGUE PAR DÉFAUT (Niveau Workspace)
# -----------------------------------------------------------------------------
resource "databricks_default_namespace_setting" "this" {
  provider = databricks.workspace
  namespace {
    value = "main"
  }
  
  # Terraform doit s'assurer que le workspace est rattaché au metastore en premier
  depends_on = [databricks_metastore_assignment.this]
}

# -----------------------------------------------------------------------------
# STORAGE CREDENTIAL (Niveau Account -> MWS)
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "this" {
  provider = databricks.mws
  name     = "${var.workspace_name}-credential"
  aws_iam_role {
    role_arn = var.cross_account_role_arn
  }
  comment = "Managed by Terraform"
}

# -----------------------------------------------------------------------------
# EXTERNAL LOCATION, CATALOG, SCHEMA (Niveau Workspace)
# -----------------------------------------------------------------------------
resource "databricks_external_location" "bronze" {
  provider        = databricks.workspace
  name            = "bronze"
  url             = "s3://${var.kafka_raw_bucket}/"
  credential_name = databricks_storage_credential.this.name
  comment         = "Location for raw data"

  # Assure que le workspace est rattaché au metastore avant de créer l'External Location
  depends_on = [databricks_metastore_assignment.this]
}

resource "databricks_catalog" "bronze_catalog" {
  provider     = databricks.workspace
  name         = "bronze"
  storage_root = "s3://${var.kafka_raw_bucket}/bronze_catalog" 
  comment      = "Catalog for bronze layer"
  
  # Le catalogue a besoin que l'External Location soit prête
  depends_on   = [databricks_external_location.bronze]
}

resource "databricks_schema" "bronze_schema" {
  provider     = databricks.workspace
  catalog_name = databricks_catalog.bronze_catalog.name
  name         = "kafka"
  comment      = "Schema for Kafka raw data"
}

# -----------------------------------------------------------------------------
# CATALOG ECOMMERCE - ARCHITECTURE MEDALLION (bronze / silver / gold)
# -----------------------------------------------------------------------------
resource "databricks_catalog" "ecommerce" {
  provider     = databricks.workspace
  name         = "ecommerce"
  storage_root = "s3://${var.kafka_raw_bucket}/uc/ecommerce"
  comment      = "Catalog for the e-commerce medallion pipeline"

  depends_on = [databricks_external_location.bronze]
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold"])
  provider     = databricks.workspace
  catalog_name = databricks_catalog.ecommerce.name
  name         = each.key
  comment      = "Medallion layer: ${each.key}"
}

resource "databricks_grants" "ecommerce_catalog_grants" {
  provider = databricks.workspace
  catalog  = databricks_catalog.ecommerce.name

  grant {
    principal  = "account users"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

# -----------------------------------------------------------------------------
# CLUSTER STANDARD (dans votre VPC)
# -----------------------------------------------------------------------------
resource "databricks_cluster" "standard" {
  provider                = databricks.workspace
  cluster_name            = "${var.workspace_name}-standard-cluster"
  spark_version           = "15.4.x-scala2.12"
  node_type_id            = "i3.xlarge"
  num_workers             = 2
  autotermination_minutes = 30

  data_security_mode = "USER_ISOLATION"

  aws_attributes {
    # On référence l'instance_profile enregistrée dans Databricks
    instance_profile_arn = databricks_instance_profile.this.id
  }

  # S'assurer que l'instance profile est bien créée/enregistrée en premier
  depends_on = [
    databricks_instance_profile.this
  ]
}

resource "databricks_grants" "bronze_catalog_grants" {
  provider = databricks.workspace
  catalog  = databricks_catalog.bronze_catalog.name

  grant {
    principal  = "account users" # Donnes les accès à tous les utilisateurs du compte
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

# Attribuer les accès au schéma Kafka
resource "databricks_grants" "kafka_schema_grants" {
  provider = databricks.workspace
  schema   = "${databricks_catalog.bronze_catalog.name}.${databricks_schema.bronze_schema.name}"

  grant {
    principal  = "account users"
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}
resource "databricks_grants" "external_location_grants" {
  provider          = databricks.workspace
  external_location = databricks_external_location.bronze.name

  grant {
    principal  = "account users"
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }

  depends_on = [databricks_external_location.bronze]
}