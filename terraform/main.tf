terraform {
  required_version = ">= 1.5"
  # backend "s3" { ... } # à configurer pour l'état distant
}

provider "aws" {
  region = var.aws_region
}

module "aws" {
  source = "./modules/aws"
  project_name           = var.project_name
  aws_region             = var.aws_region
  databricks_account_id  = var.databricks_account_id
  databricks_aws_account_id = var.databricks_aws_account_id
}

module "databricks" {
  source = "./modules/databricks"
  aws_region                     = var.aws_region
  databricks_account_id          = var.databricks_account_id
  databricks_client_id           = var.databricks_client_id
  databricks_client_secret       = var.databricks_client_secret
  databricks_workspace_client_id     = var.databricks_workspace_client_id
  databricks_workspace_client_secret = var.databricks_workspace_client_secret
  instance_profile_arn           = module.aws.databricks_instance_profile_arn   
  workspace_name                 = var.workspace_name
  bucket_artifacts               = module.aws.databricks_artifacts_bucket
  kafka_raw_bucket               = module.aws.kafka_raw_bucket_name
  cross_account_role_arn         = module.aws.databricks_cross_account_role_arn
  vpc_id                         = module.aws.vpc_id
  subnet_ids                     = module.aws.private_subnet_ids
  security_group_ids             = [module.aws.security_group_id]
}