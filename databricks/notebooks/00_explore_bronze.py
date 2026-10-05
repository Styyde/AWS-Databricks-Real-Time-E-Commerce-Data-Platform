# Databricks notebook source
# MAGIC %md
# MAGIC # Exploration des données brutes (avant / après bronze)
# MAGIC À lancer sur le cluster `my-data-workspace-standard-cluster` pour valider :
# MAGIC 1. que Databricks lit bien le bucket S3 (droits IAM + external location),
# MAGIC 2. le schéma réel de l'enveloppe Debezium,
# MAGIC 3. le résultat de chaque couche après un run du job.

# COMMAND ----------

dbutils.widgets.text("bucket", "my-data-project-kafka-raw")
dbutils.widgets.text("table", "orders")
bucket = dbutils.widgets.get("bucket")
table = dbutils.widgets.get("table")
source = f"s3://{bucket}/topics/ecommerce.public.{table}/"

# COMMAND ----------

# MAGIC %md ## 1. Fichiers déposés par le S3 Sink

# COMMAND ----------

display(dbutils.fs.ls(f"s3://{bucket}/topics/"))

# COMMAND ----------

# MAGIC %md ## 2. Lecture directe des Parquet (schéma Debezium)

# COMMAND ----------

raw = spark.read.option("recursiveFileLookup", "true").parquet(source)
raw.printSchema()
display(raw.select("op", "ts_ms", "source.lsn", "before", "after").limit(20))

# COMMAND ----------

display(raw.groupBy("op").count())

# COMMAND ----------

# MAGIC %md ## 3. Après exécution du job

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce.silver.dq_summary ORDER BY table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, errors, COUNT(*) AS nb FROM ecommerce.silver.quarantine GROUP BY ALL ORDER BY nb DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce.silver.dq_orphans WHERE orphan_rows > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce.gold.kpi_daily_sales ORDER BY order_day DESC
