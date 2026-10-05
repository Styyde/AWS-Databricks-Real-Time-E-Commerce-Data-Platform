"""Bronze : ingestion incrémentale des fichiers Parquet du S3 Sink (enveloppe Debezium brute).

Chaque table Postgres a son topic Kafka et donc son dossier S3 :
    s3://<bucket>/topics/ecommerce.public.<table>/bronze/YYYY/MM/dd/*.parquet

Auto Loader mémorise les fichiers déjà lus (checkpoint) : chaque exécution ne traite
que les nouveaux fichiers, puis s'arrête (trigger availableNow).
La table bronze garde l'enveloppe complète (before, after, op, ts_ms, source) en append-only :
c'est l'historique fidèle de tous les changements.
"""
import argparse

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession, functions as F

TABLES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "carts",
    "cart_items",
    "shipments",
    "inventory",
    "warehouses",
]
TOPIC_PREFIX = "ecommerce.public"


def has_files(dbutils, path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False


def ingest_table(spark, table, catalog, bucket):
    source = f"s3://{bucket}/topics/{TOPIC_PREFIX}.{table}/"
    checkpoint = f"s3://{bucket}/_checkpoints/bronze/{table}"

    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
        .option("pathGlobFilter", "*.parquet")
        .load(source)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.{table}")
    )
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="ecommerce")
    parser.add_argument("--raw-bucket", default="my-data-project-kafka-raw")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    failures = {}
    for table in TABLES:
        source = f"s3://{args.raw_bucket}/topics/{TOPIC_PREFIX}.{table}/"
        if not has_files(dbutils, source):
            # Auto Loader ne peut pas inférer de schéma sur un dossier vide
            print(f"[bronze] {table}: aucun fichier dans {source}, ignorée")
            continue
        try:
            ingest_table(spark, table, args.catalog, args.raw_bucket)
            count = spark.table(f"{args.catalog}.bronze.{table}").count()
            print(f"[bronze] {table}: OK ({count} événements au total)")
        except Exception as e:
            print(f"[bronze] {table}: ECHEC -> {e}")
            failures[table] = str(e)

    if failures:
        raise RuntimeError(f"Ingestion bronze en échec pour : {sorted(failures)}")


if __name__ == "__main__":
    main()
