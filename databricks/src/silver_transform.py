"""Silver : état courant, nettoyé et validé, de chaque table source.

Pour chaque table :
  1. Rejeu du CDC : on garde le dernier événement par clé primaire (ordre source.lsn, ts_ms),
     puis on retire les clés dont le dernier événement est un DELETE (op = 'd').
  2. Typage : les DateTime Postgres arrivent de Debezium en microsecondes (bigint) -> timestamp.
  3. Nettoyage : trim, casse homogène, arrondis, colonnes dérivées.
  4. Contrôles qualité : les lignes qui violent une règle partent en quarantaine
     (ecommerce.silver.quarantine) au lieu d'être publiées.

Le traitement est recalculé entièrement depuis bronze à chaque exécution : simple,
idempotent et suffisant pour ce volume. Il produit aussi deux tables de suivi :
dq_summary (volumes par table) et dq_orphans (clés étrangères sans parent).
"""
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    TimestampNTZType,
    TimestampType,
)


def trim_cols(*cols):
    return lambda df: df.select(
        *[F.trim(F.col(c)).alias(c) if c in cols else F.col(c) for c in df.columns]
    )


def lower_cols(*cols):
    return lambda df: df.select(
        *[F.lower(F.col(c)).alias(c) if c in cols else F.col(c) for c in df.columns]
    )


def clean_customers(df):
    return (
        trim_cols("first_name", "last_name", "email", "country", "city")(df)
        .withColumn("first_name", F.initcap("first_name"))
        .withColumn("last_name", F.initcap("last_name"))
        .withColumn("email", F.lower("email"))
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    )


def clean_products(df):
    return (
        trim_cols("name", "category", "brand")(df)
        .withColumn("category", F.initcap("category"))
        .withColumn("price", F.round("price", 2))
    )


def clean_orders(df):
    return (
        lower_cols("status", "payment_method")(trim_cols("status", "payment_method")(df))
        .withColumn("total_amount", F.round("total_amount", 2))
        .withColumn("order_day", F.to_date("order_date"))
    )


def clean_order_items(df):
    return df.withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))


def clean_payments(df):
    return (
        lower_cols("status", "payment_method")(trim_cols("status", "payment_method")(df))
        .withColumn("amount", F.round("amount", 2))
    )


def clean_carts(df):
    return lower_cols("status")(trim_cols("status")(df))


def clean_shipments(df):
    return (
        lower_cols("status")(trim_cols("status", "carrier", "tracking_number")(df))
        .withColumn("carrier", F.upper("carrier"))
    )


def clean_warehouses(df):
    return trim_cols("name", "city")(df)


def identity(df):
    return df


EMAIL_REGEX = "^[^@ ]+@[^@ ]+[.][^@ ]+$"

# table -> clé primaire, colonnes timestamp, fonction de nettoyage, règles qualité
# Une règle = (nom, condition SQL que la ligne DOIT respecter)
SPECS = {
    "customers": {
        "pk": ["customer_id"],
        "timestamps": ["created_at"],
        "clean": clean_customers,
        "rules": [
            ("email_invalide", f"email RLIKE '{EMAIL_REGEX}'"),
            ("created_at_manquant", "created_at IS NOT NULL"),
        ],
    },
    "products": {
        "pk": ["product_id"],
        "timestamps": [],
        "clean": clean_products,
        "rules": [
            ("nom_manquant", "name IS NOT NULL AND name <> ''"),
            ("prix_invalide", "price > 0"),
        ],
    },
    "orders": {
        "pk": ["order_id"],
        "timestamps": ["order_date"],
        "clean": clean_orders,
        "rules": [
            ("client_manquant", "customer_id IS NOT NULL"),
            ("date_manquante", "order_date IS NOT NULL"),
            ("montant_negatif", "total_amount >= 0"),
        ],
    },
    "order_items": {
        "pk": ["order_id", "product_id"],
        "timestamps": [],
        "clean": clean_order_items,
        "rules": [
            ("quantite_invalide", "quantity > 0"),
            ("prix_negatif", "unit_price >= 0"),
        ],
    },
    "payments": {
        "pk": ["payment_id"],
        "timestamps": ["payment_date"],
        "clean": clean_payments,
        "rules": [
            ("commande_manquante", "order_id IS NOT NULL"),
            ("montant_negatif", "amount >= 0"),
            ("statut_inconnu", "status IN ('pending', 'success', 'failed')"),
        ],
    },
    "carts": {
        "pk": ["cart_id"],
        "timestamps": ["created_at"],
        "clean": clean_carts,
        "rules": [
            ("client_manquant", "customer_id IS NOT NULL"),
            ("statut_inconnu", "status IN ('active', 'abandoned', 'converted')"),
        ],
    },
    "cart_items": {
        "pk": ["id"],
        "timestamps": [],
        "clean": identity,
        "rules": [
            ("panier_manquant", "cart_id IS NOT NULL"),
            ("produit_manquant", "product_id IS NOT NULL"),
            ("quantite_invalide", "quantity > 0"),
        ],
    },
    "shipments": {
        "pk": ["shipment_id"],
        "timestamps": ["delivery_date"],
        "clean": clean_shipments,
        "rules": [
            ("commande_manquante", "order_id IS NOT NULL"),
            ("statut_inconnu", "status IN ('created', 'shipped', 'delivered')"),
            ("livree_sans_date", "status <> 'delivered' OR delivery_date IS NOT NULL"),
        ],
    },
    "inventory": {
        "pk": ["id"],
        "timestamps": [],
        "clean": identity,
        "rules": [
            ("produit_manquant", "product_id IS NOT NULL"),
            ("entrepot_manquant", "warehouse_id IS NOT NULL"),
            ("stock_negatif", "quantity >= 0"),
        ],
    },
    "warehouses": {
        "pk": ["id"],
        "timestamps": [],
        "clean": clean_warehouses,
        "rules": [("nom_manquant", "name IS NOT NULL AND name <> ''")],
    },
}

# (table enfant, clé étrangère, table parent, clé parent)
FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("shipments", "order_id", "orders", "order_id"),
    ("carts", "customer_id", "customers", "customer_id"),
    ("cart_items", "cart_id", "carts", "cart_id"),
    ("cart_items", "product_id", "products", "product_id"),
    ("inventory", "product_id", "products", "product_id"),
    ("inventory", "warehouse_id", "warehouses", "id"),
]


def to_timestamp(df, col):
    """Debezium encode les `timestamp` Postgres en microsecondes depuis l'epoch (bigint)."""
    dtype = df.schema[col].dataType
    if isinstance(dtype, (TimestampType, TimestampNTZType)):
        return df
    if isinstance(dtype, (LongType, IntegerType)):
        return df.withColumn(col, F.expr(f"timestamp_micros(`{col}`)"))
    return df.withColumn(col, F.col(col).cast("timestamp"))


def current_state(bronze, pks):
    """Dernier événement CDC par clé primaire, sans les lignes supprimées."""
    keyed = bronze
    for k in pks:
        keyed = keyed.withColumn(f"_key_{k}", F.coalesce(F.col(f"after.{k}"), F.col(f"before.{k}")))

    order = Window.partitionBy(*[f"_key_{k}" for k in pks]).orderBy(
        F.col("source.lsn").desc_nulls_last(),
        F.col("ts_ms").desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    latest = keyed.withColumn("_rn", F.row_number().over(order)).filter("_rn = 1")
    return latest.filter(F.col("op") != "d").select(
        "after.*",
        F.col("op").alias("_cdc_op"),
        F.expr("timestamp_millis(ts_ms)").alias("_cdc_updated_at"),
    )


def transform_table(spark, catalog, table, spec):
    bronze = spark.table(f"{catalog}.bronze.{table}")
    pks = spec["pk"]

    df = current_state(bronze, pks)
    for col in spec["timestamps"]:
        df = to_timestamp(df, col)
    df = spec["clean"](df)

    rules = [(f"{k}_manquant", f"{k} IS NOT NULL") for k in pks] + spec["rules"]
    errors = F.array_compact(
        F.array(*[F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules])
    )
    df = df.withColumn("_dq_errors", errors).cache()

    valid = df.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    rejected = df.filter(F.size("_dq_errors") > 0).select(
        F.lit(table).alias("table_name"),
        F.concat_ws("|", *[F.col(k).cast("string") for k in pks]).alias("record_key"),
        F.col("_dq_errors").alias("errors"),
        F.to_json(F.struct(*[F.col(c) for c in df.columns if c != "_dq_errors"])).alias("record"),
        F.col("_cdc_updated_at"),
    )

    (
        valid.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.{table}")
    )

    stats = {
        "table_name": table,
        "bronze_events": bronze.count(),
        "current_rows": df.count(),
        "valid_rows": spark.table(f"{catalog}.silver.{table}").count(),
    }
    stats["quarantined_rows"] = stats["current_rows"] - stats["valid_rows"]
    df.unpersist()
    return rejected, stats


def check_orphans(spark, catalog, written):
    rows = []
    for child, fk, parent, pk in FOREIGN_KEYS:
        if child not in written or parent not in written:
            continue
        c = spark.table(f"{catalog}.silver.{child}").filter(F.col(fk).isNotNull())
        p = spark.table(f"{catalog}.silver.{parent}").select(F.col(pk).alias(fk)).distinct()
        orphans = c.join(p, on=fk, how="left_anti").count()
        rows.append((child, fk, parent, orphans))
    return spark.createDataFrame(
        rows, "child_table string, foreign_key string, parent_table string, orphan_rows long"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="ecommerce")
    args = parser.parse_args()
    catalog = args.catalog

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rejected_all, stats_all, written = None, [], []

    for table, spec in SPECS.items():
        if not spark.catalog.tableExists(f"{catalog}.bronze.{table}"):
            print(f"[silver] {table}: pas de table bronze, ignorée")
            continue
        rejected, stats = transform_table(spark, catalog, table, spec)
        written.append(table)
        stats_all.append(stats)
        rejected_all = rejected if rejected_all is None else rejected_all.unionByName(rejected)
        print(f"[silver] {table}: {stats}")

    if not written:
        raise RuntimeError("Aucune table bronze disponible : lancer d'abord bronze_ingest.py")

    (
        rejected_all.withColumn("_run_at", F.lit(run_at))
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.quarantine")
    )
    (
        spark.createDataFrame(
            [
                (s["table_name"], s["bronze_events"], s["current_rows"], s["valid_rows"], s["quarantined_rows"])
                for s in stats_all
            ],
            "table_name string, bronze_events long, current_rows long, valid_rows long, quarantined_rows long",
        )
        .withColumn("_run_at", F.lit(run_at))
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.dq_summary")
    )
    (
        check_orphans(spark, catalog, written)
        .withColumn("_run_at", F.lit(run_at))
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.silver.dq_orphans")
    )


if __name__ == "__main__":
    main()
