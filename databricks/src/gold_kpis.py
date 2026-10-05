"""Gold : tables métier prêtes pour le reporting.

fact_orders est la table centrale (une ligne par commande, enrichie du client, du
dernier paiement et de l'expédition). Les KPIs sont calculés à partir d'elle et des
tables silver.

Règles métier (déduites de l'API) :
  - orders.status vaut toujours 'completed' : une commande n'est réellement vendue que si
    son dernier paiement est 'success' (environ 10 % des paiements échouent).
  - Le moyen de paiement de référence est celui de payments (orders.payment_method peut différer).
  - Un panier n'est jamais passé à 'abandoned' par l'API : un panier encore 'active' après
    --abandon-after-hours est compté comme abandonné.
"""
import argparse

from pyspark.sql import SparkSession

GOLD_TABLES = [
    (
        "fact_orders",
        """
        WITH last_payment AS (
          SELECT * FROM (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_date DESC) AS rn
            FROM {c}.silver.payments p
          ) WHERE rn = 1
        ),
        items AS (
          SELECT order_id, COUNT(*) AS nb_lines, SUM(quantity) AS units, SUM(line_amount) AS items_amount
          FROM {c}.silver.order_items
          GROUP BY order_id
        ),
        last_shipment AS (
          SELECT * FROM (
            SELECT s.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivery_date DESC NULLS LAST) AS rn
            FROM {c}.silver.shipments s
          ) WHERE rn = 1
        )
        SELECT
          o.order_id,
          o.customer_id,
          c.country,
          c.city,
          o.order_date,
          o.order_day,
          o.total_amount,
          COALESCE(i.nb_lines, 0)                    AS nb_lines,
          COALESCE(i.units, 0)                       AS units,
          lp.payment_method,
          COALESCE(lp.status, 'no_payment')          AS payment_status,
          COALESCE(lp.status = 'success', FALSE)     AS is_paid,
          s.carrier,
          COALESCE(s.status, 'not_shipped')          AS shipment_status,
          s.delivery_date,
          ROUND((unix_timestamp(s.delivery_date) - unix_timestamp(o.order_date)) / 3600.0, 2) AS delivery_hours
        FROM {c}.silver.orders o
        LEFT JOIN {c}.silver.customers c ON o.customer_id = c.customer_id
        LEFT JOIN items i                 ON o.order_id = i.order_id
        LEFT JOIN last_payment lp         ON o.order_id = lp.order_id
        LEFT JOIN last_shipment s         ON o.order_id = s.order_id
        """,
    ),
    (
        "kpi_daily_sales",
        """
        SELECT
          order_day,
          COUNT(*)                                                   AS nb_orders,
          COUNT_IF(is_paid)                                          AS nb_paid_orders,
          ROUND(COUNT_IF(is_paid) / COUNT(*), 4)                     AS paid_rate,
          ROUND(SUM(CASE WHEN is_paid THEN total_amount END), 2)     AS revenue,
          ROUND(AVG(CASE WHEN is_paid THEN total_amount END), 2)     AS avg_basket,
          SUM(CASE WHEN is_paid THEN units END)                      AS units_sold,
          COUNT(DISTINCT CASE WHEN is_paid THEN customer_id END)     AS nb_buyers
        FROM {c}.gold.fact_orders
        GROUP BY order_day
        """,
    ),
    (
        "kpi_sales_by_country",
        """
        SELECT
          COALESCE(country, 'Inconnu')         AS country,
          COUNT(*)                             AS nb_paid_orders,
          COUNT(DISTINCT customer_id)          AS nb_buyers,
          ROUND(SUM(total_amount), 2)          AS revenue,
          ROUND(AVG(total_amount), 2)          AS avg_basket
        FROM {c}.gold.fact_orders
        WHERE is_paid
        GROUP BY ALL
        """,
    ),
    (
        "kpi_product_performance",
        """
        WITH sales AS (
          SELECT oi.product_id,
                 SUM(oi.quantity)               AS units_sold,
                 ROUND(SUM(oi.line_amount), 2)  AS revenue,
                 COUNT(DISTINCT oi.order_id)    AS nb_orders
          FROM {c}.silver.order_items oi
          JOIN {c}.gold.fact_orders f ON oi.order_id = f.order_id AND f.is_paid
          GROUP BY oi.product_id
        ),
        stock AS (
          SELECT product_id, SUM(quantity) AS stock_total
          FROM {c}.silver.inventory
          GROUP BY product_id
        )
        SELECT
          p.product_id, p.name, p.category, p.brand, p.price,
          COALESCE(s.units_sold, 0)   AS units_sold,
          COALESCE(s.revenue, 0)      AS revenue,
          COALESCE(s.nb_orders, 0)    AS nb_orders,
          COALESCE(st.stock_total, 0) AS stock_total,
          DENSE_RANK() OVER (ORDER BY COALESCE(s.revenue, 0) DESC) AS revenue_rank
        FROM {c}.silver.products p
        LEFT JOIN sales s  ON p.product_id = s.product_id
        LEFT JOIN stock st ON p.product_id = st.product_id
        """,
    ),
    (
        "kpi_category_sales",
        """
        SELECT
          category,
          COUNT(*)                     AS nb_products,
          SUM(units_sold)              AS units_sold,
          ROUND(SUM(revenue), 2)       AS revenue,
          ROUND(try_divide(SUM(revenue), SUM(SUM(revenue)) OVER ()), 4) AS revenue_share
        FROM {c}.gold.kpi_product_performance
        GROUP BY category
        """,
    ),
    (
        "kpi_customer_rfm",
        """
        WITH per_customer AS (
          SELECT
            c.customer_id, c.full_name, c.email, c.country, c.city, c.created_at,
            COUNT(f.order_id)                       AS nb_orders,
            ROUND(COALESCE(SUM(f.total_amount), 0), 2) AS lifetime_value,
            ROUND(AVG(f.total_amount), 2)           AS avg_basket,
            MIN(f.order_date)                       AS first_order_at,
            MAX(f.order_date)                       AS last_order_at
          FROM {c}.silver.customers c
          LEFT JOIN {c}.gold.fact_orders f ON c.customer_id = f.customer_id AND f.is_paid
          GROUP BY ALL
        ),
        scored AS (
          SELECT *,
            DATEDIFF(current_date(), last_order_at) AS recency_days,
            CASE WHEN nb_orders > 0 THEN NTILE(4) OVER (
              PARTITION BY nb_orders > 0 ORDER BY lifetime_value) END AS value_quartile
          FROM per_customer
        )
        SELECT *,
          CASE
            WHEN nb_orders = 0       THEN 'prospect'
            WHEN value_quartile = 4  THEN 'vip'
            WHEN value_quartile = 3  THEN 'fidele'
            WHEN value_quartile = 2  THEN 'regulier'
            ELSE 'occasionnel'
          END AS segment
        FROM scored
        """,
    ),
    (
        "kpi_payment_methods",
        """
        SELECT
          payment_method,
          COUNT(*)                                                   AS nb_payments,
          COUNT_IF(status = 'success')                               AS nb_success,
          COUNT_IF(status = 'failed')                                AS nb_failed,
          ROUND(COUNT_IF(status = 'success') / COUNT(*), 4)          AS success_rate,
          ROUND(SUM(CASE WHEN status = 'success' THEN amount END), 2) AS amount_collected,
          ROUND(SUM(CASE WHEN status = 'failed' THEN amount END), 2)  AS amount_failed
        FROM {c}.silver.payments
        GROUP BY payment_method
        """,
    ),
    (
        "kpi_cart_funnel",
        """
        WITH cart_value AS (
          SELECT ci.cart_id, COUNT(*) AS nb_items, ROUND(SUM(ci.quantity * p.price), 2) AS cart_amount
          FROM {c}.silver.cart_items ci
          JOIN {c}.silver.products p ON ci.product_id = p.product_id
          GROUP BY ci.cart_id
        ),
        carts AS (
          SELECT ca.*, COALESCE(v.nb_items, 0) AS nb_items, COALESCE(v.cart_amount, 0) AS cart_amount,
            CASE
              WHEN ca.status = 'converted' THEN 'converted'
              WHEN ca.status = 'abandoned'
                OR ca.created_at < current_timestamp() - INTERVAL {abandon_hours} HOURS THEN 'abandoned'
              ELSE 'in_progress'
            END AS funnel_status
          FROM {c}.silver.carts ca
          LEFT JOIN cart_value v ON ca.cart_id = v.cart_id
        )
        SELECT
          to_date(created_at)                                         AS cart_day,
          COUNT(*)                                                    AS nb_carts,
          COUNT_IF(nb_items > 0)                                      AS nb_carts_with_items,
          COUNT_IF(funnel_status = 'converted')                       AS nb_converted,
          COUNT_IF(funnel_status = 'abandoned')                       AS nb_abandoned,
          COUNT_IF(funnel_status = 'in_progress')                     AS nb_in_progress,
          ROUND(COUNT_IF(funnel_status = 'converted') / COUNT(*), 4)  AS conversion_rate,
          ROUND(COUNT_IF(funnel_status = 'abandoned') / COUNT(*), 4)  AS abandonment_rate,
          ROUND(SUM(CASE WHEN funnel_status = 'abandoned' THEN cart_amount END), 2) AS abandoned_amount
        FROM carts
        GROUP BY ALL
        """,
    ),
    (
        "kpi_delivery_performance",
        """
        SELECT
          carrier,
          COUNT(*)                                                     AS nb_shipments,
          COUNT_IF(shipment_status = 'delivered')                      AS nb_delivered,
          ROUND(COUNT_IF(shipment_status = 'delivered') / COUNT(*), 4) AS delivered_rate,
          ROUND(AVG(delivery_hours), 2)                                AS avg_delivery_hours,
          ROUND(PERCENTILE_APPROX(delivery_hours, 0.9), 2)             AS p90_delivery_hours
        FROM {c}.gold.fact_orders
        WHERE carrier IS NOT NULL
        GROUP BY carrier
        """,
    ),
    (
        "kpi_inventory_status",
        """
        SELECT
          p.product_id, p.name AS product_name, p.category,
          w.id AS warehouse_id, w.name AS warehouse_name, w.city AS warehouse_city,
          i.quantity,
          CASE
            WHEN i.quantity = 0  THEN 'rupture'
            WHEN i.quantity < 10 THEN 'faible'
            ELSE 'ok'
          END AS stock_status
        FROM {c}.silver.inventory i
        JOIN {c}.silver.products p   ON i.product_id = p.product_id
        JOIN {c}.silver.warehouses w ON i.warehouse_id = w.id
        """,
    ),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="ecommerce")
    parser.add_argument("--abandon-after-hours", type=int, default=1)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    failures = {}
    for name, sql in GOLD_TABLES:
        query = sql.format(c=args.catalog, abandon_hours=args.abandon_after_hours)
        try:
            spark.sql(f"CREATE OR REPLACE TABLE {args.catalog}.gold.{name} AS {query}")
            count = spark.table(f"{args.catalog}.gold.{name}").count()
            print(f"[gold] {name}: OK ({count} lignes)")
        except Exception as e:
            print(f"[gold] {name}: ECHEC -> {e}")
            failures[name] = str(e)

    if failures:
        raise RuntimeError(f"Tables gold en échec : {sorted(failures)}")


if __name__ == "__main__":
    main()
