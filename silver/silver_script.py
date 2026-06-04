import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables (Bronze)
# ------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# 2) stores_silver
#    Output columns: store_id, store_name
#    De-dup: by store_id (no ingestion time provided; deterministic pick)
# ------------------------------------------------------------
stores_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING) AS store_id,
    CAST(TRIM(sb.store_name) AS STRING) AS store_name
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name
FROM dedup
WHERE rn = 1
"""
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# 3) products_silver
#    Output columns: product_id, product_name
#    De-dup: by product_id (no recency/active flag provided; deterministic pick)
# ------------------------------------------------------------
products_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING) AS product_id,
    CAST(TRIM(pb.product_name) AS STRING) AS product_name
  FROM products_bronze pb
  WHERE pb.product_id IS NOT NULL
),
dedup AS (
  SELECT
    product_id,
    product_name,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name
FROM dedup
WHERE rn = 1
"""
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# 4) transactions_silver
#    Output columns: transaction_id, store_id, product_id, transaction_date, quantity_sold, total_revenue
#    De-dup: by transaction_id (keep latest by transaction_time)
#    Cleanse:
#      quantity_sold = COALESCE(quantity,0)
#      total_revenue = COALESCE(sale_amount,0.0)
#      transaction_date = CAST(transaction_time AS DATE)
#    Join to conformed stores/products for referential integrity
# ------------------------------------------------------------
transactions_silver_df = spark.sql(
    """
WITH joined AS (
  SELECT
    CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
    CAST(TRIM(stb.store_id) AS STRING) AS store_id,
    CAST(TRIM(stb.product_id) AS STRING) AS product_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    CAST(COALESCE(stb.quantity, 0) AS INT) AS quantity_sold,
    CAST(COALESCE(stb.sale_amount, 0.0) AS DOUBLE) AS total_revenue,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  INNER JOIN stores_silver ss
    ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
  INNER JOIN products_silver ps
    ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
  WHERE stb.transaction_id IS NOT NULL
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM joined
)
SELECT
  transaction_id,
  store_id,
  product_id,
  transaction_date,
  quantity_sold,
  total_revenue
FROM dedup
WHERE rn = 1
"""
)
transactions_silver_df.createOrReplaceTempView("transactions_silver")

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# ------------------------------------------------------------
# 5) sales_aggregated_silver
#    Output columns: store_id, product_id, sales_date, total_sales_amount, sales_count
# ------------------------------------------------------------
sales_aggregated_silver_df = spark.sql(
    """
SELECT
  ts.store_id AS store_id,
  ts.product_id AS product_id,
  ts.transaction_date AS sales_date,
  CAST(SUM(ts.total_revenue) AS DOUBLE) AS total_sales_amount,
  CAST(COUNT(DISTINCT ts.transaction_id) AS BIGINT) AS sales_count
FROM transactions_silver ts
GROUP BY
  ts.store_id,
  ts.product_id,
  ts.transaction_date
"""
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# ------------------------------------------------------------
# 6) data_quality_metrics_silver
#    Output columns:
#      metric_date, data_freshness, data_completeness_score, record_accuracy_percentage, deduplication_status
#    Notes:
#      - metric_date uses CURRENT_DATE
#      - data_freshness uses hours since MAX(bronze.transaction_time)
# ------------------------------------------------------------
data_quality_metrics_silver_df = spark.sql(
    """
WITH freshness AS (
  SELECT
    (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(stb.transaction_time))) / 3600.0 AS data_freshness
  FROM sales_transactions_bronze stb
),
metrics AS (
  SELECT
    CURRENT_DATE() AS metric_date,
    f.data_freshness AS data_freshness,
    CAST(
      SUM(
        CASE
          WHEN ts.transaction_id IS NOT NULL
           AND ts.store_id IS NOT NULL
           AND ts.product_id IS NOT NULL
           AND ts.transaction_date IS NOT NULL
          THEN 1 ELSE 0
        END
      ) / COUNT(1) AS DOUBLE
    ) AS data_completeness_score,
    CAST(
      100.0 * SUM(
        CASE
          WHEN ts.quantity_sold >= 0
           AND ts.total_revenue >= 0
           AND ss.store_id IS NOT NULL
           AND ps.product_id IS NOT NULL
          THEN 1 ELSE 0
        END
      ) / COUNT(1) AS DOUBLE
    ) AS record_accuracy_percentage,
    CASE
      WHEN COUNT(1) = COUNT(DISTINCT ts.transaction_id) THEN 'PASS' ELSE 'FAIL'
    END AS deduplication_status
  FROM transactions_silver ts
  LEFT JOIN stores_silver ss
    ON ts.store_id = ss.store_id
  LEFT JOIN products_silver ps
    ON ts.product_id = ps.product_id
  CROSS JOIN freshness f
)
SELECT
  metric_date,
  CAST(data_freshness AS DOUBLE) AS data_freshness,
  CAST(data_completeness_score AS DOUBLE) AS data_completeness_score,
  CAST(record_accuracy_percentage AS DOUBLE) AS record_accuracy_percentage,
  deduplication_status
FROM metrics
"""
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()
