import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# Read Source Tables (Bronze)
# =========================
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# =========================
# Target: sales_transactions_silver
# =========================
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS revenue,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE
        stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND CAST(stb.quantity AS INT) >= 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      transaction_date,
      quantity_sold,
      revenue
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =========================
# Target: products_silver
# =========================
products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        TRIM(pb.product_name) AS product_name,
        pb.category AS product_category,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id DESC
        ) AS rn
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
    )
    SELECT
      product_id,
      product_name,
      product_category
    FROM ranked
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

# =========================
# Target: stores_silver
# =========================
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        sb.store_id AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT_WS(', ', sb.city, sb.state) AS store_location,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id DESC
        ) AS rn
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    )
    SELECT
      store_id,
      store_name,
      store_location
    FROM ranked
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

# =========================
# Target: data_quality_metrics_silver
# =========================
data_quality_metrics_silver_df = spark.sql(
    """
    SELECT
      CAST(xxhash64(CONCAT('% null keys in sales', CAST(sts.transaction_date AS STRING))) AS STRING) AS metric_id,
      '% null keys in sales' AS metric_name,
      (
        SUM(
          CASE
            WHEN sts.transaction_id IS NULL OR sts.store_id IS NULL OR sts.product_id IS NULL THEN 1
            ELSE 0
          END
        ) / COUNT(*)
      ) * 100 AS value,
      CAST(0 AS DOUBLE) AS target_value,
      sts.transaction_date AS date_measured
    FROM sales_transactions_silver sts
    CROSS JOIN products_silver ps
    CROSS JOIN stores_silver ss
    GROUP BY sts.transaction_date
    """
)

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()
