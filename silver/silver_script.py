import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------
# Read source tables (S3)
# -------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -------------------------
# Create temp views
# -------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =========================
# Target: products_silver
# =========================
products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
      WHERE pb.is_active = true
    )
    SELECT
      product_id,
      product_name,
      category
    FROM ranked
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("sales_transactions_silver")  # placeholder removed below

# =========================
# Target: stores_silver
# =========================
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        CONCAT(sb.city, ', ', sb.state) AS location,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location
    FROM ranked
    WHERE rn = 1
    """
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =========================
# Target: sales_transactions_silver
# =========================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.quantity AS int) AS quantity,
        CAST(stb.sale_amount AS double) AS revenue,
        CAST(stb.transaction_time AS date) AS transaction_date,
        stb.transaction_time AS transaction_time
      FROM sales_transactions_bronze stb
      WHERE CAST(stb.quantity AS int) >= 0
        AND CAST(stb.sale_amount AS double) >= 0
    ),
    ranked AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        quantity,
        revenue,
        transaction_date,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id, product_id, store_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      quantity,
      revenue,
      transaction_date
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# =========================
# Target: daily_sales_aggregated_silver
# =========================
daily_sales_aggregated_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      SUM(sts.quantity) AS total_quantity,
      SUM(sts.revenue) AS total_revenue,
      CASE
        WHEN SUM(sts.quantity) = 0 THEN NULL
        ELSE SUM(sts.revenue) / SUM(sts.quantity)
      END AS average_price,
      sts.transaction_date AS sales_date
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.transaction_date
    """
)

(
    daily_sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregated_silver.csv")
)

job.commit()