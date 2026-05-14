import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Source Reads + Temp Views
# -----------------------------
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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------
# Target: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(pb.product_id AS string) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        CAST(pb.price AS float) AS pricing,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
        AND pb.is_active = 'true'
    )
    SELECT
      product_id,
      product_name,
      category,
      pricing
    FROM base
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

# -----------------------------
# Target: silver.stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(sb.store_id AS string) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    )
    SELECT
      store_id,
      store_name,
      location
    FROM base
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

# -----------------------------
# Target: silver.sales_transactions_silver
# -----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(stb.transaction_id AS string) AS transaction_id,
        ps.product_id AS product_id,
        ss.store_id AS store_id,
        CASE
          WHEN CAST(stb.quantity AS int) < 0 THEN 0
          ELSE CAST(stb.quantity AS int)
        END AS quantity,
        CAST(stb.sale_amount AS double) AS revenue,
        CAST(stb.transaction_time AS date) AS transaction_date,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_id
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN products_silver ps
        ON stb.product_id = ps.product_id
      LEFT JOIN stores_silver ss
        ON stb.store_id = ss.store_id
      WHERE stb.transaction_id IS NOT NULL
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      quantity,
      revenue,
      transaction_date
    FROM joined
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

# -----------------------------
# Target: silver.daily_sales_aggregates_silver
# -----------------------------
daily_sales_aggregates_silver_df = spark.sql(
    """
    SELECT
      CAST(sts.transaction_date AS date) AS date,
      sts.product_id AS product_id,
      sts.store_id AS store_id,
      CAST(SUM(sts.quantity) AS int) AS total_quantity_sold,
      SUM(sts.revenue) AS total_revenue
    FROM sales_transactions_silver sts
    WHERE sts.product_id IS NOT NULL
      AND sts.store_id IS NOT NULL
    GROUP BY
      CAST(sts.transaction_date AS date),
      sts.product_id,
      sts.store_id
    """
)

(
    daily_sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregates_silver.csv")
)

job.commit()