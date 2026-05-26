import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
# Read Source Tables (Bronze)
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

# -----------------------------------------
# Target: product_details_silver (pds)
# -----------------------------------------
product_details_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        pb.brand AS brand,
        CAST(pb.price AS double) AS base_price,
        CAST(pb.is_active AS boolean) AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      base_price,
      is_active
    FROM ranked
    WHERE rn = 1
      AND is_active = true
    """
)
product_details_silver_df.createOrReplaceTempView("product_details_silver")

(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_details_silver.csv")
)

# -----------------------------------------
# Target: store_details_silver (sds)
# -----------------------------------------
store_details_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        sb.open_date AS opening_date,
        sb.store_type AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location,
      opening_date,
      store_type
    FROM ranked
    WHERE rn = 1
    """
)
store_details_silver_df.createOrReplaceTempView("store_details_silver")

(
    store_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_details_silver.csv")
)

# -----------------------------------------------
# Target: transaction_details_silver (tds)
# -----------------------------------------------
transaction_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.sale_amount AS double) AS total_revenue,
        stb.transaction_time AS transaction_time
      FROM sales_transactions_bronze stb
      WHERE stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
        AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
        AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
    ),
    cleansed AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        CASE WHEN quantity_sold < 0 THEN NULL ELSE quantity_sold END AS quantity_sold,
        CASE WHEN total_revenue < 0 THEN NULL ELSE total_revenue END AS total_revenue,
        transaction_time
      FROM base
    ),
    ranked AS (
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
      FROM cleansed
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      transaction_date,
      quantity_sold,
      total_revenue
    FROM ranked
    WHERE rn = 1
    """
)
transaction_details_silver_df.createOrReplaceTempView("transaction_details_silver")

(
    transaction_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transaction_details_silver.csv")
)

# -----------------------------------------------
# Target: sales_time_dimension_silver (stds)
# -----------------------------------------------
sales_time_dimension_silver_df = spark.sql(
    """
    SELECT
      CAST(DATE_FORMAT(tds.transaction_date, 'yyyyMMdd') AS int) AS date_key,
      tds.transaction_date AS standardized_date,
      DAYOFWEEK(tds.transaction_date) AS day_of_week,
      MONTH(tds.transaction_date) AS month,
      QUARTER(tds.transaction_date) AS quarter,
      YEAR(tds.transaction_date) AS year
    FROM (
      SELECT DISTINCT transaction_date
      FROM transaction_details_silver
      WHERE transaction_date IS NOT NULL
    ) tds
    """
)
sales_time_dimension_silver_df.createOrReplaceTempView("sales_time_dimension_silver")

(
    sales_time_dimension_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_time_dimension_silver.csv")
)

# -----------------------------------------------
# Target: enriched_transactions_silver (ets)
# -----------------------------------------------
enriched_transactions_silver_df = spark.sql(
    """
    SELECT
      tds.transaction_id AS transaction_id,
      stds.standardized_date AS standardized_date,
      tds.product_id AS product_id,
      pds.product_name AS product_name,
      tds.store_id AS store_id,
      sds.store_name AS store_name,
      tds.quantity_sold AS quantity_sold,
      tds.total_revenue AS total_revenue
    FROM transaction_details_silver tds
    INNER JOIN product_details_silver pds
      ON tds.product_id = pds.product_id
    INNER JOIN store_details_silver sds
      ON tds.store_id = sds.store_id
    INNER JOIN sales_time_dimension_silver stds
      ON tds.transaction_date = stds.standardized_date
    """
)
enriched_transactions_silver_df.createOrReplaceTempView("enriched_transactions_silver")

(
    enriched_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/enriched_transactions_silver.csv")
)

# -----------------------------------------------
# Target: daily_aggregates_silver (das)
# -----------------------------------------------
daily_aggregates_silver_df = spark.sql(
    """
    SELECT
      tds.transaction_date AS date,
      SUM(tds.total_revenue) AS total_revenue,
      COUNT(DISTINCT tds.transaction_id) AS total_transactions,
      SUM(tds.quantity_sold) AS total_quantity_sold
    FROM transaction_details_silver tds
    GROUP BY tds.transaction_date
    """
)
daily_aggregates_silver_df.createOrReplaceTempView("daily_aggregates_silver")

(
    daily_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_aggregates_silver.csv")
)

job.commit()