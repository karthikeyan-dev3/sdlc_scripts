import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init("silver_layer_job", sys.argv)

# ------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# 3) Transform + 4) Save output per target table
# ------------------------------------------------------------

# ============================================================
# product_details_silver
# Source: bronze.products_bronze pb
# ============================================================
product_details_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        UPPER(TRIM(pb.product_name)) AS product_name,
        UPPER(TRIM(pb.category)) AS category,
        UPPER(TRIM(pb.brand)) AS brand,
        CAST(pb.is_active AS boolean) AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_id)
        ) AS rn
      FROM products_bronze pb
      WHERE CAST(pb.is_active AS boolean) = true
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      is_active
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_details_silver.csv")
)

product_details_silver_df.createOrReplaceTempView("product_details_silver")

# ============================================================
# store_information_silver
# Source: bronze.stores_bronze sb
# ============================================================
store_information_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        UPPER(TRIM(sb.store_name)) AS store_name,
        UPPER(TRIM(sb.city)) || ', ' || UPPER(TRIM(sb.state)) AS location,
        COALESCE(LOWER(TRIM(sb.store_type)), 'unknown') AS store_size,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY TRIM(sb.store_id)
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location,
      store_size
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_information_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_information_silver.csv")
)

store_information_silver_df.createOrReplaceTempView("store_information_silver")

# ============================================================
# sales_transactions_silver
# Source: bronze.sales_transactions_bronze stb
# Joins: silver.product_details_silver pds, silver.store_information_silver sis
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.sale_amount AS double) AS total_sales_amount,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
    )
    SELECT
      r.transaction_id,
      r.transaction_date,
      r.store_id,
      r.product_id,
      r.quantity_sold,
      r.total_sales_amount
    FROM ranked r
    LEFT JOIN product_details_silver pds
      ON r.product_id = pds.product_id
    LEFT JOIN store_information_silver sis
      ON r.store_id = sis.store_id
    WHERE r.rn = 1
      AND r.quantity_sold > 0
      AND r.total_sales_amount >= 0
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

# ============================================================
# sales_aggregation_silver
# Source: silver.sales_transactions_silver sts
# Group by: CAST(sts.transaction_date AS date), sts.store_id, sts.product_id
# ============================================================
sales_aggregation_silver_df = spark.sql(
    """
    SELECT
      CAST(sts.transaction_date AS date) AS date,
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      SUM(sts.quantity_sold) AS total_quantity_sold,
      SUM(sts.total_sales_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
      CAST(sts.transaction_date AS date),
      sts.store_id,
      sts.product_id
    """
)

(
    sales_aggregation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregation_silver.csv")
)

job.commit()