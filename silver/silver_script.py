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

# -----------------------------
# Target: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING) AS product_category,
            CAST(TRIM(pb.brand) AS STRING) AS product_brand,
            CAST(pb.price AS DOUBLE) AS product_price,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
                ORDER BY CAST(TRIM(pb.product_id) AS STRING) DESC
            ) AS rn
        FROM products_bronze pb
        WHERE COALESCE(CAST(pb.is_active AS BOOLEAN), FALSE) = TRUE
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_brand,
        product_price
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
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CAST(TRIM(sb.state) AS STRING) AS store_region,
            CAST(NULL AS STRING) AS store_manager,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(TRIM(sb.store_id) AS STRING)
                ORDER BY CAST(TRIM(sb.store_id) AS STRING) DESC
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        store_region,
        store_manager
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
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(stb.transaction_time AS DATE) AS sold_date,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS total_revenue,
            ps.product_category AS product_category,
            ss.store_region AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(TRIM(stb.transaction_id) AS STRING)
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON CAST(TRIM(stb.product_id) AS STRING) = ps.product_id
        LEFT JOIN stores_silver ss
            ON CAST(TRIM(stb.store_id) AS STRING) = ss.store_id
        WHERE
            COALESCE(CAST(stb.quantity AS INT), 0) >= 0
            AND COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) >= 0.0
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        sold_date,
        quantity,
        total_revenue,
        product_category,
        store_region
    FROM base
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

job.commit()
