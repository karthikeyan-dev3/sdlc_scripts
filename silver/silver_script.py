
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

# ------------------------------------------------------------
# Read source tables (Bronze) and create temp views
# ------------------------------------------------------------
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# Target: silver.products_silver
# ------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS product_category,
            CAST(pb.price AS DOUBLE) AS product_cost,
            TRIM(pb.brand) AS brand,
            CAST(pb.is_active AS BOOLEAN) AS is_active,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY TRIM(pb.product_id)
            ) AS rn
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_cost,
        brand,
        is_active
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

# ------------------------------------------------------------
# Target: silver.stores_silver
# ------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            TRIM(sb.store_name) AS store_name,
            TRIM(sb.city) AS city,
            TRIM(sb.state) AS state,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
            TRIM(sb.store_type) AS store_type,
            CAST(sb.open_date AS DATE) AS open_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY TRIM(sb.store_id)
            ) AS rn
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
    )
    SELECT
        store_id,
        store_name,
        store_location,
        city,
        state,
        store_type,
        open_date
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

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_revenue,
            CAST(COALESCE(ps.product_cost, 0) AS DOUBLE) * CAST(stb.quantity AS INT) AS product_cost,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON TRIM(stb.product_id) = ps.product_id
    )
    SELECT
        transaction_id,
        transaction_time,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_revenue,
        product_cost
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

# ------------------------------------------------------------
# Target: silver.data_refresh_log_silver
# ------------------------------------------------------------
data_refresh_log_silver_df = spark.sql(
    """
    SELECT
        CAST(stb.transaction_time AS DATE) AS refresh_date,
        (COUNT(stb.transaction_id) > 0) AS success,
        CAST(NULL AS DOUBLE) AS time_taken
    FROM sales_transactions_bronze stb
    GROUP BY CAST(stb.transaction_time AS DATE)
    """
)
data_refresh_log_silver_df.createOrReplaceTempView("data_refresh_log_silver")

(
    data_refresh_log_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_refresh_log_silver.csv")
)

job.commit()
