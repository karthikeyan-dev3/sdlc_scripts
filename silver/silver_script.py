import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables from S3 (CSV) and create temp views
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2) products_silver
# -------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.product_id AS STRING)     AS product_id,
            CAST(pb.product_name AS STRING)   AS product_name,
            CAST(pb.category AS STRING)       AS category,
            CAST(pb.brand AS STRING)          AS brand,
            CAST(pb.price AS FLOAT)           AS price,
            CAST(pb.is_active AS BOOLEAN)     AS is_active,
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
        price,
        is_active
    FROM base
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

# -------------------------------------------------------------------
# 3) stores_silver
# -------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS STRING)     AS store_id,
            CAST(sb.store_name AS STRING)   AS store_name,
            CAST(sb.city AS STRING)         AS city,
            CAST(sb.state AS STRING)        AS state,
            CAST(sb.store_type AS STRING)   AS store_type,
            DATE(sb.open_date)              AS open_date,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.store_id
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date
    FROM base
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

# -------------------------------------------------------------------
# 4) sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(stb.transaction_id AS STRING)   AS transaction_id,
            CAST(stb.store_id AS STRING)         AS store_id,
            CAST(stb.product_id AS STRING)       AS product_id,
            CAST(stb.quantity AS INT)            AS quantity,
            CAST(stb.sale_amount AS DOUBLE)      AS sale_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_id
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time
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