import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables from S3
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

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

# -----------------------------
# Create Temp Views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            stb.transaction_id,
            CAST(stb.transaction_time AS date) AS date,
            stb.product_id,
            stb.store_id,
            CAST(stb.sale_amount AS double) AS sales_amount,
            CAST(stb.quantity AS int) AS quantity_sold,
            CASE
                WHEN stb.transaction_id IS NOT NULL
                 AND stb.product_id IS NOT NULL
                 AND stb.store_id IS NOT NULL
                 AND stb.sale_amount IS NOT NULL
                 AND stb.quantity IS NOT NULL
                 AND CAST(stb.sale_amount AS double) >= 0
                 AND CAST(stb.quantity AS int) >= 0
                THEN true ELSE false
            END AS cleaned,
            CASE
                WHEN pb.product_id IS NOT NULL
                 AND sb.store_id IS NOT NULL
                THEN true ELSE false
            END AS validated,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_id
            ) AS rn
        FROM sales_transactions_bronze stb
        LEFT JOIN products_bronze pb
            ON stb.product_id = pb.product_id
        LEFT JOIN stores_bronze sb
            ON stb.store_id = sb.store_id
    )
    SELECT
        transaction_id,
        date,
        product_id,
        store_id,
        sales_amount,
        quantity_sold,
        cleaned,
        validated
    FROM joined
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

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            pb.product_id,
            pb.product_name,
            pb.category,
            pb.brand,
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
        brand
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

# ============================================================
# Target: silver.stores_silver
# ============================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sb.store_id,
            sb.store_name,
            CONCAT(sb.city, ', ', sb.state) AS location,
            sb.store_type,
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
        store_type
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

job.commit()