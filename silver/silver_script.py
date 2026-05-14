
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
# Read Source Tables + Create Temp Views
# ------------------------------------------------------------

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")

store_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_bronze.{FILE_FORMAT}/")
)
store_details_bronze_df.createOrReplaceTempView("store_details_bronze")

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(stb.transaction_id) AS sales_transaction_id,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            NULLIF(TRIM(stb.product_id), '') AS product_id,
            NULLIF(TRIM(stb.store_id), '') AS store_id,
            CASE
                WHEN COALESCE(CAST(stb.quantity AS INT), 0) < 0 THEN 0
                ELSE COALESCE(CAST(stb.quantity AS INT), 0)
            END AS quantity_sold,
            CASE
                WHEN COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) < 0.0 THEN 0.0
                ELSE COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0)
            END AS total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        sales_transaction_id,
        sale_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
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

# ------------------------------------------------------------
# Target: silver.product_master_silver
# ------------------------------------------------------------

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pdb.product_id) AS product_id,
            TRIM(pdb.product_name) AS product_name,
            TRIM(pdb.category) AS product_category,
            TRIM(pdb.brand) AS product_brand,
            CASE
                WHEN CAST(pdb.price AS DOUBLE) < 0 THEN NULL
                ELSE CAST(pdb.price AS DOUBLE)
            END AS product_price,
            COALESCE(CAST(pdb.is_active AS BOOLEAN), true) AS is_active
        FROM product_details_bronze pdb
    ),
    filtered AS (
        SELECT *
        FROM base
        WHERE is_active = true
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            product_category,
            product_brand,
            product_price,
            is_active,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_name DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_brand,
        product_price,
        is_active
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.store_master_silver
# ------------------------------------------------------------

store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(sdb.store_id) AS store_id,
            TRIM(sdb.store_name) AS store_name,
            TRIM(sdb.store_type) AS store_type,
            TRIM(concat_ws(', ', sdb.city, sdb.state)) AS store_location,
            TRIM(sdb.state) AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY sdb.store_id
                ORDER BY sdb.open_date DESC
            ) AS rn
        FROM store_details_bronze sdb
    )
    SELECT
        store_id,
        store_name,
        store_type,
        store_location,
        store_region
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

