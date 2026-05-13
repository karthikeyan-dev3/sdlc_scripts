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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ------------------------------------------------------------------------------

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------------------------

gold_product_master_df = spark.sql("""
WITH ranked AS (
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand,
        CAST(pms.price AS DOUBLE) AS price,
        CAST(NULL AS DOUBLE) AS cost,
        ROW_NUMBER() OVER (
            PARTITION BY pms.product_id
            ORDER BY pms.product_id
        ) AS rn
    FROM product_master_silver pms
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    cost
FROM ranked
WHERE rn = 1
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------------------------

gold_store_master_df = spark.sql("""
WITH ranked AS (
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        DATE(CAST(sms.opening_date AS STRING)) AS opening_date,
        CAST(sms.store_type AS STRING) AS store_type,
        ROW_NUMBER() OVER (
            PARTITION BY sms.store_id
            ORDER BY sms.store_id
        ) AS rn
    FROM store_master_silver sms
)
SELECT
    store_id,
    store_name,
    location,
    opening_date,
    store_type
FROM ranked
WHERE rn = 1
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_sales_transactions
# ------------------------------------------------------------------------------

gold_sales_transactions_df = spark.sql("""
WITH ranked AS (
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(CAST(sts.transaction_date AS STRING)) AS transaction_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sts.discount_amount AS DOUBLE) AS discount_amount,
        CAST(sts.net_sales_amount AS DOUBLE) AS net_sales_amount,
        ROW_NUMBER() OVER (
            PARTITION BY sts.transaction_id
            ORDER BY sts.transaction_id
        ) AS rn
    FROM sales_transactions_silver sts
)
SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales_amount,
    discount_amount,
    net_sales_amount
FROM ranked
WHERE rn = 1
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------------------------
# Target: gold_sales_aggregated
# ------------------------------------------------------------------------------

gold_sales_aggregated_df = spark.sql("""
SELECT
    DATE(CAST(sts.transaction_date AS STRING)) AS aggregation_date,
    CAST(sts.store_id AS STRING) AS store_id,
    CAST(sts.product_id AS STRING) AS product_id,
    SUM(CAST(sts.net_sales_amount AS DOUBLE)) AS total_sales,
    SUM(CAST(sts.quantity_sold AS INT)) AS total_quantity_sold,
    AVG(CAST(sts.discount_amount AS DOUBLE)) AS average_discount
FROM sales_transactions_silver sts
GROUP BY
    DATE(CAST(sts.transaction_date AS STRING)),
    CAST(sts.store_id AS STRING),
    CAST(sts.product_id AS STRING)
""")

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()