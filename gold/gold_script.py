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

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)

dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -------------------------------------------------------------------
# 3) Transform + 4) Save each target table separately
# -------------------------------------------------------------------

# gold.gold_dim_product
gold_dim_product_df = spark.sql(
    """
    SELECT
        CAST(dps.product_id AS STRING)      AS product_id,
        CAST(dps.product_name AS STRING)    AS product_name,
        CAST(dps.brand AS STRING)           AS brand,
        CAST(dps.category AS STRING)        AS category
    FROM dim_product_silver dps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product")
)

# gold.gold_dim_store
gold_dim_store_df = spark.sql(
    """
    SELECT
        CAST(dss.store_id AS STRING)     AS store_id,
        CAST(dss.store_name AS STRING)   AS store_name,
        CAST(dss.city AS STRING)         AS city,
        CAST(dss.state AS STRING)        AS state
    FROM dim_store_silver dss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store")
)

# gold.gold_sales_transactions
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)        AS sales_txn_id,
        CAST(sts.transaction_time AS DATE)        AS sales_date,
        CAST(sts.store_id AS STRING)              AS store_id,
        CAST(sts.product_id AS STRING)            AS product_id,
        CAST(sts.quantity AS INT)                 AS quantity,
        CAST(NULL AS DOUBLE)                      AS unit_price,
        CAST(sts.sale_amount AS DOUBLE)           AS gross_sales_amount,
        CAST(0 AS DOUBLE)                         AS discount_amount,
        CAST(sts.sale_amount AS DOUBLE)           AS net_sales_amount,
        CAST(NULL AS STRING)                      AS currency_code
    FROM sales_transactions_silver sts
    LEFT JOIN dim_store_silver dss
        ON sts.store_id = dss.store_id
    LEFT JOIN dim_product_silver dps
        ON sts.product_id = dps.product_id
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions")
)

# gold.gold_sales_daily_store_product
gold_sales_daily_store_product_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE)        AS sales_date,
        CAST(sts.store_id AS STRING)              AS store_id,
        CAST(sts.product_id AS STRING)            AS product_id,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)        AS total_quantity,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS gross_sales_amount,
        CAST(0 AS DOUBLE)                         AS discount_amount,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS net_sales_amount,
        CAST(COUNT(sts.transaction_id) AS BIGINT) AS transaction_count
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(sts.product_id AS STRING)
    """
)

(
    gold_sales_daily_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store_product")
)

job.commit()