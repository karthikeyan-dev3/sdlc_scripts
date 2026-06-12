import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

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
# Read Source Tables (S3) + Temp Views
# -------------------------------------------------------------------
product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
product_silver_df.createOrReplaceTempView("product_silver")

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
store_silver_df.createOrReplaceTempView("store_silver")

sales_transaction_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transaction_silver.{FILE_FORMAT}/")
)
sales_transaction_silver_df.createOrReplaceTempView("sales_transaction_silver")

# -------------------------------------------------------------------
# Target: gold_dim_product
# Mapping: silver.product_silver ps
# -------------------------------------------------------------------
gold_dim_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.brand AS STRING)        AS brand,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.is_active AS BOOLEAN)   AS is_active
    FROM product_silver ps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# -------------------------------------------------------------------
# Target: gold_dim_store
# Mapping: silver.store_silver ss
# -------------------------------------------------------------------
gold_dim_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)    AS store_id,
        CAST(ss.store_name AS STRING)  AS store_name,
        CAST(ss.city AS STRING)        AS city,
        CAST(ss.state AS STRING)       AS state,
        CAST(ss.store_type AS STRING)  AS store_type,
        CAST(ss.open_date AS DATE)     AS open_date
    FROM store_silver ss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# -------------------------------------------------------------------
# Target: gold_fct_sales
# Mapping: silver.sales_transaction_silver sts
#          INNER JOIN silver.store_silver ss ON sts.store_id = ss.store_id
#          INNER JOIN silver.product_silver ps ON sts.product_id = ps.product_id
# -------------------------------------------------------------------
gold_fct_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)          AS sales_txn_id,
        CAST(sts.store_id AS STRING)               AS store_id,
        CAST(sts.product_id AS STRING)             AS product_id,
        CAST(sts.quantity AS INT)                  AS quantity,
        CAST(sts.sale_amount AS DOUBLE)            AS gross_sales_amount,
        CAST(sts.transaction_time AS DATE)         AS sales_date
    FROM sales_transaction_silver sts
    INNER JOIN store_silver ss
        ON sts.store_id = ss.store_id
    INNER JOIN product_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    gold_fct_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_fct_sales.csv")
)

# -------------------------------------------------------------------
# Target: gold_agg_sales_daily_store_product
# Mapping: silver.sales_transaction_silver sts
#          INNER JOIN silver.store_silver ss ON sts.store_id = ss.store_id
#          INNER JOIN silver.product_silver ps ON sts.product_id = ps.product_id
# -------------------------------------------------------------------
gold_agg_sales_daily_store_product_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE)          AS sales_date,
        CAST(sts.store_id AS STRING)               AS store_id,
        CAST(sts.product_id AS STRING)             AS product_id,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT)          AS units_sold,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS gross_sales_amount,
        CAST(COUNT(DISTINCT sts.transaction_id) AS INT)      AS txn_count
    FROM sales_transaction_silver sts
    INNER JOIN store_silver ss
        ON sts.store_id = ss.store_id
    INNER JOIN product_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(sts.product_id AS STRING)
    """
)

(
    gold_agg_sales_daily_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_sales_daily_store_product.csv")
)

job.commit()
