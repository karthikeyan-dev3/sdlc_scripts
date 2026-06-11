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

# -----------------------------------------------------------------------------------
# 1) READ SOURCE TABLES (S3) + TEMP VIEWS
# -----------------------------------------------------------------------------------
dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")

dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# 2) TARGET: gold_dim_product
# -----------------------------------------------------------------------------------
gold_dim_product_df = spark.sql(
    """
    SELECT
        dps.product_id AS product_id,
        dps.product_name AS product_name,
        dps.brand AS brand,
        dps.category AS category
    FROM dim_product_silver dps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# -----------------------------------------------------------------------------------
# 3) TARGET: gold_dim_store
# -----------------------------------------------------------------------------------
gold_dim_store_df = spark.sql(
    """
    SELECT
        dss.store_id AS store_id,
        dss.store_name AS store_name,
        dss.city AS city,
        dss.state AS state
    FROM dim_store_silver dss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# -----------------------------------------------------------------------------------
# 4) TARGET: gold_sales_transactions
# -----------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS sales_txn_id,
        CAST(sts.transaction_time AS DATE) AS sales_date,
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.sale_amount AS DOUBLE) AS gross_sales_amount,
        CAST(sts.sale_amount AS DOUBLE) AS net_sales_amount
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
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -----------------------------------------------------------------------------------
# 5) TARGET: gold_sales_daily_store_product
# -----------------------------------------------------------------------------------
gold_sales_daily_store_product_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE) AS sales_date,
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS gross_sales_amount,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS net_sales_amount,
        COUNT(sts.transaction_id) AS transaction_count
    FROM sales_transactions_silver sts
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        sts.store_id,
        sts.product_id
    """
)

(
    gold_sales_daily_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store_product.csv")
)

job.commit()