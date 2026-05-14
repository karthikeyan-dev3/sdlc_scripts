
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

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

uss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/unified_sales_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
uss_df.createOrReplaceTempView("unified_sales_silver")

# ------------------------------------------------------------
# 3) Transform + 4) Save each target table separately
# ------------------------------------------------------------

# gold_sales_transactions
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.transaction_date)         AS transaction_date,
        CAST(sts.store_id AS STRING)       AS store_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.quantity AS INT)          AS quantity,
        CAST(sts.revenue AS DOUBLE)        AS revenue
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# gold_store_performance
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(uss.store_id AS STRING)        AS store_id,
        CAST(uss.store_name AS STRING)      AS store_name,
        CAST(uss.region AS STRING)          AS region,
        uss.analysis_date                   AS analysis_date,
        CAST(SUM(uss.revenue) AS DOUBLE)    AS total_revenue,
        CAST(SUM(uss.quantity) AS INT)      AS total_quantity_sold,
        CAST(COUNT(uss.transaction_id) AS STRING) AS total_transactions
    FROM unified_sales_silver uss
    GROUP BY
        uss.store_id,
        uss.store_name,
        uss.region,
        uss.analysis_date
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# gold_product_performance
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(uss.product_id AS STRING)      AS product_id,
        CAST(uss.product_name AS STRING)    AS product_name,
        CAST(uss.category AS STRING)        AS category,
        uss.analysis_date                   AS analysis_date,
        CAST(SUM(uss.revenue) AS DOUBLE)    AS total_revenue,
        CAST(SUM(uss.quantity) AS INT)      AS total_quantity_sold,
        CAST(COUNT(uss.transaction_id) AS STRING) AS total_transactions
    FROM unified_sales_silver uss
    GROUP BY
        uss.product_id,
        uss.product_name,
        uss.category,
        uss.analysis_date
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# gold_unified_dataset
gold_unified_dataset_df = spark.sql(
    """
    SELECT
        CAST(uss.transaction_id AS STRING)  AS transaction_id,
        DATE(uss.transaction_date)          AS transaction_date,
        CAST(uss.store_id AS STRING)        AS store_id,
        CAST(uss.store_name AS STRING)      AS store_name,
        CAST(uss.product_id AS STRING)      AS product_id,
        CAST(uss.product_name AS STRING)    AS product_name,
        CAST(uss.category AS STRING)        AS category,
        CAST(uss.quantity AS INT)           AS quantity,
        CAST(uss.revenue AS DOUBLE)         AS revenue,
        CAST(uss.region AS STRING)          AS region
    FROM unified_sales_silver uss
    """
)

(
    gold_unified_dataset_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_unified_dataset.csv")
)

job.commit()
