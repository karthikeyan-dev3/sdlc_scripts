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

# -----------------------------
# Read source tables from S3
# -----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# -----------------------------
# Create temp views
# -----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# ============================================================
# Target: gold_sales_transactions
# ============================================================
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.revenue AS DOUBLE) AS revenue,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
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

# ============================================================
# Target: gold_store_performance
# ============================================================
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(sts.transaction_id) AS BIGINT) AS transaction_count,
        CAST(SUM(CAST(sts.quantity_sold AS BIGINT)) AS BIGINT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        sts.store_id,
        ss.store_name
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ============================================================
# Target: gold_product_performance
# ============================================================
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue_contribution,
        CAST(SUM(CAST(sts.quantity_sold AS BIGINT)) AS BIGINT) AS units_sold
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        sts.product_id,
        ps.product_name,
        ps.category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ============================================================
# Target: gold_data_quality
# ============================================================
gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqms.run_date AS DATE) AS run_date,
        CAST(dqms.duplicate_count AS BIGINT) AS duplicate_count,
        CAST(dqms.validation_errors AS BIGINT) AS validation_errors,
        CAST(dqms.data_quality_score AS DOUBLE) AS data_quality_score
    FROM data_quality_metrics_silver dqms
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()