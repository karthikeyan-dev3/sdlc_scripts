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

# ----------------------------
# Read Source Tables (S3)
# ----------------------------
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
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
sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
transactions_silver_df.createOrReplaceTempView("transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# ============================================================
# Target: gold_sales_performance
# ============================================================
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ts.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        DATE(ts.transaction_date) AS transaction_date,
        CAST(ts.total_revenue AS DOUBLE) AS total_revenue,
        CAST(ts.quantity_sold AS INT) AS quantity_sold
    FROM transactions_silver ts
    INNER JOIN stores_silver ss
        ON ts.store_id = ss.store_id
    INNER JOIN products_silver ps
        ON ts.product_id = ps.product_id
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ============================================================
# Target: gold_sales_aggregated
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.product_id AS STRING) AS product_id,
        DATE(sas.sales_date) AS sales_date,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sas.sales_count AS BIGINT) AS sales_count
    FROM sales_aggregated_silver sas
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ============================================================
# Target: gold_data_quality_metrics
# ============================================================
gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        DATE(dqms.metric_date) AS metric_date,
        CAST(dqms.data_freshness AS DOUBLE) AS data_freshness,
        CAST(dqms.data_completeness_score AS DOUBLE) AS data_completeness_score,
        CAST(dqms.record_accuracy_percentage AS DOUBLE) AS record_accuracy_percentage,
        CAST(dqms.deduplication_status AS STRING) AS deduplication_status
    FROM data_quality_metrics_silver dqms
    """
)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)

job.commit()