import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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
# Read Source Tables from S3
# -------------------------------------------------------------------
transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
data_quality_assessments_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_assessments_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create Temp Views
# -------------------------------------------------------------------
transactions_silver_df.createOrReplaceTempView("transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
data_quality_assessments_silver_df.createOrReplaceTempView("data_quality_assessments_silver")

# -------------------------------------------------------------------
# Target: gold_sales_performance (gsp)
# -------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        ts.transaction_id AS transaction_id,
        CAST(ts.transaction_time AS date) AS date,
        ts.store_id AS store_id,
        ts.product_id AS product_id,
        CAST(ts.quantity AS int) AS quantity,
        CAST(ts.sale_amount AS double) AS revenue,
        ps.category AS category
    FROM transactions_silver ts
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

# -------------------------------------------------------------------
# Target: gold_store_performance (gstore)
# -------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        ts.store_id AS store_id,
        CAST(ts.transaction_time AS date) AS date,
        SUM(CAST(ts.sale_amount AS double)) AS total_revenue,
        COUNT(DISTINCT ts.transaction_id) AS total_transactions,
        SUM(CAST(ts.quantity AS bigint)) AS total_quantity_sold
    FROM transactions_silver ts
    INNER JOIN stores_silver ss
        ON ts.store_id = ss.store_id
    GROUP BY
        ts.store_id,
        CAST(ts.transaction_time AS date)
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_performance (gpp)
# -------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        ts.product_id AS product_id,
        CAST(ts.transaction_time AS date) AS date,
        SUM(CAST(ts.sale_amount AS double)) AS total_revenue,
        SUM(CAST(ts.quantity AS bigint)) AS total_quantity_sold,
        ps.category AS category_performance
    FROM transactions_silver ts
    INNER JOIN products_silver ps
        ON ts.product_id = ps.product_id
    GROUP BY
        ts.product_id,
        CAST(ts.transaction_time AS date),
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

# -------------------------------------------------------------------
# Target: gold_data_quality_metrics (gdqm)
# Note: Columns not provided in UDT columns section; selecting base table as-is is avoided.
# -------------------------------------------------------------------
gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        *
    FROM data_quality_assessments_silver dq
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