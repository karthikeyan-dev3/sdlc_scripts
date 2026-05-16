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

# ---------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# ---------------------------------------------------------------------
sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)
sales_silver_df.createOrReplaceTempView("sales_silver")

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

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

data_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

# ---------------------------------------------------------------------
# Target: gold_sales
# ---------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sls.transaction_id AS STRING)           AS transaction_id,
        DATE(sls.sale_date)                          AS sale_date,
        CAST(sls.product_id AS STRING)               AS product_id,
        CAST(sls.store_id AS STRING)                 AS store_id,
        CAST(sls.quantity_sold AS INT)               AS quantity_sold,
        CAST(sls.total_sales_amount AS DOUBLE)       AS total_sales_amount
    FROM sales_silver sls
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ---------------------------------------------------------------------
# Target: gold_product
# ---------------------------------------------------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)    AS product_id,
        CAST(ps.product_name AS STRING)  AS product_name,
        CAST(ps.category AS STRING)      AS category,
        CAST(ps.brand AS STRING)         AS brand,
        CAST(ps.price AS FLOAT)          AS price
    FROM product_silver ps
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ---------------------------------------------------------------------
# Target: gold_store
# ---------------------------------------------------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)      AS store_id,
        CAST(ss.store_name AS STRING)    AS store_name,
        CAST(ss.region AS STRING)        AS region,
        CAST(ss.store_type AS STRING)    AS store_type
    FROM store_silver ss
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ---------------------------------------------------------------------
# Target: gold_sales_aggregated
# ---------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sas.store_id AS STRING)                 AS store_id,
        CAST(sas.product_id AS STRING)               AS product_id,
        DATE(sas.sale_date)                          AS sale_date,
        CAST(sas.total_quantity_sold AS INT)         AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE)       AS total_sales_amount
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

# ---------------------------------------------------------------------
# Target: gold_data_quality
# ---------------------------------------------------------------------
gold_data_quality_df = spark.sql(
    """
    SELECT
        CAST(dqs.record_id AS STRING)                AS record_id,
        CAST(dqs.data_quality_score AS DOUBLE)       AS data_quality_score,
        CAST(dqs.validation_status AS STRING)        AS validation_status,
        CAST(dqs.last_update_timestamp AS TIMESTAMP) AS last_update_timestamp
    FROM data_quality_silver dqs
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