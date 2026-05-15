```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContex
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
# Read Source Tables from S3
# -----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_aggregated_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

data_validation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_validation_silver.{FILE_FORMAT}/")
)
data_validation_silver_df.createOrReplaceTempView("data_validation_silver")

# -----------------------------
# Target: gold_sales
# -----------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)  AS transaction_id,
        CAST(sts.transaction_date AS DATE)  AS transaction_date,
        CAST(sts.product_id AS STRING)      AS product_id,
        CAST(sts.store_id AS STRING)        AS store_id,
        CAST(sts.quantity_sold AS INT)      AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE)    AS sales_amount
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -----------------------------
# Target: gold_product_details
# -----------------------------
gold_product_details_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)     AS product_id,
        CAST(ps.product_name AS STRING)   AS product_name,
        CAST(ps.category AS STRING)       AS category,
        CAST(ps.price AS FLOAT)           AS price
    FROM products_silver ps
    """
)

(
    gold_product_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_details.csv")
)

# -----------------------------
# Target: gold_store_details
# -----------------------------
gold_store_details_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)        AS store_id,
        CAST(ss.store_name AS STRING)      AS store_name,
        CAST(ss.region AS STRING)          AS region,
        CAST(ss.store_manager AS STRING)   AS store_manager
    FROM stores_silver ss
    """
)

(
    gold_store_details_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_details.csv")
)

# -----------------------------
# Target: gold_sales_aggregated
# -----------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(sas.aggregation_date AS DATE)       AS aggregation_date,
        CAST(sas.total_sales AS DOUBLE)          AS total_sales,
        CAST(sas.total_quantity AS INT)          AS total_quantity,
        CAST(sas.top_selling_product_id AS STRING) AS top_selling_product_id,
        CAST(sas.top_selling_store_id AS STRING)   AS top_selling_store_id
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

# -----------------------------
# Target: gold_data_validation
# -----------------------------
gold_data_validation_df = spark.sql(
    """
    SELECT
        CAST(dvs.record_id AS STRING)           AS record_id,
        CAST(dvs.is_valid AS BOOLEAN)           AS is_valid,
        CAST(dvs.validation_errors AS STRING)   AS validation_errors
    FROM data_validation_silver dvs
    """
)

(
    gold_data_validation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_validation.csv")
)

job.commit()
```