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
# 1) Read source tables from S3
# ----------------------------
sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sales_silver_df.createOrReplaceTempView("sales_silver")
product_silver_df.createOrReplaceTempView("product_silver")
store_silver_df.createOrReplaceTempView("store_silver")

# ----------------------------
# gold.gold_sales
# ----------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sls.transaction_id AS STRING) AS transaction_id,
        DATE(sls.date) AS date,
        CAST(sls.product_id AS STRING) AS product_id,
        CAST(sls.store_id AS STRING) AS store_id,
        CAST(sls.quantity_sold AS INT) AS quantity_sold,
        CAST(sls.total_sales_value AS DOUBLE) AS total_sales_value
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

# ----------------------------
# gold.gold_product
# ----------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.brand AS STRING) AS brand,
        CAST(ps.price AS FLOAT) AS price
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

# ----------------------------
# gold.gold_store
# ----------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        DATE(ss.opening_date) AS opening_date
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

# ----------------------------
# gold.gold_aggregated_sales
# ----------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(sls.date) AS date,
        CAST(SUM(CAST(sls.total_sales_value AS DOUBLE)) AS DOUBLE) AS total_sales_value,
        CAST(SUM(CAST(sls.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
        CAST(
            SUM(CAST(sls.total_sales_value AS DOUBLE)) / COUNT(DISTINCT CAST(sls.store_id AS STRING))
            AS DOUBLE
        ) AS average_sales_per_store
    FROM sales_silver sls
    GROUP BY DATE(sls.date)
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()