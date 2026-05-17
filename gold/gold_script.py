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
# Source Reads + Temp Views
# -------------------------------------------------------------------

sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

product_performance_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_performance_silver.{FILE_FORMAT}/")
)
product_performance_silver_df.createOrReplaceTempView("product_performance_silver")

store_performance_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_performance_silver.{FILE_FORMAT}/")
)
store_performance_silver_df.createOrReplaceTempView("store_performance_silver")

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------------------------------------------------
# Target: gold_sales
# -------------------------------------------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ses.sale_id AS STRING) AS sale_id,
        CAST(ses.transaction_date AS DATE) AS transaction_date,
        CAST(ses.product_id AS STRING) AS product_id,
        CAST(ses.store_id AS STRING) AS store_id,
        CAST(ses.customer_id AS STRING) AS customer_id,
        CAST(ses.quantity_sold AS INT) AS quantity_sold,
        CAST(ses.total_revenue AS DOUBLE) AS total_revenue
    FROM sales_enriched_silver ses
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -------------------------------------------------------------------
# Target: gold_product_performance
# -------------------------------------------------------------------

gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(pps.product_id AS STRING) AS product_id,
        CAST(pps.category AS STRING) AS category,
        CAST(pps.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(pps.total_revenue AS DOUBLE) AS total_revenue,
        CAST(pps.average_price AS FLOAT) AS average_price
    FROM product_performance_silver pps
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
# Target: gold_store_performance
# -------------------------------------------------------------------

gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.store_id AS STRING) AS store_id,
        CAST(sps.total_transactions AS INT) AS total_transactions,
        CAST(sps.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sps.total_quantity_sold AS INT) AS total_quantity_sold
    FROM store_performance_silver sps
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
# Target: gold_product_master
# -------------------------------------------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.price AS FLOAT) AS price,
        CAST(pms.supplier_id AS STRING) AS supplier_id
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.store_type AS STRING) AS store_type,
        CAST(sms.manager_id AS STRING) AS manager_id
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

job.commit()