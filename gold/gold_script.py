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

# ============================================================
# Read Source Tables (S3) + Temp Views
# ============================================================

sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)
sales_silver_df.createOrReplaceTempView("sales_silver")

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

# ============================================================
# Target: gold.gold_sales
# ============================================================

gold_sales_df = spark.sql(
    """
    SELECT
        ss.transaction_id AS transaction_id,
        ss.store_id AS store_id,
        ss.product_id AS product_id,
        CAST(ss.sale_date AS DATE) AS sale_date,
        CAST(ss.total_revenue AS DOUBLE) AS total_revenue,
        CAST(ss.quantity_sold AS INT) AS quantity_sold,
        COUNT(ss.transaction_id) OVER (PARTITION BY ss.store_id) AS transaction_count
    FROM sales_silver ss
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ============================================================
# Target: gold.gold_product_master
# ============================================================

gold_product_master_df = spark.sql(
    """
    SELECT
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        CAST(pms.price AS FLOAT) AS price,
        pms.vendor AS vendor
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

# ============================================================
# Target: gold.gold_store_master
# ============================================================

gold_store_master_df = spark.sql(
    """
    SELECT
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.location AS location
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