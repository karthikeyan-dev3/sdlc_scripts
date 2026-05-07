import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =====================================================================================
# Read Source Tables (S3) + Temp Views
# =====================================================================================

sales_daily_store_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_daily_store_product_silver.{FILE_FORMAT}/")
)
sales_daily_store_product_silver_df.createOrReplaceTempView("sales_daily_store_product_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# =====================================================================================
# Target: gold.gold_sales_performance
# Mapping: silver.sales_daily_store_product_silver sdsp
# =====================================================================================

gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sdsp.store_id AS STRING)      AS store_id,
        CAST(sdsp.store_name AS STRING)    AS store_name,
        CAST(sdsp.region AS STRING)        AS region,
        CAST(sdsp.product_id AS STRING)    AS product_id,
        CAST(sdsp.product_name AS STRING)  AS product_name,
        CAST(sdsp.category AS STRING)      AS category,
        DATE(sdsp.sales_date)             AS sales_date,
        CAST(sdsp.units_sold AS INT)       AS units_sold,
        CAST(sdsp.sales_revenue AS DOUBLE) AS sales_revenue
    FROM sales_daily_store_product_silver sdsp
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# =====================================================================================
# Target: gold.gold_store_master
# Mapping: silver.store_master_silver sms
# =====================================================================================

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)   AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.region AS STRING)     AS region
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_master.csv")
)

# =====================================================================================
# Target: gold.gold_product_master
# Mapping: silver.product_master_silver pms
# =====================================================================================

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)   AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING)     AS category
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_master.csv")
)

job.commit()
