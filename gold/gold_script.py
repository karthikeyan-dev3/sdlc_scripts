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

# -------------------------
# Read Sources + Temp Views
# -------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sts")

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ss_df.createOrReplaceTempView("ss")

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ps_df.createOrReplaceTempView("ps")

dsas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregates_silver.{FILE_FORMAT}/")
)
dsas_df.createOrReplaceTempView("dsas")

# -------------------------
# Target: gold_sales_performance
# -------------------------

gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        DATE(sts.transaction_date) AS transaction_date,
        CAST(sts.quantity AS INT) AS quantity_sold,
        CAST(sts.sale_amount AS DOUBLE) AS total_revenue
    FROM sts
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# -------------------------
# Target: gold_store_master
# -------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.store_location AS STRING) AS store_location,
        CAST(ss.store_region AS STRING) AS store_region
    FROM ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------
# Target: gold_product_master
# -------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.price AS FLOAT) AS price
    FROM ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------
# Target: gold_daily_aggregates
# -------------------------

gold_daily_aggregates_df = spark.sql(
    """
    SELECT
        CAST(dsas.store_id AS STRING) AS store_id,
        DATE(dsas.transaction_date) AS transaction_date,
        CAST(dsas.total_revenue_per_store AS DOUBLE) AS total_revenue_per_store,
        CAST(dsas.total_quantity_sold_per_store AS INT) AS total_quantity_sold_per_store
    FROM dsas
    """
)

(
    gold_daily_aggregates_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_aggregates.csv")
)

job.commit()