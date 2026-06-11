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

# ------------------------------------------------------------------
# Read Sources (S3) + Create Temp Views
# ------------------------------------------------------------------
sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
sms_df.createOrReplaceTempView("store_master_silver")

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
pms_df.createOrReplaceTempView("product_master_silver")

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sts_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING)        AS store_id,
        CAST(sms.store_location AS STRING) AS store_location
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

# ------------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING)        AS product_id,
        CAST(pms.product_name AS STRING)      AS product_name,
        CAST(pms.product_category AS STRING)  AS product_category,
        CAST(pms.product_price AS DOUBLE)     AS product_price
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

# ------------------------------------------------------------------
# Target: gold_sales_performance
# ------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS STRING) AS total_transactions,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY
        sts.store_id,
        sms.store_name,
        sts.transaction_date
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ------------------------------------------------------------------
# Target: gold_product_performance
# ------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.product_category AS STRING) AS product_category,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS revenue_contribution,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS units_sold
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    GROUP BY
        sts.product_id,
        pms.product_name,
        pms.product_category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ------------------------------------------------------------------
# Target: gold_sales_aggregate
# ------------------------------------------------------------------
gold_sales_aggregate_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_date AS DATE) AS date,
        CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT sts.transaction_id) AS STRING) AS total_transactions,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_units_sold
    FROM sales_transactions_silver sts
    GROUP BY
        sts.transaction_date
    """
)

(
    gold_sales_aggregate_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregate.csv")
)

job.commit()