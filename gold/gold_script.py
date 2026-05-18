import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Read source tables from S3
# -------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# Create temp views
# -------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.revenue AS DOUBLE) AS revenue,
        CAST(sts.transaction_date AS DATE) AS transaction_date
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
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
        CAST(pms.price AS DECIMAL(10,2)) AS price
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
        CAST(sms.location AS STRING) AS location
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

# -------------------------------------------------------------------
# Target: gold_aggregated_store_performance
# -------------------------------------------------------------------
gold_aggregated_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.transaction_date AS DATE) AS aggregation_date,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity_sold,
        CAST(COUNT(DISTINCT CAST(sts.product_id AS STRING)) AS INT) AS unique_products_sold
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.transaction_date
    """
)

(
    gold_aggregated_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_store_performance.csv")
)

# -------------------------------------------------------------------
# Target: gold_aggregated_product_performance
# -------------------------------------------------------------------
gold_aggregated_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.transaction_date AS DATE) AS aggregation_date,
        CAST(SUM(CAST(sts.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_quantity_sold,
        CAST(COUNT(DISTINCT CAST(sts.store_id AS STRING)) AS INT) AS number_of_stores_sold
    FROM sales_transactions_silver sts
    GROUP BY
        sts.product_id,
        sts.transaction_date
    """
)

(
    gold_aggregated_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_product_performance.csv")
)

job.commit()
