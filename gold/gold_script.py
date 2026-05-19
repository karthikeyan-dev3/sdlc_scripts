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
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
ps_df.createOrReplaceTempView("products_silver")
ss_df.createOrReplaceTempView("stores_silver")

# ----------------------------
# gold.gold_sales
# ----------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.clean_transaction_date AS DATE) AS transaction_date,
        CAST(sts.validated_store_id AS STRING) AS store_id,
        CAST(sts.validated_product_id AS STRING) AS product_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue
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

# ----------------------------
# gold.gold_product_performance
# ----------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.validated_product_id AS STRING) AS product_id,
        CAST(ps.category AS STRING) AS product_category,
        CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS quantity_sold,
        CAST(
            SUM(CAST(sts.total_revenue AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS INT))
            AS DOUBLE
        ) AS average_price
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.validated_product_id = ps.product_id
    GROUP BY
        sts.validated_product_id,
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

# ----------------------------
# gold.gold_store_performance
# ----------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.validated_store_id AS STRING) AS store_id,
        CAST(ss.store_location AS STRING) AS store_location,
        CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(sts.transaction_id) AS INT) AS total_transactions,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.validated_store_id = ss.store_id
    GROUP BY
        sts.validated_store_id,
        ss.store_location
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ----------------------------
# gold.gold_cleaned_sales
# ----------------------------
gold_cleaned_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.clean_transaction_date AS DATE) AS clean_transaction_date,
        CAST(sts.validated_store_id AS STRING) AS validated_store_id,
        CAST(sts.validated_product_id AS STRING) AS validated_product_id,
        CAST(sts.deduplicated_records AS BOOLEAN) AS deduplicated_records
    FROM sales_transactions_silver sts
    """
)

(
    gold_cleaned_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_sales.csv")
)

# ----------------------------
# gold.gold_aggregated_sales
# ----------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.clean_transaction_date AS DATE) AS date,
        CAST(SUM(CAST(sts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(sts.transaction_id) AS INT) AS total_transactions,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
        sts.clean_transaction_date
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