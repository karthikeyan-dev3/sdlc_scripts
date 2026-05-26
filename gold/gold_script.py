import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------------
transaction_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transaction_details_silver.{FILE_FORMAT}/")
)

enriched_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/enriched_transactions_silver.{FILE_FORMAT}/")
)

product_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)

store_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")
)

daily_aggregates_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_aggregates_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
transaction_details_silver_df.createOrReplaceTempView("transaction_details_silver")
enriched_transactions_silver_df.createOrReplaceTempView("enriched_transactions_silver")
product_details_silver_df.createOrReplaceTempView("product_details_silver")
store_details_silver_df.createOrReplaceTempView("store_details_silver")
daily_aggregates_silver_df.createOrReplaceTempView("daily_aggregates_silver")

# ------------------------------------------------------------------------------------
# Target: gold_sales_transactions
# Source: silver.transaction_details_silver tds
# ------------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(tds.transaction_id AS STRING) AS transaction_id,
        CAST(tds.store_id AS STRING) AS store_id,
        CAST(tds.product_id AS STRING) AS product_id,
        CAST(tds.transaction_date AS DATE) AS transaction_date,
        CAST(tds.quantity_sold AS INT) AS quantity_sold,
        CAST(tds.total_revenue AS DOUBLE) AS total_revenue
    FROM transaction_details_silver tds
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_product_performance
# Source: silver.enriched_transactions_silver ets
#         INNER JOIN silver.product_details_silver pds ON ets.product_id = pds.product_id
# ------------------------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(ets.product_id AS STRING) AS product_id,
        CAST(pds.product_name AS STRING) AS product_name,
        CAST(SUM(CAST(ets.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(ets.quantity_sold AS INT)) AS INT) AS quantity_sold,
        CAST(
            SUM(CAST(ets.total_revenue AS DOUBLE)) / SUM(CAST(ets.quantity_sold AS INT))
            AS DOUBLE
        ) AS average_unit_price
    FROM enriched_transactions_silver ets
    INNER JOIN product_details_silver pds
        ON ets.product_id = pds.product_id
    GROUP BY
        ets.product_id,
        pds.product_name
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_store_performance
# Source: silver.enriched_transactions_silver ets
#         INNER JOIN silver.store_details_silver sds ON ets.store_id = sds.store_id
# ------------------------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(ets.store_id AS STRING) AS store_id,
        CAST(sds.store_name AS STRING) AS store_name,
        CAST(SUM(CAST(ets.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT ets.transaction_id) AS BIGINT) AS transaction_count,
        CAST(SUM(CAST(ets.quantity_sold AS INT)) AS BIGINT) AS total_items_sold
    FROM enriched_transactions_silver ets
    INNER JOIN store_details_silver sds
        ON ets.store_id = sds.store_id
    GROUP BY
        ets.store_id,
        sds.store_name
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_sales_enrichment
# Source: silver.enriched_transactions_silver ets
# ------------------------------------------------------------------------------------
gold_sales_enrichment_df = spark.sql(
    """
    SELECT
        CAST(ets.transaction_id AS STRING) AS transaction_id,
        CAST(ets.product_name AS STRING) AS product_name,
        CAST(ets.store_name AS STRING) AS store_name,
        CAST(ets.standardized_date AS DATE) AS standardized_date
    FROM enriched_transactions_silver ets
    """
)

(
    gold_sales_enrichment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_enrichment.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_aggregated_data
# Source: silver.daily_aggregates_silver das
# ------------------------------------------------------------------------------------
gold_aggregated_data_df = spark.sql(
    """
    SELECT
        CAST(das.date AS DATE) AS date,
        CAST(das.total_revenue AS DOUBLE) AS total_revenue,
        CAST(das.total_transactions AS BIGINT) AS total_transactions,
        CAST(das.total_quantity_sold AS BIGINT) AS total_quantity_sold
    FROM daily_aggregates_silver das
    """
)

(
    gold_aggregated_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_data.csv")
)

job.commit()
