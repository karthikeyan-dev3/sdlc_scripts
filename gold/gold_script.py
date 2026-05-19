import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =============================================================================
# Read Source Tables (S3)
# =============================================================================

sales_cleaned_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_cleaned_silver.{FILE_FORMAT}/")
)
sales_cleaned_silver_df.createOrReplaceTempView("sales_cleaned_silver")

sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

store_transaction_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_transaction_summary_silver.{FILE_FORMAT}/")
)
store_transaction_summary_silver_df.createOrReplaceTempView("store_transaction_summary_silver")

product_sales_summary_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_sales_summary_silver.{FILE_FORMAT}/")
)
product_sales_summary_silver_df.createOrReplaceTempView("product_sales_summary_silver")

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# =============================================================================
# Target: gold_cleaned_sales (gcs) from silver.sales_cleaned_silver (scs)
# Columns: transaction_id, store_id, product_id, quantity_sold, transaction_date, cleaned
# =============================================================================

gold_cleaned_sales_df = spark.sql(
    """
    SELECT
        CAST(scs.transaction_id AS STRING)                           AS transaction_id,
        CAST(scs.store_id AS STRING)                                 AS store_id,
        CAST(scs.product_id AS STRING)                               AS product_id,
        CAST(scs.quantity_sold AS INT)                               AS quantity_sold,
        CAST(scs.transaction_date AS DATE)                           AS transaction_date,
        CAST(scs.cleaned AS BOOLEAN)                                 AS cleaned
    FROM sales_cleaned_silver scs
    """
)

(
    gold_cleaned_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_sales.csv")
)

# =============================================================================
# Target: gold_sales_enriched (gse) from silver.sales_enriched_silver (ses)
# Columns: transaction_id, store_id, product_id, quantity_sold, transaction_date, product_name, store_name, category
# =============================================================================

gold_sales_enriched_df = spark.sql(
    """
    SELECT
        CAST(ses.transaction_id AS STRING)                           AS transaction_id,
        CAST(ses.store_id AS STRING)                                 AS store_id,
        CAST(ses.product_id AS STRING)                               AS product_id,
        CAST(ses.quantity_sold AS INT)                               AS quantity_sold,
        CAST(ses.transaction_date AS DATE)                           AS transaction_date,
        CAST(ses.product_name AS STRING)                             AS product_name,
        CAST(ses.store_name AS STRING)                               AS store_name,
        CAST(ses.category AS STRING)                                 AS category
    FROM sales_enriched_silver ses
    """
)

(
    gold_sales_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_enriched.csv")
)

# =============================================================================
# Target: gold_store_performance (gsp) from silver.store_transaction_summary_silver (stss)
# Columns: store_id, store_name, total_revenue, transaction_count, last_updated
# NOTE: last_updated not provided in column mappings; selecting only mapped columns.
# =============================================================================

gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(stss.store_id AS STRING)                                AS store_id,
        CAST(stss.store_name AS STRING)                              AS store_name,
        CAST(stss.total_revenue AS DOUBLE)                           AS total_revenue,
        CAST(stss.transaction_count AS INT)                          AS transaction_count
    FROM store_transaction_summary_silver stss
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# =============================================================================
# Target: gold_product_performance (gpp) from silver.product_sales_summary_silver (pss)
# Columns: product_id, product_name, category, revenue_contribution, units_sold, last_updated
# NOTE: last_updated not provided in column mappings; selecting only mapped columns.
# =============================================================================

gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(pss.product_id AS STRING)                               AS product_id,
        CAST(pss.product_name AS STRING)                             AS product_name,
        CAST(pss.category AS STRING)                                 AS category,
        CAST(pss.revenue_contribution AS DOUBLE)                     AS revenue_contribution,
        CAST(pss.units_sold AS INT)                                  AS units_sold
    FROM product_sales_summary_silver pss
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# =============================================================================
# Target: gold_aggregated_sales (gas) from silver.aggregated_sales_silver (ass)
# Columns: date, store_id, total_revenue, total_transactions, unique_products_sold
# NOTE: total_revenue not provided in column mappings; selecting only mapped columns.
# =============================================================================

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.date AS DATE)                                       AS date,
        CAST(ass.store_id AS STRING)                                 AS store_id,
        CAST(ass.total_transactions AS INT)                          AS total_transactions,
        CAST(ass.unique_products_sold AS INT)                        AS unique_products_sold
    FROM aggregated_sales_silver ass
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