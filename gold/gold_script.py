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
# Source: silver.sales_daily_store_silver (sdss) -> Target: gold.sales_daily_store_gold
# ------------------------------------------------------------------------------------
sdss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_store_silver.{FILE_FORMAT}/")
)
sdss_df.createOrReplaceTempView("sales_daily_store_silver")

sales_daily_store_gold_df = spark.sql(
    """
    SELECT
        CAST(sdss.sales_date AS DATE) AS sales_date,
        CAST(sdss.store_id AS STRING) AS store_id,
        CAST(sdss.store_name AS STRING) AS store_name,
        CAST(sdss.store_city AS STRING) AS store_city,
        CAST(sdss.store_state AS STRING) AS store_state,
        CAST(sdss.store_region AS STRING) AS store_region,
        CAST(sdss.store_type AS STRING) AS store_type,
        CAST(sdss.total_revenue_amount AS DOUBLE) AS total_revenue_amount,
        CAST(sdss.total_transactions_count AS INT) AS total_transactions_count,
        CAST(sdss.total_quantity_units AS INT) AS total_quantity_units,
        CAST(sdss.avg_basket_value_amount AS DOUBLE) AS avg_basket_value_amount,
        CAST(sdss.avg_units_per_transaction AS DOUBLE) AS avg_units_per_transaction,
        CAST(sdss.data_refresh_date AS DATE) AS data_refresh_date
    FROM sales_daily_store_silver sdss
    """
)

(
    sales_daily_store_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_gold.csv")
)

# --------------------------------------------------------------------------------------
# Source: silver.sales_daily_product_silver (sdps) -> Target: gold.sales_daily_product_gold
# --------------------------------------------------------------------------------------
sdps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_product_silver.{FILE_FORMAT}/")
)
sdps_df.createOrReplaceTempView("sales_daily_product_silver")

sales_daily_product_gold_df = spark.sql(
    """
    SELECT
        CAST(sdps.sales_date AS DATE) AS sales_date,
        CAST(sdps.product_id AS STRING) AS product_id,
        CAST(sdps.product_name AS STRING) AS product_name,
        CAST(sdps.brand AS STRING) AS brand,
        CAST(sdps.category AS STRING) AS category,
        CAST(sdps.total_revenue_amount AS DOUBLE) AS total_revenue_amount,
        CAST(sdps.total_transactions_count AS INT) AS total_transactions_count,
        CAST(sdps.total_quantity_units AS INT) AS total_quantity_units,
        CAST(sdps.avg_selling_price_amount AS DOUBLE) AS avg_selling_price_amount,
        CAST(sdps.data_refresh_date AS DATE) AS data_refresh_date
    FROM sales_daily_product_silver sdps
    """
)

(
    sales_daily_product_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_product_gold.csv")
)

# --------------------------------------------------------------------------------------------------
# Source: silver.sales_daily_store_product_silver (sdsps) -> Target: gold.sales_daily_store_product_gold
# --------------------------------------------------------------------------------------------------
sdsps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_store_product_silver.{FILE_FORMAT}/")
)
sdsps_df.createOrReplaceTempView("sales_daily_store_product_silver")

sales_daily_store_product_gold_df = spark.sql(
    """
    SELECT
        CAST(sdsps.sales_date AS DATE) AS sales_date,
        CAST(sdsps.store_id AS STRING) AS store_id,
        CAST(sdsps.store_name AS STRING) AS store_name,
        CAST(sdsps.store_region AS STRING) AS store_region,
        CAST(sdsps.product_id AS STRING) AS product_id,
        CAST(sdsps.product_name AS STRING) AS product_name,
        CAST(sdsps.category AS STRING) AS category,
        CAST(sdsps.total_revenue_amount AS DOUBLE) AS total_revenue_amount,
        CAST(sdsps.total_transactions_count AS INT) AS total_transactions_count,
        CAST(sdsps.total_quantity_units AS INT) AS total_quantity_units,
        CAST(sdsps.data_refresh_date AS DATE) AS data_refresh_date
    FROM sales_daily_store_product_silver sdsps
    """
)

(
    sales_daily_store_product_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_product_gold.csv")
)

# --------------------------------------------------------------------------------
# Source: silver.data_quality_daily_silver (dqds) -> Target: gold.data_quality_daily_gold
# --------------------------------------------------------------------------------
dqds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_daily_silver.{FILE_FORMAT}/")
)
dqds_df.createOrReplaceTempView("data_quality_daily_silver")

data_quality_daily_gold_df = spark.sql(
    """
    SELECT
        CAST(dqds.run_date AS DATE) AS run_date,
        CAST(dqds.dataset_name AS STRING) AS dataset_name,
        CAST(dqds.records_processed_count AS INT) AS records_processed_count,
        CAST(dqds.duplicate_records_removed_count AS INT) AS duplicate_records_removed_count,
        CAST(dqds.invalid_identifier_count AS INT) AS invalid_identifier_count,
        CAST(dqds.completeness_score_pct AS DOUBLE) AS completeness_score_pct,
        CAST(dqds.accuracy_score_pct AS DOUBLE) AS accuracy_score_pct,
        CAST(dqds.overall_data_quality_score_pct AS DOUBLE) AS overall_data_quality_score_pct
    FROM data_quality_daily_silver dqds
    """
)

(
    data_quality_daily_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_gold.csv")
)

job.commit()
