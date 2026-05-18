import sys
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -------------------------------------------------------------------
sst_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sales_transactions.{FILE_FORMAT}/")
)
sst_df.createOrReplaceTempView("silver_sales_transactions")

spm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_product_master.{FILE_FORMAT}/")
)
spm_df.createOrReplaceTempView("silver_product_master")

ssm_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_store_master.{FILE_FORMAT}/")
)
ssm_df.createOrReplaceTempView("silver_store_master")

sds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_daily_sales_agg.{FILE_FORMAT}/")
)
sds_df.createOrReplaceTempView("silver_daily_sales_agg")

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sst.transaction_id AS STRING) AS transaction_id,
        CAST(sst.product_id AS STRING) AS product_id,
        CAST(sst.store_id AS STRING) AS store_id,
        CAST(sst.quantity_sold AS INT) AS quantity_sold,
        DATE(CAST(sst.sale_date AS DATE)) AS sale_date
    FROM silver_sales_transactions sst
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
        CAST(spm.product_id AS STRING) AS product_id,
        CAST(spm.product_name AS STRING) AS product_name,
        CAST(spm.category AS STRING) AS category,
        CAST(spm.price AS DOUBLE) AS price
    FROM silver_product_master spm
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
        CAST(ssm.store_id AS STRING) AS store_id,
        CAST(ssm.store_name AS STRING) AS store_name,
        CAST(ssm.store_type AS STRING) AS store_type,
        CAST(ssm.location AS STRING) AS location
    FROM silver_store_master ssm
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
# Target: gold_aggregated_sales_data
# -------------------------------------------------------------------
gold_aggregated_sales_data_df = spark.sql(
    """
    SELECT
        CAST(sds.product_id AS STRING) AS product_id,
        CAST(sds.store_id AS STRING) AS store_id,
        CAST(sds.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sds.total_quantity_sold AS BIGINT) AS total_quantity_sold,
        DATE(CAST(sds.sale_date AS DATE)) AS sale_date
    FROM silver_daily_sales_agg sds
    """
)

(
    gold_aggregated_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales_data.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_performance
# mapping_details: silver_daily_sales_agg sds INNER JOIN silver_store_master ssm ON sds.store_id = ssm.store_id
# -------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sds.store_id AS STRING) AS store_id,
        CAST(SUM(CAST(sds.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(COUNT(sds.sale_date) AS BIGINT) AS total_transactions,
        CAST(SUM(CAST(sds.total_revenue AS DOUBLE)) AS DOUBLE) AS comparison_metrics
    FROM silver_daily_sales_agg sds
    INNER JOIN silver_store_master ssm
        ON sds.store_id = ssm.store_id
    GROUP BY
        sds.store_id
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
# Target: gold_product_performance
# mapping_details: silver_daily_sales_agg sds INNER JOIN silver_product_master spm ON sds.product_id = spm.product_id
# -------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sds.product_id AS STRING) AS product_id,
        CAST(SUM(CAST(sds.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue_contribution,
        CAST(SUM(CAST(sds.total_quantity_sold AS BIGINT)) AS BIGINT) AS total_quantity_sold,
        CAST(SUM(CAST(sds.total_quantity_sold AS BIGINT)) AS BIGINT) AS category_performance_metrics
    FROM silver_daily_sales_agg sds
    INNER JOIN silver_product_master spm
        ON sds.product_id = spm.product_id
    GROUP BY
        sds.product_id
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)