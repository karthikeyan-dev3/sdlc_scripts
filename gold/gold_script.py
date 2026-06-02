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

# ------------------------------------------------------------------------------
# 1) READ SOURCE TABLES (SILVER) + TEMP VIEWS
# ------------------------------------------------------------------------------

dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")

dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# 2) TARGET: gold_dim_product
# ------------------------------------------------------------------------------

gold_dim_product_df = spark.sql(
    """
    SELECT
        CAST(dps.product_id AS STRING)               AS product_id,
        CAST(NULL AS STRING)                        AS sku,
        CAST(dps.product_name AS STRING)            AS product_name,
        CAST(dps.brand AS STRING)                   AS brand,
        CAST(dps.category AS STRING)                AS category,
        CAST(NULL AS STRING)                        AS subcategory,
        CAST(NULL AS STRING)                        AS department,
        CAST(NULL AS STRING)                        AS size,
        CAST(NULL AS STRING)                        AS color,
        CAST(NULL AS STRING)                        AS uom,
        CAST(dps.is_active AS BOOLEAN)              AS active_flag
    FROM dim_product_silver dps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_dim_product.csv")
)

gold_dim_product_df.createOrReplaceTempView("gold_sales_dim_product_view_unused")  # no downstream dependency

# ------------------------------------------------------------------------------
# 3) TARGET: gold_dim_store
# ------------------------------------------------------------------------------

gold_dim_store_df = spark.sql(
    """
    SELECT
        CAST(dss.store_id AS STRING)                                        AS store_id,
        CAST(dss.store_name AS STRING)                                      AS store_name,
        CAST(dss.store_type AS STRING)                                      AS store_type,
        CAST(NULL AS STRING)                                                AS region,
        CAST(NULL AS STRING)                                                AS district,
        CAST(dss.city AS STRING)                                            AS city,
        CAST(dss.state AS STRING)                                           AS state_province,
        CAST(NULL AS STRING)                                                AS country,
        CAST(dss.open_date AS DATE)                                         AS open_date,
        CAST(NULL AS DATE)                                                  AS close_date,
        CAST(CASE WHEN dss.store_id IS NOT NULL THEN TRUE ELSE FALSE END
             AS BOOLEAN)                                                    AS active_flag
    FROM dim_store_silver dss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_dim_store.csv")
)

gold_dim_store_df.createOrReplaceTempView("gold_sales_dim_store_view_unused")  # no downstream dependency

# ------------------------------------------------------------------------------
# 4) TARGET: gold_sales_transaction
#    Note: enrichment joins required per UDT
# ------------------------------------------------------------------------------

gold_sales_transaction_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)                 AS sales_txn_id,
        CAST(sts.transaction_time AS TIMESTAMP)            AS txn_ts,
        CAST(sts.transaction_time AS DATE)                 AS business_date,
        CAST(sts.store_id AS STRING)                       AS store_id,
        CAST(sts.product_id AS STRING)                     AS product_id,
        CAST(sts.quantity AS INT)                          AS qty_sold,
        CAST(sts.sale_amount AS DOUBLE)                    AS gross_sales_amt,
        CAST(sts.sale_amount AS DOUBLE)                    AS net_sales_amt,
        CAST(0 AS DOUBLE)                                  AS discount_amt,
        CAST(0 AS DOUBLE)                                  AS tax_amt,
        CAST(NULL AS STRING)                               AS currency_cd
    FROM sales_transactions_silver sts
    INNER JOIN dim_store_silver dss
        ON sts.store_id = dss.store_id
    INNER JOIN dim_product_silver dps
        ON sts.product_id = dps.product_id
    """
)

(
    gold_sales_transaction_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_transaction.csv")
)

gold_sales_transaction_df.createOrReplaceTempView("gold_sales_transaction")

# ------------------------------------------------------------------------------
# 5) TARGET: gold_sales_daily_store (aggregate from gold_sales_transaction)
# ------------------------------------------------------------------------------

gold_sales_daily_store_df = spark.sql(
    """
    SELECT
        gst.business_date                                  AS business_date,
        gst.store_id                                       AS store_id,
        COUNT(DISTINCT gst.sales_txn_id)                   AS txns_cnt,
        SUM(gst.qty_sold)                                  AS units_sold,
        SUM(gst.gross_sales_amt)                           AS gross_sales_amt,
        SUM(gst.net_sales_amt)                             AS net_sales_amt,
        SUM(gst.discount_amt)                              AS discount_amt,
        SUM(gst.tax_amt)                                   AS tax_amt
    FROM gold_sales_transaction gst
    GROUP BY
        gst.business_date,
        gst.store_id
    """
)

(
    gold_sales_daily_store_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_daily_store.csv")
)

# ------------------------------------------------------------------------------
# 6) TARGET: gold_sales_daily_product (aggregate from gold_sales_transaction)
# ------------------------------------------------------------------------------

gold_sales_daily_product_df = spark.sql(
    """
    SELECT
        gst.business_date                                  AS business_date,
        gst.product_id                                     AS product_id,
        COUNT(DISTINCT gst.sales_txn_id)                   AS txns_cnt,
        SUM(gst.qty_sold)                                  AS units_sold,
        SUM(gst.gross_sales_amt)                           AS gross_sales_amt,
        SUM(gst.net_sales_amt)                             AS net_sales_amt,
        SUM(gst.discount_amt)                              AS discount_amt,
        SUM(gst.tax_amt)                                   AS tax_amt
    FROM gold_sales_transaction gst
    GROUP BY
        gst.business_date,
        gst.product_id
    """
)

(
    gold_sales_daily_product_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_sales_daily_product.csv")
)

job.commit()