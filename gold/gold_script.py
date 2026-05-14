
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

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# Target: gold.gold_sales_data
# ============================================================
gold_sales_data_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_transaction_id AS STRING) AS sales_transaction_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(NULL AS STRING) AS customer_id,
        CAST(NULL AS DOUBLE) AS discount_applied,
        CAST(NULL AS STRING) AS payment_method
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_data.csv")
)

# ============================================================
# Target: gold.gold_product_master
# ============================================================
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.product_category AS STRING) AS product_category,
        CAST(pms.product_price AS DOUBLE) AS product_price,
        CAST(pms.product_brand AS STRING) AS product_brand
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

# ============================================================
# Target: gold.gold_store_master
# ============================================================
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.store_location AS STRING) AS store_location,
        CAST(sms.store_region AS STRING) AS store_region,
        CAST(sms.store_type AS STRING) AS store_type
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

# ============================================================
# Target: gold.gold_sales_aggregated
# ============================================================
gold_sales_aggregated_df = spark.sql(
    """
    WITH daily_agg AS (
        SELECT
            CAST(sts.sale_date AS DATE) AS aggregation_date,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(pms.product_category AS STRING) AS product_category,
            CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount,
            CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
            CAST(NULL AS DOUBLE) AS average_discount
        FROM sales_transactions_silver sts
        LEFT JOIN product_master_silver pms
            ON sts.product_id = pms.product_id
        GROUP BY
            CAST(sts.sale_date AS DATE),
            CAST(sts.store_id AS STRING),
            CAST(pms.product_category AS STRING)
    )
    SELECT
        aggregation_date,
        store_id,
        product_category,
        total_sales_amount,
        total_quantity_sold,
        average_discount,
        CASE
            WHEN LAG(total_sales_amount) OVER (
                PARTITION BY store_id, product_category
                ORDER BY aggregation_date
            ) IS NULL THEN CAST(NULL AS STRING)
            WHEN total_sales_amount > LAG(total_sales_amount) OVER (
                PARTITION BY store_id, product_category
                ORDER BY aggregation_date
            ) THEN 'UP'
            WHEN total_sales_amount < LAG(total_sales_amount) OVER (
                PARTITION BY store_id, product_category
                ORDER BY aggregation_date
            ) THEN 'DOWN'
            ELSE 'FLAT'
        END AS sales_trend_indicator
    FROM daily_agg
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ============================================================
# Target: gold.gold_sales_performance
# ============================================================
gold_sales_performance_df = spark.sql(
    """
    WITH monthly_agg AS (
        SELECT
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(date_trunc('month', CAST(sts.sale_date AS DATE)) AS DATE) AS month,
            CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount
        FROM sales_transactions_silver sts
        GROUP BY
            CAST(sts.store_id AS STRING),
            CAST(sts.product_id AS STRING),
            CAST(date_trunc('month', CAST(sts.sale_date AS DATE)) AS DATE)
    )
    SELECT
        store_id,
        product_id,
        month,
        total_sales_amount,
        CASE
            WHEN LAG(total_sales_amount) OVER (
                PARTITION BY store_id, product_id
                ORDER BY month
            ) IS NULL THEN CAST(NULL AS DOUBLE)
            WHEN LAG(total_sales_amount) OVER (
                PARTITION BY store_id, product_id
                ORDER BY month
            ) = 0 THEN CAST(NULL AS DOUBLE)
            ELSE CAST(
                (total_sales_amount - LAG(total_sales_amount) OVER (
                    PARTITION BY store_id, product_id
                    ORDER BY month
                )) / LAG(total_sales_amount) OVER (
                    PARTITION BY store_id, product_id
                    ORDER BY month
                )
            AS DOUBLE)
        END AS sales_growth_rate,
        CAST(NULL AS DOUBLE) AS sales_target_achievement,
        CAST(NULL AS DOUBLE) AS customer_satisfaction_score
    FROM monthly_agg
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

job.commit()
