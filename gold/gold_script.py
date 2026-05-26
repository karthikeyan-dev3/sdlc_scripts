import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

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
# Read Source Tables (S3)
# ----------------------------
mps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/master_product_silver.{FILE_FORMAT}/")
)
mss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/master_store_silver.{FILE_FORMAT}/")
)
sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
mps_df.createOrReplaceTempView("master_product_silver")
mss_df.createOrReplaceTempView("master_store_silver")
sps_df.createOrReplaceTempView("sales_performance_silver")

# ============================================================
# Target Table: gold_master_product
# ============================================================
gold_master_product_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(mps.product_id AS STRING)  AS product_id,
            CAST(mps.product_name AS STRING) AS product_name,
            CAST(mps.category AS STRING)     AS category,
            CAST(mps.brand AS STRING)        AS brand,
            ROW_NUMBER() OVER (
                PARTITION BY mps.product_id
                ORDER BY mps.product_id
            ) AS rn
        FROM master_product_silver mps
    )
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM base
    WHERE rn = 1
    """
)

(
    gold_master_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_product.csv")
)

# ============================================================
# Target Table: gold_master_store
# ============================================================
gold_master_store_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(mss.store_id AS STRING)    AS store_id,
            CAST(mss.store_name AS STRING)  AS store_name,
            CAST(mss.region AS STRING)      AS region,
            CAST(mss.store_type AS STRING)  AS store_type,
            ROW_NUMBER() OVER (
                PARTITION BY mss.store_id
                ORDER BY mss.store_id
            ) AS rn
        FROM master_store_silver mss
    )
    SELECT
        store_id,
        store_name,
        region,
        store_type
    FROM base
    WHERE rn = 1
    """
)

(
    gold_master_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_store.csv")
)

# ============================================================
# Target Table: gold_sales_performance
# ============================================================
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sps.store_id AS STRING) AS store_id,
        CAST(mss.store_name AS STRING) AS store_name,
        CAST(sps.product_id AS STRING) AS product_id,
        CAST(mps.product_name AS STRING) AS product_name,
        DATE(sps.date) AS date,
        CAST(SUM(CAST(sps.sales_amount AS DOUBLE)) AS DOUBLE) AS sales_amount,
        CAST(SUM(CAST(sps.units_sold AS INT)) AS INT) AS units_sold,
        CAST(SUM(CAST(sps.sales_amount AS DOUBLE)) AS DOUBLE) AS aggregated_sales
    FROM sales_performance_silver sps
    LEFT JOIN master_store_silver mss
        ON sps.store_id = mss.store_id
    LEFT JOIN master_product_silver mps
        ON sps.product_id = mps.product_id
    GROUP BY
        CAST(sps.store_id AS STRING),
        CAST(mss.store_name AS STRING),
        CAST(sps.product_id AS STRING),
        CAST(mps.product_name AS STRING),
        DATE(sps.date)
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