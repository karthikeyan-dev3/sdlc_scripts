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

# ---------------------------------------------------------------------------
# Read Source Tables (S3)
# ---------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# ---------------------------------------------------------------------------
# Create Temp Views
# ---------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
ss_df.createOrReplaceTempView("ss")
ps_df.createOrReplaceTempView("ps")

# ---------------------------------------------------------------------------
# Target: gold_sales_transactions
# ---------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(sts.transaction_id AS STRING) AS transaction_id,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(sts.transaction_date AS DATE) AS transaction_date,
            CAST(sts.quantity_sold AS INT) AS quantity_sold,
            CAST(sts.total_amount AS DECIMAL(18,2)) AS total_amount,
            ROW_NUMBER() OVER (
                PARTITION BY sts.transaction_id
                ORDER BY sts.transaction_id
            ) AS rn
        FROM sts
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        total_amount
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_store_master
# ---------------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(ss.store_id AS STRING) AS store_id,
            CAST(ss.store_name AS STRING) AS store_name,
            CAST(ss.region AS STRING) AS region,
            CAST(ss.store_open_date AS DATE) AS store_open_date,
            ROW_NUMBER() OVER (
                PARTITION BY ss.store_id
                ORDER BY ss.store_id
            ) AS rn
        FROM ss
    )
    SELECT
        store_id,
        store_name,
        region,
        store_open_date
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_product_master
# ---------------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(ps.product_id AS STRING) AS product_id,
            CAST(ps.product_name AS STRING) AS product_name,
            CAST(ps.category AS STRING) AS category,
            CAST(ps.brand AS STRING) AS brand,
            CAST(ps.price AS DECIMAL(18,2)) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY ps.product_id
                ORDER BY ps.product_id
            ) AS rn
        FROM ps
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_sales_performance
# ---------------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(SUM(CAST(sts.total_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue,
        CAST(COUNT(CAST(sts.transaction_id AS STRING)) AS BIGINT) AS total_transactions,
        CAST(
            CASE
                WHEN COUNT(CAST(sts.transaction_id AS STRING)) = 0 THEN NULL
                ELSE SUM(CAST(sts.total_amount AS DECIMAL(18,2))) / COUNT(CAST(sts.transaction_id AS STRING))
            END
            AS DECIMAL(18,2)
        ) AS performance_comparison_metric
    FROM sts
    INNER JOIN ss
        ON CAST(sts.store_id AS STRING) = CAST(ss.store_id AS STRING)
    GROUP BY
        CAST(sts.store_id AS STRING),
        CAST(ss.store_name AS STRING),
        CAST(sts.transaction_date AS DATE)
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ---------------------------------------------------------------------------
# Target: gold_product_performance
# ---------------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(SUM(CAST(sts.total_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_revenue_contribution,
        CAST(SUM(CAST(sts.quantity_sold AS BIGINT)) AS BIGINT) AS total_units_sold,
        CAST(
            CASE
                WHEN SUM(CAST(sts.quantity_sold AS BIGINT)) = 0 THEN NULL
                ELSE SUM(CAST(sts.total_amount AS DECIMAL(18,2))) / SUM(CAST(sts.quantity_sold AS BIGINT))
            END
            AS DECIMAL(18,2)
        ) AS category_performance_metric
    FROM sts
    INNER JOIN ps
        ON CAST(sts.product_id AS STRING) = CAST(ps.product_id AS STRING)
    GROUP BY
        CAST(sts.product_id AS STRING),
        CAST(ps.product_name AS STRING),
        CAST(ps.category AS STRING)
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

job.commit()