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

# ============================================================
# Read Source Tables (silver)
# ============================================================

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: gold.gold_operational_metrics_daily
# ============================================================

gold_operational_metrics_daily_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_time AS DATE) AS metric_date,
        CAST('internal_sales' AS VARCHAR(50)) AS source_system,
        ss.store_type AS business_unit,
        CAST('total_sales_amount' AS VARCHAR(100)) AS metric_name,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS metric_value
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        ss.store_type

    UNION ALL

    SELECT
        CAST(sts.transaction_time AS DATE) AS metric_date,
        CAST('internal_sales' AS VARCHAR(50)) AS source_system,
        ss.store_type AS business_unit,
        CAST('total_quantity' AS VARCHAR(100)) AS metric_name,
        CAST(SUM(CAST(sts.quantity AS INT)) AS DOUBLE) AS metric_value
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        ss.store_type

    UNION ALL

    SELECT
        CAST(sts.transaction_time AS DATE) AS metric_date,
        CAST('internal_sales' AS VARCHAR(50)) AS source_system,
        ss.store_type AS business_unit,
        CAST('transaction_count' AS VARCHAR(100)) AS metric_name,
        CAST(COUNT(sts.transaction_id) AS DOUBLE) AS metric_value
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.transaction_time AS DATE),
        ss.store_type
    """
)

(
    gold_operational_metrics_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_operational_metrics_daily.csv")
)

job.commit()