import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------
product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# --------------------------------------------------------------------
# Target: gold_product_master
# --------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        pms.metadata_attributes AS metadata_attributes
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

# --------------------------------------------------------------------
# Target: gold_store_master
# --------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.city AS city,
        sms.store_type AS store_type,
        sms.metadata_attributes AS metadata_attributes
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

# --------------------------------------------------------------------
# Target: gold_store_performance
# --------------------------------------------------------------------
gold_store_performance_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sms.store_name AS store_name,
        sms.city AS city,
        sms.store_type AS store_type,
        CAST(sts.reporting_date AS DATE) AS reporting_date,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS total_revenue,
        COUNT(sts.transaction_id) AS transaction_count,
        SUM(CAST(sts.quantity AS INT)) AS quantities_sold
    FROM sales_transactions_silver sts
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY
        sts.store_id,
        sms.store_name,
        sms.city,
        sms.store_type,
        CAST(sts.reporting_date AS DATE)
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

# --------------------------------------------------------------------
# Target: gold_product_performance
# --------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        sts.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        CAST(sts.reporting_date AS DATE) AS reporting_date,
        SUM(CAST(sts.sale_amount AS DOUBLE)) AS revenue_contribution,
        SUM(CAST(sts.quantity AS INT)) AS quantities_sold,
        SUM(CAST(sts.sale_amount AS DOUBLE)) OVER (
            PARTITION BY pms.category, CAST(sts.reporting_date AS DATE)
        ) AS category_performance
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    GROUP BY
        sts.product_id,
        pms.product_name,
        pms.category,
        CAST(sts.reporting_date AS DATE)
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
