from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import sys
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
pds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)
sis_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")
)
dss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_summary_silver.{FILE_FORMAT}/")
)
dqa_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_assurance_silver.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pds_df.createOrReplaceTempView("product_details_silver")
sis_df.createOrReplaceTempView("store_information_silver")
dss_df.createOrReplaceTempView("daily_sales_summary_silver")
dqa_df.createOrReplaceTempView("data_quality_assurance_silver")

# ----------------------------
# Target: gold_sales
# ----------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        DATE(sts.date) AS date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ----------------------------
# Target: gold_product
# ----------------------------
gold_product_df = spark.sql(
    """
    SELECT
        pds.product_id AS product_id,
        pds.product_name AS product_name,
        pds.product_category AS product_category,
        CAST(pds.product_price AS FLOAT) AS product_price
    FROM product_details_silver pds
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ----------------------------
# Target: gold_store
# ----------------------------
gold_store_df = spark.sql(
    """
    SELECT
        sis.store_id AS store_id,
        sis.store_name AS store_name,
        sis.store_location AS store_location,
        sis.store_region AS store_region
    FROM store_information_silver sis
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ----------------------------
# Target: gold_aggregated_sales
# ----------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(dss.date) AS date,
        dss.store_id AS store_id,
        dss.product_id AS product_id,
        CAST(dss.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(dss.total_quantity_sold AS INT) AS total_quantity_sold
    FROM daily_sales_summary_silver dss
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ----------------------------
# Target: gold_data_quality
# ----------------------------
gold_data_quality_df = spark.sql(
    """
    SELECT
        DATE(dqa.data_date) AS data_date,
        CAST(dqa.total_records AS BIGINT) AS total_records,
        CAST(dqa.valid_records AS BIGINT) AS valid_records,
        CAST(dqa.invalid_records AS BIGINT) AS invalid_records,
        CAST(dqa.quality_score AS DOUBLE) AS quality_score
    FROM data_quality_assurance_silver dqa
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit() 