import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------
# Read source tables from S3
# -------------------------------

sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
sdq_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_data_quality_silver.{FILE_FORMAT}/")
)

# -------------------------------
# Create temp views
# -------------------------------

sts_df.createOrReplaceTempView("sts")
ps_df.createOrReplaceTempView("ps")
ss_df.createOrReplaceTempView("ss")
ass_df.createOrReplaceTempView("ass")
sdq_df.createOrReplaceTempView("sdq")

# -------------------------------
# Target: gold_sales_transactions
# -------------------------------

gold_sales_transactions_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(sts.transaction_id AS STRING) AS transaction_id,
            DATE(sts.transaction_date) AS transaction_date,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(sts.quantity_sold AS INT) AS quantity_sold,
            CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
            ROW_NUMBER() OVER (PARTITION BY sts.transaction_id ORDER BY sts.transaction_date DESC) AS rn
        FROM sts
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount
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

# -------------------------------
# Target: gold_product_attributes
# -------------------------------

gold_product_attributes_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.brand AS STRING) AS brand,
        CAST(ps.price AS FLOAT) AS price
    FROM ps
    """
)

(
    gold_product_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_attributes.csv")
)

# -------------------------------
# Target: gold_store_attributes
# -------------------------------

gold_store_attributes_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.location AS STRING) AS location,
        CAST(ss.region AS STRING) AS region
    FROM ss
    """
)

(
    gold_store_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_attributes.csv")
)

# -------------------------------
# Target: gold_aggregated_sales
# -------------------------------

gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING) AS store_id,
        CAST(ass.product_id AS STRING) AS product_id,
        DATE(ass.aggregation_date) AS aggregation_date,
        CAST(ass.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(ass.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(ass.average_price AS DOUBLE) AS average_price
    FROM ass
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# -------------------------------
# Target: gold_sales_data_quality
# -------------------------------

gold_sales_data_quality_df = spark.sql(
    """
    SELECT
        DATE(sdq.data_date) AS data_date,
        CAST(sdq.total_records AS INT) AS total_records,
        CAST(sdq.duplicate_records AS INT) AS duplicate_records,
        CAST(sdq.missing_identifiers AS INT) AS missing_identifiers,
        CAST(sdq.accuracy_percentage AS DOUBLE) AS accuracy_percentage
    FROM sdq
    """
)

(
    gold_sales_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_data_quality.csv")
)

job.commit()