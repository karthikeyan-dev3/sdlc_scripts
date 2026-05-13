import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Read source tables (S3)
# ------------------------------------------------------------------------------------
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
sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
ps_df.createOrReplaceTempView("ps")
ss_df.createOrReplaceTempView("ss")
sas_df.createOrReplaceTempView("sas")

# ------------------------------------------------------------------------------------
# TARGET: gold_sales
# ------------------------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.sale_date) AS sale_date,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.sale_amount AS DOUBLE) AS total_amount
    FROM sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold_product
# ------------------------------------------------------------------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.price AS FLOAT) AS price
    FROM ps
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold_store
# ------------------------------------------------------------------------------------
gold_store_df = spark.sql(
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
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: gold_aggregated_sales
# ------------------------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(sas.store_id AS STRING) AS store_id,
        CAST(sas.product_id AS STRING) AS product_id,
        DATE(sas.sale_date) AS sale_date,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM sas
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()