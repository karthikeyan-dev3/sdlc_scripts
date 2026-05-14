import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
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
sds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")
)
ass_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_summary_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sales_transactions_silver")
pds_df.createOrReplaceTempView("product_details_silver")
sds_df.createOrReplaceTempView("store_details_silver")
ass_df.createOrReplaceTempView("aggregated_sales_summary_silver")

# -----------------------------------------------------------------------------------
# 3) Transformations using Spark SQL + 4) Save outputs
# -----------------------------------------------------------------------------------

# -------------------------
# gold_sales
# -------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        DATE(sts.date) AS date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
        CAST(sts.quantity_sold AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_sales.csv")
)

# -------------------------
# gold_product
# -------------------------
gold_product_df = spark.sql(
    """
    SELECT
        CAST(pds.product_id AS STRING) AS product_id,
        CAST(pds.product_name AS STRING) AS product_name,
        CAST(pds.category AS STRING) AS category,
        CAST(pds.price AS FLOAT) AS price,
        CAST(pds.brand AS STRING) AS brand
    FROM product_details_silver pds
    """
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_product.csv")
)

# -------------------------
# gold_store
# -------------------------
gold_store_df = spark.sql(
    """
    SELECT
        CAST(sds.store_id AS STRING) AS store_id,
        CAST(sds.store_name AS STRING) AS store_name,
        CAST(sds.location AS STRING) AS location,
        DATE(sds.open_date) AS open_date
    FROM store_details_silver sds
    """
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_store.csv")
)

# -------------------------
# gold_aggregated_sales
# -------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ass.store_id AS STRING) AS store_id,
        CAST(ass.product_id AS STRING) AS product_id,
        DATE(ass.date) AS date,
        CAST(ass.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(ass.total_quantity_sold AS INT) AS total_quantity_sold
    FROM aggregated_sales_summary_silver ass
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_aggregated_sales.csv")
)

job.commit()