import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------
# 1) Read source tables from S3
# -------------------------------------
sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)

# -------------------------------------
# 2) Create temp views
# -------------------------------------
sales_silver_df.createOrReplaceTempView("sales_silver")
product_silver_df.createOrReplaceTempView("product_silver")
store_silver_df.createOrReplaceTempView("store_silver")

# -------------------------------------
# 3) Transformations using Spark SQL
# -------------------------------------

# ---- Target: gold_sales (gs) ----
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ss.transaction_id AS STRING) AS transaction_id,
        CAST(ss.product_id AS STRING) AS product_id,
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.sales_date AS DATE) AS sales_date,
        CAST(ss.quantity AS INT) AS quantity,
        CAST(ss.sales_amount AS DOUBLE) AS sales_amount
    FROM sales_silver ss
    """
)

# ---- Target: gold_product (gp) ----
gold_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.price AS FLOAT) AS price
    FROM product_silver ps
    """
)

# ---- Target: gold_store (gsts) ----
gold_store_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.store_name AS STRING) AS store_name,
        CAST(sts.location AS STRING) AS location,
        CAST(sts.region AS STRING) AS region
    FROM store_silver sts
    """
)

# ---- Target: gold_aggregated_sales (gas) ----
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.product_id AS STRING) AS product_id,
        CAST(ss.sales_date AS DATE) AS sales_date,
        CAST(SUM(CAST(ss.sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount,
        CAST(SUM(CAST(ss.quantity AS INT)) AS INT) AS total_quantity
    FROM sales_silver ss
    INNER JOIN product_silver ps
        ON ss.product_id = ps.product_id
    INNER JOIN store_silver sts
        ON ss.store_id = sts.store_id
    GROUP BY
        ss.store_id,
        ss.product_id,
        ss.sales_date
    """
)

# -------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# -------------------------------------
(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()