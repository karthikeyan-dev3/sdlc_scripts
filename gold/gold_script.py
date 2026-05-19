import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue_gold_layer_job", {})

# -------------------------------------------------------------------
# Read Source Tables (Silver) and Create Temp Views
# -------------------------------------------------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

sales_analysis_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_analysis_silver.{FILE_FORMAT}/")
)
sales_analysis_silver_df.createOrReplaceTempView("sales_analysis_silver")

# -------------------------------------------------------------------
# Target: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.brand AS STRING)        AS brand
    FROM products_silver ps
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)     AS store_id,
        CAST(ss.store_name AS STRING)   AS store_name,
        CAST(ss.region AS STRING)       AS region,
        CAST(ss.store_type AS STRING)   AS store_type
    FROM stores_silver ss
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------------------------------------
# Target: gold_sales_transactions
# -------------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.product_id AS STRING)     AS product_id,
        CAST(sts.store_id AS STRING)       AS store_id,
        DATE(sts.transaction_date)         AS transaction_date,
        CAST(sts.quantity_sold AS INT)     AS quantity_sold,
        CAST(sts.total_amount AS DOUBLE)   AS total_amount
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# -------------------------------------------------------------------
# Target: gold_aggregated_sales
# -------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        DATE(sas.date)                     AS date,
        CAST(sas.product_id AS STRING)     AS product_id,
        CAST(sas.store_id AS STRING)       AS store_id,
        CAST(sas.total_quantity_sold AS INT)   AS total_quantity_sold,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount
    FROM aggregated_sales_silver sas
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# -------------------------------------------------------------------
# Target: gold_sales_analysis
# -------------------------------------------------------------------
gold_sales_analysis_df = spark.sql(
    """
    SELECT
        DATE(sai.analysis_date)               AS analysis_date,
        CAST(sai.total_store_sales AS DOUBLE) AS total_store_sales,
        CAST(sai.average_product_sales AS DOUBLE) AS average_product_sales,
        CAST(sai.top_selling_product_id AS STRING) AS top_selling_product_id,
        CAST(sai.top_selling_store_id AS STRING)   AS top_selling_store_id
    FROM sales_analysis_silver sai
    """
)

(
    gold_sales_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_analysis.csv")
)

job.commit()
