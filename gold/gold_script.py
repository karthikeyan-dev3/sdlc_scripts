import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark init
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init("gold_layer_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read source tables from S3
# -----------------------------------------------------------------------------------
sales_enriched_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# Create temp views
# -----------------------------------------------------------------------------------
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# -----------------------------------------------------------------------------------
# Target: gold_sales
# -----------------------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ses.transaction_id AS STRING) AS transaction_id,
        CAST(ses.sale_date AS DATE) AS sale_date,
        CAST(ses.product_id AS STRING) AS product_id,
        CAST(ses.store_id AS STRING) AS store_id,
        CAST(ses.quantity_sold AS INT) AS quantity_sold,
        CAST(ses.total_amount AS DOUBLE) AS total_amount
    FROM sales_enriched_silver ses
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_products
# -----------------------------------------------------------------------------------
gold_products_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.brand AS STRING) AS brand
    FROM products_silver ps
    """
)

(
    gold_products_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_products.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_stores
# -----------------------------------------------------------------------------------
gold_stores_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.store_name AS STRING) AS store_name,
        CAST(sts.location AS STRING) AS location,
        CAST(sts.store_type AS STRING) AS store_type
    FROM stores_silver sts
    """
)

(
    gold_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_stores.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_sales_aggregated
# -----------------------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql(
    """
    SELECT
        CAST(ses.sale_date AS DATE) AS aggregation_date,
        CAST(SUM(CAST(ses.total_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
        CAST(SUM(CAST(ses.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
        CAST(AVG(CAST(ses.total_amount AS DOUBLE)) AS DOUBLE) AS average_sale_value,
        CAST(COUNT(DISTINCT CAST(ses.transaction_id AS STRING)) AS BIGINT) AS number_of_transactions
    FROM sales_enriched_silver ses
    GROUP BY CAST(ses.sale_date AS DATE)
    """
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

job.commit()
