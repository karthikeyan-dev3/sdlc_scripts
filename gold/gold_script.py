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

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_silver.{FILE_FORMAT}/")
)
transactions_silver_df.createOrReplaceTempView("transactions_silver")

# -----------------------------------------------------------------------------------
# Target: gold_store_master
# Source: silver.stores_silver ss
# -----------------------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
        ss.store_id   AS store_id,
        ss.store_name AS store_name,
        ss.location   AS location,
        ss.region     AS region
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

# -----------------------------------------------------------------------------------
# Target: gold_product_master
# Source: silver.products_silver ps
# -----------------------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
        ps.product_id   AS product_id,
        ps.product_name AS product_name,
        ps.category     AS category,
        ps.brand        AS brand
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

# -----------------------------------------------------------------------------------
# Target: gold_sales
# Source: silver.transactions_silver ts
# -----------------------------------------------------------------------------------
gold_sales_df = spark.sql(
    """
    SELECT
        ts.transaction_id           AS transaction_id,
        ts.store_id                 AS store_id,
        ts.product_id               AS product_id,
        CAST(ts.sale_date AS DATE)  AS sale_date,
        CAST(ts.revenue AS DOUBLE)  AS total_revenue
    FROM transactions_silver ts
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
# Target: gold_aggregated_sales
# Source: silver.transactions_silver ts
# -----------------------------------------------------------------------------------
gold_aggregated_sales_df = spark.sql(
    """
    SELECT
        ts.store_id                               AS store_id,
        ts.product_id                             AS product_id,
        CAST(ts.sale_date AS DATE)                AS sale_date,
        SUM(CAST(ts.revenue AS DOUBLE))           AS total_revenue,
        SUM(CAST(ts.units_sold AS INT))           AS units_sold
    FROM transactions_silver ts
    GROUP BY
        ts.store_id,
        ts.product_id,
        CAST(ts.sale_date AS DATE)
    """
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# -----------------------------------------------------------------------------------
# Target: gold_product_performance
# Source: silver.transactions_silver ts INNER JOIN silver.products_silver ps ON ts.product_id = ps.product_id
# -----------------------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
        ts.product_id                             AS product_id,
        ps.product_name                           AS product_name,
        ps.category                               AS category,
        SUM(CAST(ts.revenue AS DOUBLE))           AS total_revenue,
        SUM(CAST(ts.units_sold AS INT))           AS units_sold
    FROM transactions_silver ts
    INNER JOIN products_silver ps
        ON ts.product_id = ps.product_id
    GROUP BY
        ts.product_id,
        ps.product_name,
        ps.category
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