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

# ------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
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

# ------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------

# Target: gold_sales_transactions
gold_sales_transactions_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING)       AS transaction_id,
        CAST(sts.transaction_date AS DATE)       AS transaction_date,
        CAST(sts.product_id AS STRING)           AS product_id,
        CAST(sts.store_id AS STRING)             AS store_id,
        CAST(sts.quantity_sold AS INT)           AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE)   AS total_sales_amount,
        sts.currency                             AS currency
    FROM sales_transactions_silver sts
    """
)

# Target: gold_product_master
gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)    AS product_id,
        ps.product_name                 AS product_name,
        ps.category                     AS category,
        ps.brand                        AS brand,
        CAST(ps.price AS FLOAT)         AS price
    FROM products_silver ps
    """
)

# Target: gold_store_master
gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)     AS store_id,
        ss.store_name                   AS store_name,
        ss.store_location               AS store_location,
        ss.region                       AS region,
        ss.store_manager                AS store_manager
    FROM stores_silver ss
    """
)

# Target: gold_sales_summary
gold_sales_summary_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_date AS DATE)                  AS date,
        CAST(sts.store_id AS STRING)                        AS store_id,
        CAST(sts.product_id AS STRING)                      AS product_id,
        CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)    AS total_quantity_sold,
        CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales_revenue,
        CAST(
            SUM(CAST(sts.total_sales_amount AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS DOUBLE))
            AS DOUBLE
        ) AS average_price
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.transaction_date AS DATE),
        CAST(sts.store_id AS STRING),
        CAST(sts.product_id AS STRING)
    """
)

# ------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------
(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_summary.csv")
)

job.commit()