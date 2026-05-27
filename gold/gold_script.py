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

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------------------------
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

sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_silver_df.createOrReplaceTempView("sales_silver")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Write each target table separately as SINGLE CSV file under TARGET_PATH
# --------------------------------------------------------------------------------------

# gold_master_product
gold_master_product_df = spark.sql(
    """
    SELECT
        CAST(ps.product_id AS STRING)   AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS DOUBLE)        AS price
    FROM products_silver ps
    """
)

(
    gold_master_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_product.csv")
)

# gold_master_store
gold_master_store_df = spark.sql(
    """
    SELECT
        CAST(ss.store_id AS STRING)     AS store_id,
        CAST(ss.store_name AS STRING)   AS store_name,
        CAST(ss.location AS STRING)     AS location,
        CAST(ss.store_type AS STRING)   AS store_type
    FROM stores_silver ss
    """
)

(
    gold_master_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_store.csv")
)

# gold_sales_performance
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sl.sale_id AS STRING)              AS sale_id,
        CAST(sl.transaction_date AS DATE)       AS transaction_date,
        CAST(sl.store_id AS STRING)             AS store_id,
        CAST(sl.product_id AS STRING)           AS product_id,
        CAST(sl.quantity_sold AS INT)           AS quantity_sold,
        CAST(sl.revenue AS DOUBLE)              AS revenue
    FROM sales_silver sl
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# gold_product_performance
gold_product_performance_df = spark.sql(
    """
    SELECT
        CAST(sl.product_id AS STRING)           AS product_id,
        CAST(ps.product_name AS STRING)         AS product_name,
        CAST(ps.category AS STRING)             AS category,
        CAST(SUM(CAST(sl.revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
        CAST(SUM(CAST(sl.quantity_sold AS INT)) AS INT) AS units_sold
    FROM sales_silver sl
    INNER JOIN products_silver ps
        ON sl.product_id = ps.product_id
    GROUP BY
        sl.product_id,
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

# gold_store_performance
gold_store_performance_df = spark.sql(
    """
    SELECT
        CAST(sl.store_id AS STRING)                      AS store_id,
        CAST(ss.store_name AS STRING)                    AS store_name,
        CAST(SUM(CAST(sl.revenue AS DOUBLE)) AS DOUBLE)  AS total_revenue,
        CAST(COUNT(sl.sale_id) AS STRING)                AS total_transactions,
        CAST(SUM(CAST(sl.quantity_sold AS INT)) AS INT)  AS total_quantity_sold
    FROM sales_silver sl
    INNER JOIN stores_silver ss
        ON sl.store_id = ss.store_id
    GROUP BY
        sl.store_id,
        ss.store_name
    """
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

job.commit()