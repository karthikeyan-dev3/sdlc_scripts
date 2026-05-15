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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Source Reads (Bronze)
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# 2) product_information_silver
# -----------------------------------------------------------------------------------
product_information_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(pb.product_id AS STRING)) AS product_id,
            TRIM(CAST(pb.product_name AS STRING)) AS product_name,
            TRIM(CAST(pb.category AS STRING)) AS product_category,
            CAST(pb.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(pb.product_id AS STRING))
                ORDER BY TRIM(CAST(pb.product_id AS STRING))
            ) AS rn
        FROM products_bronze pb
        WHERE COALESCE(CAST(pb.is_active AS BOOLEAN), FALSE) = TRUE
    )
    SELECT
        product_id,
        product_name,
        product_category,
        price
    FROM base
    WHERE rn = 1
    """
)
product_information_silver_df.createOrReplaceTempView("product_information_silver")

(
    product_information_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/product_information_silver.csv")
)

# -----------------------------------------------------------------------------------
# 3) store_information_silver
# -----------------------------------------------------------------------------------
store_information_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(sb.store_id AS STRING)) AS store_id,
            TRIM(CAST(sb.store_name AS STRING)) AS store_name,
            CONCAT(TRIM(CAST(sb.city AS STRING)), ', ', TRIM(CAST(sb.state AS STRING))) AS store_location,
            TRIM(CAST(sb.state AS STRING)) AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(sb.store_id AS STRING))
                ORDER BY TRIM(CAST(sb.store_id AS STRING))
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
    FROM base
    WHERE rn = 1
    """
)
store_information_silver_df.createOrReplaceTempView("store_information_silver")

(
    store_information_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/store_information_silver.csv")
)

# -----------------------------------------------------------------------------------
# 4) sales_transaction_details_silver
# -----------------------------------------------------------------------------------
sales_transaction_details_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            TRIM(CAST(stb.transaction_id AS STRING)) AS transaction_id,
            DATE(stb.transaction_time) AS sales_date,
            TRIM(CAST(stb.product_id AS STRING)) AS product_id,
            TRIM(CAST(stb.store_id AS STRING)) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
            TRIM(CAST(pis.product_category AS STRING)) AS product_category,
            TRIM(CAST(sis.store_region AS STRING)) AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(CAST(stb.transaction_id AS STRING))
                ORDER BY TRIM(CAST(stb.transaction_id AS STRING))
            ) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN product_information_silver pis
            ON TRIM(CAST(stb.product_id AS STRING)) = TRIM(CAST(pis.product_id AS STRING))
        INNER JOIN store_information_silver sis
            ON TRIM(CAST(stb.store_id AS STRING)) = TRIM(CAST(sis.store_id AS STRING))
        WHERE COALESCE(CAST(stb.quantity AS INT), 0) >= 0
          AND COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) >= 0.0
    )
    SELECT
        transaction_id,
        sales_date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount,
        product_category,
        store_region
    FROM joined
    WHERE rn = 1
    """
)
sales_transaction_details_silver_df.createOrReplaceTempView("sales_transaction_details_silver")

(
    sales_transaction_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_transaction_details_silver.csv")
)

# -----------------------------------------------------------------------------------
# 5) sales_summary_reports_silver
# -----------------------------------------------------------------------------------
sales_summary_reports_silver_df = spark.sql(
    """
    SELECT
        stds.sales_date AS reporting_date,
        stds.store_id AS store_id,
        stds.product_category AS product_category,
        CAST(SUM(stds.quantity_sold) AS INT) AS total_quantity_sold,
        CAST(SUM(stds.sales_amount) AS DOUBLE) AS total_sales_amount,
        CASE
            WHEN SUM(stds.quantity_sold) > 0 THEN CAST(SUM(stds.sales_amount) / SUM(stds.quantity_sold) AS DOUBLE)
            ELSE NULL
        END AS average_sales_price
    FROM sales_transaction_details_silver stds
    GROUP BY
        stds.sales_date,
        stds.store_id,
        stds.product_category
    """
)

(
    sales_summary_reports_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_summary_reports_silver.csv")
)

job.commit()