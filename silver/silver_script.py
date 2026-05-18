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
# Read Source Tables (Bronze)
# -----------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# Target: silver.store_details_silver
# -----------------------------------------------------------------------------------
store_details_silver_sql = """
WITH base AS (
    SELECT
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sb.store_name) AS STRING) AS store_name,
        CAST(TRIM(sb.city) AS STRING) AS city,
        CAST(TRIM(sb.store_type) AS STRING) AS store_type
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        city,
        store_type,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM base
    WHERE store_id IS NOT NULL AND TRIM(store_id) <> ''
)
SELECT
    store_id,
    store_name,
    city,
    store_type
FROM dedup
WHERE rn = 1
"""
store_details_silver_df = spark.sql(store_details_silver_sql)
store_details_silver_df.createOrReplaceTempView("store_details_silver")

(
    store_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_details_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.product_catalog_silver
# -----------------------------------------------------------------------------------
product_catalog_silver_sql = """
WITH base AS (
    SELECT
        CAST(TRIM(pb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pb.product_name) AS STRING) AS product_name,
        CAST(TRIM(pb.category) AS STRING) AS product_category
    FROM products_bronze pb
),
dedup AS (
    SELECT
        product_id,
        product_name,
        product_category,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM base
    WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
)
SELECT
    product_id,
    product_name,
    product_category
FROM dedup
WHERE rn = 1
"""
product_catalog_silver_df = spark.sql(product_catalog_silver_sql)
product_catalog_silver_df.createOrReplaceTempView("product_catalog_silver")

(
    product_catalog_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_catalog_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -----------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
    SELECT
        CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
        CAST(TRIM(stb.store_id) AS STRING) AS store_id,
        CAST(TRIM(stb.product_id) AS STRING) AS product_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS date
    FROM sales_transactions_bronze stb
),
filtered AS (
    SELECT
        b.*
    FROM base b
    INNER JOIN store_details_silver sds
        ON b.store_id = sds.store_id
    INNER JOIN product_catalog_silver pcs
        ON b.product_id = pcs.product_id
    WHERE b.transaction_id IS NOT NULL AND TRIM(b.transaction_id) <> ''
      AND b.store_id IS NOT NULL AND TRIM(b.store_id) <> ''
      AND b.product_id IS NOT NULL AND TRIM(b.product_id) <> ''
      AND b.quantity IS NOT NULL AND b.quantity >= 0
      AND b.sale_amount IS NOT NULL AND b.sale_amount >= 0
),
dedup AS (
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        date,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time DESC
        ) AS rn
    FROM filtered
)
SELECT
    transaction_id,
    store_id,
    product_id,
    quantity,
    sale_amount,
    transaction_time,
    date
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.store_daily_sales_silver
# -----------------------------------------------------------------------------------
store_daily_sales_silver_sql = """
SELECT
    sts.store_id AS store_id,
    sts.date AS date,
    SUM(sts.sale_amount) AS total_revenue,
    COUNT(DISTINCT sts.transaction_id) AS transaction_count,
    SUM(sts.quantity) AS quantity_sold,
    sds.store_name AS store_name,
    sds.city AS city,
    sds.store_type AS store_type
FROM sales_transactions_silver sts
INNER JOIN store_details_silver sds
    ON sts.store_id = sds.store_id
GROUP BY
    sts.store_id,
    sts.date,
    sds.store_name,
    sds.city,
    sds.store_type
"""
store_daily_sales_silver_df = spark.sql(store_daily_sales_silver_sql)
store_daily_sales_silver_df.createOrReplaceTempView("store_daily_sales_silver")

(
    store_daily_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_daily_sales_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.product_daily_sales_silver
# -----------------------------------------------------------------------------------
product_daily_sales_silver_sql = """
SELECT
    sts.product_id AS product_id,
    sts.date AS date,
    SUM(sts.sale_amount) AS total_revenue,
    SUM(sts.quantity) AS quantity_sold,
    pcs.product_name AS product_name,
    pcs.product_category AS product_category
FROM sales_transactions_silver sts
INNER JOIN product_catalog_silver pcs
    ON sts.product_id = pcs.product_id
GROUP BY
    sts.product_id,
    sts.date,
    pcs.product_name,
    pcs.product_category
"""
product_daily_sales_silver_df = spark.sql(product_daily_sales_silver_sql)
product_daily_sales_silver_df.createOrReplaceTempView("product_daily_sales_silver")

(
    product_daily_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_daily_sales_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.product_daily_rank_silver
# -----------------------------------------------------------------------------------
product_daily_rank_silver_sql = """
SELECT
    pdss.product_id AS product_id,
    pdss.date AS date,
    DENSE_RANK() OVER (
        PARTITION BY pdss.date
        ORDER BY pdss.total_revenue DESC, pdss.quantity_sold DESC, pdss.product_id ASC
    ) AS top_performer_rank
FROM product_daily_sales_silver pdss
"""
product_daily_rank_silver_df = spark.sql(product_daily_rank_silver_sql)
product_daily_rank_silver_df.createOrReplaceTempView("product_daily_rank_silver")

(
    product_daily_rank_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_daily_rank_silver.csv")
)

job.commit()