import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------
# 1) products_silver
# ------------------------------------------------------------------------------

products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(pb.product_id)   AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category)     AS category,
        TRIM(pb.brand)        AS brand
    FROM products_bronze pb
    WHERE pb.is_active = true
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN brand IS NOT NULL AND brand <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    category,
    brand
FROM dedup
WHERE rn = 1
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------------------------
# 2) stores_silver
# ------------------------------------------------------------------------------

stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(sb.store_id)   AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
        CASE UPPER(TRIM(sb.state))
            WHEN 'CT' THEN 'Northeast' WHEN 'ME' THEN 'Northeast' WHEN 'MA' THEN 'Northeast'
            WHEN 'NH' THEN 'Northeast' WHEN 'RI' THEN 'Northeast' WHEN 'VT' THEN 'Northeast'
            WHEN 'NJ' THEN 'Northeast' WHEN 'NY' THEN 'Northeast' WHEN 'PA' THEN 'Northeast'
            WHEN 'IL' THEN 'Midwest' WHEN 'IN' THEN 'Midwest' WHEN 'MI' THEN 'Midwest'
            WHEN 'OH' THEN 'Midwest' WHEN 'WI' THEN 'Midwest' WHEN 'IA' THEN 'Midwest'
            WHEN 'KS' THEN 'Midwest' WHEN 'MN' THEN 'Midwest' WHEN 'MO' THEN 'Midwest'
            WHEN 'NE' THEN 'Midwest' WHEN 'ND' THEN 'Midwest' WHEN 'SD' THEN 'Midwest'
            WHEN 'DE' THEN 'South' WHEN 'FL' THEN 'South' WHEN 'GA' THEN 'South'
            WHEN 'MD' THEN 'South' WHEN 'NC' THEN 'South' WHEN 'SC' THEN 'South'
            WHEN 'VA' THEN 'South' WHEN 'DC' THEN 'South' WHEN 'WV' THEN 'South'
            WHEN 'AL' THEN 'South' WHEN 'KY' THEN 'South' WHEN 'MS' THEN 'South'
            WHEN 'TN' THEN 'South' WHEN 'AR' THEN 'South' WHEN 'LA' THEN 'South'
            WHEN 'OK' THEN 'South' WHEN 'TX' THEN 'South'
            WHEN 'AZ' THEN 'West' WHEN 'CO' THEN 'West' WHEN 'ID' THEN 'West'
            WHEN 'MT' THEN 'West' WHEN 'NV' THEN 'West' WHEN 'NM' THEN 'West'
            WHEN 'UT' THEN 'West' WHEN 'WY' THEN 'West' WHEN 'AK' THEN 'West'
            WHEN 'CA' THEN 'West' WHEN 'HI' THEN 'West' WHEN 'OR' THEN 'West'
            WHEN 'WA' THEN 'West'
            ELSE NULL
        END AS region
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        store_location,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY
                CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN store_location IS NOT NULL AND store_location <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN region IS NOT NULL AND region <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    store_location,
    region
FROM dedup
WHERE rn = 1
""")
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) sales_transactions_silver
# ------------------------------------------------------------------------------

sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id)     AS product_id,
        TRIM(stb.store_id)       AS store_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        stb.sale_amount          AS sales_amount,
        stb.quantity             AS quantity_sold,
        stb.transaction_time     AS transaction_time
    FROM sales_transactions_bronze stb
    INNER JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
    INNER JOIN stores_silver ss
        ON TRIM(stb.store_id) = ss.store_id
    WHERE stb.sale_amount >= 0
      AND stb.quantity > 0
),
dedup AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        sales_amount,
        quantity_sold,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time DESC
        ) AS rn
    FROM base
)
SELECT
    transaction_id,
    product_id,
    store_id,
    transaction_date,
    sales_amount,
    quantity_sold
FROM dedup
WHERE rn = 1
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------
# 4) sales_aggregated_silver
# ------------------------------------------------------------------------------

sales_aggregated_silver_df = spark.sql("""
SELECT
    'daily' AS aggregation_level,
    sts.transaction_date AS date,
    SUM(sts.sales_amount) AS total_sales_amount,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    AVG(sts.sales_amount) AS average_sales_per_transaction
FROM sales_transactions_silver sts
GROUP BY sts.transaction_date
""")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()