import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# ------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ------------------------------------------------------------
# 3) Transform and write: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
    SELECT
        CAST(stb.transaction_id AS STRING)                                   AS transaction_id,
        DATE(stb.transaction_time)                                          AS transaction_date,
        CAST(stb.product_id AS STRING)                                      AS product_id,
        CAST(stb.store_id AS STRING)                                        AS store_id,
        CAST(stb.quantity AS INT)                                           AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE)                                     AS total_sales_amount,
        stb.transaction_time                                                AS transaction_time
    FROM sales_transactions_bronze stb
    WHERE
        stb.transaction_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.store_id IS NOT NULL
),
dedup AS (
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time DESC
        ) AS rn
    FROM base
),
final AS (
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
    FROM dedup
    WHERE
        rn = 1
        AND quantity_sold > 0
        AND total_sales_amount >= 0
)
SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales_amount
FROM final
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
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

# ------------------------------------------------------------
# 4) Transform and write: silver.product_master_silver
# ------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
    SELECT
        CAST(pb.product_id AS STRING)                   AS product_id,
        TRIM(pb.product_name)                          AS product_name,
        TRIM(pb.category)                              AS category,
        TRIM(pb.brand)                                 AS brand,
        pb.is_active                                   AS is_active
    FROM products_bronze pb
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM base
    WHERE is_active = true
),
final AS (
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM dedup
    WHERE rn = 1
)
SELECT
    product_id,
    product_name,
    category,
    brand
FROM final
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------
# 5) Transform and write: silver.store_master_silver
# ------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
    SELECT
        CAST(sb.store_id AS STRING)                                 AS store_id,
        TRIM(sb.store_name)                                        AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state))                 AS location,
        TRIM(sb.store_type)                                        AS store_type
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        location,
        store_type,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM base
),
final AS (
    SELECT
        store_id,
        store_name,
        location,
        store_type
    FROM dedup
    WHERE rn = 1
)
SELECT
    store_id,
    store_name,
    location,
    store_type
FROM final
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------
# 6) Transform and write: silver.aggregated_sales_daily_silver
# ------------------------------------------------------------
aggregated_sales_daily_silver_sql = """
SELECT
    sts.transaction_date                                AS date,
    sts.store_id                                        AS store_id,
    sts.product_id                                      AS product_id,
    SUM(sts.total_sales_amount)                         AS total_sales_amount,
    SUM(sts.quantity_sold)                              AS total_quantity_sold,
    COUNT(DISTINCT sts.transaction_id)                  AS number_of_transactions
FROM sales_transactions_silver sts
GROUP BY
    sts.transaction_date,
    sts.store_id,
    sts.product_id
"""

aggregated_sales_daily_silver_df = spark.sql(aggregated_sales_daily_silver_sql)
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

(
    aggregated_sales_daily_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_daily_silver.csv")
)

job.commit()