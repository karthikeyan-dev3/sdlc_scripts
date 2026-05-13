
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

# --------------------------------------------------------------------------------------
# 1) Read source tables (Bronze)
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views (Bronze)
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# --------------------------------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# --------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        stb.transaction_id AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
        CAST(stb.quantity AS INT) AS quantity_sold,
        ROW_NUMBER() OVER (
            PARTITION BY stb.transaction_id
            ORDER BY stb.transaction_time DESC
        ) AS rn
    FROM sales_transactions_bronze stb
),
dedup AS (
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        sales_amount,
        quantity_sold
    FROM base
    WHERE rn = 1
)
SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    sales_amount,
    quantity_sold
FROM dedup
WHERE transaction_id IS NOT NULL
  AND store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity_sold > 0
  AND sales_amount >= 0
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.stores_silver
# --------------------------------------------------------------------------------------
stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        sb.store_id AS store_id,
        TRIM(sb.store_name) AS store_name,
        COALESCE(TRIM(sb.state), 'UNKNOWN') AS region,
        ROW_NUMBER() OVER (
            PARTITION BY sb.store_id
            ORDER BY sb.open_date DESC
        ) AS rn
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        region
    FROM base
    WHERE rn = 1
)
SELECT
    store_id,
    store_name,
    region
FROM dedup
WHERE store_id IS NOT NULL
""")
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.products_silver
# --------------------------------------------------------------------------------------
products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        pb.product_id AS product_id,
        TRIM(pb.product_name) AS product_name,
        COALESCE(TRIM(pb.category), 'UNKNOWN') AS category,
        ROW_NUMBER() OVER (
            PARTITION BY pb.product_id
            ORDER BY CAST(pb.is_active AS BOOLEAN) DESC
        ) AS rn
    FROM products_bronze pb
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category
    FROM base
    WHERE rn = 1
)
SELECT
    product_id,
    product_name,
    category
FROM dedup
WHERE product_id IS NOT NULL
""")
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.sales_performance_silver
# --------------------------------------------------------------------------------------
sales_performance_silver_df = spark.sql("""
SELECT
    sts.transaction_id AS transaction_id,
    sts.transaction_date AS transaction_date,
    sts.store_id AS store_id,
    ss.store_name AS store_name,
    sts.product_id AS product_id,
    ps.product_name AS product_name,
    sts.sales_amount AS sales_amount,
    sts.quantity_sold AS quantity_sold,
    ss.region AS region,
    ps.category AS category
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
    ON sts.store_id = ss.store_id
INNER JOIN products_silver ps
    ON sts.product_id = ps.product_id
""")
sales_performance_silver_df.createOrReplaceTempView("sales_performance_silver")

(
    sales_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_performance_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.daily_store_metrics_silver
# --------------------------------------------------------------------------------------
daily_store_metrics_silver_df = spark.sql("""
SELECT
    sts.store_id AS store_id,
    CAST(sts.transaction_date AS DATE) AS date,
    SUM(sts.sales_amount) AS total_sales_amount,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    COUNT(DISTINCT sts.transaction_id) AS number_of_transactions
FROM sales_transactions_silver sts
GROUP BY
    sts.store_id,
    CAST(sts.transaction_date AS DATE)
""")
daily_store_metrics_silver_df.createOrReplaceTempView("daily_store_metrics_silver")

(
    daily_store_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_store_metrics_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.daily_product_metrics_silver
# --------------------------------------------------------------------------------------
daily_product_metrics_silver_df = spark.sql("""
SELECT
    sts.product_id AS product_id,
    CAST(sts.transaction_date AS DATE) AS date,
    SUM(sts.sales_amount) AS total_sales_amount,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    COUNT(DISTINCT sts.transaction_id) AS number_of_transactions
FROM sales_transactions_silver sts
GROUP BY
    sts.product_id,
    CAST(sts.transaction_date AS DATE)
""")
daily_product_metrics_silver_df.createOrReplaceTempView("daily_product_metrics_silver")

(
    daily_product_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_product_metrics_silver.csv")
)

# --------------------------------------------------------------------------------------
# TARGET: silver.daily_sales_summary_silver
# --------------------------------------------------------------------------------------
daily_sales_summary_silver_df = spark.sql("""
SELECT
    CAST(sts.transaction_date AS DATE) AS date,
    SUM(sts.sales_amount) AS total_sales_amount,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    COUNT(DISTINCT sts.transaction_id) AS number_of_transactions,
    COUNT(DISTINCT sts.store_id) AS total_stores
FROM sales_transactions_silver sts
GROUP BY
    CAST(sts.transaction_date AS DATE)
""")
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

(
    daily_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_summary_silver.csv")
)

job.commit()