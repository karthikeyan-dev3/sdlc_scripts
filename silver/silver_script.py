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

# -----------------------------
# Read Source Tables (Bronze)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH typed AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.sale_amount AS double) AS total_sales_amount,
        stb.transaction_time AS transaction_time
    FROM sales_transactions_bronze stb
),
filtered AS (
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_sales_amount,
        transaction_time
    FROM typed
    WHERE transaction_id IS NOT NULL
      AND store_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity_sold > 0
      AND total_sales_amount >= 0
),
deduped AS (
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_sales_amount,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, store_id, product_id, transaction_time
            ORDER BY transaction_time DESC
        ) AS rn
    FROM filtered
)
SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_sales_amount
FROM deduped
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# Create temp view for downstream aggregation
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: silver.product_master_silver
# ============================================================
product_master_silver_sql = """
WITH typed AS (
    SELECT
        TRIM(pmb.product_id) AS product_id,
        TRIM(pmb.product_name) AS product_name,
        TRIM(pmb.category) AS category,
        CAST(pmb.price AS float) AS price
    FROM product_master_bronze pmb
),
normalized AS (
    SELECT
        product_id,
        product_name,
        category,
        CASE
            WHEN price <= 0 THEN NULL
            ELSE price
        END AS price
    FROM typed
),
deduped AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM normalized
    WHERE product_id IS NOT NULL
)
SELECT
    product_id,
    product_name,
    category,
    price
FROM deduped
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ============================================================
# Target: silver.store_master_silver
# ============================================================
store_master_silver_sql = """
WITH typed AS (
    SELECT
        TRIM(smb.store_id) AS store_id,
        TRIM(smb.store_name) AS store_name,
        TRIM(smb.state) AS store_region
    FROM store_master_bronze smb
),
deduped AS (
    SELECT
        store_id,
        store_name,
        store_region,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM typed
    WHERE store_id IS NOT NULL
)
SELECT
    store_id,
    store_name,
    store_region
FROM deduped
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ============================================================
# Target: silver.sales_aggregates_silver
# ============================================================
sales_aggregates_silver_sql = """
SELECT
    sts.transaction_date AS date,
    sts.store_id AS store_id,
    sts.product_id AS product_id,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    SUM(sts.total_sales_amount) AS total_sales_revenue
FROM sales_transactions_silver sts
WHERE sts.transaction_date IS NOT NULL
  AND sts.store_id IS NOT NULL
  AND sts.product_id IS NOT NULL
GROUP BY
    sts.transaction_date,
    sts.store_id,
    sts.product_id
"""

sales_aggregates_silver_df = spark.sql(sales_aggregates_silver_sql)

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_silver.csv")
)

job.commit()