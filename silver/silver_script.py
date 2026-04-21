```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# Glue bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended CSV options (kept explicit and simple)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT path format: {SOURCE_PATH}/table.{FILE_FORMAT}/)
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ======================================================================================
# TARGET TABLE: silver.sales_transactions_silver
# - Apply EXACT UDT transformations:
#   sts.transaction_id = stb.transaction_id
#   sts.store_id        = stb.store_id
#   sts.product_id      = stb.product_id
#   sts.transaction_time= stb.transaction_time
#   sts.sales_date      = CAST(stb.transaction_time AS DATE)
#   sts.quantity        = stb.quantity
#   sts.sale_amount     = stb.sale_amount
# - Dedup: one record per transaction_id, keep latest transaction_time
# ======================================================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(stb.transaction_id AS STRING)      AS transaction_id,
        CAST(stb.store_id AS STRING)            AS store_id,
        CAST(stb.product_id AS STRING)          AS product_id,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        CAST(stb.transaction_time AS DATE)      AS sales_date,
        CAST(stb.quantity AS INT)               AS quantity,
        CAST(stb.sale_amount AS DECIMAL(38, 10)) AS sale_amount,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      sales_date,
      transaction_time,
      quantity,
      sale_amount
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ======================================================================================
# TARGET TABLE: silver.products_silver
# - Apply EXACT UDT transformations:
#   ps.product_id   = pb.product_id
#   ps.product_name = pb.product_name
#   ps.brand        = pb.brand
#   ps.category     = pb.category
#   ps.price        = pb.price
#   ps.is_active    = pb.is_active
# - Dedup: one row per product_id
# ======================================================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(pb.product_id AS STRING)            AS product_id,
        CAST(pb.product_name AS STRING)          AS product_name,
        CAST(pb.brand AS STRING)                 AS brand,
        CAST(pb.category AS STRING)              AS category,
        CAST(pb.price AS DECIMAL(38, 10))        AS price,
        CAST(pb.is_active AS BOOLEAN)            AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      brand,
      category,
      price,
      is_active
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ======================================================================================
# TARGET TABLE: silver.stores_silver
# - Apply EXACT UDT transformations:
#   ss.store_id    = sb.store_id
#   ss.store_name  = sb.store_name
#   ss.city        = sb.city
#   ss.state       = sb.state
#   ss.store_type  = sb.store_type
#   ss.open_date   = sb.open_date
# - Dedup: one row per store_id
# ======================================================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(sb.store_id AS STRING)     AS store_id,
        CAST(sb.store_name AS STRING)   AS store_name,
        CAST(sb.city AS STRING)         AS city,
        CAST(sb.state AS STRING)        AS state,
        CAST(sb.store_type AS STRING)   AS store_type,
        CAST(sb.open_date AS DATE)      AS open_date,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      open_date
    FROM base
    WHERE rn = 1
    """
)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

job.commit()
```