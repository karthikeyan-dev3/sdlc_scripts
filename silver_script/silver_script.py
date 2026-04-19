```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Job params / constants
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# TABLE: silver.stores_silver
# 1) Read source tables
# -----------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# 2) Create temp views (already done above)

# 3) Transform (Spark SQL) with de-dup using ROW_NUMBER
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(stb.store_id AS STRING)                                           AS store_id,
        INITCAP(TRIM(stb.store_name))                                          AS store_name,
        INITCAP(TRIM(stb.city))                                                AS city,
        UPPER(TRIM(stb.state))                                                 AS state,
        CASE WHEN stb.state IS NOT NULL THEN 'USA' ELSE 'UNKNOWN' END          AS country,
        TRIM(stb.store_type)                                                   AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY stb.store_id
          ORDER BY stb.store_id DESC
        ) AS rn
      FROM stores_bronze stb
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      country,
      store_type
    FROM ranked
    WHERE rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------------------------------------------------------------
# TABLE: silver.products_silver
# 1) Read source tables
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# 2) Create temp views (already done above)

# 3) Transform (Spark SQL) with de-dup using ROW_NUMBER
# Note: UDT mentions price/is_active, but no column mappings were provided for them,
# so only EXACT provided transformations/columns are implemented.
products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(prb.product_id AS STRING)                          AS product_id,
        TRIM(prb.product_name)                                  AS product_name,
        TRIM(prb.brand)                                         AS brand,
        COALESCE(TRIM(prb.category), 'UNKNOWN')                 AS category,
        ROW_NUMBER() OVER (
          PARTITION BY prb.product_id
          ORDER BY prb.product_id DESC
        ) AS rn
      FROM products_bronze prb
    )
    SELECT
      product_id,
      product_name,
      brand,
      category
    FROM ranked
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------------------------------------------------------------
# TABLE: silver.sales_transactions_silver
# 1) Read source tables
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# 2) Create temp views for joins (stores_silver, products_silver already registered)

# 3) Transform (Spark SQL)
# - Conform store_id/product_id using dimensions when possible (COALESCE)
# - sales_date = CAST(transaction_time AS DATE)
# - quantity/sale_amount forced to >= 0
# - remove exact duplicate rows using ROW_NUMBER over the full business key
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(stxb.transaction_id AS STRING)                                         AS transaction_id,
        COALESCE(CAST(sts.store_id AS STRING), CAST(stxb.store_id AS STRING))       AS store_id,
        COALESCE(CAST(prs.product_id AS STRING), CAST(stxb.product_id AS STRING))   AS product_id,
        stxb.transaction_time                                                       AS transaction_time,
        CAST(stxb.transaction_time AS DATE)                                         AS sales_date,
        CASE WHEN stxb.quantity < 0 THEN 0 ELSE stxb.quantity END                   AS quantity,
        CASE WHEN stxb.sale_amount < 0 THEN 0 ELSE stxb.sale_amount END             AS sale_amount
      FROM sales_transactions_bronze stxb
      LEFT JOIN stores_silver sts
        ON stxb.store_id = sts.store_id
      LEFT JOIN products_silver prs
        ON stxb.product_id = prs.product_id
    ),
    dedup AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_time,
        sales_date,
        quantity,
        sale_amount,
        ROW_NUMBER() OVER (
          PARTITION BY
            transaction_id,
            store_id,
            product_id,
            transaction_time,
            quantity,
            sale_amount
          ORDER BY transaction_id
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      transaction_time,
      sales_date,
      quantity,
      sale_amount
    FROM dedup
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)
```