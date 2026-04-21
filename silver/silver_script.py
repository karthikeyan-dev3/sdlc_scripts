```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended for CSV I/O
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# ===================================================================================
# 1) stores_silver
#    Source: bronze.stores_bronze (sb)
# ===================================================================================

# 1. Read source table(s)
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# 2. Create temp view(s)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# 3. Transformation SQL (clean + dedup with ROW_NUMBER)
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.store_id) AS STRING)                              AS store_id,
        CAST(TRIM(sb.store_name) AS STRING)                            AS store_name,
        CAST(TRIM(sb.city) AS STRING)                                  AS city,
        CAST(TRIM(sb.state) AS STRING)                                 AS state,
        CAST(TRIM(sb.store_type) AS STRING)                            AS store_type,

        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY TRIM(sb.store_id)
        ) AS rn
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
    )
    SELECT
      store_id,
      -- Standardize casing/whitespace (use allowed SQL functions)
      UPPER(TRIM(store_name))  AS store_name,
      UPPER(TRIM(city))        AS city,
      UPPER(TRIM(state))       AS state,
      UPPER(TRIM(store_type))  AS store_type
    FROM base
    WHERE rn = 1
    """
)

# 4. Save output (single CSV file directly under TARGET_PATH)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# Create temp view for downstream joins
stores_silver_df.createOrReplaceTempView("stores_silver")

# ===================================================================================
# 2) products_silver
#    Source: bronze.products_bronze (pb)
# ===================================================================================

# 1. Read source table(s)
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# 2. Create temp view(s)
products_bronze_df.createOrReplaceTempView("products_bronze")

# 3. Transformation SQL (clean + dedup with ROW_NUMBER)
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.product_id) AS STRING)                            AS product_id,
        CAST(TRIM(pb.product_name) AS STRING)                          AS product_name,
        CAST(TRIM(pb.brand) AS STRING)                                 AS brand,
        CAST(TRIM(pb.category) AS STRING)                              AS category,

        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_id)
        ) AS rn
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
    )
    SELECT
      product_id,
      UPPER(TRIM(product_name)) AS product_name,
      UPPER(TRIM(brand))        AS brand,
      UPPER(TRIM(category))     AS category
    FROM base
    WHERE rn = 1
    """
)

# 4. Save output (single CSV file directly under TARGET_PATH)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# Create temp view for downstream joins
products_silver_df.createOrReplaceTempView("products_silver")

# ===================================================================================
# 3) sales_transactions_silver
#    Source: bronze.sales_transactions_bronze (stb)
#    Join:  silver.stores_silver (ss) + silver.products_silver (ps)
# ===================================================================================

# 1. Read source table(s)
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# 2. Create temp view(s)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# 3. Transformation SQL (dedup + conform dims + casts + derived sales_date)
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING)                       AS transaction_id,
        CAST(TRIM(stb.store_id) AS STRING)                             AS store_id,
        CAST(TRIM(stb.product_id) AS STRING)                           AS product_id,

        -- Standardize quantity/sale_amount to be non-negative; keep NULLs as NULLs
        CAST(
          CASE
            WHEN stb.quantity IS NULL THEN NULL
            WHEN CAST(stb.quantity AS INT) < 0 THEN -CAST(stb.quantity AS INT)
            ELSE CAST(stb.quantity AS INT)
          END AS INT
        )                                                              AS quantity,

        CAST(
          CASE
            WHEN stb.sale_amount IS NULL THEN NULL
            WHEN CAST(stb.sale_amount AS DECIMAL(18,2)) < 0 THEN -CAST(stb.sale_amount AS DECIMAL(18,2))
            ELSE CAST(stb.sale_amount AS DECIMAL(18,2))
          END AS DECIMAL(18,2)
        )                                                              AS sale_amount,

        CAST(stb.transaction_time AS TIMESTAMP)                         AS transaction_time,

        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
        AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
        AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
    )
    SELECT
      b.transaction_id,
      b.store_id,
      b.product_id,
      b.quantity,
      b.sale_amount,
      b.transaction_time,
      DATE(b.transaction_time) AS sales_date
    FROM base b
    INNER JOIN stores_silver ss
      ON b.store_id = ss.store_id
    INNER JOIN products_silver ps
      ON b.product_id = ps.product_id
    WHERE b.rn = 1
    """
)

# 4. Save output (single CSV file directly under TARGET_PATH)
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
```