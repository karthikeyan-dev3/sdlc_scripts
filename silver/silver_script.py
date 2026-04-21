```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Recommended for deterministic single-file outputs
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
#    IMPORTANT: path must be constructed as: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# ====================================================================================
# TABLE: dim_store_silver
# ====================================================================================
dim_store_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(sb.store_id) AS STRING)                                  AS store_id,
    CAST(TRIM(sb.store_name) AS STRING)                                AS store_name,
    CAST(TRIM(sb.city) AS STRING)                                      AS city,
    CAST(UPPER(TRIM(sb.state)) AS STRING)                              AS state,
    CAST(LOWER(TRIM(sb.store_type)) AS STRING)                         AS store_type,
    CAST(sb.open_date AS DATE)                                         AS open_date,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(sb.store_id) AS STRING)
      ORDER BY CAST(sb.open_date AS DATE) DESC
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
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

dim_store_silver_df = spark.sql(dim_store_silver_sql)
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    dim_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_store_silver.csv")
)

# ====================================================================================
# TABLE: dim_product_silver
# ====================================================================================
dim_product_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)                                AS product_id,
    CAST(TRIM(pb.product_name) AS STRING)                              AS product_name,
    CAST(TRIM(pb.brand) AS STRING)                                     AS brand,
    CAST(LOWER(TRIM(pb.category)) AS STRING)                           AS category,
    CAST(
      CASE
        WHEN CAST(pb.price AS DECIMAL(18,2)) < 0 THEN NULL
        ELSE CAST(pb.price AS DECIMAL(18,2))
      END AS DECIMAL(18,2)
    )                                                                  AS price,
    CAST(COALESCE(CAST(pb.is_active AS BOOLEAN), TRUE) AS BOOLEAN)     AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
      ORDER BY CAST(
        COALESCE(CAST(pb.is_active AS BOOLEAN), FALSE) AS INT
      ) DESC
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
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

dim_product_silver_df = spark.sql(dim_product_silver_sql)
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    dim_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_product_silver.csv")
)

# ====================================================================================
# TABLE: fact_transaction_line_silver
# ====================================================================================
fact_transaction_line_silver_sql = """
SELECT
  CAST(TRIM(tb.transaction_id) AS STRING)                         AS transaction_id,
  CAST(tb.transaction_time AS DATE)                               AS sales_date,
  CAST(tb.transaction_time AS TIMESTAMP)                          AS transaction_time,
  CAST(TRIM(tb.store_id) AS STRING)                               AS store_id,
  CAST(TRIM(tb.product_id) AS STRING)                             AS product_id,
  CAST(tb.quantity AS INT)                                        AS quantity,
  CAST(tb.sale_amount AS DECIMAL(18,2))                           AS sale_amount
FROM transactions_bronze tb
INNER JOIN dim_store_silver ds
  ON CAST(TRIM(tb.store_id) AS STRING) = ds.store_id
INNER JOIN dim_product_silver dp
  ON CAST(TRIM(tb.product_id) AS STRING) = dp.product_id
WHERE
  TRIM(tb.transaction_id) IS NOT NULL AND TRIM(tb.transaction_id) <> ''
  AND tb.transaction_time IS NOT NULL
  AND TRIM(tb.store_id) IS NOT NULL AND TRIM(tb.store_id) <> ''
  AND TRIM(tb.product_id) IS NOT NULL AND TRIM(tb.product_id) <> ''
  AND CAST(tb.quantity AS INT) > 0
  AND CAST(tb.sale_amount AS DECIMAL(18,2)) >= 0
"""

fact_transaction_line_silver_df = spark.sql(fact_transaction_line_silver_sql)

# 4) Save output (single CSV file directly under TARGET_PATH)
(
    fact_transaction_line_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/fact_transaction_line_silver.csv")
)

job.commit()
```