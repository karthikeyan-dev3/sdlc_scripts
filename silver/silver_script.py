```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Init
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -----------------------------------------------------------------------------------
product_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_bronze.{FILE_FORMAT}/")
)
product_bronze_df.createOrReplaceTempView("product_bronze")

order_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_bronze.{FILE_FORMAT}/")
)
order_bronze_df.createOrReplaceTempView("order_bronze")

# ===================================================================================
# TARGET TABLE: silver.product_silver
# ===================================================================================
product_silver_sql = """
WITH base AS (
  SELECT
    CAST(TRIM(pb.product_id) AS STRING)                                             AS product_id,
    CAST(TRIM(pb.product_name) AS STRING)                                           AS product_name,
    CAST(TRIM(pb.category) AS STRING)                                               AS category,
    CAST(TRIM(pb.brand) AS STRING)                                                  AS brand,
    CAST(pb.price AS DECIMAL(18,2))                                                 AS price,
    CAST(pb.is_active AS BOOLEAN)                                                   AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
      ORDER BY CAST(TRIM(pb.product_id) AS STRING) DESC
    ) AS rn
  FROM product_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price,
  is_active
FROM base
WHERE rn = 1
"""

product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ===================================================================================
# TARGET TABLE: silver.order_silver
# ===================================================================================
order_silver_sql = """
SELECT
  CAST(TRIM(ob.transaction_id) AS STRING)                                           AS order_id,
  CAST(ob.transaction_time AS TIMESTAMP)                                            AS order_date,
  CAST(TRIM(ob.product_id) AS STRING)                                               AS product_id,
  CAST(ob.quantity AS INT)                                                          AS quantity,

  CAST(
    COALESCE(
      CAST(ps.price AS DECIMAL(18,2)),
      CAST(ob.sale_amount AS DECIMAL(18,2)) / NULLIF(CAST(ob.quantity AS INT), 0)
    ) AS DECIMAL(18,2)
  )                                                                                 AS unit_price,

  CAST(
    COALESCE(
      CAST(ob.quantity AS INT) * CAST(ps.price AS DECIMAL(18,2)),
      CAST(ob.sale_amount AS DECIMAL(18,2))
    ) AS DECIMAL(18,2)
  )                                                                                 AS order_total_amount,

  MD5(CONCAT(
    CAST(ob.transaction_id AS STRING),
    CAST(ob.transaction_time AS STRING),
    CAST(ob.product_id AS STRING),
    CAST(ob.quantity AS STRING),
    CAST(ob.sale_amount AS STRING),
    CAST(ps.price AS STRING)
  ))                                                                                AS record_hash,

  (
    ob.transaction_id IS NOT NULL
    AND ob.transaction_time IS NOT NULL
    AND ob.product_id IS NOT NULL
    AND CAST(ob.quantity AS INT) > 0
    AND COALESCE(
      CAST(ps.price AS DECIMAL(18,2)),
      CAST(ob.sale_amount AS DECIMAL(18,2)) / NULLIF(CAST(ob.quantity AS INT), 0)
    ) >= 0
    AND ps.product_id IS NOT NULL
  )                                                                                 AS is_valid_record,

  CASE
    WHEN ob.transaction_id IS NULL THEN 'missing_order_id'
    WHEN ob.transaction_time IS NULL THEN 'missing_order_date'
    WHEN ob.product_id IS NULL THEN 'missing_product_id'
    WHEN ob.quantity IS NULL OR CAST(ob.quantity AS INT) <= 0 THEN 'invalid_quantity'
    WHEN COALESCE(
           CAST(ps.price AS DECIMAL(18,2)),
           CAST(ob.sale_amount AS DECIMAL(18,2)) / NULLIF(CAST(ob.quantity AS INT), 0)
         ) IS NULL
         OR COALESCE(
              CAST(ps.price AS DECIMAL(18,2)),
              CAST(ob.sale_amount AS DECIMAL(18,2)) / NULLIF(CAST(ob.quantity AS INT), 0)
            ) < 0 THEN 'invalid_unit_price'
    WHEN ps.product_id IS NULL THEN 'invalid_product_reference'
  END                                                                                AS validation_error_reason
FROM order_bronze ob
LEFT JOIN product_silver ps
  ON CAST(TRIM(ob.product_id) AS STRING) = CAST(TRIM(ps.product_id) AS STRING)
"""

order_silver_df = spark.sql(order_silver_sql)

(
    order_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_silver.csv")
)

job.commit()
```