import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# =========================
# 1) Read Source Tables
# =========================
pmb_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)

smb_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)

stb_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# =========================
# 2) Create Temp Views
# =========================
pmb_df.createOrReplaceTempView("product_master_bronze")
smb_df.createOrReplaceTempView("store_master_bronze")
stb_df.createOrReplaceTempView("sales_transactions_bronze")

# ==========================================================
# TABLE: silver.product_master_silver
# ==========================================================
product_master_silver_df = spark.sql("""
WITH cleaned AS (
  SELECT
    UPPER(TRIM(pmb.product_id)) AS product_id,
    TRIM(pmb.product_name)      AS product_name,
    TRIM(pmb.category)          AS category,
    CAST(pmb.price AS float)    AS price,
    TRIM(pmb.brand)             AS brand
  FROM product_master_bronze pmb
  WHERE UPPER(TRIM(pmb.product_id)) IS NOT NULL
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    brand,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY product_name DESC, category DESC, brand DESC, price DESC
    ) AS rn
  FROM cleaned
)
SELECT
  product_id,
  product_name,
  category,
  price,
  brand
FROM dedup
WHERE rn = 1
""")

(
    product_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ==========================================================
# TABLE: silver.store_master_silver
# ==========================================================
store_master_silver_df = spark.sql("""
WITH cleaned AS (
  SELECT
    UPPER(TRIM(smb.store_id))                    AS store_id,
    TRIM(smb.store_name)                        AS store_name,
    CONCAT(TRIM(smb.city), ', ', TRIM(smb.state)) AS location,
    TRIM(smb.state)                             AS region
  FROM store_master_bronze smb
  WHERE UPPER(TRIM(smb.store_id)) IS NOT NULL
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY store_name DESC, location DESC, region DESC
    ) AS rn
  FROM cleaned
)
SELECT
  store_id,
  store_name,
  location,
  region
FROM dedup
WHERE rn = 1
""")

(
    store_master_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ==========================================================
# TABLE: silver.sales_transactions_silver
# ==========================================================
sales_transactions_silver_df = spark.sql("""
WITH cleaned AS (
  SELECT
    UPPER(TRIM(stb.transaction_id))      AS transaction_id,
    CAST(stb.transaction_time AS date)  AS date,
    UPPER(TRIM(stb.store_id))           AS store_id,
    UPPER(TRIM(stb.product_id))         AS product_id,
    CAST(stb.quantity AS int)           AS quantity,
    CAST(stb.sale_amount AS double)     AS total_sales,
    stb.transaction_time                AS transaction_time
  FROM sales_transactions_bronze stb
),
validated AS (
  SELECT
    c.transaction_id,
    c.date,
    c.store_id,
    c.product_id,
    c.quantity,
    c.total_sales,
    c.transaction_time
  FROM cleaned c
  INNER JOIN product_master_silver pms
    ON pms.product_id = c.product_id
  INNER JOIN store_master_silver sms
    ON sms.store_id = c.store_id
  WHERE c.transaction_id IS NOT NULL
    AND c.store_id IS NOT NULL
    AND c.product_id IS NOT NULL
    AND c.quantity >= 0
    AND c.total_sales >= 0
),
dedup AS (
  SELECT
    transaction_id,
    date,
    store_id,
    product_id,
    quantity,
    total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM validated
)
SELECT
  transaction_id,
  date,
  store_id,
  product_id,
  quantity,
  total_sales
FROM dedup
WHERE rn = 1
""")

(
    sales_transactions_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ==========================================================
# TABLE: silver.sales_aggregated_silver
# ==========================================================
sales_aggregated_silver_df = spark.sql("""
WITH base AS (
  SELECT
    sts.date       AS date,
    sts.store_id   AS store_id,
    sts.product_id AS product_id,
    sts.quantity   AS quantity,
    sts.total_sales AS total_sales,
    pms.category   AS category
  FROM sales_transactions_silver sts
  INNER JOIN product_master_silver pms
    ON pms.product_id = sts.product_id
),
agg AS (
  SELECT
    date,
    store_id,
    product_id,
    SUM(quantity)   AS total_quantity,
    SUM(total_sales) AS total_revenue,
    category
  FROM base
  GROUP BY date, store_id, product_id, category
)
SELECT
  date,
  store_id,
  product_id,
  total_quantity,
  total_revenue,
  SUM(total_revenue) OVER (PARTITION BY date, store_id, category) AS category_revenue
FROM agg
""")

(
    sales_aggregated_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

# ==========================================================
# TABLE: silver.data_quality_metrics_silver
# ==========================================================
data_quality_metrics_silver_df = spark.sql("""
SELECT
  CAST(stb.transaction_time AS date) AS date,
  COUNT(stb.transaction_id) AS total_records,
  COUNT(stb.transaction_id) - COUNT(DISTINCT stb.transaction_id) AS duplicate_records,
  SUM(
    CASE
      WHEN stb.transaction_id IS NULL
        OR stb.store_id IS NULL
        OR stb.product_id IS NULL
        OR CAST(stb.quantity AS int) < 0
        OR CAST(stb.sale_amount AS double) < 0
        OR pms.product_id IS NULL
        OR sms.store_id IS NULL
      THEN 1 ELSE 0
    END
  ) AS invalid_records,
  1 - (
    SUM(
      CASE
        WHEN stb.transaction_id IS NULL
          OR stb.store_id IS NULL
          OR stb.product_id IS NULL
          OR CAST(stb.quantity AS int) < 0
          OR CAST(stb.sale_amount AS double) < 0
          OR pms.product_id IS NULL
          OR sms.store_id IS NULL
        THEN 1 ELSE 0
      END
    ) / NULLIF(COUNT(*), 0)
  ) AS accuracy_score
FROM sales_transactions_bronze stb
LEFT JOIN product_master_silver pms
  ON pms.product_id = UPPER(TRIM(stb.product_id))
LEFT JOIN store_master_silver sms
  ON sms.store_id = UPPER(TRIM(stb.store_id))
GROUP BY CAST(stb.transaction_time AS date)
""")

(
    data_quality_metrics_silver_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()