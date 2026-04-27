```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue bootstrap
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

# For consistent CSV parsing (adjust if your bronze has headers/quotes differently)
read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

# ------------------------------------------------------------------------------------
# 1) Read SOURCE tables (Bronze) from S3
#    IMPORTANT: path must be constructed as: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .options(**read_options)
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create TEMP views (Bronze)
# ------------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ====================================================================================
# TARGET TABLE: products_silver
# ====================================================================================
# 3) SQL transformations (dedup + standardization per UDT)
products_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(pb.product_id AS STRING)                                              AS product_id,
    TRIM(CAST(pb.product_name AS STRING))                                      AS product_name,
    UPPER(TRIM(CAST(pb.category AS STRING)))                                   AS category_name_standardized,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(pb.product_id AS STRING)
      ORDER BY CAST(pb.product_id AS STRING) DESC
    )                                                                          AS rn
  FROM products_bronze pb
  WHERE CAST(pb.product_id AS STRING) IS NOT NULL
)
SELECT
  product_id,
  product_name,
  category_name_standardized
FROM ranked
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ====================================================================================
# TARGET TABLE: stores_silver
# ====================================================================================
# 3) SQL transformations (dedup + cleaning per UDT)
stores_silver_sql = """
WITH ranked AS (
  SELECT
    CAST(sb.store_id AS STRING)                         AS store_id,
    TRIM(CAST(sb.store_name AS STRING))                 AS store_name,
    CAST(sb.state AS STRING)                            AS region,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sb.store_id AS STRING)
      ORDER BY CAST(sb.store_id AS STRING) DESC
    )                                                   AS rn
  FROM stores_bronze sb
  WHERE CAST(sb.store_id AS STRING) IS NOT NULL
)
SELECT
  store_id,
  store_name,
  region
FROM ranked
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ====================================================================================
# TARGET TABLE: sales_transactions_silver
# ====================================================================================
# 3) SQL transformations (joins + dedup by transaction_id keeping latest transaction_time)
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    CAST(stb.transaction_id AS STRING)                                 AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP)                            AS transaction_timestamp,
    CAST(stb.transaction_time AS DATE)                                 AS transaction_date,
    CAST(stb.store_id AS STRING)                                       AS store_id,
    CAST(stb.product_id AS STRING)                                     AS product_id,
    CAST(stb.quantity AS INT)                                          AS quantity,
    CAST((stb.sale_amount / stb.quantity) AS DECIMAL(38, 10))          AS unit_price,
    CAST(stb.sale_amount AS DECIMAL(38, 10))                           AS gross_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(stb.transaction_id AS STRING)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    )                                                                  AS rn
  FROM sales_transactions_bronze stb
  LEFT JOIN products_bronze pb
    ON CAST(stb.product_id AS STRING) = CAST(pb.product_id AS STRING)
  LEFT JOIN stores_bronze sb
    ON CAST(stb.store_id AS STRING) = CAST(sb.store_id AS STRING)
  WHERE
    CAST(stb.transaction_id AS STRING) IS NOT NULL
    AND CAST(stb.transaction_time AS TIMESTAMP) IS NOT NULL
    AND CAST(stb.store_id AS STRING) IS NOT NULL
    AND CAST(stb.product_id AS STRING) IS NOT NULL
    -- include only transactions with valid store_id and product_id (referential integrity via joins)
    AND CAST(pb.product_id AS STRING) IS NOT NULL
    AND CAST(sb.store_id AS STRING) IS NOT NULL
)
SELECT
  transaction_id,
  transaction_timestamp,
  transaction_date,
  store_id,
  product_id,
  quantity,
  unit_price,
  gross_sales_amount
FROM joined
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ====================================================================================
# TARGET TABLE: store_daily_sales_silver
# ====================================================================================
# 3) SQL transformations (aggregations per UDT)
store_daily_sales_silver_sql = """
SELECT
  CAST(sts.store_id AS STRING)                                  AS store_id,
  CAST(sts.transaction_date AS DATE)                            AS transaction_date,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)               AS total_transactions,
  CAST(SUM(sts.quantity) AS INT)                                AS total_units_sold,
  CAST(SUM(sts.gross_sales_amount) AS DECIMAL(38, 10))          AS total_gross_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.transaction_date AS DATE)
"""

store_daily_sales_silver_df = spark.sql(store_daily_sales_silver_sql)
store_daily_sales_silver_df.createOrReplaceTempView("store_daily_sales_silver")

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
(
    store_daily_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_daily_sales_silver.csv")
)

# ====================================================================================
# TARGET TABLE: product_daily_sales_silver
# ====================================================================================
# 3) SQL transformations (aggregations per UDT)
product_daily_sales_silver_sql = """
SELECT
  CAST(sts.product_id AS STRING)                                AS product_id,
  CAST(sts.transaction_date AS DATE)                            AS transaction_date,
  CAST(COUNT(DISTINCT sts.transaction_id) AS INT)               AS total_transactions,
  CAST(SUM(sts.quantity) AS INT)                                AS total_units_sold,
  CAST(SUM(sts.gross_sales_amount) AS DECIMAL(38, 10))          AS total_gross_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.product_id AS STRING),
  CAST(sts.transaction_date AS DATE)
"""

product_daily_sales_silver_df = spark.sql(product_daily_sales_silver_sql)
product_daily_sales_silver_df.createOrReplaceTempView("product_daily_sales_silver")

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
(
    product_daily_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_daily_sales_silver.csv")
)

job.commit()
```