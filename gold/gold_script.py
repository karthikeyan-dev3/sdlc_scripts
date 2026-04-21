```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Glue / Spark init
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Config
# ----------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Ensure single output file per target (critical requirement)
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ----------------------------
# 1) Read source tables from S3
#    (MUST use: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ----------------------------
fsls_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/fact_sales_line_silver.{FILE_FORMAT}/")
)
dss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)
dps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
fsls_df.createOrReplaceTempView("fact_sales_line_silver")
dss_df.createOrReplaceTempView("dim_store_silver")
dps_df.createOrReplaceTempView("dim_product_silver")

# ============================================================
# TARGET TABLE: gold_store_daily_sales
# ============================================================
gold_store_daily_sales_df = spark.sql("""
SELECT
  CAST(DATE(fsls.transaction_time) AS DATE)                                          AS sales_date,
  CAST(fsls.store_id AS STRING)                                                      AS store_id,
  CAST(dss.store_name AS STRING)                                                     AS store_name,
  CAST(dss.city AS STRING)                                                           AS city,
  CAST(dss.state AS STRING)                                                          AS state,
  CAST(dss.store_type AS STRING)                                                     AS store_type,
  CAST(SUM(fsls.sale_amount) AS DECIMAL(38, 10))                                     AS total_revenue,
  CAST(SUM(fsls.quantity) AS BIGINT)                                                 AS total_quantity_sold,
  CAST(COUNT(DISTINCT fsls.transaction_id) AS BIGINT)                                AS total_transactions
FROM fact_sales_line_silver fsls
LEFT JOIN dim_store_silver dss
  ON fsls.store_id = dss.store_id
GROUP BY
  CAST(DATE(fsls.transaction_time) AS DATE),
  CAST(fsls.store_id AS STRING),
  CAST(dss.store_name AS STRING),
  CAST(dss.city AS STRING),
  CAST(dss.state AS STRING),
  CAST(dss.store_type AS STRING)
""")

(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# ============================================================
# TARGET TABLE: gold_product_daily_sales
# ============================================================
gold_product_daily_sales_df = spark.sql("""
SELECT
  CAST(DATE(fsls.transaction_time) AS DATE)                                          AS sales_date,
  CAST(fsls.product_id AS STRING)                                                    AS product_id,
  CAST(dps.product_name AS STRING)                                                   AS product_name,
  CAST(dps.brand AS STRING)                                                          AS brand,
  CAST(dps.category AS STRING)                                                       AS category,
  CAST(SUM(fsls.sale_amount) AS DECIMAL(38, 10))                                     AS total_revenue,
  CAST(SUM(fsls.quantity) AS BIGINT)                                                 AS total_quantity_sold,
  CAST(COUNT(DISTINCT fsls.transaction_id) AS BIGINT)                                AS total_transactions
FROM fact_sales_line_silver fsls
LEFT JOIN dim_product_silver dps
  ON fsls.product_id = dps.product_id
GROUP BY
  CAST(DATE(fsls.transaction_time) AS DATE),
  CAST(fsls.product_id AS STRING),
  CAST(dps.product_name AS STRING),
  CAST(dps.brand AS STRING),
  CAST(dps.category AS STRING)
""")

(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

# ============================================================
# TARGET TABLE: gold_category_daily_sales
# ============================================================
gold_category_daily_sales_df = spark.sql("""
SELECT
  CAST(DATE(fsls.transaction_time) AS DATE)                                          AS sales_date,
  CAST(dps.category AS STRING)                                                       AS category,
  CAST(SUM(fsls.sale_amount) AS DECIMAL(38, 10))                                     AS total_revenue,
  CAST(SUM(fsls.quantity) AS BIGINT)                                                 AS total_quantity_sold,
  CAST(COUNT(DISTINCT fsls.transaction_id) AS BIGINT)                                AS total_transactions
FROM fact_sales_line_silver fsls
LEFT JOIN dim_product_silver dps
  ON fsls.product_id = dps.product_id
GROUP BY
  CAST(DATE(fsls.transaction_time) AS DATE),
  CAST(dps.category AS STRING)
""")

(
    gold_category_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_category_daily_sales.csv")
)

# ============================================================
# TARGET TABLE: gold_store_category_daily_sales
# ============================================================
gold_store_category_daily_sales_df = spark.sql("""
SELECT
  CAST(DATE(fsls.transaction_time) AS DATE)                                          AS sales_date,
  CAST(fsls.store_id AS STRING)                                                      AS store_id,
  CAST(dss.store_name AS STRING)                                                     AS store_name,
  CAST(dss.city AS STRING)                                                           AS city,
  CAST(dss.store_type AS STRING)                                                     AS store_type,
  CAST(dps.category AS STRING)                                                       AS category,
  CAST(SUM(fsls.sale_amount) AS DECIMAL(38, 10))                                     AS total_revenue,
  CAST(SUM(fsls.quantity) AS BIGINT)                                                 AS total_quantity_sold,
  CAST(COUNT(DISTINCT fsls.transaction_id) AS BIGINT)                                AS total_transactions
FROM fact_sales_line_silver fsls
LEFT JOIN dim_store_silver dss
  ON fsls.store_id = dss.store_id
LEFT JOIN dim_product_silver dps
  ON fsls.product_id = dps.product_id
GROUP BY
  CAST(DATE(fsls.transaction_time) AS DATE),
  CAST(fsls.store_id AS STRING),
  CAST(dss.store_name AS STRING),
  CAST(dss.city AS STRING),
  CAST(dss.store_type AS STRING),
  CAST(dps.category AS STRING)
""")

(
    gold_store_category_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/gold_store_category_daily_sales.csv")
)

job.commit()
```