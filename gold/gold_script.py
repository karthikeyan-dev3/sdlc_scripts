```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (Silver)
#    IMPORTANT: Always construct paths using: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# --------------------------------------------------------------------------------------
orders_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

order_items_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_silver.{FILE_FORMAT}/")
)

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
orders_silver_df.createOrReplaceTempView("orders_silver")
order_items_silver_df.createOrReplaceTempView("order_items_silver")
customers_silver_df.createOrReplaceTempView("customers_silver")

# ======================================================================================
# TARGET TABLE: gold_orders_daily
# ======================================================================================
gold_orders_daily_sql = """
WITH os_dedup AS (
  SELECT
    os.*,
    ROW_NUMBER() OVER (
      PARTITION BY os.order_id
      ORDER BY os.ingestion_date DESC
    ) AS rn
  FROM orders_silver os
)
SELECT
  CAST(os.order_id AS STRING) AS order_id,
  DATE(os.order_date) AS order_date,
  CAST(os.customer_id AS STRING) AS customer_id,
  CAST(COALESCE(os.order_status, 'COMPLETED') AS STRING) AS order_status,
  CAST(os.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
  CAST(COALESCE(os.currency_code, 'USD') AS STRING) AS currency_code,
  CAST(os.items_count AS INT) AS items_count,
  CAST(os.source_s3_file_path AS STRING) AS source_s3_file_path,
  DATE(os.ingestion_date) AS ingestion_date,
  CURRENT_TIMESTAMP AS load_timestamp
FROM os_dedup os
WHERE os.rn = 1
"""

gold_orders_daily_df = spark.sql(gold_orders_daily_sql)

gold_orders_daily_output_path = f"{TARGET_PATH}/gold_orders_daily.csv"
(
    gold_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_orders_daily_output_path)
)

# ======================================================================================
# TARGET TABLE: gold_order_items_daily
# ======================================================================================
gold_order_items_daily_sql = """
WITH os_dedup AS (
  SELECT
    os.*,
    ROW_NUMBER() OVER (
      PARTITION BY os.order_id
      ORDER BY os.ingestion_date DESC
    ) AS rn
  FROM orders_silver os
),
os_one AS (
  SELECT
    *
  FROM os_dedup
  WHERE rn = 1
),
ois_dedup AS (
  SELECT
    ois.*,
    ROW_NUMBER() OVER (
      PARTITION BY ois.order_item_id
      ORDER BY ois.ingestion_date DESC
    ) AS rn
  FROM order_items_silver ois
),
ois_one AS (
  SELECT
    *
  FROM ois_dedup
  WHERE rn = 1
)
SELECT
  CAST(ois.order_id AS STRING) AS order_id,
  CAST(ois.order_item_id AS STRING) AS order_item_id,
  CAST(ois.product_id AS STRING) AS product_id,
  CAST(ois.quantity AS INT) AS quantity,
  CAST(ois.unit_price AS DECIMAL(38, 10)) AS unit_price,
  CAST(ois.line_amount AS DECIMAL(38, 10)) AS line_amount,
  CAST(ois.currency_code AS STRING) AS currency_code,
  DATE(os.order_date) AS order_date,
  CAST(os.customer_id AS STRING) AS customer_id,
  CAST(ois.source_s3_file_path AS STRING) AS source_s3_file_path,
  DATE(ois.ingestion_date) AS ingestion_date,
  CURRENT_TIMESTAMP AS load_timestamp
FROM ois_one ois
INNER JOIN os_one os
  ON ois.order_id = os.order_id
"""

gold_order_items_daily_df = spark.sql(gold_order_items_daily_sql)

gold_order_items_daily_output_path = f"{TARGET_PATH}/gold_order_items_daily.csv"
(
    gold_order_items_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_order_items_daily_output_path)
)

# ======================================================================================
# TARGET TABLE: gold_customers
# ======================================================================================
gold_customers_sql = """
WITH cs_dedup AS (
  SELECT
    cs.*,
    ROW_NUMBER() OVER (
      PARTITION BY cs.customer_id
      ORDER BY cs.ingestion_date DESC
    ) AS rn
  FROM customers_silver cs
)
SELECT
  CAST(cs.customer_id AS STRING) AS customer_id,
  CAST(cs.customer_name AS STRING) AS customer_name,
  CAST(cs.customer_email AS STRING) AS customer_email,
  DATE(cs.customer_created_date) AS customer_created_date,
  CAST(cs.customer_status AS STRING) AS customer_status,
  CURRENT_TIMESTAMP AS load_timestamp
FROM cs_dedup cs
WHERE cs.rn = 1
"""

gold_customers_df = spark.sql(gold_customers_sql)

gold_customers_output_path = f"{TARGET_PATH}/gold_customers.csv"
(
    gold_customers_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customers_output_path)
)

job.commit()
```