```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Job / Context
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Config
# ----------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ============================================================
# 1) Read source tables (bronze) and create temp views
# ============================================================
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

order_statuses_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_statuses_bronze.{FILE_FORMAT}/")
)
order_statuses_bronze_df.createOrReplaceTempView("order_statuses_bronze")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

# ============================================================
# TABLE: silver.orders_silver
# 1) Read source tables: orders_bronze, order_statuses_bronze
# 2) Temp views: already created above
# 3) SQL transform
# 4) Write output
# ============================================================
orders_silver_sql = """
WITH latest_status AS (
  SELECT
    order_id,
    MAX(status_timestamp) AS latest_status_timestamp
  FROM order_statuses_bronze
  GROUP BY order_id
),
joined AS (
  SELECT
    ob.order_id                                                   AS order_id,
    CAST(ob.order_timestamp AS TIMESTAMP)                         AS order_timestamp,
    DATE(CAST(ob.order_timestamp AS TIMESTAMP))                   AS order_date,
    osb.status                                                    AS order_status,
    CAST(ob.order_total_amount AS DECIMAL(38, 10))                AS total_order_amount,
    'USD'                                                         AS currency_code,
    DATE(CAST(ob.order_timestamp AS TIMESTAMP))                   AS source_file_date,
    current_timestamp()                                           AS load_timestamp
  FROM orders_bronze ob
  LEFT JOIN latest_status ost
    ON ob.order_id = ost.order_id
  LEFT JOIN order_statuses_bronze osb
    ON osb.order_id = ost.order_id
   AND osb.status_timestamp = ost.latest_status_timestamp
),
dedup AS (
  SELECT
    order_id,
    order_timestamp,
    order_date,
    order_status,
    total_order_amount,
    currency_code,
    source_file_date,
    load_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_timestamp DESC
    ) AS rn
  FROM joined
)
SELECT
  CAST(order_id AS STRING)                                        AS order_id,
  CAST(order_timestamp AS TIMESTAMP)                              AS order_timestamp,
  CAST(order_date AS DATE)                                        AS order_date,
  CAST(COALESCE(order_status, 'COMPLETED') AS STRING)             AS order_status,
  CAST(total_order_amount AS DECIMAL(38, 10))                     AS total_order_amount,
  CAST(currency_code AS STRING)                                   AS currency_code,
  CAST(source_file_date AS DATE)                                  AS source_file_date,
  CAST(load_timestamp AS TIMESTAMP)                               AS load_timestamp
FROM dedup
WHERE rn = 1
"""
orders_silver_df = spark.sql(orders_silver_sql)
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

# ============================================================
# TABLE: silver.order_items_agg_silver
# 1) Read source tables: order_items_bronze
# 2) Temp views: already created above
# 3) SQL transform
# 4) Write output
# ============================================================
order_items_agg_silver_sql = """
SELECT
  CAST(oib.order_id AS STRING)                         AS order_id,
  CAST(SUM(COALESCE(oib.quantity, 0)) AS INT)          AS item_count
FROM order_items_bronze oib
GROUP BY oib.order_id
"""
order_items_agg_silver_df = spark.sql(order_items_agg_silver_sql)
order_items_agg_silver_df.createOrReplaceTempView("order_items_agg_silver")

(
    order_items_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_agg_silver.csv")
)

# ============================================================
# TABLE: silver.customer_orders_silver
# 1) Read source tables: orders_silver, order_items_agg_silver
# 2) Temp views: created above as results of prior steps
# 3) SQL transform
# 4) Write output
# ============================================================
customer_orders_silver_sql = """
SELECT
  CAST(os.order_id AS STRING)                                       AS order_id,
  CAST('UNKNOWN' AS STRING)                                         AS customer_id,
  CAST(DATE(os.order_timestamp) AS DATE)                            AS order_date,
  CAST(os.order_timestamp AS TIMESTAMP)                             AS order_timestamp,
  CAST(os.order_status AS STRING)                                   AS order_status,
  CAST(os.total_order_amount AS DECIMAL(38, 10))                    AS total_order_amount,
  CAST(os.currency_code AS STRING)                                  AS currency_code,
  CAST(COALESCE(oias.item_count, 0) AS INT)                         AS item_count,
  CAST(os.source_file_date AS DATE)                                 AS source_file_date,
  CAST(os.load_timestamp AS TIMESTAMP)                              AS load_timestamp
FROM orders_silver os
LEFT JOIN order_items_agg_silver oias
  ON os.order_id = oias.order_id
"""
customer_orders_silver_df = spark.sql(customer_orders_silver_sql)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

job.commit()
```