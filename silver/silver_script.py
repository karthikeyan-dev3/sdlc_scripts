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
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Optional: make Spark write a single CSV file by coalescing to 1 partition.
# Note: Spark will still create a directory at the output path containing one part file.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ====================================================================================
# 1) orders_silver
#    Source: bronze.orders_bronze
#    Rules:
#      - Deduplicate by order_id keeping latest transaction_time (ROW_NUMBER)
#      - order_date = CAST(transaction_time AS DATE)
#      - order_status = 'COMPLETED'
#      - currency_code = 'USD'
#      - customer_id = store_id
#      - order_total_amount = CASE WHEN COALESCE(sale_amount,0) < 0 THEN 0 ELSE COALESCE(sale_amount,0) END
# ====================================================================================

orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

orders_silver_sql = """
WITH ranked AS (
  SELECT
    ob.*,
    ROW_NUMBER() OVER (
      PARTITION BY ob.order_id
      ORDER BY ob.transaction_time DESC
    ) AS rn
  FROM orders_bronze ob
)
SELECT
  CAST(ob.order_id AS STRING) AS order_id,
  CAST(ob.store_id AS STRING) AS customer_id,
  CAST(ob.transaction_time AS DATE) AS order_date,
  CAST('COMPLETED' AS STRING) AS order_status,
  CAST('USD' AS STRING) AS currency_code,
  CAST(
    CASE
      WHEN COALESCE(ob.sale_amount, 0) < 0 THEN 0
      ELSE COALESCE(ob.sale_amount, 0)
    END
    AS DECIMAL(18,2)
  ) AS order_total_amount
FROM ranked ob
WHERE ob.rn = 1
"""

orders_silver_df = spark.sql(orders_silver_sql)
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ====================================================================================
# 2) order_items_silver
#    Source: bronze.order_items_bronze INNER JOIN silver.orders_silver
#    Rules:
#      - Deterministic order_item_id via ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY product_id, COALESCE(sale_amount,0) DESC)
#      - quantity = COALESCE(NULLIF(quantity,0),1)
#      - line_total_amount = COALESCE(sale_amount,0)
#      - unit_price_amount = CASE WHEN quantity>0 THEN line_total_amount/quantity ELSE NULL END
#      - enforce non-null keys (order_id, product_id)
#      - drop duplicates on (order_id, product_id, line_total_amount, quantity) keeping first by ordering
# ====================================================================================

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

order_items_silver_sql = """
WITH base AS (
  SELECT
    CAST(oib.order_id AS STRING)  AS order_id,
    CAST(oib.product_id AS STRING) AS product_id,
    CAST(COALESCE(NULLIF(oib.quantity, 0), 1) AS INT) AS quantity,
    CAST(COALESCE(oib.sale_amount, 0) AS DECIMAL(18,2)) AS line_total_amount
  FROM order_items_bronze oib
  INNER JOIN orders_silver os
    ON CAST(oib.order_id AS STRING) = CAST(os.order_id AS STRING)
  WHERE oib.order_id IS NOT NULL
    AND oib.product_id IS NOT NULL
),
dedup AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY b.order_id, b.product_id, b.line_total_amount, b.quantity
      ORDER BY b.product_id, b.line_total_amount DESC
    ) AS rn_dedup
  FROM base b
),
final_rank AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY d.order_id
      ORDER BY d.product_id, d.line_total_amount DESC
    ) AS order_item_id
  FROM dedup d
  WHERE d.rn_dedup = 1
)
SELECT
  CAST(fr.order_id AS STRING) AS order_id,
  CAST(fr.order_item_id AS INT) AS order_item_id,
  CAST(fr.product_id AS STRING) AS product_id,
  CAST(fr.quantity AS INT) AS quantity,
  CAST(fr.line_total_amount AS DECIMAL(18,2)) AS line_total_amount,
  CAST(
    CASE
      WHEN fr.quantity > 0 THEN fr.line_total_amount / fr.quantity
      ELSE NULL
    END
    AS DECIMAL(18,2)
  ) AS unit_price_amount
FROM final_rank fr
"""

order_items_silver_df = spark.sql(order_items_silver_sql)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ====================================================================================
# 3) daily_order_kpis_silver
#    Source: silver.orders_silver
#    Rules:
#      - Group by order_date
#      - total_orders = COUNT(DISTINCT order_id)
#      - total_customers = COUNT(DISTINCT customer_id)
#      - gross_sales_amount = SUM(order_total_amount)
# ====================================================================================

daily_order_kpis_silver_sql = """
SELECT
  CAST(os.order_date AS DATE) AS order_date,
  CAST(COUNT(DISTINCT os.order_id) AS INT) AS total_orders,
  CAST(COUNT(DISTINCT os.customer_id) AS INT) AS total_customers,
  CAST(SUM(os.order_total_amount) AS DECIMAL(18,2)) AS gross_sales_amount
FROM orders_silver os
GROUP BY os.order_date
"""

daily_order_kpis_silver_df = spark.sql(daily_order_kpis_silver_sql)
daily_order_kpis_silver_df.createOrReplaceTempView("daily_order_kpis_silver")

(
    daily_order_kpis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_order_kpis_silver.csv")
)

# ====================================================================================
# 4) data_quality_daily_silver
#    Source:
#      bronze.orders_bronze ob
#      LEFT JOIN bronze.order_items_bronze oib ON ob.order_id = oib.order_id
#      LEFT JOIN silver.orders_silver os       ON ob.order_id = os.order_id
#      LEFT JOIN silver.order_items_silver ois ON ob.order_id = ois.order_id
#    Rules:
#      - run_date = CURRENT_DATE
#      - source_record_count = COUNT(ob.order_id)
#      - loaded_order_count = COUNT(DISTINCT os.order_id)
#      - loaded_order_item_count = COUNT(ois.order_id)
#      - rejected_record_count = source_record_count - loaded_order_count
#      - accuracy_rate = CASE WHEN source_record_count>0 THEN loaded_order_count*1.0/source_record_count ELSE 1.0 END
# ====================================================================================

data_quality_daily_silver_sql = """
SELECT
  CAST(CURRENT_DATE AS DATE) AS run_date,
  CAST(COUNT(ob.order_id) AS INT) AS source_record_count,
  CAST(COUNT(DISTINCT os.order_id) AS INT) AS loaded_order_count,
  CAST(COUNT(ois.order_id) AS INT) AS loaded_order_item_count,
  CAST((COUNT(ob.order_id) - COUNT(DISTINCT os.order_id)) AS INT) AS rejected_record_count,
  CAST(
    CASE
      WHEN COUNT(ob.order_id) > 0 THEN (COUNT(DISTINCT os.order_id) * 1.0) / COUNT(ob.order_id)
      ELSE 1.0
    END
    AS DECIMAL(18,6)
  ) AS accuracy_rate
FROM orders_bronze ob
LEFT JOIN order_items_bronze oib
  ON CAST(ob.order_id AS STRING) = CAST(oib.order_id AS STRING)
LEFT JOIN orders_silver os
  ON CAST(ob.order_id AS STRING) = CAST(os.order_id AS STRING)
LEFT JOIN order_items_silver ois
  ON CAST(ob.order_id AS STRING) = CAST(ois.order_id AS STRING)
"""

data_quality_daily_silver_df = spark.sql(data_quality_daily_silver_sql)
data_quality_daily_silver_df.createOrReplaceTempView("data_quality_daily_silver")

(
    data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/data_quality_daily_silver.csv")
)

job.commit()
```