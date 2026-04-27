```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended for deterministic single-file writes (best-effort)
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ===================================================================================
# SOURCE READS + TEMP VIEWS
# ===================================================================================

# silver.customer_orders_silver
df_customer_orders_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)
df_customer_orders_silver.createOrReplaceTempView("customer_orders_silver")

# silver.customer_order_items_silver
df_customer_order_items_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_silver.{FILE_FORMAT}/")
)
df_customer_order_items_silver.createOrReplaceTempView("customer_order_items_silver")

# ===================================================================================
# TARGET TABLE 1: gold.gold_customer_orders
# Mapping: silver.customer_orders_silver cos
#          LEFT JOIN (SELECT order_id, COUNT(*) AS items_count FROM silver.customer_order_items_silver GROUP BY order_id) cois_agg
#          ON cos.order_id = cois_agg.order_id
# ===================================================================================

df_gold_customer_orders = spark.sql(
    """
    WITH cois_agg AS (
      SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(COUNT(*) AS INT) AS items_count
      FROM customer_order_items_silver
      GROUP BY CAST(order_id AS STRING)
    )
    SELECT
      CAST(cos.order_id AS STRING) AS order_id,
      CAST(cos.order_date AS DATE) AS order_date,
      CAST(cos.customer_id AS STRING) AS customer_id,
      CAST(cos.order_status AS STRING) AS order_status,
      CAST(cos.currency_code AS STRING) AS currency_code,
      CAST(cos.order_total_amount AS DECIMAL(38, 10)) AS order_total_amount,
      CAST(COALESCE(cois_agg.items_count, 0) AS INT) AS items_count,
      CAST(cos.payment_method AS STRING) AS payment_method,
      CAST(cos.shipping_country_code AS STRING) AS shipping_country_code,
      CAST(cos.source_system AS STRING) AS source_system,
      CAST(cos.ingestion_date AS DATE) AS ingestion_date
    FROM customer_orders_silver cos
    LEFT JOIN cois_agg
      ON CAST(cos.order_id AS STRING) = cois_agg.order_id
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_orders.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

# ===================================================================================
# TARGET TABLE 2: gold.gold_customer_order_items
# Mapping: silver.customer_order_items_silver cois
# ===================================================================================

df_gold_customer_order_items = spark.sql(
    """
    SELECT
      CAST(cois.order_id AS STRING) AS order_id,
      CAST(cois.order_item_id AS STRING) AS order_item_id,
      CAST(cois.product_id AS STRING) AS product_id,
      CAST(cois.quantity AS INT) AS quantity,
      CAST(cois.unit_price_amount AS DECIMAL(38, 10)) AS unit_price_amount,
      CAST(cois.line_total_amount AS DECIMAL(38, 10)) AS line_total_amount,
      CAST(cois.currency_code AS STRING) AS currency_code,
      CAST(cois.ingestion_date AS DATE) AS ingestion_date
    FROM customer_order_items_silver cois
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_order_items.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_items.csv")
)

job.commit()
```