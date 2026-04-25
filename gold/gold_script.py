```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------------
# AWS Glue Context
# --------------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

# --------------------------------------------------------------------------------------
# Parameters / Constants
# --------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# ======================================================================================
# Target Table: gold.customer_orders_gold
# ======================================================================================

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT FORMAT)
# --------------------------------------------------------------------------------------
cos_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_silver.{FILE_FORMAT}/")
)

cs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)

ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

oss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_status_silver.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
cos_df.createOrReplaceTempView("customer_orders_silver")
cs_df.createOrReplaceTempView("customers_silver")
ps_df.createOrReplaceTempView("products_silver")
oss_df.createOrReplaceTempView("order_status_silver")

# --------------------------------------------------------------------------------------
# 3) Transform using Spark SQL (EXACT mappings + ROW_NUMBER dedup)
#    Base grain: (order_id, product_id)
# --------------------------------------------------------------------------------------
customer_orders_gold_df = spark.sql("""
WITH joined AS (
    SELECT
        -- Mappings (as specified)
        CAST(cos.order_id AS STRING)                        AS order_id,
        CAST(cos.order_date AS DATE)                        AS order_date,
        CAST(cs.customer_id AS STRING)                      AS customer_id,
        CAST(ps.product_id AS STRING)                       AS product_id,
        CAST(cos.quantity AS INT)                           AS quantity,
        CAST(cos.unit_price AS DECIMAL(38, 10))             AS unit_price,
        CAST(cos.order_amount AS DECIMAL(38, 10))           AS order_amount,
        CAST(oss.order_status AS STRING)                    AS order_status,
        CAST(cos.s3_ingest_date AS DATE)                    AS s3_ingest_date,
        CAST(cos.etl_load_ts AS TIMESTAMP)                  AS etl_load_ts,

        ROW_NUMBER() OVER (
            PARTITION BY cos.order_id, cos.product_id
            ORDER BY cos.etl_load_ts DESC
        ) AS rn
    FROM customer_orders_silver cos
    LEFT JOIN customers_silver cs
        ON cos.customer_id = cs.customer_id
    LEFT JOIN products_silver ps
        ON cos.product_id = ps.product_id
    LEFT JOIN order_status_silver oss
        ON cos.order_status = oss.order_status
)
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    order_amount,
    order_status,
    s3_ingest_date,
    etl_load_ts
FROM joined
WHERE rn = 1
""")

# --------------------------------------------------------------------------------------
# 4) Write output as SINGLE CSV directly under TARGET_PATH (STRICT FORMAT)
# --------------------------------------------------------------------------------------
(
    customer_orders_gold_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_gold.csv")
)
```