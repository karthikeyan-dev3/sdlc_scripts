```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue bootstrap
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ===================================================================================
# 1) orders_silver
#    Source: bronze.orders_bronze (S3)
#    Dedup: ROW_NUMBER() by transaction_id order by transaction_time desc
# ===================================================================================
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

orders_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        ob.*,
        ROW_NUMBER() OVER (
          PARTITION BY ob.transaction_id
          ORDER BY ob.transaction_time DESC
        ) AS rn
      FROM orders_bronze ob
    )
    SELECT
      ob.transaction_id                               AS order_id,
      CAST(ob.transaction_time AS DATE)               AS order_date,
      ob.transaction_time                             AS order_timestamp,
      ob.store_id                                     AS customer_id,
      CAST(ob.sale_amount AS DECIMAL(18,2))           AS order_total_amount
    FROM ranked ob
    WHERE ob.rn = 1
    """
)
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ===================================================================================
# 2) order_items_silver
#    Source: bronze.order_items_bronze (S3)
#    Dedup: ROW_NUMBER() by (transaction_id, product_id)
# ===================================================================================
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

order_items_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        oib.*,
        ROW_NUMBER() OVER (
          PARTITION BY oib.transaction_id, oib.product_id
          ORDER BY oib.transaction_id
        ) AS rn
      FROM order_items_bronze oib
    )
    SELECT
      oib.transaction_id                      AS order_id,
      oib.product_id                          AS product_id,
      COALESCE(oib.quantity, 0)               AS quantity,
      CAST(oib.sale_amount AS DECIMAL(18,2))  AS line_amount
    FROM ranked oib
    WHERE oib.rn = 1
    """
)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ===================================================================================
# 3) products_silver
#    Source: bronze.products_bronze (S3)
#    Dedup: ROW_NUMBER() by product_id order by is_active desc
# ===================================================================================
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.*,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.is_active DESC
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      pb.product_id                             AS product_id,
      TRIM(pb.product_name)                     AS product_name,
      TRIM(pb.category)                         AS category,
      TRIM(pb.brand)                            AS brand,
      CAST(pb.price AS DECIMAL(18,2))           AS list_price,
      COALESCE(pb.is_active, false)             AS is_active
    FROM ranked pb
    WHERE pb.rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# ===================================================================================
# 4) order_statuses_silver
#    Source: silver.orders_silver (temp view)
# ===================================================================================
order_statuses_silver_df = spark.sql(
    """
    SELECT
      os.order_id AS order_id,
      CASE
        WHEN os.order_total_amount IS NOT NULL THEN 'COMPLETED'
        ELSE 'UNKNOWN'
      END AS order_status
    FROM orders_silver os
    """
)
order_statuses_silver_df.createOrReplaceTempView("order_statuses_silver")

(
    order_statuses_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_statuses_silver.csv")
)

# ===================================================================================
# 5) currencies_silver
#    Source: silver.orders_silver (temp view)
# ===================================================================================
currencies_silver_df = spark.sql(
    """
    SELECT
      os.order_id AS order_id,
      'USD'       AS currency_code
    FROM orders_silver os
    """
)
currencies_silver_df.createOrReplaceTempView("currencies_silver")

(
    currencies_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/currencies_silver.csv")
)

# ===================================================================================
# 6) order_validations_silver
#    Source: silver.orders_silver LEFT JOIN silver.order_items_silver LEFT JOIN silver.products_silver
#    Outputs:
#      order_id, is_valid, validation_errors (pipe-delimited)
# ===================================================================================
order_validations_silver_df = spark.sql(
    """
    SELECT
      os.order_id AS order_id,

      CASE
        WHEN os.order_id IS NULL
          OR os.order_date IS NULL
          OR os.customer_id IS NULL
          OR SUM(CASE WHEN ois.product_id IS NULL THEN 1 ELSE 0 END) > 0
          OR SUM(CASE WHEN ois.quantity <= 0 THEN 1 ELSE 0 END) > 0
          OR SUM(CASE WHEN ps.list_price IS NULL THEN 1 ELSE 0 END) > 0
          OR ABS(
              COALESCE(SUM(ois.quantity * ps.list_price), 0) - COALESCE(os.order_total_amount, 0)
            ) > 0.01
        THEN false
        ELSE true
      END AS is_valid,

      TRIM(BOTH '|' FROM CONCAT(
        CASE WHEN os.order_id IS NULL THEN 'missing_order_id|' ELSE '' END,
        CASE WHEN os.order_date IS NULL THEN 'missing_order_date|' ELSE '' END,
        CASE WHEN os.customer_id IS NULL THEN 'missing_customer_id|' ELSE '' END,
        CASE WHEN SUM(CASE WHEN ois.product_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'missing_product_id|' ELSE '' END,
        CASE WHEN SUM(CASE WHEN ois.quantity <= 0 THEN 1 ELSE 0 END) > 0 THEN 'invalid_quantity|' ELSE '' END,
        CASE WHEN SUM(CASE WHEN ps.list_price IS NULL THEN 1 ELSE 0 END) > 0 THEN 'missing_unit_price|' ELSE '' END,
        CASE WHEN ABS(
              COALESCE(SUM(ois.quantity * ps.list_price), 0) - COALESCE(os.order_total_amount, 0)
            ) > 0.01 THEN 'amount_mismatch|' ELSE '' END
      )) AS validation_errors

    FROM orders_silver os
    LEFT JOIN order_items_silver ois
      ON os.order_id = ois.order_id
    LEFT JOIN products_silver ps
      ON ois.product_id = ps.product_id

    GROUP BY
      os.order_id,
      os.order_date,
      os.customer_id,
      os.order_total_amount
    """
)
order_validations_silver_df.createOrReplaceTempView("order_validations_silver")

(
    order_validations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_validations_silver.csv")
)

job.commit()
```