```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue boilerplate
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

# Ensure single CSV output file per target (one part) and avoid extra folders besides the target file path
spark.conf.set("spark.sql.shuffle.partitions", "1")

# ====================================================================================
# 1) orders_silver
#    Source: bronze.orders_bronze (S3)
#    De-dup rule: keep latest transaction_time per transaction_id
# ====================================================================================
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
        WHERE ob.transaction_id IS NOT NULL
    )
    SELECT
        CAST(ob.transaction_id AS STRING)                         AS order_id,
        CAST(ob.transaction_time AS DATE)                         AS order_date,
        CAST('COMPLETED' AS STRING)                               AS order_status,
        CAST(NULL AS STRING)                                      AS customer_id,
        CAST(ob.sale_amount AS DECIMAL(38, 10))                   AS order_total_amount,
        CAST('USD' AS STRING)                                     AS currency_code,
        CAST('sales_transactions_raw' AS STRING)                  AS source_system,
        CURRENT_DATE                                              AS ingestion_date
    FROM ranked ob
    WHERE ob.rn = 1
    """
)
orders_silver_df.createOrReplaceTempView("orders_silver")

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/orders_silver.csv")
)

# ====================================================================================
# 2) order_items_silver
#    Source: bronze.order_items_bronze (S3)
#    De-dup rule: remove exact duplicates on (transaction_id, product_id, quantity, sale_amount)
#               keeping one record
#    Derivations: order_item_id = ROW_NUMBER() over (transaction_id) order by product_id
# ====================================================================================
order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

order_items_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            oib.*,
            ROW_NUMBER() OVER (
                PARTITION BY oib.transaction_id, oib.product_id, oib.quantity, oib.sale_amount
                ORDER BY oib.transaction_id
            ) AS rn_dedup
        FROM order_items_bronze oib
        WHERE oib.transaction_id IS NOT NULL
    ),
    kept AS (
        SELECT
            *
        FROM dedup
        WHERE rn_dedup = 1
    )
    SELECT
        CAST(k.transaction_id AS STRING) AS order_id,
        ROW_NUMBER() OVER (
            PARTITION BY k.transaction_id
            ORDER BY k.product_id
        ) AS order_item_id,
        CAST(k.product_id AS STRING) AS product_id,
        CAST(COALESCE(NULLIF(k.quantity, 0), 1) AS INT) AS quantity,
        CAST(
            CASE
                WHEN k.quantity IS NOT NULL AND k.quantity <> 0 THEN k.sale_amount / k.quantity
                ELSE NULL
            END AS DECIMAL(38, 10)
        ) AS unit_price,
        CAST(k.sale_amount AS DECIMAL(38, 10)) AS line_amount
    FROM kept k
    """
)
order_items_silver_df.createOrReplaceTempView("order_items_silver")

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_items_silver.csv")
)

# ====================================================================================
# 3) products_silver
#    Source: bronze.products_bronze (S3)
#    De-dup rule: keep latest record per product_id (using row_number; no timestamp provided,
#                 so keep one record deterministically)
# ====================================================================================
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
                ORDER BY pb.product_id
            ) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    )
    SELECT
        CAST(pb.product_id AS STRING) AS product_id,
        TRIM(CAST(pb.product_name AS STRING)) AS product_name,
        TRIM(CAST(pb.category AS STRING)) AS product_category,
        CAST(
            CASE
                WHEN pb.is_active = true THEN 'ACTIVE'
                WHEN pb.is_active = false THEN 'INACTIVE'
                ELSE 'UNKNOWN'
            END AS STRING
        ) AS product_status
    FROM ranked pb
    WHERE pb.rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ====================================================================================
# 4) daily_order_summary_silver
#    Source: silver.orders_silver (temp view created above)
#    Aggregations by order_date; excludes NULL order_date
# ====================================================================================
daily_order_summary_silver_df = spark.sql(
    """
    SELECT
        os.order_date AS order_date,
        CAST(COUNT(DISTINCT os.order_id) AS INT) AS orders_count,
        CAST(COUNT(DISTINCT os.customer_id) AS INT) AS unique_customers_count,
        CAST(SUM(os.order_total_amount) AS DECIMAL(38, 10)) AS gross_sales_amount
    FROM orders_silver os
    WHERE os.order_date IS NOT NULL
    GROUP BY os.order_date
    """
)
daily_order_summary_silver_df.createOrReplaceTempView("daily_order_summary_silver")

(
    daily_order_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_order_summary_silver.csv")
)

job.commit()
```