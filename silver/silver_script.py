```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# TABLE: customer_orders_silver
# Sources: bronze.customer_orders_bronze (cob)
# -----------------------------------------------------------------------------------
customer_orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_bronze.{FILE_FORMAT}/")
)
customer_orders_bronze_df.createOrReplaceTempView("customer_orders_bronze")

customer_orders_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            cob.transaction_id                                       AS order_id,
            CAST(cob.transaction_time AS DATE)                       AS order_date,
            cob.store_id                                             AS customer_id,
            CASE WHEN cob.transaction_id IS NOT NULL
                THEN 'COMPLETED'
                ELSE 'UNKNOWN'
            END                                                      AS order_status,
            'USD'                                                    AS currency_code,
            SUM(
                CASE
                    WHEN COALESCE(cob.sale_amount, 0) < 0 THEN 0
                    ELSE COALESCE(cob.sale_amount, 0)
                END
            ) OVER (PARTITION BY cob.transaction_id)                  AS order_total_amount,
            CURRENT_DATE                                             AS ingestion_date,
            cob.transaction_time                                     AS transaction_time
        FROM customer_orders_bronze cob
        WHERE cob.transaction_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            order_id,
            order_date,
            customer_id,
            order_status,
            currency_code,
            order_total_amount,
            ingestion_date,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        currency_code,
        order_total_amount,
        ingestion_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    customer_orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_orders_silver.csv")
)

# -----------------------------------------------------------------------------------
# TABLE: customer_order_items_silver
# Sources: bronze.customer_order_items_bronze (coib)
# -----------------------------------------------------------------------------------
customer_order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_items_bronze.{FILE_FORMAT}/")
)
customer_order_items_bronze_df.createOrReplaceTempView("customer_order_items_bronze")

customer_order_items_silver_df = spark.sql(
    """
    WITH cleaned AS (
        SELECT
            coib.transaction_id AS order_id,
            TRIM(coib.product_id) AS product_id,
            CASE
                WHEN coib.quantity IS NULL OR coib.quantity <= 0 THEN 1
                ELSE coib.quantity
            END AS quantity,
            coib.sale_amount AS sale_amount
        FROM customer_order_items_bronze coib
        WHERE coib.transaction_id IS NOT NULL
          AND TRIM(coib.product_id) IS NOT NULL
          AND TRIM(coib.product_id) <> ''
    ),
    dedup_lines AS (
        SELECT
            order_id,
            product_id,
            quantity,
            sale_amount
        FROM (
            SELECT
                order_id,
                product_id,
                quantity,
                sale_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id, product_id, quantity, sale_amount
                    ORDER BY product_id
                ) AS rn
            FROM cleaned
        ) x
        WHERE rn = 1
    )
    SELECT
        order_id,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY product_id, sale_amount, quantity
        ) AS order_item_id,
        product_id,
        quantity,
        CASE
            WHEN sale_amount IS NULL THEN 0
            ELSE sale_amount / NULLIF(quantity, 0)
        END AS unit_price,
        COALESCE(sale_amount, 0) AS line_amount
    FROM dedup_lines
    """
)

(
    customer_order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customer_order_items_silver.csv")
)

# -----------------------------------------------------------------------------------
# TABLE: order_load_quality_silver
# Sources: bronze.order_load_quality_bronze (olqb)
# -----------------------------------------------------------------------------------
order_load_quality_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_load_quality_bronze.{FILE_FORMAT}/")
)
order_load_quality_bronze_df.createOrReplaceTempView("order_load_quality_bronze")

order_load_quality_silver_df = spark.sql(
    """
    SELECT
        CURRENT_DATE AS ingestion_date,
        COUNT(1) AS total_records,
        SUM(
            CASE
                WHEN NULLIF(TRIM(olqb.transaction_id), '') IS NOT NULL
                 AND olqb.transaction_time IS NOT NULL
                 AND olqb.sale_amount IS NOT NULL
                 AND olqb.quantity IS NOT NULL
                 AND olqb.quantity > 0
                 AND olqb.sale_amount >= 0
                THEN 1 ELSE 0
            END
        ) AS valid_records,
        COUNT(1) - SUM(
            CASE
                WHEN NULLIF(TRIM(olqb.transaction_id), '') IS NOT NULL
                 AND olqb.transaction_time IS NOT NULL
                 AND olqb.sale_amount IS NOT NULL
                 AND olqb.quantity IS NOT NULL
                 AND olqb.quantity > 0
                 AND olqb.sale_amount >= 0
                THEN 1 ELSE 0
            END
        ) AS invalid_records,
        CASE
            WHEN COUNT(1) = 0 THEN 0
            ELSE ROUND(
                (
                    (COUNT(1) - SUM(
                        CASE
                            WHEN NULLIF(TRIM(olqb.transaction_id), '') IS NOT NULL
                             AND olqb.transaction_time IS NOT NULL
                             AND olqb.sale_amount IS NOT NULL
                             AND olqb.quantity IS NOT NULL
                             AND olqb.quantity > 0
                             AND olqb.sale_amount >= 0
                            THEN 1 ELSE 0
                        END
                    )) * 100.0
                ) / COUNT(1)
            , 2)
        END AS error_rate_pct
    FROM order_load_quality_bronze olqb
    """
)

(
    order_load_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/order_load_quality_silver.csv")
)

job.commit()
```