import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ----------------------------
# 1) READ SOURCE TABLES (S3)
# ----------------------------

orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

order_items_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_items_bronze.{FILE_FORMAT}/")
)
order_items_bronze_df.createOrReplaceTempView("order_items_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# ----------------------------
# 2) TRANSFORM + DEDUP: orders_silver
# ----------------------------

orders_silver_df = spark.sql(
    """
    WITH staged AS (
        SELECT
            TRIM(ob.order_id)                                           AS order_id,
            ob.order_time                                               AS order_timestamp,
            CAST(ob.order_time AS DATE)                                 AS order_date,
            CAST(COALESCE(ob.order_total_amount, 0) AS DECIMAL(18,2))    AS order_total_amount,

            CAST(NULL AS STRING)                                        AS customer_id,
            CAST(NULL AS STRING)                                        AS currency_code,
            CAST(NULL AS STRING)                                        AS payment_method,
            CAST(NULL AS STRING)                                        AS shipping_address_1,
            CAST(NULL AS STRING)                                        AS shipping_address_2,
            CAST(NULL AS STRING)                                        AS shipping_city,
            CAST(NULL AS STRING)                                        AS shipping_state,
            CAST(NULL AS STRING)                                        AS shipping_postal_code,
            CAST(NULL AS STRING)                                        AS shipping_country,
            CAST(NULL AS STRING)                                        AS order_status,

            ob.source_file_date                                         AS source_file_date,
            ob.ingested_at                                              AS ingested_at,
            ob.loaded_at                                                AS loaded_at,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(ob.order_id)
                ORDER BY ob.ingested_at DESC, ob.loaded_at DESC
            ) AS rn
        FROM orders_bronze ob
    )
    SELECT
        order_id,
        order_timestamp,
        order_date,
        order_total_amount,
        customer_id,
        currency_code,
        payment_method,
        shipping_address_1,
        shipping_address_2,
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        order_status,
        source_file_date,
        ingested_at,
        loaded_at
    FROM staged
    WHERE rn = 1
    """
)

(
    orders_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/orders_silver.csv")
)

# ----------------------------
# 3) TRANSFORM + DEDUP: order_items_silver
# ----------------------------

order_items_silver_df = spark.sql(
    """
    WITH staged AS (
        SELECT
            TRIM(oib.order_item_id)                                         AS order_item_id,
            TRIM(oib.order_id)                                              AS order_id,
            TRIM(oib.product_id)                                            AS product_id,
            CAST(COALESCE(oib.quantity, 1) AS INT)                          AS quantity,
            CAST(
                CASE
                    WHEN COALESCE(oib.quantity, 0) <> 0 THEN oib.line_amount / oib.quantity
                    ELSE NULL
                END
            AS DECIMAL(18,2))                                               AS unit_price,
            CAST(COALESCE(oib.line_amount, 0) AS DECIMAL(18,2))             AS line_amount,

            oib.source_file_date                                            AS source_file_date,
            oib.ingested_at                                                 AS ingested_at,
            oib.loaded_at                                                   AS loaded_at,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(oib.order_item_id)
                ORDER BY oib.ingested_at DESC, oib.loaded_at DESC
            ) AS rn
        FROM order_items_bronze oib
    )
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_amount,
        source_file_date,
        ingested_at,
        loaded_at
    FROM staged
    WHERE rn = 1
    """
)

(
    order_items_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/order_items_silver.csv")
)

# ----------------------------
# 4) TRANSFORM + DEDUP: products_silver
# ----------------------------

products_silver_df = spark.sql(
    """
    WITH staged AS (
        SELECT
            TRIM(pb.product_id)                                AS product_id,
            NULLIF(TRIM(pb.product_name), '')                  AS product_name,
            NULLIF(TRIM(pb.category), '')                      AS product_category,
            pb.is_active                                       AS is_active,

            pb.source_file_date                                AS source_file_date,
            pb.ingested_at                                     AS ingested_at,
            pb.loaded_at                                       AS loaded_at,

            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY pb.ingested_at DESC, pb.loaded_at DESC
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        product_category,
        is_active,
        source_file_date,
        ingested_at,
        loaded_at
    FROM staged
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# ----------------------------
# 5) DERIVE: customers_silver (from orders_bronze)
# ----------------------------

customers_silver_df = spark.sql(
    """
    SELECT
        MD5(TRIM(ob.store_id))                     AS customer_id,
        CAST(NULL AS STRING)                       AS customer_name,
        CAST(NULL AS STRING)                       AS customer_email,
        CAST(NULL AS STRING)                       AS customer_phone,
        MIN(CAST(ob.order_time AS DATE))           AS customer_created_date
    FROM orders_bronze ob
    GROUP BY MD5(TRIM(ob.store_id))
    """
)

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/customers_silver.csv")
)

job.commit()
