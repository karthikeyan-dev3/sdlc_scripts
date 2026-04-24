```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue boilerplate
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

# -----------------------------------------------------------------------------------
# Read source tables from S3 (Bronze) + create temp views
# SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
orders_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_bronze.{FILE_FORMAT}/")
)
orders_bronze_df.createOrReplaceTempView("orders_bronze")

source_systems_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/source_systems_bronze.{FILE_FORMAT}/")
)
source_systems_bronze_df.createOrReplaceTempView("source_systems_bronze")

order_lines_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_lines_bronze.{FILE_FORMAT}/")
)
order_lines_bronze_df.createOrReplaceTempView("order_lines_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# ===================================================================================
# Target: silver.customer_order_silver
# 1) Read source tables (done above)
# 2) Create temp views (done above)
# 3) Transform using Spark SQL (apply EXACT UDT transformations + dedup)
# 4) Save output as SINGLE CSV directly under TARGET_PATH
# ===================================================================================

customer_order_silver_df = spark.sql(
    """
    WITH staged AS (
        SELECT
            -- UDT column transformations
            ob.order_id                                               AS order_id,
            CAST(ob.transaction_time AS DATE)                         AS order_date,
            ob.store_id                                               AS customer_id,
            'UNKNOWN'                                                 AS order_status,
            ob.sale_amount                                            AS order_total_amount,
            'USD'                                                     AS currency_code,
            COALESCE(ssb.store_name, ob.store_id)                     AS source_system,
            CURRENT_DATE                                              AS ingestion_date,

            -- Dedup (order grain)
            ROW_NUMBER() OVER (
                PARTITION BY ob.order_id
                ORDER BY ob.transaction_time DESC
            )                                                         AS rn
        FROM orders_bronze ob
        LEFT JOIN source_systems_bronze ssb
            ON ob.store_id = ssb.source_system_key
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        order_status,
        order_total_amount,
        currency_code,
        source_system,
        ingestion_date
    FROM staged
    WHERE rn = 1
    """
)

customer_order_silver_output_path = f"{TARGET_PATH}/customer_order_silver.csv"
(
    customer_order_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(customer_order_silver_output_path)
)

# ===================================================================================
# Target: silver.customer_order_line_silver
# 1) Read source tables (done above)
# 2) Create temp views (done above)
# 3) Transform using Spark SQL (apply EXACT UDT transformations + dedup)
# 4) Save output as SINGLE CSV directly under TARGET_PATH
# ===================================================================================

customer_order_line_silver_df = spark.sql(
    """
    WITH staged AS (
        SELECT
            -- UDT column transformations
            olb.order_id                                                                 AS order_id,
            ROW_NUMBER() OVER (
                PARTITION BY olb.order_id
                ORDER BY olb.product_id, olb.sale_amount, olb.quantity
            )                                                                            AS order_line_id,
            COALESCE(pb.product_id, olb.product_id)                                      AS product_id,
            COALESCE(olb.quantity, 1)                                                    AS quantity,
            CASE
                WHEN COALESCE(olb.quantity, 1) > 0 THEN olb.sale_amount / COALESCE(olb.quantity, 1)
                ELSE NULL
            END                                                                          AS unit_price_amount,
            olb.sale_amount                                                              AS line_total_amount,
            'USD'                                                                        AS currency_code,
            CURRENT_DATE                                                                 AS ingestion_date,

            -- Dedup at (order_id, order_line_id) grain (stable order_line_id already)
            ROW_NUMBER() OVER (
                PARTITION BY olb.order_id,
                             ROW_NUMBER() OVER (PARTITION BY olb.order_id ORDER BY olb.product_id, olb.sale_amount, olb.quantity)
                ORDER BY olb.product_id, olb.sale_amount, olb.quantity
            )                                                                            AS rn
        FROM order_lines_bronze olb
        LEFT JOIN products_bronze pb
            ON olb.product_id = pb.product_id
    )
    SELECT
        order_id,
        CAST(order_line_id AS INT) AS order_line_id,
        product_id,
        CAST(quantity AS INT)      AS quantity,
        unit_price_amount,
        line_total_amount,
        currency_code,
        ingestion_date
    FROM staged
    WHERE rn = 1
    """
)

customer_order_line_silver_output_path = f"{TARGET_PATH}/customer_order_line_silver.csv"
(
    customer_order_line_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(customer_order_line_silver_output_path)
)

job.commit()
```