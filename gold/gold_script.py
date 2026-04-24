```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
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

# -----------------------------------------------------------------------------------
# TABLE: gold.gold_customer_orders
# 1) Read source tables from S3
# 2) Create temp views
# 3) Transform using Spark SQL
# 4) Write output as a SINGLE CSV file directly under TARGET_PATH
# -----------------------------------------------------------------------------------

# 1) Read source tables
df_so = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_orders.{FILE_FORMAT}/")
)
df_soc = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_order_customers.{FILE_FORMAT}/")
)
df_sof = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_order_financials.{FILE_FORMAT}/")
)
df_sop = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_order_payments.{FILE_FORMAT}/")
)
df_sosa = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_order_shipping_addresses.{FILE_FORMAT}/")
)

# 2) Create temp views
df_so.createOrReplaceTempView("silver_orders")
df_soc.createOrReplaceTempView("silver_order_customers")
df_sof.createOrReplaceTempView("silver_order_financials")
df_sop.createOrReplaceTempView("silver_order_payments")
df_sosa.createOrReplaceTempView("silver_order_shipping_addresses")

# 3) Transform using Spark SQL (includes ROW_NUMBER dedup per contributing table)
gold_customer_orders_df = spark.sql(
    """
    WITH
    so_dedup AS (
      SELECT
        *
      FROM (
        SELECT
          so.*,
          ROW_NUMBER() OVER (
            PARTITION BY so.order_id
            ORDER BY so.order_timestamp DESC
          ) AS rn
        FROM silver_orders so
      ) x
      WHERE x.rn = 1
    ),
    soc_dedup AS (
      SELECT
        *
      FROM (
        SELECT
          soc.*,
          ROW_NUMBER() OVER (
            PARTITION BY soc.order_id
            ORDER BY soc.order_id
          ) AS rn
        FROM silver_order_customers soc
      ) x
      WHERE x.rn = 1
    ),
    sof_dedup AS (
      SELECT
        *
      FROM (
        SELECT
          sof.*,
          ROW_NUMBER() OVER (
            PARTITION BY sof.order_id
            ORDER BY sof.order_id
          ) AS rn
        FROM silver_order_financials sof
      ) x
      WHERE x.rn = 1
    ),
    sop_dedup AS (
      SELECT
        *
      FROM (
        SELECT
          sop.*,
          ROW_NUMBER() OVER (
            PARTITION BY sop.order_id
            ORDER BY sop.order_id
          ) AS rn
        FROM silver_order_payments sop
      ) x
      WHERE x.rn = 1
    ),
    sosa_dedup AS (
      SELECT
        *
      FROM (
        SELECT
          sosa.*,
          ROW_NUMBER() OVER (
            PARTITION BY sosa.order_id
            ORDER BY sosa.order_id
          ) AS rn
        FROM silver_order_shipping_addresses sosa
      ) x
      WHERE x.rn = 1
    )
    SELECT
      -- EXACT mappings per UDT
      CAST(so.order_id AS STRING)                                  AS order_id,
      CAST(so.order_date AS DATE)                                  AS order_date,
      CAST(so.order_timestamp AS TIMESTAMP)                        AS order_timestamp,
      CAST(soc.customer_id AS STRING)                              AS customer_id,
      CAST(sof.currency_code AS STRING)                            AS currency_code,
      CAST(sof.order_subtotal_amount AS DECIMAL(38, 10))           AS order_subtotal_amount,
      CAST(sof.order_tax_amount AS DECIMAL(38, 10))                AS order_tax_amount,
      CAST(sof.order_shipping_amount AS DECIMAL(38, 10))           AS order_shipping_amount,
      CAST(sof.order_discount_amount AS DECIMAL(38, 10))           AS order_discount_amount,
      CAST(sof.order_total_amount AS DECIMAL(38, 10))              AS order_total_amount,
      CAST(sop.payment_method AS STRING)                           AS payment_method,
      CAST(sosa.shipping_country AS STRING)                        AS shipping_country,
      CAST(sosa.shipping_state AS STRING)                          AS shipping_state,
      CAST(sosa.shipping_city AS STRING)                           AS shipping_city,
      CAST(so.items_count AS INT)                                  AS items_count
    FROM so_dedup so
    LEFT JOIN soc_dedup soc
      ON so.order_id = soc.order_id
    LEFT JOIN sof_dedup sof
      ON so.order_id = sof.order_id
    LEFT JOIN sop_dedup sop
      ON so.order_id = sop.order_id
    LEFT JOIN sosa_dedup sosa
      ON so.order_id = sosa.order_id
    """
)

# 4) Write output as SINGLE CSV file directly under TARGET_PATH
#    CRITICAL: path format must be TARGET_PATH + "/" + target_table + ".csv"
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders.csv")
)

job.commit()
```