```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Job Init
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ===================================================================================
# Target Table: gold.gold_customer_orders_daily
# Sources:
#  - silver.order_amounts_silver oas
#  - silver.orders_silver os
#  - silver.order_validation_silver ovs
#  - silver.order_lines_silver ols
# ===================================================================================

# -------------------------------------
# 1) Read source tables from S3
# -------------------------------------
oas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_amounts_silver.{FILE_FORMAT}/")
)

os_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/orders_silver.{FILE_FORMAT}/")
)

ovs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_validation_silver.{FILE_FORMAT}/")
)

ols_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_lines_silver.{FILE_FORMAT}/")
)

# -------------------------------------
# 2) Create temp views
# -------------------------------------
oas_df.createOrReplaceTempView("order_amounts_silver")
os_df.createOrReplaceTempView("orders_silver")
ovs_df.createOrReplaceTempView("order_validation_silver")
ols_df.createOrReplaceTempView("order_lines_silver")

# -------------------------------------
# 3) Transform using Spark SQL (EXACT UDT intent)
#    - One row per order_date
#    - Aggregations by oas.order_date
#    - avg_order_value = SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_id), 0)
#    - load_timestamp = MAX(GREATEST(...)) by order_date
# -------------------------------------
gold_customer_orders_daily_df = spark.sql(
    """
    SELECT
        CAST(oas.order_date AS DATE) AS order_date,

        CAST(COUNT(DISTINCT os.order_id) AS INT) AS total_orders,
        CAST(COUNT(ols.order_id) AS INT) AS total_order_lines,
        CAST(COALESCE(SUM(CAST(ols.quantity AS INT)), 0) AS INT) AS total_quantity,

        CAST(COALESCE(SUM(CAST(oas.gross_sales_amount AS DECIMAL(38, 10))), 0) AS DECIMAL(38, 10)) AS gross_sales_amount,
        CAST(COALESCE(SUM(CAST(oas.net_sales_amount   AS DECIMAL(38, 10))), 0) AS DECIMAL(38, 10)) AS net_sales_amount,
        CAST(COALESCE(SUM(CAST(oas.discount_amount    AS DECIMAL(38, 10))), 0) AS DECIMAL(38, 10)) AS discount_amount,
        CAST(COALESCE(SUM(CAST(oas.tax_amount         AS DECIMAL(38, 10))), 0) AS DECIMAL(38, 10)) AS tax_amount,
        CAST(COALESCE(SUM(CAST(oas.shipping_amount    AS DECIMAL(38, 10))), 0) AS DECIMAL(38, 10)) AS shipping_amount,

        CAST(
            COALESCE(SUM(CAST(oas.net_sales_amount AS DECIMAL(38, 10))), 0)
            /
            NULLIF(COUNT(DISTINCT os.order_id), 0)
            AS DECIMAL(38, 10)
        ) AS avg_order_value,

        CAST(
            COALESCE(SUM(CASE WHEN CAST(ovs.is_valid_order AS BOOLEAN) THEN 1 ELSE 0 END), 0)
            AS INT
        ) AS valid_orders_count,

        CAST(
            COALESCE(SUM(CASE WHEN CAST(ovs.is_valid_order AS BOOLEAN) THEN 0 ELSE 1 END), 0)
            AS INT
        ) AS invalid_orders_count,

        MAX(
            GREATEST(
                CAST(oas.load_timestamp AS TIMESTAMP),
                CAST(os.load_timestamp  AS TIMESTAMP),
                CAST(ovs.load_timestamp AS TIMESTAMP),
                CAST(ols.load_timestamp AS TIMESTAMP)
            )
        ) AS load_timestamp

    FROM order_amounts_silver oas
    INNER JOIN orders_silver os
        ON oas.order_id = os.order_id
    INNER JOIN order_validation_silver ovs
        ON oas.order_id = ovs.order_id
    LEFT JOIN order_lines_silver ols
        ON oas.order_id = ols.order_id
    GROUP BY CAST(oas.order_date AS DATE)
    """
)

# -------------------------------------
# 4) Save output (SINGLE CSV file directly under TARGET_PATH)
#    Output path MUST be: s3://.../gold_customer_orders_daily.csv
# -------------------------------------
(
    gold_customer_orders_daily_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_orders_daily.csv")
)

job.commit()
```