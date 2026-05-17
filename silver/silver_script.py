import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables (Bronze)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)

store_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")
store_details_bronze_df.createOrReplaceTempView("store_details_bronze")

# ============================================================
# TABLE: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
    SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(stb.product_id AS STRING) AS product_id,
        CAST(stb.store_id AS STRING) AS store_id,
        CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sale_date,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(stb.transaction_id AS STRING),
                CAST(stb.product_id AS STRING),
                CAST(stb.store_id AS STRING),
                CAST(stb.transaction_time AS TIMESTAMP)
            ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
    FROM sales_transactions_bronze stb
)
SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount
FROM base
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# TABLE: silver.product_master_silver
# ============================================================
product_master_silver_sql = """
WITH base AS (
    SELECT
        CAST(pdb.product_id AS STRING) AS product_id,
        CAST(pdb.product_name AS STRING) AS product_name,
        CAST(pdb.category AS STRING) AS category,
        CAST(pdb.price AS FLOAT) AS price,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(pdb.product_id AS STRING)
            ORDER BY CAST(pdb.product_id AS STRING) DESC
        ) AS rn
    FROM product_details_bronze pdb
)
SELECT
    product_id,
    product_name,
    category,
    price
FROM base
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# TABLE: silver.store_master_silver
# ============================================================
store_master_silver_sql = """
WITH base AS (
    SELECT
        CAST(sdb.store_id AS STRING) AS store_id,
        CAST(sdb.store_name AS STRING) AS store_name,
        CAST(sdb.state AS STRING) AS region,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(sdb.store_id AS STRING)
            ORDER BY CAST(sdb.store_id AS STRING) DESC
        ) AS rn
    FROM store_details_bronze sdb
)
SELECT
    store_id,
    store_name,
    region
FROM base
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# TABLE: silver.sales_aggregates_silver
# ============================================================
sales_aggregates_silver_sql = """
SELECT
    CAST(sts.sale_date AS DATE) AS aggregation_date,
    CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS total_sales_amount,
    CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
    CAST(SUM(sts.total_sales_amount) / COUNT(DISTINCT sts.transaction_id) AS DOUBLE) AS average_sales_per_transaction,
    CAST(sms.region AS STRING) AS region,
    CAST(pms.category AS STRING) AS category
FROM sales_transactions_silver sts
INNER JOIN product_master_silver pms
    ON sts.product_id = pms.product_id
INNER JOIN store_master_silver sms
    ON sts.store_id = sms.store_id
GROUP BY
    sts.sale_date,
    sms.region,
    pms.category
"""

sales_aggregates_silver_df = spark.sql(sales_aggregates_silver_sql)

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_silver.csv")
)

job.commit()