import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------------------
# Glue / Spark Session
# ---------------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("silver_layer_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ---------------------------------------------------------------------------------------
# SOURCE: bronze.store_details_bronze
# ---------------------------------------------------------------------------------------
store_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_details_bronze.{FILE_FORMAT}/")
)
store_details_bronze_df.createOrReplaceTempView("store_details_bronze")

# ---------------------------------------------------------------------------------------
# TARGET: silver.store_master_silver
# - TRIM/UPPER store_id
# - region derived from state mapping
# - enforce non-null store_id
# - dedup by store_id (no ordering column provided in UDT -> deterministic pick)
# ---------------------------------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
    SELECT
        TRIM(UPPER(sdb.store_id)) AS store_id,
        sdb.store_name AS store_name,
        CASE
            WHEN sdb.state IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'Northeast'
            WHEN sdb.state IN ('OH','MI','IN','IL','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'Midwest'
            WHEN sdb.state IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'South'
            WHEN sdb.state IN ('MT','ID','WY','CO','NM','AZ','UT','NV','WA','OR','CA','AK','HI') THEN 'West'
            ELSE 'Unknown'
        END AS region
    FROM store_details_bronze sdb
    WHERE TRIM(UPPER(sdb.store_id)) IS NOT NULL
      AND TRIM(UPPER(sdb.store_id)) <> ''
),
dedup AS (
    SELECT
        store_id,
        store_name,
        region,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    region
FROM dedup
WHERE rn = 1
"""
store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_master_silver.csv")
)

# ---------------------------------------------------------------------------------------
# SOURCE: bronze.product_details_bronze
# ---------------------------------------------------------------------------------------
product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")

# ---------------------------------------------------------------------------------------
# TARGET: silver.product_master_silver
# - TRIM/UPPER product_id
# - keep only active products where is_active = true (column referenced by table description)
# - dedup by product_id (prefer active/latest not possible without specified ordering cols in UDT)
# ---------------------------------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
    SELECT
        TRIM(UPPER(pdb.product_id)) AS product_id,
        pdb.product_name AS product_name,
        pdb.category AS category
    FROM product_details_bronze pdb
    WHERE TRIM(UPPER(pdb.product_id)) IS NOT NULL
      AND TRIM(UPPER(pdb.product_id)) <> ''
      AND COALESCE(pdb.is_active, false) = true
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    category
FROM dedup
WHERE rn = 1
"""
product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_master_silver.csv")
)

# ---------------------------------------------------------------------------------------
# SOURCE: bronze.sales_transactions_bronze
# ---------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ---------------------------------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# - TRIM/UPPER store_id/product_id
# - sales_date = DATE(transaction_time)
# - units_sold = quantity; sales_revenue = sale_amount
# - enforce positive quantity and non-negative sale_amount (per description)
# - dedup on transaction_id keep latest transaction_time
# ---------------------------------------------------------------------------------------
sales_transactions_silver_sql = """
WITH base AS (
    SELECT
        stb.transaction_id AS transaction_id,
        TRIM(UPPER(stb.store_id)) AS store_id,
        TRIM(UPPER(stb.product_id)) AS product_id,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts,
        DATE(stb.transaction_time) AS sales_date,
        CAST(stb.quantity AS INT) AS units_sold,
        CAST(stb.sale_amount AS DOUBLE) AS sales_revenue
    FROM sales_transactions_bronze stb
    WHERE stb.transaction_id IS NOT NULL
      AND stb.transaction_id <> ''
      AND TRIM(UPPER(stb.store_id)) IS NOT NULL
      AND TRIM(UPPER(stb.store_id)) <> ''
      AND TRIM(UPPER(stb.product_id)) IS NOT NULL
      AND TRIM(UPPER(stb.product_id)) <> ''
      AND CAST(stb.quantity AS INT) > 0
      AND CAST(stb.sale_amount AS DOUBLE) >= 0
),
dedup AS (
    SELECT
        transaction_id,
        store_id,
        product_id,
        sales_date,
        units_sold,
        sales_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time_ts DESC
        ) AS rn
    FROM base
)
SELECT
    transaction_id,
    store_id,
    product_id,
    sales_date,
    units_sold,
    sales_revenue
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ---------------------------------------------------------------------------------------
# TARGET: silver.sales_daily_store_product_silver
# - inner join conformed keys
# - group and aggregate
# ---------------------------------------------------------------------------------------
sales_daily_store_product_silver_sql = """
SELECT
    sts.store_id AS store_id,
    sms.store_name AS store_name,
    sms.region AS region,
    sts.product_id AS product_id,
    pms.product_name AS product_name,
    pms.category AS category,
    sts.sales_date AS sales_date,
    SUM(sts.units_sold) AS units_sold,
    SUM(sts.sales_revenue) AS sales_revenue
FROM sales_transactions_silver sts
INNER JOIN store_master_silver sms
    ON sts.store_id = sms.store_id
INNER JOIN product_master_silver pms
    ON sts.product_id = pms.product_id
GROUP BY
    sts.store_id,
    sms.store_name,
    sms.region,
    sts.product_id,
    pms.product_name,
    pms.category,
    sts.sales_date
"""
sales_daily_store_product_silver_df = spark.sql(sales_daily_store_product_silver_sql)
sales_daily_store_product_silver_df.createOrReplaceTempView("sales_daily_store_product_silver")

(
    sales_daily_store_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_daily_store_product_silver.csv")
)

job.commit()
