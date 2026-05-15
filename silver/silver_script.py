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

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")
store_details_bronze_df.createOrReplaceTempView("store_details_bronze")

# ------------------------------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# - enforce not null: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time
# - cast types
# - ensure quantity > 0 and sale_amount >= 0
# - de-duplicate by transaction_id keeping latest transaction_time
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
        CAST(TRIM(stb.store_id) AS STRING) AS store_id,
        CAST(TRIM(stb.product_id) AS STRING) AS product_id,
        CAST(TRIM(stb.quantity) AS INT) AS quantity,
        CAST(TRIM(stb.sale_amount) AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(TRIM(stb.transaction_id) AS STRING)
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE
        stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.quantity IS NOT NULL
        AND stb.sale_amount IS NOT NULL
        AND stb.transaction_time IS NOT NULL
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      quantity,
      sale_amount,
      transaction_time
    FROM base
    WHERE
      rn = 1
      AND quantity > 0
      AND sale_amount >= 0
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: silver.product_master_silver
# - enforce not null: product_id, product_name, category, price
# - trim and standardize text fields
# - price > 0
# - filter to active products (is_active = true)
# - de-duplicate by product_id keeping latest record (requires ordering column; not present in UDT)
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pdb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pdb.product_name) AS STRING) AS product_name,
        CAST(UPPER(TRIM(pdb.category)) AS STRING) AS category,
        CAST(TRIM(pdb.price) AS FLOAT) AS price
      FROM product_details_bronze pdb
      WHERE
        pdb.product_id IS NOT NULL
        AND pdb.product_name IS NOT NULL
        AND pdb.category IS NOT NULL
        AND pdb.price IS NOT NULL
        AND pdb.is_active = true
    ),
    ranked AS (
      SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NULL THEN 1 ELSE 0 END ASC,
            CASE WHEN category IS NULL THEN 1 ELSE 0 END ASC,
            CASE WHEN price IS NULL THEN 1 ELSE 0 END ASC
        ) AS rn
      FROM base
      WHERE price > 0
    )
    SELECT
      product_id,
      product_name,
      category,
      price
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: silver.store_master_silver
# - enforce not null: store_id, store_name, store_type
# - trim and standardize text fields
# - derive region from state (region = state) (region not in UDT output columns; use only provided columns)
# - validate open_date (open_date not provided in UDT; cannot apply)
# - de-duplicate by store_id keeping latest record (requires ordering column; not present in UDT)
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sdb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sdb.store_name) AS STRING) AS store_name,
        CAST(UPPER(TRIM(sdb.state)) AS STRING) AS state,
        CAST(UPPER(TRIM(sdb.store_type)) AS STRING) AS store_type
      FROM store_details_bronze sdb
      WHERE
        sdb.store_id IS NOT NULL
        AND sdb.store_name IS NOT NULL
        AND sdb.store_type IS NOT NULL
        AND sdb.state IS NOT NULL
    ),
    ranked AS (
      SELECT
        store_id,
        store_name,
        state,
        store_type,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NULL THEN 1 ELSE 0 END ASC,
            CASE WHEN state IS NULL THEN 1 ELSE 0 END ASC,
            CASE WHEN store_type IS NULL THEN 1 ELSE 0 END ASC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      state,
      store_type
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

job.commit()