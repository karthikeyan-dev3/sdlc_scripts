```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------
# AWS Glue bootstrap
# --------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------------
# Parameters (as provided)
# --------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV options for consistent reads/writes
csv_read_options = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}

csv_write_options = {
    "header": "true",
}

# =================================================================================
# SOURCE READS (Silver -> Temp Views)
# =================================================================================

# --- silver.silver_dim_product ---
df_silver_dim_product = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/silver_dim_product.{FILE_FORMAT}/")
)
df_silver_dim_product.createOrReplaceTempView("silver_dim_product")

# --- silver.silver_dim_store ---
df_silver_dim_store = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/silver_dim_store.{FILE_FORMAT}/")
)
df_silver_dim_store.createOrReplaceTempView("silver_dim_store")

# --- silver.silver_fact_sales_transaction ---
df_silver_fact_sales_transaction = (
    spark.read.format(FILE_FORMAT)
    .options(**csv_read_options)
    .load(f"{SOURCE_PATH}/silver_fact_sales_transaction.{FILE_FORMAT}/")
)
df_silver_fact_sales_transaction.createOrReplaceTempView("silver_fact_sales_transaction")

# =================================================================================
# TARGET TABLE: gold.gold_dim_product
# Mapping: silver.silver_dim_product sdp
# =================================================================================
sql_gold_dim_product = """
SELECT
    CAST(sdp.product_id AS STRING)   AS product_id,
    CAST(sdp.product_name AS STRING) AS product_name,
    CAST(sdp.category AS STRING)     AS category,
    CAST(sdp.brand AS STRING)        AS brand,
    CAST(sdp.price AS DECIMAL(38, 18)) AS price,
    CAST(sdp.is_active AS BOOLEAN)   AS is_active
FROM silver_dim_product sdp
"""

df_gold_dim_product = spark.sql(sql_gold_dim_product)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    df_gold_dim_product.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# =================================================================================
# TARGET TABLE: gold.gold_dim_store
# Mapping: silver.silver_dim_store sds
# =================================================================================
sql_gold_dim_store = """
SELECT
    CAST(sds.store_id AS STRING)     AS store_id,
    CAST(sds.store_name AS STRING)   AS store_name,
    CAST(sds.city AS STRING)         AS city,
    CAST(sds.state AS STRING)        AS state,
    CAST(sds.store_type AS STRING)   AS store_type,
    CAST(sds.open_date AS DATE)      AS open_date
FROM silver_dim_store sds
"""

df_gold_dim_store = spark.sql(sql_gold_dim_store)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    df_gold_dim_store.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# =================================================================================
# TARGET TABLE: gold.gold_fact_sales_transaction
# Mapping:
#   silver.silver_fact_sales_transaction sfst
#   INNER JOIN silver.silver_dim_store sds   ON sfst.store_id = sds.store_id
#   INNER JOIN silver.silver_dim_product sdp ON sfst.product_id = sdp.product_id
# Note: Use ROW_NUMBER for dedup (transaction_id) - keep latest by transaction_time
# =================================================================================
sql_gold_fact_sales_transaction = """
WITH joined_base AS (
    SELECT
        sfst.transaction_id,
        sfst.transaction_time,
        sfst.transaction_date,
        sfst.store_id,
        sfst.product_id,
        sfst.quantity,
        sfst.sale_amount,
        ROW_NUMBER() OVER (
            PARTITION BY sfst.transaction_id
            ORDER BY sfst.transaction_time DESC
        ) AS rn
    FROM silver_fact_sales_transaction sfst
    INNER JOIN silver_dim_store sds
        ON sfst.store_id = sds.store_id
    INNER JOIN silver_dim_product sdp
        ON sfst.product_id = sdp.product_id
)
SELECT
    CAST(transaction_id AS STRING)      AS transaction_id,
    CAST(transaction_time AS TIMESTAMP) AS transaction_time,
    CAST(transaction_date AS DATE)      AS transaction_date,
    CAST(store_id AS STRING)            AS store_id,
    CAST(product_id AS STRING)          AS product_id,
    CAST(quantity AS INT)               AS quantity,
    CAST(sale_amount AS DECIMAL(38, 18)) AS sale_amount
FROM joined_base
WHERE rn = 1
"""

df_gold_fact_sales_transaction = spark.sql(sql_gold_fact_sales_transaction)

# Write as SINGLE CSV file directly under TARGET_PATH
(
    df_gold_fact_sales_transaction.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**csv_write_options)
    .save(f"{TARGET_PATH}/gold_fact_sales_transaction.csv")
)

job.commit()
```