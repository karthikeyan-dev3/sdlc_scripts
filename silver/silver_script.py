```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark init
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ===================================================================================
# 1) SOURCE READS (S3) + TEMP VIEWS
# ===================================================================================

# bronze.bronze_products_raw (bpr)
bpr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_products_raw.{FILE_FORMAT}/")
)
bpr_df.createOrReplaceTempView("bpr")

# bronze.bronze_stores_raw (bsr)
bsr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_stores_raw.{FILE_FORMAT}/")
)
bsr_df.createOrReplaceTempView("bsr")

# bronze.bronze_sales_transactions_raw (bstr)
bstr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/bronze_sales_transactions_raw.{FILE_FORMAT}/")
)
bstr_df.createOrReplaceTempView("bstr")

# ===================================================================================
# 2) TARGET: silver.silver_dim_product
#    - Trim/case normalize text
#    - Enforce non-null product_id
#    - Cast to target types
#    - Dedup by product_id via ROW_NUMBER (latest record retained)
# ===================================================================================

silver_dim_product_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(bpr.product_id) AS STRING)                                              AS product_id,
            CAST(NULLIF(TRIM(bpr.product_name), '') AS STRING)                                 AS product_name,
            CAST(NULLIF(TRIM(bpr.category), '') AS STRING)                                     AS category,
            CAST(NULLIF(TRIM(bpr.brand), '') AS STRING)                                        AS brand,
            CAST(bpr.price AS DECIMAL(38,10))                                                  AS price,
            CAST(bpr.is_active AS BOOLEAN)                                                     AS is_active,

            ROW_NUMBER() OVER (
                PARTITION BY CAST(TRIM(bpr.product_id) AS STRING)
                ORDER BY
                    CAST(TRIM(bpr.product_id) AS STRING) DESC
            )                                                                                  AS rn
        FROM bpr
        WHERE COALESCE(TRIM(bpr.product_id), '') <> ''
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        is_active
    FROM base
    WHERE rn = 1
    """
)

silver_dim_product_output = f"{TARGET_PATH}/silver_dim_product.csv"
(
    silver_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(silver_dim_product_output)
)

# Create temp view for downstream join (as per mapping_details)
silver_dim_product_df.createOrReplaceTempView("sdp")

# ===================================================================================
# 3) TARGET: silver.silver_dim_store
#    - Trim/case normalize text
#    - Enforce non-null store_id
#    - Cast to target types
#    - Dedup by store_id via ROW_NUMBER (latest record retained)
# ===================================================================================

silver_dim_store_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(bsr.store_id) AS STRING)                                                 AS store_id,
            CAST(NULLIF(TRIM(bsr.store_name), '') AS STRING)                                    AS store_name,
            CAST(NULLIF(TRIM(bsr.city), '') AS STRING)                                          AS city,
            CAST(NULLIF(TRIM(bsr.state), '') AS STRING)                                         AS state,
            CAST(NULLIF(TRIM(bsr.store_type), '') AS STRING)                                    AS store_type,
            CAST(DATE(bsr.open_date) AS DATE)                                                   AS open_date,

            ROW_NUMBER() OVER (
                PARTITION BY CAST(TRIM(bsr.store_id) AS STRING)
                ORDER BY
                    CAST(TRIM(bsr.store_id) AS STRING) DESC
            )                                                                                   AS rn
        FROM bsr
        WHERE COALESCE(TRIM(bsr.store_id), '') <> ''
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date
    FROM base
    WHERE rn = 1
    """
)

silver_dim_store_output = f"{TARGET_PATH}/silver_dim_store.csv"
(
    silver_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(silver_dim_store_output)
)

# Create temp view for downstream join (as per mapping_details)
silver_dim_store_df.createOrReplaceTempView("sds")

# ===================================================================================
# 4) TARGET: silver.silver_fact_sales_transaction
#    - Enforce non-null transaction_id/store_id/product_id
#    - Cast numeric/timestamp fields
#    - Derive transaction_date from transaction_time
#    - Dedup by transaction_id (keep latest by transaction_time)
#    - Keep only rows with valid store_id and product_id via joins to silver dims
# ===================================================================================

silver_fact_sales_transaction_df = spark.sql(
    """
    WITH typed AS (
        SELECT
            CAST(TRIM(bstr.transaction_id) AS STRING)                                           AS transaction_id,
            CAST(bstr.transaction_time AS TIMESTAMP)                                            AS transaction_time,
            CAST(DATE(bstr.transaction_time) AS DATE)                                           AS transaction_date,
            CAST(TRIM(bstr.store_id) AS STRING)                                                 AS store_id,
            CAST(TRIM(bstr.product_id) AS STRING)                                               AS product_id,
            CAST(bstr.quantity AS INT)                                                          AS quantity,
            CAST(bstr.sale_amount AS DECIMAL(38,10))                                            AS sale_amount
        FROM bstr
        WHERE COALESCE(TRIM(bstr.transaction_id), '') <> ''
          AND COALESCE(TRIM(bstr.store_id), '') <> ''
          AND COALESCE(TRIM(bstr.product_id), '') <> ''
    ),
    joined AS (
        SELECT
            t.transaction_id,
            t.transaction_time,
            t.transaction_date,
            t.store_id,
            t.product_id,
            t.quantity,
            t.sale_amount,

            ROW_NUMBER() OVER (
                PARTITION BY t.transaction_id
                ORDER BY t.transaction_time DESC
            )                                                                                   AS rn
        FROM typed t
        INNER JOIN sds
            ON t.store_id = sds.store_id
        INNER JOIN sdp
            ON t.product_id = sdp.product_id
    )
    SELECT
        transaction_id,
        transaction_time,
        transaction_date,
        store_id,
        product_id,
        quantity,
        sale_amount
    FROM joined
    WHERE rn = 1
    """
)

silver_fact_sales_transaction_output = f"{TARGET_PATH}/silver_fact_sales_transaction.csv"
(
    silver_fact_sales_transaction_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(silver_fact_sales_transaction_output)
)

job.commit()
```