```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Ensure single CSV output file per target (no subfolders) via coalesce(1)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")

# ===================================================================================
# TARGET: silver.sales_transactions_clean_silver
# Source: bronze.sales_transactions_bronze stb
# ===================================================================================

sales_transactions_clean_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sales_date,
            CAST(stb.store_id AS STRING) AS store_id,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(
                CASE
                    WHEN stb.quantity IS NULL OR CAST(stb.quantity AS INT) < 0 THEN 0
                    ELSE CAST(stb.quantity AS INT)
                END AS INT
            ) AS quantity,
            CAST(
                CASE
                    WHEN stb.sale_amount IS NULL OR CAST(stb.sale_amount AS DECIMAL(18,2)) < 0 THEN 0
                    ELSE CAST(stb.sale_amount AS DECIMAL(18,2))
                END AS DECIMAL(18,2)
            ) AS sale_amount
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND TRIM(CAST(stb.transaction_id AS STRING)) <> ''
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        sales_date,
        transaction_time,
        store_id,
        product_id,
        quantity,
        sale_amount
    FROM dedup
    WHERE rn = 1
    """
)
sales_transactions_clean_silver_df.createOrReplaceTempView("sales_transactions_clean_silver")

(
    sales_transactions_clean_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_clean_silver.csv")
)

# ===================================================================================
# TARGET: silver.stores_conformed_silver
# Source: bronze.stores_bronze sb
# ===================================================================================

stores_conformed_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            CAST(sb.store_name AS STRING) AS store_name,
            UPPER(TRIM(CAST(sb.city AS STRING))) AS city,
            UPPER(TRIM(CAST(sb.state AS STRING))) AS state,
            CAST('UNKNOWN' AS STRING) AS country,
            CAST(sb.store_type AS STRING) AS store_type,
            CAST(sb.open_date AS DATE) AS open_date
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND TRIM(CAST(sb.store_id AS STRING)) <> ''
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY open_date DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        country,
        store_type,
        open_date
    FROM dedup
    WHERE rn = 1
    """
)
stores_conformed_silver_df.createOrReplaceTempView("stores_conformed_silver")

(
    stores_conformed_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_conformed_silver.csv")
)

# ===================================================================================
# TARGET: silver.products_conformed_silver
# Source: bronze.products_bronze pb
# ===================================================================================

products_conformed_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            TRIM(CAST(pb.product_name AS STRING)) AS product_name,
            TRIM(CAST(pb.brand AS STRING)) AS brand,
            TRIM(CAST(pb.category AS STRING)) AS category,
            CAST(pb.price AS DECIMAL(18,2)) AS price,
            CAST(pb.is_active AS BOOLEAN) AS is_active
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND TRIM(CAST(pb.product_id AS STRING)) <> ''
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_id DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        brand,
        category,
        price,
        is_active
    FROM dedup
    WHERE rn = 1
    """
)
products_conformed_silver_df.createOrReplaceTempView("products_conformed_silver")

(
    products_conformed_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_conformed_silver.csv")
)

# ===================================================================================
# TARGET: silver.product_category_standard_silver
# Source: bronze.products_bronze pb
# ===================================================================================

product_category_standard_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CASE
                WHEN pb.category IS NULL OR TRIM(CAST(pb.category AS STRING)) = '' THEN 'UNKNOWN'
                ELSE UPPER(TRIM(CAST(pb.category AS STRING)))
            END AS category_std
        FROM products_bronze pb
    ),
    dedup AS (
        SELECT
            category_std,
            ROW_NUMBER() OVER (
                PARTITION BY category_std
                ORDER BY category_std
            ) AS rn
        FROM base
    )
    SELECT
        category_std
    FROM dedup
    WHERE rn = 1
    """
)
product_category_standard_silver_df.createOrReplaceTempView("product_category_standard_silver")

(
    product_category_standard_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_category_standard_silver.csv")
)

# ===================================================================================
# TARGET: silver.sales_enriched_silver
# Source: silver.sales_transactions_clean_silver stcs
#         LEFT JOIN silver.stores_conformed_silver scs ON stcs.store_id = scs.store_id
#         LEFT JOIN silver.products_conformed_silver pcs ON stcs.product_id = pcs.product_id
# ===================================================================================

sales_enriched_silver_df = spark.sql(
    """
    SELECT
        stcs.transaction_id AS transaction_id,
        stcs.sales_date AS sales_date,
        stcs.transaction_time AS transaction_time,
        stcs.store_id AS store_id,
        scs.store_name AS store_name,
        scs.city AS city,
        scs.state AS state,
        scs.country AS country,
        scs.store_type AS store_type,
        stcs.product_id AS product_id,
        pcs.product_name AS product_name,
        pcs.brand AS brand,
        CASE
            WHEN pcs.category IS NULL OR TRIM(pcs.category) = '' THEN 'UNKNOWN'
            ELSE UPPER(TRIM(pcs.category))
        END AS category_std,
        stcs.quantity AS quantity,
        stcs.sale_amount AS sale_amount
    FROM sales_transactions_clean_silver stcs
    LEFT JOIN stores_conformed_silver scs
        ON stcs.store_id = scs.store_id
    LEFT JOIN products_conformed_silver pcs
        ON stcs.product_id = pcs.product_id
    """
)

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

job.commit()
```