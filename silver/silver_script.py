```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ----------------------------
# Glue / Spark setup
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

store_locations_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_locations_bronze.{FILE_FORMAT}/")
)
store_locations_bronze_df.createOrReplaceTempView("store_locations_bronze")

store_types_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/store_types_bronze.{FILE_FORMAT}/")
)
store_types_bronze_df.createOrReplaceTempView("store_types_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

brands_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/brands_bronze.{FILE_FORMAT}/")
)
brands_bronze_df.createOrReplaceTempView("brands_bronze")

categories_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/categories_bronze.{FILE_FORMAT}/")
)
categories_bronze_df.createOrReplaceTempView("categories_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transaction_lines_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transaction_lines_bronze.{FILE_FORMAT}/")
)
sales_transaction_lines_bronze_df.createOrReplaceTempView("sales_transaction_lines_bronze")

# ============================================================
# TARGET TABLE: dim_store_silver
# 1) Read sources (done above)
# 2) Temp views (done above)
# 3) SQL transformations + ROW_NUMBER de-dup by store_id
# 4) Write single CSV to TARGET_PATH/dim_store_silver.csv
# ============================================================
dim_store_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,

            NULLIF(TRIM(CAST(sb.store_name AS STRING)), '') AS store_name,
            NULLIF(TRIM(CAST(slb.city AS STRING)), '')      AS city,
            NULLIF(UPPER(TRIM(CAST(slb.state AS STRING))), '') AS state,
            NULLIF(UPPER(TRIM(CAST(stb.store_type AS STRING))), '') AS store_type

        FROM stores_bronze sb
        LEFT JOIN store_locations_bronze slb
            ON sb.store_id = slb.store_id
        LEFT JOIN store_types_bronze stb
            ON sb.store_id = stb.store_id
        WHERE sb.store_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            city,
            state,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN city IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN state IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_type IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM joined
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type
    FROM ranked
    WHERE rn = 1
    """
)

(
    dim_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_store_silver.csv")
)

# ============================================================
# TARGET TABLE: dim_product_silver
# 1) Read sources (done above)
# 2) Temp views (done above)
# 3) SQL transformations + ROW_NUMBER de-dup by product_id
# 4) Write single CSV to TARGET_PATH/dim_product_silver.csv
# ============================================================
dim_product_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,

            NULLIF(TRIM(CAST(pb.product_name AS STRING)), '') AS product_name,
            NULLIF(TRIM(CAST(bb.brand AS STRING)), '')        AS brand,

            -- Standardize category with upper/lower consistency:
            -- trim -> null-if-blank -> lower (canonical)
            CASE
                WHEN NULLIF(TRIM(CAST(cb.category AS STRING)), '') IS NULL THEN NULL
                ELSE LOWER(TRIM(CAST(cb.category AS STRING)))
            END AS category
        FROM products_bronze pb
        LEFT JOIN brands_bronze bb
            ON pb.product_id = bb.product_id
        LEFT JOIN categories_bronze cb
            ON pb.product_id = cb.product_id
        WHERE pb.product_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            brand,
            category,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN brand IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM joined
    )
    SELECT
        product_id,
        product_name,
        brand,
        category
    FROM ranked
    WHERE rn = 1
    """
)

(
    dim_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_product_silver.csv")
)

# ============================================================
# TARGET TABLE: fact_sales_line_silver
# 1) Read sources (done above)
# 2) Temp views (done above)
# 3) SQL transformations + ROW_NUMBER de-dup by transaction_id+product_id
# 4) Write single CSV to TARGET_PATH/fact_sales_line_silver.csv
# ============================================================
fact_sales_line_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.store_id AS STRING)       AS store_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sales_date,

            CAST(stlb.product_id AS STRING)    AS product_id,
            COALESCE(CAST(stlb.quantity AS INT), 0) AS quantity,
            COALESCE(CAST(stlb.sale_amount AS DECIMAL(38,10)), CAST(0 AS DECIMAL(38,10))) AS sale_amount
        FROM sales_transactions_bronze stb
        INNER JOIN sales_transaction_lines_bronze stlb
            ON stb.transaction_id = stlb.transaction_id
        WHERE stb.transaction_id IS NOT NULL
          AND stb.store_id IS NOT NULL
          AND stlb.product_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            store_id,
            transaction_time,
            sales_date,
            product_id,
            quantity,
            sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id, product_id
                ORDER BY
                    CASE WHEN transaction_time IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        store_id,
        transaction_time,
        sales_date,
        product_id,
        quantity,
        sale_amount
    FROM dedup
    WHERE rn = 1
    """
)

(
    fact_sales_line_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/fact_sales_line_silver.csv")
)

job.commit()
```