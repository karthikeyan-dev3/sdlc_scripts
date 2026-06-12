import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables (S3)
# -----------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}sales_transactions_silver.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: gold_dim_product (SCD2)
# Columns (from UDT + required SCD2 mechanics):
# product_key, product_id, product_name, brand, category, status,
# effective_start_date, effective_end_date, is_current
# ============================================================
gold_dim_product_df = spark.sql(
    """
    WITH ps_typed AS (
      SELECT
        CAST(TRIM(product_id) AS STRING)        AS product_id,
        CAST(product_name AS STRING)           AS product_name,
        CAST(brand AS STRING)                  AS brand,
        CAST(category AS STRING)               AS category,
        CAST(is_active AS BOOLEAN)             AS is_active
      FROM products_silver
      WHERE product_id IS NOT NULL
    ),
    ps_dedup AS (
      SELECT
        product_id,
        product_name,
        brand,
        category,
        is_active
      FROM (
        SELECT
          product_id,
          product_name,
          brand,
          category,
          is_active,
          ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
          ) AS rn
        FROM ps_typed
      ) x
      WHERE rn = 1
    ),
    scd2_versions AS (
      SELECT
        product_id,
        product_name,
        brand,
        category,
        is_active,
        DATE(CURRENT_DATE()) AS effective_start_date
      FROM ps_dedup
    )
    SELECT
      CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS INT) AS product_key,
      CAST(product_id AS STRING)                          AS product_id,
      CAST(product_name AS STRING)                        AS product_name,
      CAST(brand AS STRING)                               AS brand,
      CAST(category AS STRING)                            AS category,
      CAST(is_active AS BOOLEAN)                          AS status,
      CAST(effective_start_date AS DATE)                  AS effective_start_date,
      CAST(NULL AS DATE)                                  AS effective_end_date,
      CAST(TRUE AS BOOLEAN)                               AS is_current
    FROM scd2_versions
    """
)

gold_dim_product_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_dim_product.csv"
)

gold_dim_product_df.createOrReplaceTempView("gold_dim_product")

# ============================================================
# Target: gold_dim_store (SCD2)
# Columns (from UDT + required SCD2 mechanics):
# store_key, store_id, store_name, city, state, store_type, open_date,
# effective_start_date, effective_end_date, is_current
# ============================================================
gold_dim_store_df = spark.sql(
    """
    WITH ss_typed AS (
      SELECT
        CAST(TRIM(store_id) AS STRING)         AS store_id,
        CAST(store_name AS STRING)             AS store_name,
        CAST(city AS STRING)                   AS city,
        CAST(state AS STRING)                  AS state,
        CAST(store_type AS STRING)             AS store_type,
        CAST(open_date AS DATE)                AS open_date
      FROM stores_silver
      WHERE store_id IS NOT NULL
    ),
    ss_dedup AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date
      FROM (
        SELECT
          store_id,
          store_name,
          city,
          state,
          store_type,
          open_date,
          ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
          ) AS rn
        FROM ss_typed
      ) x
      WHERE rn = 1
    ),
    scd2_versions AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date,
        DATE(CURRENT_DATE()) AS effective_start_date
      FROM ss_dedup
    )
    SELECT
      CAST(ROW_NUMBER() OVER (ORDER BY store_id) AS INT) AS store_key,
      CAST(store_id AS STRING)                           AS store_id,
      CAST(store_name AS STRING)                         AS store_name,
      CAST(city AS STRING)                               AS city,
      CAST(state AS STRING)                              AS state,
      CAST(store_type AS STRING)                         AS store_type,
      CAST(open_date AS DATE)                            AS open_date,
      CAST(effective_start_date AS DATE)                 AS effective_start_date,
      CAST(NULL AS DATE)                                 AS effective_end_date,
      CAST(TRUE AS BOOLEAN)                              AS is_current
    FROM scd2_versions
    """
)

gold_dim_store_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_dim_store.csv"
)

gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

# ============================================================
# Target: gold_fact_sales
# Columns (from UDT):
# transaction_id, transaction_timestamp, business_date,
# store_key, product_key, qty_sold, gross_sales_amount, net_sales_amount
# ============================================================
gold_fact_sales_df = spark.sql(
    """
    SELECT
      CAST(sts.transaction_id AS STRING)                       AS transaction_id,
      CAST(sts.transaction_time AS TIMESTAMP)                  AS transaction_timestamp,
      CAST(sts.transaction_time AS DATE)                       AS business_date,
      CAST(gds.store_key AS INT)                               AS store_key,
      CAST(gdp.product_key AS INT)                             AS product_key,
      CAST(sts.quantity AS INT)                                AS qty_sold,
      CAST(sts.sale_amount AS DOUBLE)                          AS gross_sales_amount,
      CAST(sts.sale_amount AS DOUBLE)                          AS net_sales_amount
    FROM sales_transactions_silver sts
    LEFT JOIN gold_dim_store gds
      ON sts.store_id = gds.store_id
     AND gds.is_current = TRUE
    LEFT JOIN gold_dim_product gdp
      ON sts.product_id = gdp.product_id
     AND gdp.is_current = TRUE
    """
)

gold_fact_sales_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_fact_sales.csv"
)

gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

# ============================================================
# Target: gold_sales_daily_store_product
# Columns (from UDT):
# business_date, store_key, product_key, qty_sold,
# gross_sales_amount, net_sales_amount, transaction_count
# ============================================================
gold_sales_daily_store_product_df = spark.sql(
    """
    SELECT
      CAST(gfs.business_date AS DATE)                      AS business_date,
      CAST(gfs.store_key AS INT)                           AS store_key,
      CAST(gfs.product_key AS INT)                         AS product_key,
      CAST(SUM(gfs.qty_sold) AS BIGINT)                    AS qty_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE)          AS gross_sales_amount,
      CAST(SUM(gfs.net_sales_amount) AS DOUBLE)            AS net_sales_amount,
      CAST(COUNT(DISTINCT gfs.transaction_id) AS BIGINT)   AS transaction_count
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.business_date,
      gfs.store_key,
      gfs.product_key
    """
)

gold_sales_daily_store_product_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_sales_daily_store_product.csv"
)

# ============================================================
# Target: gold_sales_daily_store
# Columns (from UDT):
# business_date, store_key, qty_sold,
# gross_sales_amount, net_sales_amount, transaction_count
# ============================================================
gold_sales_daily_store_df = spark.sql(
    """
    SELECT
      CAST(gfs.business_date AS DATE)                      AS business_date,
      CAST(gfs.store_key AS INT)                           AS store_key,
      CAST(SUM(gfs.qty_sold) AS BIGINT)                    AS qty_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE)          AS gross_sales_amount,
      CAST(SUM(gfs.net_sales_amount) AS DOUBLE)            AS net_sales_amount,
      CAST(COUNT(DISTINCT gfs.transaction_id) AS BIGINT)   AS transaction_count
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.business_date,
      gfs.store_key
    """
)

gold_sales_daily_store_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_sales_daily_store.csv"
)

# ============================================================
# Target: gold_sales_daily_product
# Columns (from UDT):
# business_date, product_key, qty_sold,
# gross_sales_amount, net_sales_amount, transaction_count
# ============================================================
gold_sales_daily_product_df = spark.sql(
    """
    SELECT
      CAST(gfs.business_date AS DATE)                      AS business_date,
      CAST(gfs.product_key AS INT)                         AS product_key,
      CAST(SUM(gfs.qty_sold) AS BIGINT)                    AS qty_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE)          AS gross_sales_amount,
      CAST(SUM(gfs.net_sales_amount) AS DOUBLE)            AS net_sales_amount,
      CAST(COUNT(DISTINCT gfs.transaction_id) AS BIGINT)   AS transaction_count
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.business_date,
      gfs.product_key
    """
)

gold_sales_daily_product_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}gold_sales_daily_product.csv"
)

job.commit()
