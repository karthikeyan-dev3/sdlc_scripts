import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

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
# 1) Read source tables (silver)
# -----------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
products_silver_df.createOrReplaceTempView("products_silver")

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------
# 2) gold_dim_product
# -----------------------------
gold_dim_product_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(ps.product_id AS STRING)  AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.brand AS STRING)        AS brand,
        CAST(ps.category AS STRING)     AS category,
        CAST(ps.price AS FLOAT)         AS list_price,
        CAST(ps.is_active AS BOOLEAN)   AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY ps.product_id
          ORDER BY ps.product_id
        ) AS rn
      FROM products_silver ps
    )
    SELECT
      CAST(ROW_NUMBER() OVER (ORDER BY product_id) AS BIGINT) AS product_key,
      product_id,
      product_name,
      brand,
      category,
      list_price,
      is_active
    FROM base
    WHERE rn = 1
    """
)
gold_dim_product_df.createOrReplaceTempView("gold_dim_product")

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# -----------------------------
# 3) gold_dim_store
# -----------------------------
gold_dim_store_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(ss.store_id AS STRING)    AS store_id,
        CAST(ss.store_name AS STRING)  AS store_name,
        CAST(ss.store_type AS STRING)  AS store_type,
        CAST(ss.city AS STRING)        AS city,
        CAST(ss.state AS STRING)       AS state,
        CAST(ss.open_date AS DATE)     AS open_date,
        ROW_NUMBER() OVER (
          PARTITION BY ss.store_id
          ORDER BY ss.store_id
        ) AS rn
      FROM stores_silver ss
    )
    SELECT
      CAST(ROW_NUMBER() OVER (ORDER BY store_id) AS BIGINT) AS store_key,
      store_id,
      store_name,
      store_type,
      city,
      state,
      open_date
    FROM base
    WHERE rn = 1
    """
)
gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# -----------------------------
# 4) gold_dim_date
# -----------------------------
gold_dim_date_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(sts.transaction_time AS TIMESTAMP) AS calendar_date,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(sts.transaction_time AS TIMESTAMP)
          ORDER BY CAST(sts.transaction_time AS TIMESTAMP)
        ) AS rn
      FROM sales_transactions_silver sts
      WHERE sts.transaction_time IS NOT NULL
    )
    SELECT
      CAST(DATE(calendar_date) AS DATE) AS date_key,
      calendar_date
    FROM base
    WHERE rn = 1
    """
)
gold_dim_date_df.createOrReplaceTempView("gold_dim_date")

(
    gold_dim_date_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_date.csv")
)

# -----------------------------
# 5) gold_fact_sales
# -----------------------------
gold_fact_sales_df = spark.sql(
    """
    SELECT
      CAST(sts.transaction_id AS STRING) AS transaction_id,
      CAST(gdd.date_key AS DATE)         AS date_key,
      CAST(gds.store_key AS BIGINT)      AS store_key,
      CAST(gdp.product_key AS BIGINT)    AS product_key,
      CAST(sts.quantity AS INT)          AS quantity,
      CAST(sts.sale_amount AS DOUBLE)    AS gross_sales_amount
    FROM sales_transactions_silver sts
    INNER JOIN gold_dim_store gds
      ON CAST(sts.store_id AS STRING) = CAST(gds.store_id AS STRING)
    INNER JOIN gold_dim_product gdp
      ON CAST(sts.product_id AS STRING) = CAST(gdp.product_id AS STRING)
    INNER JOIN gold_dim_date gdd
      ON CAST(sts.transaction_time AS TIMESTAMP) = CAST(gdd.calendar_date AS TIMESTAMP)
    """
)
gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

(
    gold_fact_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_fact_sales.csv")
)

# -----------------------------
# 6) gold_agg_daily_sales_store
# -----------------------------
gold_agg_daily_sales_store_df = spark.sql(
    """
    SELECT
      CAST(gfs.date_key AS DATE)     AS date_key,
      CAST(gfs.store_key AS BIGINT)  AS store_key,
      CAST(COUNT(gfs.transaction_id) AS BIGINT) AS transactions_count,
      CAST(SUM(gfs.quantity) AS BIGINT)         AS units_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS gross_sales_amount
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.date_key,
      gfs.store_key
    """
)
gold_agg_daily_sales_store_df.createOrReplaceTempView("gold_agg_daily_sales_store")

(
    gold_agg_daily_sales_store_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_daily_sales_store.csv")
)

# -----------------------------
# 7) gold_agg_daily_sales_product
# -----------------------------
gold_agg_daily_sales_product_df = spark.sql(
    """
    SELECT
      CAST(gfs.date_key AS DATE)      AS date_key,
      CAST(gfs.product_key AS BIGINT) AS product_key,
      CAST(COUNT(gfs.transaction_id) AS BIGINT) AS transactions_count,
      CAST(SUM(gfs.quantity) AS BIGINT)         AS units_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS gross_sales_amount
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.date_key,
      gfs.product_key
    """
)
gold_agg_daily_sales_product_df.createOrReplaceTempView("gold_agg_daily_sales_product")

(
    gold_agg_daily_sales_product_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_daily_sales_product.csv")
)

# -----------------------------
# 8) gold_agg_daily_sales_region
# NOTE: UDT references gds.region, but region is not available in provided silver attributes.
# This query will fail unless `region` exists in gold_dim_store input view.
# -----------------------------
gold_agg_daily_sales_region_df = spark.sql(
    """
    SELECT
      CAST(gfs.date_key AS DATE) AS date_key,
      CAST(gds.region AS STRING) AS region,
      CAST(COUNT(gfs.transaction_id) AS BIGINT) AS transactions_count,
      CAST(SUM(gfs.quantity) AS BIGINT)         AS units_sold,
      CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS gross_sales_amount
    FROM gold_fact_sales gfs
    INNER JOIN gold_dim_store gds
      ON CAST(gfs.store_key AS BIGINT) = CAST(gds.store_key AS BIGINT)
    GROUP BY
      gfs.date_key,
      gds.region
    """
)
gold_agg_daily_sales_region_df.createOrReplaceTempView("gold_agg_daily_sales_region")

(
    gold_agg_daily_sales_region_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_daily_sales_region.csv")
)

job.commit()