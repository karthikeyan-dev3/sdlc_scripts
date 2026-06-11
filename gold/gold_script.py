import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: gold.gold_dim_product  (gdp)
# ============================================================
gold_dim_product_df = spark.sql(
    """
    SELECT
        CAST(ROW_NUMBER() OVER (ORDER BY ps.product_id) AS BIGINT) AS product_key,
        CAST(ps.product_id AS STRING) AS product_id,
        CAST(ps.product_name AS STRING) AS product_name,
        CAST(ps.brand AS STRING) AS brand,
        CAST(ps.category AS STRING) AS category,
        CAST(ps.is_active AS BOOLEAN) AS is_active,
        CURRENT_DATE AS effective_start_date,
        CAST('9999-12-31' AS DATE) AS effective_end_date
    FROM products_silver ps
    """
)

gold_dim_product_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_dim_product.csv"
)

# ============================================================
# Target: gold.gold_dim_store  (gds)
# ============================================================
gold_dim_store_df = spark.sql(
    """
    SELECT
        CAST(ROW_NUMBER() OVER (ORDER BY ss.store_id) AS BIGINT) AS store_key,
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.store_name AS STRING) AS store_name,
        CAST(ss.city AS STRING) AS city,
        CAST(ss.state AS STRING) AS state,
        CAST(ss.store_type AS STRING) AS store_type,
        CAST(ss.open_date AS DATE) AS open_date,
        CAST(ss.region AS STRING) AS region,
        CAST(TRUE AS BOOLEAN) AS is_active,
        CURRENT_DATE AS effective_start_date,
        CAST('9999-12-31' AS DATE) AS effective_end_date
    FROM stores_silver ss
    """
)

gold_dim_store_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_dim_store.csv"
)

# ------------------------------------------------------------
# Create temp views for gold dims (for downstream joins)
# ------------------------------------------------------------
gold_dim_product_df.createOrReplaceTempView("gold_dim_product")
gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

# ============================================================
# Target: gold.gold_fact_sales  (gfs)
# ============================================================
gold_fact_sales_df = spark.sql(
    """
    SELECT
        CAST(ROW_NUMBER() OVER (ORDER BY sts.transaction_id) AS BIGINT) AS sales_txn_key,
        CAST(sts.transaction_id AS STRING) AS txn_id,
        CAST(sts.txn_date AS DATE) AS txn_date,
        CAST(gds.store_key AS BIGINT) AS store_key,
        CAST(gdp.product_key AS BIGINT) AS product_key,
        CAST(sts.quantity AS INT) AS quantity,
        CAST(sts.sale_amount AS DOUBLE) AS gross_sales_amount,
        CAST(sts.sale_amount AS DOUBLE) AS net_sales_amount,
        CAST(0 AS DOUBLE) AS discount_amount,
        CAST(0 AS DOUBLE) AS tax_amount,
        CAST(sts.data_quality_status AS STRING) AS data_quality_status,
        CURRENT_DATE AS load_date
    FROM sales_transactions_silver sts
    INNER JOIN gold_dim_store gds
        ON sts.store_id = gds.store_id
       AND CAST(sts.txn_date AS DATE) BETWEEN gds.effective_start_date AND gds.effective_end_date
    INNER JOIN gold_dim_product gdp
        ON sts.product_id = gdp.product_id
       AND CAST(sts.txn_date AS DATE) BETWEEN gdp.effective_start_date AND gdp.effective_end_date
    """
)

gold_fact_sales_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_fact_sales.csv"
)

# ------------------------------------------------------------
# Create temp view for gold fact (for downstream aggregates)
# ------------------------------------------------------------
gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

# ============================================================
# Target: gold.gold_agg_sales_daily  (gasd)
# ============================================================
gold_agg_sales_daily_df = spark.sql(
    """
    SELECT
        CAST(gfs.txn_date AS DATE) AS sales_date,
        CAST(gfs.store_key AS BIGINT) AS store_key,
        CAST(gfs.product_key AS BIGINT) AS product_key,
        CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
        CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
        CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
        CAST(SUM(gfs.discount_amount) AS DOUBLE) AS total_discount_amount,
        CAST(SUM(gfs.tax_amount) AS DOUBLE) AS total_tax_amount,
        CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
        CURRENT_DATE AS load_date
    FROM gold_fact_sales gfs
    GROUP BY
        gfs.txn_date,
        gfs.store_key,
        gfs.product_key
    """
)

gold_agg_sales_daily_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_agg_sales_daily.csv"
)

# ============================================================
# Target: gold.gold_agg_sales_daily_store  (gasds)
# ============================================================
gold_agg_sales_daily_store_df = spark.sql(
    """
    SELECT
        CAST(gfs.txn_date AS DATE) AS sales_date,
        CAST(gfs.store_key AS BIGINT) AS store_key,
        CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
        CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
        CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
        CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
        CURRENT_DATE AS load_date
    FROM gold_fact_sales gfs
    GROUP BY
        gfs.txn_date,
        gfs.store_key
    """
)

gold_agg_sales_daily_store_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_agg_sales_daily_store.csv"
)

# ============================================================
# Target: gold.gold_agg_sales_daily_region  (gasdr)
# ============================================================
gold_agg_sales_daily_region_df = spark.sql(
    """
    SELECT
        CAST(gfs.txn_date AS DATE) AS sales_date,
        CAST(gds.region AS STRING) AS region,
        CAST(SUM(gfs.quantity) AS BIGINT) AS total_quantity,
        CAST(SUM(gfs.gross_sales_amount) AS DOUBLE) AS total_gross_sales_amount,
        CAST(SUM(gfs.net_sales_amount) AS DOUBLE) AS total_net_sales_amount,
        CAST(COUNT(gfs.txn_id) AS BIGINT) AS txn_count,
        CURRENT_DATE AS load_date
    FROM gold_fact_sales gfs
    INNER JOIN gold_dim_store gds
        ON gfs.store_key = gds.store_key
       AND CAST(gfs.txn_date AS DATE) BETWEEN gds.effective_start_date AND gds.effective_end_date
    GROUP BY
        gfs.txn_date,
        gds.region
    """
)

gold_agg_sales_daily_region_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/gold_agg_sales_daily_region.csv"
)

job.commit()
