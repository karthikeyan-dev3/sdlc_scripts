import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# =========================
# 1) Read Source Tables (Silver)
# =========================
stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# =========================
# 2) Create Temp Views
# =========================
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# =========================
# Target: gold_dim_store
# =========================
gold_dim_store_df = spark.sql(
    """
    SELECT
      CAST(DENSE_RANK() OVER (ORDER BY ss.store_id) AS BIGINT) AS store_key,
      ss.store_id AS store_id,
      ss.store_name AS store_name,
      ss.store_type AS store_type,
      ss.city AS city,
      ss.state AS state_province,
      CAST(ss.open_date AS DATE) AS open_date,
      CASE WHEN ss.current_flag = true THEN true ELSE false END AS active_flag,
      CAST(ss.effective_start_date AS DATE) AS effective_start_date,
      CAST(ss.effective_end_date AS DATE) AS effective_end_date,
      ss.current_flag AS current_flag
    FROM stores_silver ss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

# =========================
# Target: gold_dim_product
# =========================
gold_dim_product_df = spark.sql(
    """
    SELECT
      CAST(DENSE_RANK() OVER (ORDER BY ps.product_id) AS BIGINT) AS product_key,
      ps.product_id AS product_id,
      ps.product_name AS product_name,
      ps.brand AS brand,
      ps.category AS category,
      ps.is_active AS active_flag,
      CAST(ps.effective_start_date AS DATE) AS effective_start_date,
      CAST(ps.effective_end_date AS DATE) AS effective_end_date,
      ps.current_flag AS current_flag
    FROM products_silver ps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

gold_dim_product_df.createOrReplaceTempView("gold_dim_product")

# =========================
# Target: gold_fact_sales
# =========================
gold_fact_sales_df = spark.sql(
    """
    SELECT
      CAST(DENSE_RANK() OVER (ORDER BY sts.transaction_id, sts.transaction_time) AS BIGINT) AS sales_key,
      sts.transaction_id AS transaction_id,
      CAST(1 AS INT) AS transaction_line_id,
      sts.transaction_time AS transaction_ts,
      CAST(sts.transaction_time AS DATE) AS sales_date,
      gds.store_key AS store_key,
      gdp.product_key AS product_key,
      CAST(sts.quantity AS INT) AS quantity,
      CASE
        WHEN sts.quantity IS NOT NULL AND CAST(sts.quantity AS INT) <> 0
          THEN CAST(sts.sale_amount AS DOUBLE) / CAST(sts.quantity AS INT)
        ELSE NULL
      END AS unit_price,
      CAST(sts.sale_amount AS DOUBLE) AS gross_amount,
      CAST(0 AS DOUBLE) AS discount_amount,
      CAST(sts.sale_amount AS DOUBLE) AS net_amount,
      CURRENT_DATE AS ingestion_date
    FROM sales_transactions_silver sts
    LEFT JOIN gold_dim_store gds
      ON sts.store_id = gds.store_id
     AND gds.current_flag = true
    LEFT JOIN gold_dim_product gdp
      ON sts.product_id = gdp.product_id
     AND gdp.current_flag = true
    """
)

(
    gold_fact_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_fact_sales.csv")
)

gold_fact_sales_df.createOrReplaceTempView("gold_fact_sales")

# =========================
# Target: gold_agg_store_day
# =========================
gold_agg_store_day_df = spark.sql(
    """
    SELECT
      gfs.sales_date AS sales_date,
      gfs.store_key AS store_key,
      COUNT(DISTINCT gfs.transaction_id) AS transactions_cnt,
      COALESCE(SUM(gfs.quantity), 0) AS units_sold_qty,
      COALESCE(SUM(gfs.gross_amount), 0) AS gross_revenue_amt,
      COALESCE(SUM(gfs.discount_amount), 0) AS discount_amt,
      COALESCE(SUM(gfs.net_amount), 0) AS net_revenue_amt
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.sales_date,
      gfs.store_key
    """
)

(
    gold_agg_store_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_store_day.csv")
)

# =========================
# Target: gold_agg_product_day
# =========================
gold_agg_product_day_df = spark.sql(
    """
    SELECT
      gfs.sales_date AS sales_date,
      gfs.product_key AS product_key,
      COUNT(DISTINCT gfs.transaction_id) AS transactions_cnt,
      COALESCE(SUM(gfs.quantity), 0) AS units_sold_qty,
      COALESCE(SUM(gfs.gross_amount), 0) AS gross_revenue_amt,
      COALESCE(SUM(gfs.discount_amount), 0) AS discount_amt,
      COALESCE(SUM(gfs.net_amount), 0) AS net_revenue_amt
    FROM gold_fact_sales gfs
    GROUP BY
      gfs.sales_date,
      gfs.product_key
    """
)

(
    gold_agg_product_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_product_day.csv")
)

# =========================
# Target: gold_agg_category_day
# =========================
gold_agg_category_day_df = spark.sql(
    """
    SELECT
      gfs.sales_date AS sales_date,
      gdp.category AS category,
      COUNT(DISTINCT gfs.transaction_id) AS transactions_cnt,
      COALESCE(SUM(gfs.quantity), 0) AS units_sold_qty,
      COALESCE(SUM(gfs.gross_amount), 0) AS gross_revenue_amt,
      COALESCE(SUM(gfs.discount_amount), 0) AS discount_amt,
      COALESCE(SUM(gfs.net_amount), 0) AS net_revenue_amt
    FROM gold_fact_sales gfs
    INNER JOIN gold_dim_product gdp
      ON gfs.product_key = gdp.product_key
     AND gdp.current_flag = true
    GROUP BY
      gfs.sales_date,
      gdp.category
    """
)

(
    gold_agg_category_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_agg_category_day.csv")
)

job.commit()
