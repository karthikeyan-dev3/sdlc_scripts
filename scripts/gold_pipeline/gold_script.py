import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark bootstrap
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3 (STRICT PATH FORMAT)
# ------------------------------------------------------------------------------------
order_line_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/order_line_sales_silver.{FILE_FORMAT}/")
)

customer_order_first_purchase_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_first_purchase_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
order_line_sales_silver_df.createOrReplaceTempView("order_line_sales_silver")
customer_order_first_purchase_silver_df.createOrReplaceTempView("customer_order_first_purchase_silver")

# ====================================================================================
# TARGET TABLE: gold_daily_sales_summary
# mapping_details:
#   silver.order_line_sales_silver olss
#   LEFT JOIN silver.customer_order_first_purchase_silver cofps
#   ON olss.transaction_id = cofps.transaction_id AND olss.customer_id = cofps.customer_id
# ====================================================================================
gold_daily_sales_summary_sql = """
WITH base AS (
  SELECT
    DATE(olss.sales_date) AS sales_date,
    CAST(olss.line_revenue AS DECIMAL(12,2)) AS line_revenue,
    CAST(olss.transaction_id AS STRING) AS transaction_id,
    CAST(olss.customer_id AS STRING) AS customer_id,
    DATE(cofps.first_purchase_date) AS first_purchase_date
  FROM order_line_sales_silver olss
  LEFT JOIN customer_order_first_purchase_silver cofps
    ON olss.transaction_id = cofps.transaction_id
   AND olss.customer_id = cofps.customer_id
),
daily AS (
  SELECT
    sales_date,
    CAST(SUM(line_revenue) AS DECIMAL(12,2)) AS total_revenue,
    CAST(COUNT(DISTINCT transaction_id) AS INT) AS total_orders,
    CAST(
      SUM(line_revenue) / NULLIF(COUNT(DISTINCT transaction_id), 0)
      AS DECIMAL(12,2)
    ) AS avg_order_value,
    CAST(COUNT(DISTINCT customer_id) AS INT) AS total_customers,
    CAST(COUNT(DISTINCT CASE WHEN first_purchase_date = sales_date THEN customer_id END) AS INT) AS new_customers,
    CAST(COUNT(DISTINCT CASE WHEN first_purchase_date < sales_date THEN customer_id END) AS INT) AS returning_customers,
    CAST(
      COUNT(DISTINCT CASE WHEN first_purchase_date < sales_date THEN customer_id END)
      / NULLIF(COUNT(DISTINCT customer_id), 0)
      AS DECIMAL(12,4)
    ) AS customer_retention_rate
  FROM base
  GROUP BY sales_date
),
final AS (
  SELECT
    d.sales_date,
    d.total_revenue,
    d.total_orders,
    d.avg_order_value,
    d.total_customers,
    d.new_customers,
    d.returning_customers,
    d.customer_retention_rate,
    CAST(
      (
        d.total_revenue
        - LAG(d.total_revenue, 1) OVER (
            PARTITION BY EXTRACT(DAY FROM d.sales_date)
            ORDER BY EXTRACT(YEAR FROM d.sales_date), EXTRACT(MONTH FROM d.sales_date)
          )
      )
      / NULLIF(
          LAG(d.total_revenue, 1) OVER (
            PARTITION BY EXTRACT(DAY FROM d.sales_date)
            ORDER BY EXTRACT(YEAR FROM d.sales_date), EXTRACT(MONTH FROM d.sales_date)
          ),
          0
        )
      AS DECIMAL(12,4)
    ) AS mom_growth_pct
  FROM daily d
)
SELECT
  sales_date,
  total_revenue,
  total_orders,
  avg_order_value,
  total_customers,
  new_customers,
  returning_customers,
  customer_retention_rate,
  mom_growth_pct
FROM final
"""

gold_daily_sales_summary_df = spark.sql(gold_daily_sales_summary_sql)

# 4) Save output (SINGLE CSV file directly under TARGET_PATH; no subfolders)
(
    gold_daily_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_summary.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_sales_by_region
# mapping_details: silver.order_line_sales_silver olss
# ====================================================================================
gold_daily_sales_by_region_sql = """
SELECT
  DATE(olss.sales_date) AS sales_date,
  CAST(olss.region_name AS STRING) AS region_name,
  CAST(SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) AS DECIMAL(12,2)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)) AS INT) AS total_orders,
  CAST(
    SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) / NULLIF(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)), 0)
    AS DECIMAL(12,2)
  ) AS avg_order_value
FROM order_line_sales_silver olss
GROUP BY
  DATE(olss.sales_date),
  CAST(olss.region_name AS STRING)
"""

gold_daily_sales_by_region_df = spark.sql(gold_daily_sales_by_region_sql)

(
    gold_daily_sales_by_region_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_by_region.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_sales_by_store
# mapping_details: silver.order_line_sales_silver olss
# ====================================================================================
gold_daily_sales_by_store_sql = """
SELECT
  DATE(olss.sales_date) AS sales_date,
  CAST(olss.store_id AS STRING) AS store_id,
  CAST(olss.store_name AS STRING) AS store_name,
  CAST(olss.region_name AS STRING) AS region_name,
  CAST(SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) AS DECIMAL(12,2)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)) AS INT) AS total_orders,
  CAST(
    SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) / NULLIF(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)), 0)
    AS DECIMAL(12,2)
  ) AS avg_order_value
FROM order_line_sales_silver olss
GROUP BY
  DATE(olss.sales_date),
  CAST(olss.store_id AS STRING),
  CAST(olss.store_name AS STRING),
  CAST(olss.region_name AS STRING)
"""

gold_daily_sales_by_store_df = spark.sql(gold_daily_sales_by_store_sql)

(
    gold_daily_sales_by_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_by_store.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_sales_by_category
# mapping_details: silver.order_line_sales_silver olss
# ====================================================================================
gold_daily_sales_by_category_sql = """
SELECT
  DATE(olss.sales_date) AS sales_date,
  CAST(olss.category_name AS STRING) AS category_name,
  CAST(SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) AS DECIMAL(12,2)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)) AS INT) AS total_orders
FROM order_line_sales_silver olss
GROUP BY
  DATE(olss.sales_date),
  CAST(olss.category_name AS STRING)
"""

gold_daily_sales_by_category_df = spark.sql(gold_daily_sales_by_category_sql)

(
    gold_daily_sales_by_category_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_by_category.csv")
)

# ====================================================================================
# TARGET TABLE: gold_daily_sales_by_sku
# mapping_details: silver.order_line_sales_silver olss
# ====================================================================================
gold_daily_sales_by_sku_sql = """
SELECT
  DATE(olss.sales_date) AS sales_date,
  CAST(olss.sku_id AS STRING) AS sku_id,
  CAST(olss.sku_name AS STRING) AS sku_name,
  CAST(olss.category_name AS STRING) AS category_name,
  CAST(SUM(CAST(olss.units_sold AS INT)) AS INT) AS units_sold,
  CAST(SUM(CAST(olss.line_revenue AS DECIMAL(12,2))) AS DECIMAL(12,2)) AS total_revenue,
  CAST(COUNT(DISTINCT CAST(olss.transaction_id AS STRING)) AS INT) AS total_orders
FROM order_line_sales_silver olss
GROUP BY
  DATE(olss.sales_date),
  CAST(olss.sku_id AS STRING),
  CAST(olss.sku_name AS STRING),
  CAST(olss.category_name AS STRING)
"""

gold_daily_sales_by_sku_df = spark.sql(gold_daily_sales_by_sku_sql)

(
    gold_daily_sales_by_sku_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_by_sku.csv")
)

job.commit()
