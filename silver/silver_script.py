import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read Source Tables (Bronze) and Create Temp Views
# -----------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze_sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze"
)
spark.sql("CREATE OR REPLACE TEMP VIEW bronze_stores_bronze AS SELECT * FROM stores_bronze")
spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze_products_bronze AS SELECT * FROM products_bronze"
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -----------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
SELECT DISTINCT
  stb.transaction_id,
  stb.store_id,
  stb.product_id,
  CAST(stb.quantity AS INT) AS quantity,
  CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
  CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
  CAST(DATE(stb.transaction_time) AS DATE) AS sales_date,
  CURRENT_DATE() AS data_refresh_date
FROM bronze_sales_transactions_bronze stb
WHERE stb.transaction_id IS NOT NULL
  AND stb.store_id IS NOT NULL
  AND stb.product_id IS NOT NULL
  AND stb.transaction_time IS NOT NULL
  AND stb.quantity IS NOT NULL
  AND stb.sale_amount IS NOT NULL
"""
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.stores_silver
# -----------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
SELECT DISTINCT
  sb.store_id,
  NULLIF(TRIM(sb.store_name),'') AS store_name,
  NULLIF(TRIM(sb.city),'') AS store_city,
  NULLIF(TRIM(sb.state),'') AS store_state,
  NULLIF(TRIM(sb.store_type),'') AS store_type,
  CAST(sb.open_date AS DATE) AS open_date,
  CASE
    WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
    WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
    WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
    WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
    ELSE 'UNKNOWN'
  END AS store_region,
  CURRENT_DATE() AS data_refresh_date
FROM bronze_stores_bronze sb
WHERE sb.store_id IS NOT NULL
"""
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.products_silver
# -----------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
SELECT DISTINCT
  pb.product_id,
  NULLIF(TRIM(pb.product_name),'') AS product_name,
  NULLIF(TRIM(pb.brand),'') AS brand,
  NULLIF(TRIM(pb.category),'') AS category,
  CAST(NULL AS STRING) AS sub_category,
  CAST(NULL AS STRING) AS department,
  CAST(NULL AS STRING) AS unit_of_measure,
  CAST(pb.price AS DOUBLE) AS current_price_amount,
  CAST(COALESCE(pb.is_active, TRUE) AS BOOLEAN) AS is_active,
  CURRENT_DATE() AS data_refresh_date
FROM bronze_products_bronze pb
WHERE pb.product_id IS NOT NULL
  AND COALESCE(pb.is_active, TRUE) = TRUE
"""
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_daily_store_silver
# -----------------------------------------------------------------------------------
sales_daily_store_silver_df = spark.sql(
    """
SELECT
  sts.sales_date,
  sts.store_id,
  ss.store_name,
  ss.store_city,
  ss.store_state,
  ss.store_region,
  ss.store_type,
  SUM(sts.sale_amount) AS total_revenue_amount,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions_count,
  SUM(sts.quantity) AS total_quantity_units,
  CASE
    WHEN COUNT(DISTINCT sts.transaction_id) = 0 THEN NULL
    ELSE SUM(sts.sale_amount) / COUNT(DISTINCT sts.transaction_id)
  END AS avg_basket_value_amount,
  CASE
    WHEN COUNT(DISTINCT sts.transaction_id) = 0 THEN NULL
    ELSE SUM(sts.quantity) / COUNT(DISTINCT sts.transaction_id)
  END AS avg_units_per_transaction,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  sts.sales_date,
  sts.store_id,
  ss.store_name,
  ss.store_city,
  ss.store_state,
  ss.store_region,
  ss.store_type
"""
)

(
    sales_daily_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_daily_product_silver
# -----------------------------------------------------------------------------------
sales_daily_product_silver_df = spark.sql(
    """
SELECT
  sts.sales_date,
  sts.product_id,
  ps.product_name,
  ps.brand,
  ps.category,
  ps.sub_category,
  ps.department,
  ps.unit_of_measure,
  SUM(sts.sale_amount) AS total_revenue_amount,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions_count,
  SUM(sts.quantity) AS total_quantity_units,
  CASE
    WHEN SUM(sts.quantity) = 0 THEN NULL
    ELSE SUM(sts.sale_amount) / SUM(sts.quantity)
  END AS avg_selling_price_amount,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.sales_date,
  sts.product_id,
  ps.product_name,
  ps.brand,
  ps.category,
  ps.sub_category,
  ps.department,
  ps.unit_of_measure
"""
)

(
    sales_daily_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_product_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sales_daily_store_product_silver
# -----------------------------------------------------------------------------------
sales_daily_store_product_silver_df = spark.sql(
    """
SELECT
  sts.sales_date,
  sts.store_id,
  ss.store_name,
  ss.store_region,
  sts.product_id,
  ps.product_name,
  ps.category,
  SUM(sts.sale_amount) AS total_revenue_amount,
  COUNT(DISTINCT sts.transaction_id) AS total_transactions_count,
  SUM(sts.quantity) AS total_quantity_units,
  CURRENT_DATE() AS data_refresh_date
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  sts.sales_date,
  sts.store_id,
  ss.store_name,
  ss.store_region,
  sts.product_id,
  ps.product_name,
  ps.category
"""
)

(
    sales_daily_store_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_product_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.data_quality_daily_silver
# -----------------------------------------------------------------------------------
data_quality_daily_silver_df = spark.sql(
    """
WITH base AS (
  SELECT CURRENT_DATE() AS run_date
),
stg AS (
  SELECT
    COUNT(*) AS raw_count,
    SUM(CASE WHEN transaction_id IS NULL OR store_id IS NULL OR product_id IS NULL THEN 1 ELSE 0 END) AS invalid_identifier_count
  FROM bronze_sales_transactions_bronze
),
dedup AS (
  SELECT COUNT(*) AS cleaned_count
  FROM sales_transactions_silver
),
dup AS (
  SELECT (stg.raw_count - dedup.cleaned_count) AS duplicate_records_removed_count
  FROM stg, dedup
)
SELECT
  base.run_date,
  'sales_transactions' AS dataset_name,
  stg.raw_count AS records_processed_count,
  dup.duplicate_records_removed_count AS duplicate_records_removed_count,
  stg.invalid_identifier_count AS invalid_identifier_count,
  CAST(ROUND(100.0 * (1 - (stg.invalid_identifier_count * 1.0 / NULLIF(stg.raw_count,0))), 2) AS DOUBLE) AS completeness_score_pct,
  CAST(NULL AS DOUBLE) AS accuracy_score_pct,
  CAST(ROUND(100.0 * (1 - (stg.invalid_identifier_count * 1.0 / NULLIF(stg.raw_count,0))), 2) AS DOUBLE) AS overall_data_quality_score_pct
FROM base
CROSS JOIN stg
CROSS JOIN dedup
CROSS JOIN dup
"""
)

(
    data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_silver.csv")
)

job.commit()
