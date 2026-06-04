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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# products_silver
# Columns: product_id, product_name, category
# -----------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  category
FROM dedup
WHERE rn = 1
"""
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# -----------------------------------------------------------------------------------
# stores_silver
# Columns: store_id, store_name, region (from state)
# -----------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.state) AS region
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    region,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  region
FROM dedup
WHERE rn = 1
"""
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# -----------------------------------------------------------------------------------
# sales_transactions_silver
# Columns: transaction_id, transaction_date, store_id, product_id, quantity_sold, total_revenue
# -----------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts,
    CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS transaction_date,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_revenue
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND TRIM(stb.store_id) IS NOT NULL
    AND TRIM(stb.store_id) <> ''
    AND TRIM(stb.product_id) IS NOT NULL
    AND TRIM(stb.product_id) <> ''
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time_ts DESC
    ) AS rn
  FROM base
),
filtered AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_revenue
  FROM dedup
  WHERE rn = 1
    AND quantity_sold IS NOT NULL
    AND quantity_sold >= 0
    AND total_revenue IS NOT NULL
    AND total_revenue >= 0
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_revenue
FROM filtered
"""
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# validated_sales_transactions_silver
# Columns (same as sales_transactions_silver): transaction_id, transaction_date, store_id, product_id, quantity_sold, total_revenue
# -----------------------------------------------------------------------------------
validated_sales_transactions_silver_df = spark.sql(
    """
WITH joined AS (
  SELECT
    sts.transaction_id,
    sts.transaction_date,
    sts.store_id,
    sts.product_id,
    sts.quantity_sold,
    sts.total_revenue,
    ROW_NUMBER() OVER (PARTITION BY sts.transaction_id ORDER BY sts.transaction_id) AS rn
  FROM sales_transactions_silver sts
  INNER JOIN stores_silver ss
    ON sts.store_id = ss.store_id
  INNER JOIN products_silver ps
    ON sts.product_id = ps.product_id
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_revenue
FROM joined
WHERE rn = 1
"""
)

(
    validated_sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/validated_sales_transactions_silver.csv")
)

validated_sales_transactions_silver_df.createOrReplaceTempView("validated_sales_transactions_silver")

# -----------------------------------------------------------------------------------
# store_sales_summary_silver
# Columns: store_id, store_name, region, total_revenue, total_transactions, total_quantity_sold
# -----------------------------------------------------------------------------------
store_sales_summary_silver_df = spark.sql(
    """
SELECT
  vsts.store_id AS store_id,
  ss.store_name AS store_name,
  ss.region AS region,
  SUM(vsts.total_revenue) AS total_revenue,
  COUNT(DISTINCT vsts.transaction_id) AS total_transactions,
  SUM(CAST(vsts.quantity_sold AS BIGINT)) AS total_quantity_sold
FROM validated_sales_transactions_silver vsts
INNER JOIN stores_silver ss
  ON vsts.store_id = ss.store_id
GROUP BY
  vsts.store_id,
  ss.store_name,
  ss.region
"""
)

(
    store_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_sales_summary_silver.csv")
)

store_sales_summary_silver_df.createOrReplaceTempView("store_sales_summary_silver")

# -----------------------------------------------------------------------------------
# product_sales_summary_silver
# Columns: product_id, product_name, category, total_revenue, total_quantity_sold
# -----------------------------------------------------------------------------------
product_sales_summary_silver_df = spark.sql(
    """
SELECT
  vsts.product_id AS product_id,
  ps.product_name AS product_name,
  ps.category AS category,
  SUM(vsts.total_revenue) AS total_revenue,
  SUM(CAST(vsts.quantity_sold AS BIGINT)) AS total_quantity_sold
FROM validated_sales_transactions_silver vsts
INNER JOIN products_silver ps
  ON vsts.product_id = ps.product_id
GROUP BY
  vsts.product_id,
  ps.product_name,
  ps.category
"""
)

(
    product_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_sales_summary_silver.csv")
)

product_sales_summary_silver_df.createOrReplaceTempView("product_sales_summary_silver")

# -----------------------------------------------------------------------------------
# data_validation_silver
# Columns: validated_transaction_id, validated_store_id, validated_product_id
# -----------------------------------------------------------------------------------
data_validation_silver_df = spark.sql(
    """
SELECT
  vsts.transaction_id AS validated_transaction_id,
  vsts.store_id AS validated_store_id,
  vsts.product_id AS validated_product_id
FROM validated_sales_transactions_silver vsts
"""
)

(
    data_validation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_validation_silver.csv")
)

job.commit()
