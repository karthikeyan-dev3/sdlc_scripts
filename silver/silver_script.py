import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

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
# 1) Read source tables from S3 (Bronze)
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
# 3) Transform: silver.product_silver
#    Output columns: product_id, product_name, product_category, product_price
# -----------------------------------------------------------------------------------
product_silver_sql = """
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    CAST(pb.price AS DOUBLE) AS product_price
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
),
dedup AS (
  SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
  FROM base
)
SELECT
  product_id,
  product_name,
  product_category,
  product_price
FROM dedup
WHERE rn = 1
"""

product_silver_df = spark.sql(product_silver_sql)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# -----------------------------------------------------------------------------------
# 4) Transform: silver.store_silver
#    Output columns: store_id, store_name, store_location, store_region
# -----------------------------------------------------------------------------------
store_silver_sql = """
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
    TRIM(sb.state) AS store_region
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
),
dedup AS (
  SELECT
    store_id,
    store_name,
    store_location,
    store_region,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
  FROM base
)
SELECT
  store_id,
  store_name,
  store_location,
  store_region
FROM dedup
WHERE rn = 1
"""

store_silver_df = spark.sql(store_silver_sql)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# -----------------------------------------------------------------------------------
# 5) Transform: silver.sales_silver
#    Output columns: transaction_id, product_id, store_id, sale_date, quantity_sold, total_sales_amount
# -----------------------------------------------------------------------------------
sales_silver_sql = """
WITH base AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sale_date,
    CAST(stb.quantity AS INT) AS quantity_sold,
    CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
  FROM sales_transactions_bronze stb
  INNER JOIN product_silver ps
    ON TRIM(stb.product_id) = ps.product_id
  INNER JOIN store_silver ss
    ON TRIM(stb.store_id) = ss.store_id
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) > 0
),
dedup AS (
  SELECT
    transaction_id,
    product_id,
    store_id,
    sale_date,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time_ts DESC) AS rn
  FROM base
)
SELECT
  transaction_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE rn = 1
"""

sales_silver_df = spark.sql(sales_silver_sql)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# -----------------------------------------------------------------------------------
# 6) Transform: silver.sales_aggregated_silver
#    Output columns: store_region, product_category, sale_date, total_sales_amount, average_quantity_sold
# -----------------------------------------------------------------------------------
sales_aggregated_silver_sql = """
SELECT
  ss.store_region AS store_region,
  ps.product_category AS product_category,
  sls.sale_date AS sale_date,
  SUM(sls.total_sales_amount) AS total_sales_amount,
  AVG(CAST(sls.quantity_sold AS DOUBLE)) AS average_quantity_sold
FROM sales_silver sls
INNER JOIN product_silver ps
  ON sls.product_id = ps.product_id
INNER JOIN store_silver ss
  ON sls.store_id = ss.store_id
GROUP BY
  ss.store_region,
  ps.product_category,
  sls.sale_date
"""

sales_aggregated_silver_df = spark.sql(sales_aggregated_silver_sql)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()