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

# -----------------------------------------
# 1) Read source tables from S3
# -----------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

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

# -----------------------------------------
# 2) Create temp views
# -----------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# TARGET TABLE: sales_transactions_silver
# ============================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id,
    CAST(stb.transaction_time AS timestamp) AS transaction_time_ts,
    CAST(stb.transaction_time AS date) AS date,
    stb.product_id,
    stb.store_id,
    CAST(stb.sale_amount AS double) AS sales_amount,
    CAST(stb.quantity AS int) AS quantity_sold,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY CAST(stb.transaction_time AS timestamp) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE
    stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
    AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
    AND CAST(stb.quantity AS int) >= 0
    AND CAST(stb.sale_amount AS double) >= 0
)
SELECT
  transaction_id,
  date,
  product_id,
  store_id,
  sales_amount,
  quantity_sold
FROM base
WHERE rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

sales_transactions_silver_output = f"{TARGET_PATH}/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_transactions_silver_output)
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# TARGET TABLE: product_details_silver
# ============================================================
product_details_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS product_category,
    CAST(pb.price AS float) AS product_price,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM products_bronze pb
  WHERE
    pb.product_id IS NOT NULL AND TRIM(pb.product_id) <> ''
    AND CAST(pb.price AS float) >= 0
    AND CAST(pb.is_active AS boolean) = true
)
SELECT
  product_id,
  product_name,
  product_category,
  product_price
FROM base
WHERE rn = 1
"""

product_details_silver_df = spark.sql(product_details_silver_sql)

product_details_silver_output = f"{TARGET_PATH}/product_details_silver.csv"
(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(product_details_silver_output)
)

product_details_silver_df.createOrReplaceTempView("product_details_silver")

# ============================================================
# TARGET TABLE: store_information_silver
# ============================================================
store_information_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','IA','KS','MI','MN','MO','NE','ND','OH','SD','WI') THEN 'Midwest'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
      WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
      ELSE NULL
    END AS store_region,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.store_id
    ) AS rn
  FROM stores_bronze sb
  WHERE sb.store_id IS NOT NULL AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_name,
  store_location,
  store_region
FROM base
WHERE rn = 1
"""

store_information_silver_df = spark.sql(store_information_silver_sql)

store_information_silver_output = f"{TARGET_PATH}/store_information_silver.csv"
(
    store_information_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(store_information_silver_output)
)

store_information_silver_df.createOrReplaceTempView("store_information_silver")

# ============================================================
# TARGET TABLE: daily_sales_summary_silver
# ============================================================
daily_sales_summary_silver_sql = """
SELECT
  sts.date AS date,
  sts.store_id AS store_id,
  sts.product_id AS product_id,
  SUM(sts.sales_amount) AS total_sales_amount,
  SUM(sts.quantity_sold) AS total_quantity_sold
FROM sales_transactions_silver sts
GROUP BY
  sts.date,
  sts.store_id,
  sts.product_id
"""

daily_sales_summary_silver_df = spark.sql(daily_sales_summary_silver_sql)

daily_sales_summary_silver_output = f"{TARGET_PATH}/daily_sales_summary_silver.csv"
(
    daily_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(daily_sales_summary_silver_output)
)

daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

# ============================================================
# TARGET TABLE: data_quality_assurance_silver
# ============================================================
data_quality_assurance_silver_sql = """
SELECT
  CAST(stb.transaction_time AS date) AS data_date,
  COUNT(stb.transaction_id) AS total_records,
  COUNT(
    CASE
      WHEN stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
       AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
       AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
       AND CAST(stb.quantity AS int) >= 0
       AND CAST(stb.sale_amount AS double) >= 0
      THEN 1
    END
  ) AS valid_records,
  COUNT(stb.transaction_id) - COUNT(
    CASE
      WHEN stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
       AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
       AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
       AND CAST(stb.quantity AS int) >= 0
       AND CAST(stb.sale_amount AS double) >= 0
      THEN 1
    END
  ) AS invalid_records,
  COUNT(
    CASE
      WHEN stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
       AND stb.store_id IS NOT NULL AND TRIM(stb.store_id) <> ''
       AND stb.product_id IS NOT NULL AND TRIM(stb.product_id) <> ''
       AND CAST(stb.quantity AS int) >= 0
       AND CAST(stb.sale_amount AS double) >= 0
      THEN 1
    END
  ) / NULLIF(COUNT(stb.transaction_id), 0) AS quality_score
FROM sales_transactions_silver sts
LEFT JOIN sales_transactions_bronze stb
  ON sts.transaction_id = stb.transaction_id
GROUP BY CAST(stb.transaction_time AS date)
"""

data_quality_assurance_silver_df = spark.sql(data_quality_assurance_silver_sql)

data_quality_assurance_silver_output = f"{TARGET_PATH}/data_quality_assurance_silver.csv"
(
    data_quality_assurance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(data_quality_assurance_silver_output)
)

job.commit()