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

# -------------------------
# 1) Read source tables
# -------------------------
product_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_bronze.{FILE_FORMAT}/")
)

store_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_bronze.{FILE_FORMAT}/")
)

sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)

# -------------------------
# 2) Create temp views
# -------------------------
product_bronze_df.createOrReplaceTempView("product_bronze")
store_bronze_df.createOrReplaceTempView("store_bronze")
sales_bronze_df.createOrReplaceTempView("sales_bronze")

# ======================================================================================
# Target: silver.product_silver
# ======================================================================================
product_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(pb.product_name) AS product_name,
    UPPER(TRIM(pb.category)) AS category,
    CAST(pb.price AS DECIMAL) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pb.product_id
      ORDER BY pb.product_id
    ) AS rn
  FROM product_bronze pb
)
SELECT
  product_id,
  product_name,
  category,
  price
FROM base
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

# ======================================================================================
# Target: silver.store_silver
# ======================================================================================
store_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(sb.store_name) AS store_name,
    CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
    ROW_NUMBER() OVER (
      PARTITION BY sb.store_id
      ORDER BY sb.store_id
    ) AS rn
  FROM store_bronze sb
)
SELECT
  store_id,
  store_name,
  location
FROM base
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

# ======================================================================================
# Target: silver.sales_silver
# ======================================================================================
sales_silver_sql = """
WITH joined AS (
  SELECT
    slb.transaction_id,
    slb.transaction_time,
    slb.product_id,
    slb.store_id,
    slb.quantity,
    slb.sale_amount
  FROM sales_bronze slb
  INNER JOIN product_silver ps
    ON slb.product_id = ps.product_id
  INNER JOIN store_silver ss
    ON slb.store_id = ss.store_id
  WHERE slb.quantity > 0
    AND slb.sale_amount >= 0
),
deduped AS (
  SELECT
    transaction_id,
    transaction_time,
    product_id,
    store_id,
    quantity,
    sale_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_id
    ) AS rn
  FROM joined
)
SELECT
  transaction_id AS sale_id,
  CAST(transaction_time AS DATE) AS date,
  product_id,
  store_id,
  CAST(quantity AS INT) AS quantity,
  CAST(sale_amount AS DOUBLE) AS total_price
FROM deduped
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

# ======================================================================================
# Target: silver.sales_aggregated_silver
# ======================================================================================
sales_aggregated_silver_sql = """
SELECT
  sls.date AS date,
  ps.category AS category,
  TRIM(sb.state) AS region,
  SUM(sls.total_price) AS total_sales,
  SUM(sls.quantity) AS total_quantity
FROM sales_silver sls
INNER JOIN product_silver ps
  ON sls.product_id = ps.product_id
INNER JOIN store_silver ss
  ON sls.store_id = ss.store_id
INNER JOIN store_bronze sb
  ON ss.store_id = sb.store_id
GROUP BY
  sls.date,
  ps.category,
  TRIM(sb.state)
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