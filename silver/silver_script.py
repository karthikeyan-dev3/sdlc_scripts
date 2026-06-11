import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# ----------------------------
# Read source tables from S3
# ----------------------------
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

# ----------------------------
# Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------
# dim_product_silver
# ----------------------------
dim_product_silver_sql = """
SELECT
  product_id,
  product_name,
  brand,
  category
FROM (
  SELECT
    TRIM(pb.product_id) AS product_id,
    NULLIF(TRIM(pb.product_name), '') AS product_name,
    NULLIF(TRIM(pb.brand), '') AS brand,
    NULLIF(TRIM(pb.category), '') AS category,
    COALESCE(pb.is_active, TRUE) AS is_active,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_id)
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND COALESCE(pb.is_active, TRUE) = TRUE
) x
WHERE x.rn = 1
"""

dim_product_silver_df = spark.sql(dim_product_silver_sql)

(
    dim_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_product_silver.csv")
)

# ----------------------------
# dim_store_silver
# ----------------------------
dim_store_silver_sql = """
SELECT
  store_id,
  store_name,
  city,
  state
FROM (
  SELECT
    TRIM(sb.store_id) AS store_id,
    NULLIF(TRIM(sb.store_name), '') AS store_name,
    NULLIF(TRIM(sb.city), '') AS city,
    NULLIF(TRIM(sb.state), '') AS state,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY TRIM(sb.store_id)
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
) x
WHERE x.rn = 1
"""

dim_store_silver_df = spark.sql(dim_store_silver_sql)

(
    dim_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_store_silver.csv")
)

# ----------------------------
# sales_transactions_silver
# ----------------------------
sales_transactions_silver_sql = """
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity,
  sale_amount,
  transaction_time
FROM (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    TRIM(stb.store_id) AS store_id,
    TRIM(stb.product_id) AS product_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
    CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.store_id) IS NOT NULL
    AND TRIM(stb.product_id) IS NOT NULL
    AND CAST(stb.quantity AS INT) IS NOT NULL
    AND CAST(stb.quantity AS INT) > 0
    AND CAST(stb.sale_amount AS DOUBLE) IS NOT NULL
    AND CAST(stb.sale_amount AS DOUBLE) >= 0
    AND CAST(stb.transaction_time AS TIMESTAMP) IS NOT NULL
) x
WHERE x.rn = 1
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()