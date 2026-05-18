import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# Read source tables (Bronze)
# ------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# Target: silver.product_master_silver
# ------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        pb.product_id AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        CAST(pb.price AS DECIMAL(10,2)) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY
            CASE WHEN pb.product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN pb.category IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      category,
      price
    FROM base
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver/")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------
# Target: silver.store_master_silver
# ------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        sb.store_id AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(COALESCE(TRIM(sb.city), ''), ', ', COALESCE(TRIM(sb.state), '')) AS location,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY
            CASE WHEN sb.store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN sb.city IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN sb.state IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location
    FROM base
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver/")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS revenue,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON stb.product_id = pms.product_id
      LEFT JOIN store_master_silver sms
        ON stb.store_id = sms.store_id
      WHERE stb.transaction_id IS NOT NULL
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      quantity,
      revenue,
      transaction_date
    FROM base
    WHERE rn = 1
      AND quantity >= 0
      AND revenue >= 0
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver/")
)

job.commit()
