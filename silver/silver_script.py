import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------------
# 3) Transform + 4) Save: product_master_silver
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS product_category,
        CAST(pb.price AS DOUBLE) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_id)
        ) AS rn
      FROM products_bronze pb
      WHERE CAST(pb.is_active AS BOOLEAN) = TRUE
    )
    SELECT
      product_id,
      product_name,
      product_category,
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
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------------
# 3) Transform + 4) Save: store_master_silver
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ',', TRIM(sb.state)) AS store_location,
        TRIM(sb.state) AS store_region,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY TRIM(sb.store_id)
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      store_location,
      store_region
    FROM base
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------------------------------
# 3) Transform + 4) Save: sales_transactions_silver
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(NULL AS STRING) AS customer_id,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY TRIM(stb.transaction_id)
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON TRIM(stb.product_id) = pms.product_id
      LEFT JOIN store_master_silver sms
        ON TRIM(stb.store_id) = sms.store_id
      WHERE pms.product_id IS NOT NULL
        AND sms.store_id IS NOT NULL
        AND COALESCE(CAST(stb.quantity AS INT), 0) >= 0
        AND COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) >= 0.0
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_date,
      sales_amount,
      quantity_sold,
      customer_id
    FROM base
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()