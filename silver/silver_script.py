import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# --------------------------------------------------------------------------------------
# Source Reads (Bronze)
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

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

# --------------------------------------------------------------------------------------
# Target: silver.sales_silver
# Mapping: bronze.sales_transactions_bronze stb
# --------------------------------------------------------------------------------------
sales_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            stb.transaction_id AS transaction_id,
            stb.store_id AS store_id,
            stb.product_id AS product_id,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            stb.sale_amount AS total_revenue,
            stb.quantity AS quantity_sold,
            stb.transaction_time AS transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.store_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND stb.transaction_time IS NOT NULL
          AND stb.sale_amount IS NOT NULL
          AND stb.quantity IS NOT NULL
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        sale_date,
        total_revenue,
        quantity_sold,
        transaction_time
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.product_master_silver
# Mapping: bronze.products_bronze pb
# --------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH filtered AS (
        SELECT
            pb.product_id AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS FLOAT) AS price,
            pb.brand AS vendor
        FROM products_bronze pb
        WHERE pb.is_active = true
          AND pb.product_id IS NOT NULL
          AND pb.product_name IS NOT NULL
          AND pb.category IS NOT NULL
          AND pb.price IS NOT NULL
          AND pb.brand IS NOT NULL
    )
    SELECT
        product_id,
        product_name,
        category,
        price,
        vendor
    FROM filtered
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.store_master_silver
# Mapping: bronze.stores_bronze sb
# --------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    SELECT
        sb.store_id AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location
    FROM stores_bronze sb
    WHERE sb.store_id IS NOT NULL
      AND sb.store_name IS NOT NULL
      AND sb.city IS NOT NULL
      AND sb.state IS NOT NULL
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

job.commit()
