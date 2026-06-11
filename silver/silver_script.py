import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Ensure the 'bronze' database exists so we can reference bronze.<table> in SQL
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables (Bronze)
# -----------------------------
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

# Create "bronze" schema views to match UDT SQL references
spark.sql("CREATE OR REPLACE TEMP VIEW bronze_products_bronze AS SELECT * FROM products_bronze")
spark.sql("CREATE OR REPLACE TEMP VIEW bronze_stores_bronze AS SELECT * FROM stores_bronze")
spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze_sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze"
)

# Persist views as tables in the bronze database so `bronze.<table>` references work
spark.sql("CREATE OR REPLACE TABLE bronze.products_bronze AS SELECT * FROM bronze_products_bronze")
spark.sql("CREATE OR REPLACE TABLE bronze.stores_bronze AS SELECT * FROM bronze_stores_bronze")
spark.sql(
    "CREATE OR REPLACE TABLE bronze.sales_transactions_bronze AS SELECT * FROM bronze_sales_transactions_bronze"
)

# -----------------------------
# Target: products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    SELECT
      product_id,
      product_name,
      category,
      brand,
      CAST(price AS DOUBLE) AS price,
      COALESCE(is_active, TRUE) AS is_active
    FROM (
      SELECT
        pb.product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        pb.price,
        pb.is_active,
        ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
      FROM bronze.products_bronze pb
      WHERE pb.product_id IS NOT NULL
    ) x
    WHERE rn = 1
      AND is_active = TRUE
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------
# Target: stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      open_date
    FROM (
      SELECT
        sb.store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.city) AS city,
        TRIM(sb.state) AS state,
        TRIM(sb.store_type) AS store_type,
        CAST(sb.open_date AS DATE) AS open_date,
        ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
      FROM bronze.stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    ) x
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

# -----------------------------
# Target: sales_transactions_silver
# -----------------------------
sales_transactions_silver_df = spark.sql(
    """
    SELECT
      transaction_id,
      store_id,
      product_id,
      quantity,
      sale_amount,
      transaction_time,
      CAST(transaction_time AS DATE) AS sales_date
    FROM (
      SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
      FROM bronze.sales_transactions_bronze stb
      WHERE stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.transaction_time IS NOT NULL
        AND stb.quantity IS NOT NULL
        AND stb.quantity > 0
        AND stb.sale_amount IS NOT NULL
        AND stb.sale_amount >= 0
    ) x
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
