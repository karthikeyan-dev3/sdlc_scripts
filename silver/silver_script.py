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

# ----------------------------
# Read source tables from S3
# ----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# Create temp views
# ----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

spark.sql("CREATE OR REPLACE TEMP VIEW bronze_sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze")
spark.sql("CREATE OR REPLACE TEMP VIEW bronze_products_bronze AS SELECT * FROM products_bronze")
spark.sql("CREATE OR REPLACE TEMP VIEW bronze_stores_bronze AS SELECT * FROM stores_bronze")

# ----------------------------
# Transform: sales_transactions_silver
# ----------------------------
sales_transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        stb.quantity AS quantity_sold,
        stb.sale_amount AS total_revenue,
        CASE WHEN stb.quantity > 0 THEN stb.sale_amount / stb.quantity END AS price_per_unit,
        ROW_NUMBER() OVER (
            PARTITION BY stb.transaction_id, stb.product_id, stb.store_id, stb.transaction_time
            ORDER BY stb.transaction_time DESC
        ) AS rn
    FROM bronze_sales_transactions_bronze stb
    INNER JOIN bronze_products_bronze pb
        ON stb.product_id = pb.product_id
       AND pb.is_active = true
    INNER JOIN bronze_stores_bronze sb
        ON stb.store_id = sb.store_id
)
SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    total_revenue,
    price_per_unit
FROM base
WHERE rn = 1
""")

# ----------------------------
# Write target table as SINGLE CSV file directly under TARGET_PATH
# ----------------------------
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()