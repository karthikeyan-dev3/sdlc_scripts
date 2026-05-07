import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Source Reads (S3) + Temp Views
# -------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# -------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            stb.transaction_id,
            stb.store_id,
            stb.product_id,
            stb.transaction_time,
            stb.quantity,
            stb.sale_amount
        FROM sales_transactions_bronze stb
        INNER JOIN products_bronze pb
            ON stb.product_id = pb.product_id
           AND pb.is_active = true
        INNER JOIN stores_bronze sb
            ON stb.store_id = sb.store_id
    ),
    dedup AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id, product_id, store_id, transaction_time
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id AS transaction_id,
        store_id AS store_id,
        product_id AS product_id,
        CAST(transaction_time AS DATE) AS transaction_date,
        quantity AS quantity_sold,
        sale_amount AS total_revenue,
        CASE WHEN quantity > 0 THEN sale_amount / quantity END AS price_per_unit
    FROM dedup
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