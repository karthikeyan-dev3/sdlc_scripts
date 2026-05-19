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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read source tables (Bronze)
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

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# -----------------------------
# Target: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    SELECT
        pb.product_id AS product_id,
        pb.category   AS category
    FROM products_bronze pb
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# -----------------------------
# Target: silver.stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    SELECT
        sb.store_id AS store_id,
        CONCAT(sb.city, ', ', sb.state) AS store_location
    FROM stores_bronze sb
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------------------
# Target: silver.sales_transactions_silver
# -----------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            tb.transaction_id AS transaction_id,
            CAST(tb.transaction_time AS date) AS clean_transaction_date,
            tb.store_id AS validated_store_id,
            tb.product_id AS validated_product_id,
            CAST(tb.quantity AS int) AS quantity_sold,
            CAST(tb.sale_amount AS double) AS total_revenue,
            CASE
                WHEN COUNT(*) OVER (PARTITION BY tb.transaction_id) > 1 THEN true
                ELSE false
            END AS deduplicated_records,
            ROW_NUMBER() OVER (PARTITION BY tb.transaction_id ORDER BY tb.transaction_id) AS rn
        FROM transactions_bronze tb
        INNER JOIN products_silver ps
            ON tb.product_id = ps.product_id
        INNER JOIN stores_silver ss
            ON tb.store_id = ss.store_id
    )
    SELECT
        transaction_id,
        clean_transaction_date,
        validated_store_id,
        validated_product_id,
        quantity_sold,
        total_revenue,
        deduplicated_records
    FROM joined
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()