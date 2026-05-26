import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# =========================
# Read Source Tables (Bronze)
# =========================
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

# =========================
# Target: silver.products_silver
# =========================
products_silver_df = spark.sql(
    """
    SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        CAST(pb.price AS FLOAT) AS price
    FROM products_bronze pb
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# =========================
# Target: silver.stores_silver
# =========================
stores_silver_df = spark.sql(
    """
    SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        CONCAT(sb.city, ',', sb.state) AS store_location,
        sb.state AS region
    FROM stores_bronze sb
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# =========================
# Target: silver.sales_transactions_silver
# =========================
sales_transactions_silver_df = spark.sql(
    """
    SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CAST(stb.transaction_time AS DATE) AS sale_date,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_sales_revenue
    FROM sales_transactions_bronze stb
    INNER JOIN stores_silver ss
        ON stb.store_id = ss.store_id
    INNER JOIN products_silver ps
        ON stb.product_id = ps.product_id
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
