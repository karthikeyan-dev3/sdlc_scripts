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

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: products_silver
# ------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            pb.product_id AS product_id,
            pb.product_name AS product_name,
            pb.category AS category,
            pb.brand AS brand,
            CAST(pb.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY pb.product_id DESC
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price
    FROM ranked
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: stores_silver
# ------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            sb.store_id AS store_id,
            sb.store_name AS store_name,
            CONCAT(sb.city, ', ', sb.state) AS store_location,
            sb.state AS region,
            sb.store_name AS store_manager,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.store_id DESC
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        store_location,
        region,
        store_manager
    FROM ranked
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

stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: sales_transactions_silver
# ------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            COALESCE(CAST(stb.quantity AS INT), 0) AS quantity_sold,
            COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) AS total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN products_silver ps
            ON stb.product_id = ps.product_id
        INNER JOIN stores_silver ss
            ON stb.store_id = ss.store_id
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
    FROM ranked
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