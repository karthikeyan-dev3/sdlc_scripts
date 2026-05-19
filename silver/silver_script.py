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
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------
# Read source tables (bronze)
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
# Target: silver.products_silver
# ------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.product_id AS string) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS float) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY pb.product_id
            ) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND TRIM(pb.product_id) <> ''
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
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.stores_silver
# ------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS string) AS store_id,
            TRIM(sb.store_name) AS store_name,
            COALESCE(TRIM(sb.state), 'UNKNOWN') AS region,
            TRIM(sb.store_type) AS store_type,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.store_id
            ) AS rn
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        region,
        store_type
    FROM base
    WHERE rn = 1
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

# ------------------------------------------------------------
# Target: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS date) AS transaction_date,
            ss.store_id AS store_id,
            ps.product_id AS product_id,
            CAST(stb.quantity AS int) AS quantity_sold,
            CAST(stb.sale_amount AS double) AS sales_amount,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON stb.product_id = ps.product_id
        LEFT JOIN stores_silver ss
            ON stb.store_id = ss.store_id
        WHERE stb.transaction_id IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
          AND CAST(stb.quantity AS int) > 0
          AND CAST(stb.sale_amount AS double) >= 0
          AND ps.product_id IS NOT NULL
          AND ss.store_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            quantity_sold,
            sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount
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

job.commit()
