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

# -------------------------------------------------------------------
# Read source tables from S3 (Bronze)
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# Target: silver.products_silver
# -------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(pb.product_id)) AS product_id,
            TRIM(pb.product_name) AS product_name,
            pb.category AS product_category,
            CAST(pb.is_active AS int) AS is_active
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            product_category,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY COALESCE(is_active, 0) DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        product_category
    FROM ranked
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

# -------------------------------------------------------------------
# Target: silver.stores_silver
# -------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(sb.store_id)) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(sb.city, ', ', sb.state) AS store_location,
            sb.updated_at AS updated_at
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            store_location,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY updated_at DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        store_location
    FROM ranked
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

# -------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(stb.transaction_id)) AS transaction_id,
            UPPER(TRIM(stb.product_id)) AS product_id,
            UPPER(TRIM(stb.store_id)) AS store_id,
            CAST(stb.transaction_time AS date) AS transaction_date,
            CASE
                WHEN COALESCE(CAST(stb.quantity AS int), 0) < 0 THEN 0
                ELSE COALESCE(CAST(stb.quantity AS int), 0)
            END AS quantity_sold,
            CASE
                WHEN COALESCE(CAST(stb.sale_amount AS double), 0) < 0 THEN 0
                ELSE COALESCE(CAST(stb.sale_amount AS double), 0)
            END AS total_revenue,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
    ),
    conformed AS (
        SELECT
            b.transaction_id,
            b.product_id,
            b.store_id,
            b.transaction_date,
            b.quantity_sold,
            b.total_revenue,
            b.transaction_time
        FROM base b
        LEFT JOIN products_silver ps
            ON ps.product_id = b.product_id
        LEFT JOIN stores_silver ss
            ON ss.store_id = b.store_id
    ),
    ranked AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            transaction_date,
            quantity_sold,
            total_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM conformed
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        quantity_sold,
        total_revenue
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -------------------------------------------------------------------
# Target: silver.aggregated_sales_silver
# -------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS date,
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_revenue) AS total_revenue,
        CASE
            WHEN COUNT(DISTINCT sts.transaction_id) = 0 THEN 0
            ELSE SUM(sts.total_revenue) / COUNT(DISTINCT sts.transaction_id)
        END AS avg_revenue_per_transaction
    FROM sales_transactions_silver sts
    WHERE sts.transaction_date IS NOT NULL
      AND sts.store_id IS NOT NULL
      AND sts.product_id IS NOT NULL
    GROUP BY
        sts.transaction_date,
        sts.store_id,
        sts.product_id
    """
)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()