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

# ============================================================
# Read Source Tables (Bronze)
# ============================================================
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

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_df = spark.sql(
    """
    SELECT
        CAST(TRIM(pb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pb.product_name) AS STRING) AS product_name,
        CAST(UPPER(TRIM(pb.category)) AS STRING) AS category,
        CAST(
            CASE
                WHEN CAST(pb.price AS DOUBLE) < 0 THEN NULL
                ELSE CAST(pb.price AS DOUBLE)
            END AS DOUBLE
        ) AS price
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

# ============================================================
# Target: silver.stores_silver
# ============================================================
stores_silver_df = spark.sql(
    """
    SELECT
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sb.store_name) AS STRING) AS store_name,
        CAST(TRIM(sb.state) AS STRING) AS region,
        CAST(UPPER(TRIM(sb.store_type)) AS STRING) AS store_type
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

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts,
            ps.product_id AS conformed_product_id,
            ss.store_id AS conformed_store_id
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON TRIM(stb.product_id) = ps.product_id
        LEFT JOIN stores_silver ss
            ON TRIM(stb.store_id) = ss.store_id
    ),
    ranked AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            transaction_date,
            CAST(
                CASE
                    WHEN sales_amount < 0 THEN NULL
                    ELSE sales_amount
                END AS DOUBLE
            ) AS sales_amount,
            CAST(
                CASE
                    WHEN quantity_sold < 0 THEN NULL
                    ELSE quantity_sold
                END AS INT
            ) AS quantity_sold,
            CAST(
                CASE
                    WHEN transaction_id IS NOT NULL
                         AND product_id IS NOT NULL
                         AND store_id IS NOT NULL
                         AND transaction_date IS NOT NULL
                         AND conformed_product_id IS NOT NULL
                         AND conformed_store_id IS NOT NULL
                    THEN TRUE
                    ELSE FALSE
                END AS BOOLEAN
            ) AS cleaned_data_flag,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time_ts DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        sales_amount,
        quantity_sold,
        cleaned_data_flag
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

# ============================================================
# Target: silver.aggregated_sales_silver
# ============================================================
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(date_format(sts.transaction_date, 'yyyy-MM') AS STRING) AS reporting_period,
        CAST(SUM(sts.sales_amount) AS DOUBLE) AS total_sales_amount,
        CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    WHERE sts.cleaned_data_flag = TRUE
    GROUP BY
        sts.store_id,
        sts.product_id,
        date_format(sts.transaction_date, 'yyyy-MM')
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