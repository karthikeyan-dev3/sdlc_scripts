
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

# --------------------------------------------------------------------------------
# 1) sales_transactions_silver (source: bronze.sales_transactions_bronze)
# --------------------------------------------------------------------------------
stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(stb.transaction_id)) AS transaction_id,
            UPPER(TRIM(stb.store_id)) AS store_id,
            UPPER(TRIM(stb.product_id)) AS product_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            COALESCE(CAST(stb.quantity AS INT), 0) AS quantity_sold,
            COALESCE(CAST(stb.sale_amount AS DOUBLE), 0D) AS total_sales_amount,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            store_id,
            product_id,
            transaction_date,
            quantity_sold,
            total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        total_sales_amount
    FROM dedup
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

# --------------------------------------------------------------------------------
# 2) product_master_silver (source: bronze.products_bronze)
# --------------------------------------------------------------------------------
pb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
pb_df.createOrReplaceTempView("products_bronze")

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(pb.product_id)) AS product_id,
            NULLIF(TRIM(pb.product_name), '') AS product_name,
            NULLIF(TRIM(pb.category), '') AS category,
            CASE
                WHEN CAST(pb.price AS DOUBLE) < 0 THEN NULL
                ELSE CAST(pb.price AS DOUBLE)
            END AS price,
            pb.is_active AS is_active
        FROM products_bronze pb
    ),
    filtered AS (
        SELECT
            product_id,
            product_name,
            category,
            price
        FROM base
        WHERE is_active = TRUE OR is_active IS NULL
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC,
                    product_name DESC,
                    category DESC,
                    price DESC
            ) AS rn
        FROM filtered
        WHERE product_id IS NOT NULL
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM dedup
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# --------------------------------------------------------------------------------
# 3) store_master_silver (source: bronze.stores_bronze)
# --------------------------------------------------------------------------------
sb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
sb_df.createOrReplaceTempView("stores_bronze")

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(sb.store_id)) AS store_id,
            NULLIF(TRIM(sb.store_name), '') AS store_name,
            CONCAT(UPPER(TRIM(sb.state)), '-', UPPER(TRIM(sb.city))) AS region,
            sb.open_date AS opening_date
        FROM stores_bronze sb
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            region,
            opening_date,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    opening_date DESC,
                    CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN region IS NOT NULL THEN 1 ELSE 0 END DESC,
                    store_name DESC,
                    region DESC
            ) AS rn
        FROM base
        WHERE store_id IS NOT NULL
    )
    SELECT
        store_id,
        store_name,
        region,
        opening_date
    FROM dedup
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# --------------------------------------------------------------------------------
# 4) daily_sales_aggregated_silver (source: silver.sales_transactions_silver)
# --------------------------------------------------------------------------------
daily_sales_aggregated_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.transaction_date AS transaction_date,
        SUM(sts.total_sales_amount) AS total_revenue,
        SUM(sts.quantity_sold) AS units_sold
    FROM sales_transactions_silver sts
    WHERE sts.store_id IS NOT NULL
      AND sts.product_id IS NOT NULL
      AND sts.transaction_date IS NOT NULL
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.transaction_date
    """
)
daily_sales_aggregated_silver_df.createOrReplaceTempView("daily_sales_aggregated_silver")

(
    daily_sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregated_silver.csv")
)

job.commit()
