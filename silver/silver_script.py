import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------
# Read source tables (bronze)
# -------------------------
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
# 1) Read source tables -> temp views already created
# 2) SQL transformations + de-dup using ROW_NUMBER
# 3) Save output
# ============================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS product_category,
            TRIM(pb.brand) AS brand,
            CASE WHEN CAST(pb.price AS DOUBLE) >= 0 THEN CAST(pb.price AS DOUBLE) ELSE 0 END AS price
        FROM products_bronze pb
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            product_category,
            brand,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    (CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN product_category IS NOT NULL AND product_category <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN brand IS NOT NULL AND brand <> '' THEN 1 ELSE 0 END) DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        product_category,
        brand,
        price
    FROM dedup
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

# ============================================================
# Target: silver.stores_silver
# 1) Read source tables -> temp views already created
# 2) SQL transformations + de-dup using ROW_NUMBER
# 3) Save output
# ============================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
            sb.state AS store_region
        FROM stores_bronze sb
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            store_location,
            store_region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    (CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN store_location IS NOT NULL AND store_location <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN store_region IS NOT NULL AND store_region <> '' THEN 1 ELSE 0 END) DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
    FROM dedup
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

# ============================================================
# Target: silver.sales_transactions_silver
# 1) Read source tables -> temp views already created
# 2) SQL transformations + joins to silver masters + de-dup using ROW_NUMBER
# 3) Save output
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CASE WHEN CAST(stb.quantity AS INT) > 0 THEN CAST(stb.quantity AS INT) ELSE 0 END AS quantity_sold,
            CASE WHEN CAST(stb.sale_amount AS DOUBLE) >= 0 THEN CAST(stb.sale_amount AS DOUBLE) ELSE 0 END AS sales_amount,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON TRIM(stb.product_id) = ps.product_id
        LEFT JOIN stores_silver ss
            ON TRIM(stb.store_id) = ss.store_id
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            product_id,
            store_id,
            quantity_sold,
            sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount
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

# ============================================================
# Target: silver.sales_summary_silver
# 1) Read source tables -> temp views already created
# 2) SQL transformations (group by + SUM)
# 3) Save output
# ============================================================
sales_summary_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS date,
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.sales_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
        sts.transaction_date,
        sts.store_id,
        sts.product_id
    """
)

(
    sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_summary_silver.csv")
)

job.commit()