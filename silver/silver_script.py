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
# 1) Read source tables (Bronze) + Create temp views
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
# 2) products_silver (Cleansed + De-duplicated)
# -------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING) AS category,
            CAST(pb.price AS DOUBLE) AS price
        FROM products_bronze pb
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
        WHERE product_id IS NOT NULL AND product_id <> ''
          AND (price IS NULL OR price >= 0)
    )
    SELECT
        product_id,
        product_name,
        category,
        CAST(price AS DOUBLE) AS price
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
# 3) stores_silver (Cleansed + De-duplicated)
# -------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
            CAST(TRIM(sb.state) AS STRING) AS store_region
        FROM stores_bronze sb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            store_location,
            store_region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_location IS NOT NULL AND store_location <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_region IS NOT NULL AND store_region <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
        WHERE store_id IS NOT NULL AND store_id <> ''
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
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
# 4) sales_transactions_silver (Cleansed + Conformed + De-duplicated, Inner Join to masters)
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(stb.transaction_time AS DATE) AS transaction_date
        FROM sales_transactions_bronze stb
    ),
    joined AS (
        SELECT
            b.transaction_id,
            b.store_id,
            b.product_id,
            b.quantity,
            b.sale_amount,
            b.transaction_time,
            b.transaction_date
        FROM base b
        INNER JOIN stores_silver ss
            ON b.store_id = ss.store_id
        INNER JOIN products_silver ps
            ON b.product_id = ps.product_id
        WHERE b.transaction_id IS NOT NULL AND b.transaction_id <> ''
          AND b.quantity > 0
          AND b.sale_amount >= 0
    ),
    ranked AS (
        SELECT
            transaction_id,
            store_id,
            product_id,
            quantity,
            sale_amount,
            transaction_time,
            transaction_date,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        transaction_date
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
# 5) daily_sales_aggregates_silver (Aggregations per store per date)
# -------------------------------------------------------------------
daily_sales_aggregates_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.transaction_date AS transaction_date,
        CAST(SUM(sts.sale_amount) AS DOUBLE) AS total_revenue_per_store,
        CAST(SUM(sts.quantity) AS INT) AS total_quantity_sold_per_store
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.transaction_date
    """
)

(
    daily_sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregates_silver.csv")
)

job.commit()