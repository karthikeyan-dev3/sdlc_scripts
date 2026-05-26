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

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------
# products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(product_id AS STRING)) AS product_id,
            TRIM(CAST(product_name AS STRING)) AS product_name,
            TRIM(CAST(category AS STRING)) AS category,
            TRIM(CAST(brand AS STRING)) AS brand,
            CAST(price AS FLOAT) AS price,
            CAST(is_active AS STRING) AS is_active
        FROM products_bronze
    ),
    cleaned AS (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            CASE
                WHEN price < 0 THEN CAST(0.0 AS FLOAT)
                ELSE price
            END AS price
        FROM base
        WHERE UPPER(TRIM(COALESCE(is_active, ''))) IN ('Y', 'YES', 'TRUE', '1')
    ),
    dedup AS (
        SELECT
            product_id,
            MAX(product_name) AS product_name,
            MAX(category) AS category,
            MAX(brand) AS brand,
            MAX(price) AS price
        FROM cleaned
        GROUP BY product_id
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price
    FROM dedup
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

# -----------------------------
# stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(store_id AS STRING)) AS store_id,
            TRIM(CAST(store_name AS STRING)) AS store_name,
            TRIM(CAST(state AS STRING)) AS state,
            TRIM(CAST(store_type AS STRING)) AS store_type,
            CAST(open_date AS DATE) AS open_date
        FROM stores_bronze
    ),
    enriched AS (
        SELECT
            store_id,
            store_name,
            CASE
                WHEN UPPER(state) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
                WHEN UPPER(state) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL') THEN 'SOUTHEAST'
                WHEN UPPER(state) IN ('KY','TN','AL','MS','AR','LA','OK','TX') THEN 'SOUTH'
                WHEN UPPER(state) IN ('OH','MI','IN','IL','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
                WHEN UPPER(state) IN ('MT','ID','WY','CO','NM','AZ','UT','NV','WA','OR','CA','AK','HI') THEN 'WEST'
                ELSE state
            END AS region,
            store_type,
            open_date
        FROM base
    ),
    dedup AS (
        SELECT
            store_id,
            MAX(store_name) AS store_name,
            MAX(region) AS region,
            MAX(store_type) AS store_type,
            MAX(open_date) AS open_date
        FROM enriched
        GROUP BY store_id
    )
    SELECT
        store_id,
        store_name,
        region,
        store_type,
        open_date
    FROM dedup
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

# -----------------------------
# sales_transactions_silver
# -----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(stb.transaction_id AS STRING)) AS transaction_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            TRIM(CAST(stb.store_id AS STRING)) AS store_id,
            TRIM(CAST(stb.product_id AS STRING)) AS product_id,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS sale_amount
        FROM sales_transactions_bronze stb
    ),
    conformed AS (
        SELECT
            b.transaction_id,
            CAST(b.transaction_time AS DATE) AS transaction_date,
            b.store_id,
            b.product_id,
            CASE
                WHEN b.quantity < 0 THEN CAST(0 AS INT)
                ELSE b.quantity
            END AS quantity_sold,
            CASE
                WHEN b.sale_amount < 0 THEN CAST(0.0 AS DOUBLE)
                ELSE b.sale_amount
            END AS total_revenue,
            CAST('UNKNOWN' AS STRING) AS payment_type,
            b.transaction_time
        FROM base b
        LEFT JOIN stores_silver ss
            ON b.store_id = ss.store_id
        LEFT JOIN products_silver ps
            ON b.product_id = ps.product_id
        WHERE ss.store_id IS NOT NULL
          AND ps.product_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            quantity_sold,
            total_revenue,
            payment_type,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM conformed
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_revenue,
        payment_type
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