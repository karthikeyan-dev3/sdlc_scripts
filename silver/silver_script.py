
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
# 1) Read source tables from S3 (Bronze) and create temp views
# ------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# 2) product_master_silver (pms) - transform + dedup by product_id
# ------------------------------------------------------------------------------

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS DOUBLE) AS price
        FROM products_bronze pb
    ),
    filtered AS (
        SELECT
            product_id,
            product_name,
            category,
            CASE
                WHEN price < 0 THEN NULL
                ELSE price
            END AS price
        FROM base
        WHERE product_id IS NOT NULL AND product_id <> ''
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
        FROM filtered
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

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------
# 3) store_master_silver (sms) - transform + dedup by store_id
# ------------------------------------------------------------------------------

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            TRIM(sb.store_name) AS store_name,
            TRIM(sb.city) AS city,
            TRIM(sb.state) AS state
        FROM stores_bronze sb
    ),
    derived AS (
        SELECT
            store_id,
            store_name,
            CONCAT(COALESCE(city, ''), ', ', COALESCE(state, '')) AS location,
            CASE
                WHEN UPPER(state) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
                WHEN UPPER(state) IN ('OH','IN','IL','MI','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
                WHEN UPPER(state) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'SOUTH'
                WHEN UPPER(state) IN ('MT','ID','WY','CO','NM','AZ','UT','NV','WA','OR','CA','AK','HI') THEN 'WEST'
                ELSE NULL
            END AS region
        FROM base
        WHERE store_id IS NOT NULL AND store_id <> ''
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN location IS NOT NULL AND location <> '' THEN 1 ELSE 0 END DESC,
                    CASE WHEN region IS NOT NULL AND region <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM derived
    )
    SELECT
        store_id,
        store_name,
        location,
        region
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------------------------
# 4) sales_transactions_silver (sts) - transform + enforce valid keys + dedup
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sale_date,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sale_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
        FROM sales_transactions_bronze stb
    ),
    filtered AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            quantity_sold,
            total_sale_amount,
            transaction_time_ts
        FROM base
        WHERE
            transaction_id IS NOT NULL AND transaction_id <> ''
            AND product_id IS NOT NULL AND product_id <> ''
            AND store_id IS NOT NULL AND store_id <> ''
            AND quantity_sold > 0
            AND total_sale_amount >= 0
    ),
    ranked AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            quantity_sold,
            total_sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    transaction_time_ts DESC,
                    CASE WHEN sale_date IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN quantity_sold IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN total_sale_amount IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sale_date,
        quantity_sold,
        total_sale_amount
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

# ------------------------------------------------------------------------------
# 5) sales_daily_agg_silver (sdas) - aggregate from silver.sales_transactions_silver
# ------------------------------------------------------------------------------

sales_daily_agg_silver_df = spark.sql(
    """
    SELECT
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        CAST(SUM(sts.total_sale_amount) AS DOUBLE) AS total_revenue,
        CAST(SUM(sts.quantity_sold) AS INT) AS quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.sale_date
    """
)

(
    sales_daily_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_agg_silver.csv")
)

job.commit()
