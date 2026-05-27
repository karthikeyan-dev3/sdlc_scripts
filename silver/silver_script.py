import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

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
# Source Reads + Temp Views
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

sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)
sales_bronze_df.createOrReplaceTempView("sales_bronze")

# -----------------------------
# TARGET: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS DOUBLE) AS price,
            pb.is_active AS is_active
        FROM products_bronze pb
        WHERE pb.is_active = 'true'
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
                    CASE WHEN product_name IS NOT NULL AND product_name <> '' THEN 0 ELSE 1 END,
                    CASE WHEN category IS NOT NULL AND category <> '' THEN 0 ELSE 1 END,
                    CASE WHEN price IS NOT NULL THEN 0 ELSE 1 END
            ) AS rn
        FROM base
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

# -----------------------------
# TARGET: silver.stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.store_id) AS store_id,
            TRIM(stb.store_name) AS store_name,
            CONCAT(TRIM(stb.city), ', ', TRIM(stb.state)) AS location,
            TRIM(stb.store_type) AS store_type
        FROM stores_bronze stb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 0 ELSE 1 END,
                    CASE WHEN location IS NOT NULL AND location <> '' THEN 0 ELSE 1 END,
                    CASE WHEN store_type IS NOT NULL AND store_type <> '' THEN 0 ELSE 1 END
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        store_type
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

# -----------------------------
# TARGET: silver.sales_silver
# -----------------------------
sales_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.transaction_id) AS sale_id,
            CAST(sb.transaction_time AS DATE) AS transaction_date,
            TRIM(sb.store_id) AS store_id,
            TRIM(sb.product_id) AS product_id,
            CAST(sb.quantity AS INT) AS quantity_sold,
            CAST(sb.sale_amount AS DOUBLE) AS revenue
        FROM sales_bronze sb
    ),
    validated AS (
        SELECT
            sale_id,
            transaction_date,
            store_id,
            product_id,
            CASE WHEN quantity_sold < 0 THEN NULL ELSE quantity_sold END AS quantity_sold,
            CASE WHEN revenue < 0 THEN NULL ELSE revenue END AS revenue
        FROM base
    ),
    joined AS (
        SELECT
            v.sale_id,
            v.transaction_date,
            v.store_id,
            v.product_id,
            v.quantity_sold,
            v.revenue
        FROM validated v
        LEFT JOIN products_silver ps
            ON v.product_id = ps.product_id
        LEFT JOIN stores_silver ss
            ON v.store_id = ss.store_id
        WHERE ps.product_id IS NOT NULL
          AND ss.store_id IS NOT NULL
    ),
    deduped AS (
        SELECT
            sale_id,
            transaction_date,
            store_id,
            product_id,
            quantity_sold,
            revenue,
            ROW_NUMBER() OVER (
                PARTITION BY sale_id
                ORDER BY
                    CASE WHEN revenue IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN quantity_sold IS NOT NULL THEN 0 ELSE 1 END
            ) AS rn
        FROM joined
    )
    SELECT
        sale_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        revenue
    FROM deduped
    WHERE rn = 1
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

job.commit()