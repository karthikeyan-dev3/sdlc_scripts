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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------
# Read Source Tables (Bronze)
# -----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# -----------------------------
# Target: silver.stores_silver
# -----------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            CAST(sb.store_name AS STRING) AS store_name,
            CAST(sb.city AS STRING) AS city,
            CAST(sb.state AS STRING) AS state,
            CAST(sb.store_type AS STRING) AS store_type,
            CAST(sb.open_date AS DATE) AS open_date,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.store_id
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        open_date
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
# Target: silver.products_silver
# -----------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            CAST(pb.product_name AS STRING) AS product_name,
            CAST(pb.category AS STRING) AS category_name,
            CAST(pb.brand AS STRING) AS brand,
            CAST(pb.price AS DOUBLE) AS price,
            CAST(pb.is_active AS BOOLEAN) AS is_active,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY pb.product_id
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        category_name,
        brand,
        price,
        is_active
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
# Target: silver.categories_silver
# -----------------------------
categories_silver_df = spark.sql(
    """
    WITH distinct_cats AS (
        SELECT DISTINCT
            CAST(ps.category_name AS STRING) AS category_name
        FROM products_silver ps
        WHERE ps.category_name IS NOT NULL
    )
    SELECT
        CAST(ABS(HASH(category_name)) AS BIGINT) AS category_id,
        category_name
    FROM distinct_cats
    """
)
categories_silver_df.createOrReplaceTempView("categories_silver")

(
    categories_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/categories_silver.csv")
)

# -----------------------------
# Target: silver.transactions_silver
# -----------------------------
transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(tb.transaction_id AS STRING) AS transaction_id,
            CAST(tb.store_id AS STRING) AS store_id,
            CAST(tb.product_id AS STRING) AS product_id,
            CAST(tb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(tb.transaction_time AS DATE) AS transaction_date,
            CAST(tb.quantity AS INT) AS quantity,
            CAST(tb.sale_amount AS DOUBLE) AS sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY tb.transaction_id
                ORDER BY tb.transaction_id
            ) AS rn
        FROM transactions_bronze tb
        INNER JOIN stores_silver ss
            ON tb.store_id = ss.store_id
        INNER JOIN products_silver ps
            ON tb.product_id = ps.product_id
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_time,
        transaction_date,
        quantity,
        sale_amount
    FROM ranked
    WHERE rn = 1
    """
)
transactions_silver_df.createOrReplaceTempView("transactions_silver")

(
    transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transactions_silver.csv")
)

# -----------------------------
# Target: silver.sales_store_day_silver
# -----------------------------
sales_store_day_silver_df = spark.sql(
    """
    SELECT
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(ts.transaction_date AS DATE) AS transaction_date,
        CAST(SUM(ts.sale_amount) AS DOUBLE) AS total_revenue,
        CAST(COUNT(DISTINCT ts.transaction_id) AS BIGINT) AS transaction_count,
        CAST(SUM(ts.quantity) AS BIGINT) AS quantities_sold,
        CURRENT_DATE AS data_refresh_date
    FROM transactions_silver ts
    GROUP BY
        ts.store_id,
        ts.transaction_date
    """
)
sales_store_day_silver_df.createOrReplaceTempView("sales_store_day_silver")

(
    sales_store_day_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_store_day_silver.csv")
)

# -----------------------------
# Target: silver.sales_product_day_silver
# -----------------------------
sales_product_day_silver_df = spark.sql(
    """
    SELECT
        CAST(ts.product_id AS STRING) AS product_id,
        CAST(cs.category_id AS BIGINT) AS category_id,
        CAST(ts.transaction_date AS DATE) AS transaction_date,
        CAST(SUM(ts.sale_amount) AS DOUBLE) AS product_revenue,
        CAST(SUM(ts.quantity) AS BIGINT) AS quantities_sold,
        CURRENT_DATE AS data_refresh_date
    FROM transactions_silver ts
    INNER JOIN products_silver ps
        ON ts.product_id = ps.product_id
    INNER JOIN categories_silver cs
        ON ps.category_name = cs.category_name
    GROUP BY
        ts.product_id,
        cs.category_id,
        ts.transaction_date
    """
)
sales_product_day_silver_df.createOrReplaceTempView("sales_product_day_silver")

(
    sales_product_day_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_product_day_silver.csv")
)

job.commit()
