import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# 3) Transform and write: silver.products_silver
# -----------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS product_category,
            CAST(pb.price AS FLOAT) AS product_price
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    )
    SELECT
        product_id,
        product_name,
        product_category,
        CASE
            WHEN product_price IS NULL THEN NULL
            WHEN product_price < 0 THEN CAST(0 AS FLOAT)
            ELSE product_price
        END AS product_price
    FROM base
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

# -----------------------------------------------------------------------------------
# 4) Transform and write: silver.stores_silver
# -----------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    SELECT
        TRIM(sb.store_id) AS store_id,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
        TRIM(sb.store_type) AS store_type
    FROM stores_bronze sb
    WHERE TRIM(sb.store_id) IS NOT NULL
      AND TRIM(sb.store_id) <> ''
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

# -----------------------------------------------------------------------------------
# 5) Transform and write: silver.sales_transactions_silver
# -----------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
            TRIM(stb.store_id) AS store_id,
            TRIM(stb.product_id) AS product_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
        FROM sales_transactions_bronze stb
        LEFT JOIN products_silver ps
            ON TRIM(stb.product_id) = ps.product_id
        LEFT JOIN stores_silver ss
            ON TRIM(stb.store_id) = ss.store_id
    ),
    filtered AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            CASE
                WHEN quantity_sold IS NULL THEN NULL
                WHEN quantity_sold < 0 THEN CAST(0 AS INT)
                ELSE quantity_sold
            END AS quantity_sold,
            CASE
                WHEN total_sales_amount IS NULL THEN NULL
                WHEN total_sales_amount < 0 THEN CAST(0 AS DOUBLE)
                ELSE total_sales_amount
            END AS total_sales_amount,
            transaction_time_ts
        FROM joined
        WHERE transaction_id IS NOT NULL
          AND transaction_id <> ''
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            quantity_sold,
            total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time_ts DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_sales_amount
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

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# 6) Transform and write: silver.sales_validation_silver
# -----------------------------------------------------------------------------------
sales_validation_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        (
            sts.transaction_id IS NOT NULL
            AND sts.store_id IS NOT NULL
            AND sts.product_id IS NOT NULL
            AND sts.quantity_sold > 0
            AND sts.total_sales_amount >= 0
        ) AS validated_transaction,
        'cleansed_and_validated' AS clean_history
    FROM sales_transactions_silver sts
    """
)

(
    sales_validation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_validation_silver.csv")
)

sales_validation_silver_df.createOrReplaceTempView("sales_validation_silver")

# -----------------------------------------------------------------------------------
# 7) Transform and write: silver.sales_aggregation_silver
# -----------------------------------------------------------------------------------
sales_aggregation_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS date,
        sts.store_id AS store_id,
        ps.product_category AS product_category,
        SUM(sts.total_sales_amount) AS total_sales_amount,
        SUM(sts.quantity_sold) AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        sts.transaction_date,
        sts.store_id,
        ps.product_category
    """
)

(
    sales_aggregation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregation_silver.csv")
)