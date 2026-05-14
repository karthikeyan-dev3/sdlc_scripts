import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables (bronze)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# 3) Transform + Write: silver.product_attributes_silver
# ------------------------------------------------------------
product_attributes_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            CAST(pb.price AS FLOAT) AS price
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
        FROM base
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

(
    product_attributes_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_attributes_silver.csv")
)

product_attributes_silver_df.createOrReplaceTempView("sales_product_attributes_silver_ignore")  # no-op view placeholder not used

# ------------------------------------------------------------
# 3) Transform + Write: silver.store_attributes_silver
# ------------------------------------------------------------
store_attributes_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            TRIM(sb.store_type) AS store_size,
            TRIM(sb.state) AS region
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            location,
            store_size,
            region,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        store_size,
        region
    FROM dedup
    WHERE rn = 1
    """
)

(
    store_attributes_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_attributes_silver.csv")
)

# ------------------------------------------------------------
# 3) Transform + Write: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND stb.store_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            product_id,
            store_id,
            quantity_sold,
            sales_amount,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) AS rn
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

# ------------------------------------------------------------
# 3) Transform + Write: silver.data_quality_silver
# ------------------------------------------------------------
data_quality_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS validation_date,
        sts.transaction_id AS dataset_name,
        COUNT(stb.transaction_id) AS total_records,
        COUNT(
            CASE
                WHEN stb.transaction_id IS NULL
                  OR stb.product_id IS NULL
                  OR stb.store_id IS NULL
                  OR CAST(stb.quantity AS INT) <= 0
                  OR CAST(stb.sale_amount AS DOUBLE) < 0
                  OR stb.transaction_time IS NULL
                THEN 1
            END
        ) AS invalid_records,
        (COUNT(stb.transaction_id) - COUNT(sts.transaction_id)) AS duplicate_records,
        (
            1 - (
                (
                    COUNT(
                        CASE
                            WHEN stb.transaction_id IS NULL
                              OR stb.product_id IS NULL
                              OR stb.store_id IS NULL
                              OR CAST(stb.quantity AS INT) <= 0
                              OR CAST(stb.sale_amount AS DOUBLE) < 0
                              OR stb.transaction_time IS NULL
                            THEN 1
                        END
                    )
                    + (COUNT(stb.transaction_id) - COUNT(sts.transaction_id))
                ) / COUNT(stb.transaction_id)
            )
        ) AS data_quality_score
    FROM sales_transactions_silver sts
    LEFT JOIN sales_transactions_bronze stb
        ON sts.transaction_id = stb.transaction_id
    GROUP BY
        sts.transaction_date,
        sts.transaction_id
    """
)

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_silver.csv")
)

job.commit()