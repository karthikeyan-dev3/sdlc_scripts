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
# 1) Read source tables from S3
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
# 2) products_silver
# ============================================================

products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            TRIM(pb.brand) AS brand,
            CAST(pb.price AS DOUBLE) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY
                    TRIM(pb.product_name) ASC,
                    TRIM(pb.category) ASC,
                    TRIM(pb.brand) ASC
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        CAST(price AS FLOAT) AS price
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

# ============================================================
# 3) stores_silver
# ============================================================

stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            TRIM(sb.state) AS region,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY
                    TRIM(sb.store_name) ASC,
                    TRIM(sb.city) ASC,
                    TRIM(sb.state) ASC
            ) AS rn
        FROM stores_bronze sb
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
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ============================================================
# 4) sales_transactions_silver
# ============================================================

sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS transaction_date,
            CAST(stb.store_id AS STRING) AS store_id,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        LEFT JOIN stores_silver ss
            ON stb.store_id = ss.store_id
        LEFT JOIN products_silver ps
            ON stb.product_id = ps.product_id
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount,
        CAST(NULL AS STRING) AS customer_id
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
# 5) aggregated_sales_silver
# ============================================================

aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.transaction_date AS aggregation_date,
        CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
        CAST(SUM(sts.sales_amount) AS DOUBLE) AS total_sales_amount,
        CAST(
            CASE
                WHEN SUM(sts.quantity_sold) > 0
                    THEN SUM(sts.sales_amount) / SUM(sts.quantity_sold)
            END
            AS DOUBLE
        ) AS average_price
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.transaction_date
    """
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

# ============================================================
# 6) sales_data_quality_silver
# ============================================================

sales_data_quality_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_date AS data_date,
        CAST(COUNT(sts.transaction_id) AS INT) AS total_records,
        CAST(COUNT(DISTINCT sts.transaction_id) - COUNT(sts.transaction_id) AS INT) AS duplicate_records,
        CAST(
            SUM(
                CASE
                    WHEN sts.transaction_id IS NULL
                      OR sts.store_id IS NULL
                      OR sts.product_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS INT
        ) AS missing_identifiers,
        CAST(
            100 * (
                1 - (
                    SUM(
                        CASE
                            WHEN sts.transaction_id IS NULL
                              OR sts.store_id IS NULL
                              OR sts.product_id IS NULL
                            THEN 1 ELSE 0
                        END
                    ) / NULLIF(COUNT(*), 0)
                )
            )
            AS DOUBLE
        ) AS accuracy_percentage
    FROM sales_transactions_silver sts
    GROUP BY
        sts.transaction_date
    """
)
sales_data_quality_silver_df.createOrReplaceTempView("sales_data_quality_silver")

(
    sales_data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_data_quality_silver.csv")
)

job.commit()