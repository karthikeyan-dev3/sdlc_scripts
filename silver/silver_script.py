import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables (Bronze)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)

store_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views (Bronze)
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")
store_details_bronze_df.createOrReplaceTempView("store_details_bronze")

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            TRIM(stb.store_id)       AS store_id,
            TRIM(stb.product_id)     AS product_id,
            DATE(stb.transaction_time) AS transaction_date,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
            CAST(stb.quantity AS INT)       AS quantity_sold,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
    ),
    ranked AS (
        SELECT
            transaction_id,
            store_id,
            product_id,
            transaction_date,
            sales_amount,
            quantity_sold,
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
        sales_amount,
        quantity_sold
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: silver.product_master_silver
# ============================================================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pdb.product_id)   AS product_id,
            TRIM(pdb.product_name) AS product_name,
            pdb.category           AS product_category,
            CAST(pdb.price AS FLOAT) AS product_price
        FROM product_details_bronze pdb
        WHERE pdb.is_active = true
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            product_category,
            product_price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_name DESC, product_category DESC, product_price DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_price
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# Target: silver.store_master_silver
# ============================================================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sdb.store_id)   AS store_id,
            TRIM(sdb.store_name) AS store_name,
            CONCAT_WS(', ', sdb.city, sdb.state) AS store_location,
            TRIM(sdb.store_type) AS store_type,
            sdb.city AS city,
            sdb.state AS state
        FROM store_details_bronze sdb
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            store_location,
            store_type,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY store_name DESC, city DESC, state DESC, store_type DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_type
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# Target: silver.aggregated_sales_silver
# ============================================================
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.transaction_date AS aggregation_date,
        SUM(sts.sales_amount) AS total_sales_amount,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        CASE
            WHEN SUM(sts.quantity_sold) = 0 THEN NULL
            ELSE SUM(sts.sales_amount) / SUM(sts.quantity_sold)
        END AS average_sales_price
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.transaction_date
    """
)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
