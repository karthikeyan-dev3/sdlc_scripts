
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables from S3
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

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

# -----------------------------
# 2) Create temp views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# Target: sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING)            AS transaction_id,
            CAST(stb.transaction_time AS DATE)                 AS sale_date,
            CAST(TRIM(stb.product_id) AS STRING)               AS product_id,
            CAST(TRIM(stb.store_id) AS STRING)                 AS store_id,
            CAST(stb.quantity AS INT)                          AS quantity,
            CAST(stb.sale_amount AS DOUBLE)                    AS sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        transaction_id,
        sale_date,
        product_id,
        store_id,
        CAST(COALESCE(quantity, 0) AS INT) AS quantity,
        CAST(COALESCE(sale_amount, 0D) AS DOUBLE) AS sale_amount
    FROM base
    WHERE rn = 1
    """
)

sales_transactions_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)

# Create view for downstream silver tables
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# Target: product_master_silver
# ============================================================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING) AS product_category,
            CAST(pb.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY TRIM(pb.product_id)
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        product_category,
        price
    FROM base
    WHERE rn = 1
    """
)

product_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/product_master_silver.csv"
)

# Create view for downstream silver tables
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# Target: store_master_silver
# ============================================================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CAST(CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS STRING) AS store_location,
            CAST(TRIM(sb.state) AS STRING) AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY TRIM(sb.store_id)
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
    FROM base
    WHERE rn = 1
    """
)

store_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/store_master_silver.csv"
)

# Create view for downstream silver tables
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# Target: sales_analysis_silver
# ============================================================
sales_analysis_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS sale_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        SUM(sts.quantity) AS total_quantity,
        SUM(sts.sale_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
        sts.sale_date,
        sts.product_id,
        sts.store_id
    """
)

sales_analysis_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_analysis_silver.csv"
)

# ============================================================
# Target: aggregated_sales_silver
# ============================================================
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS sale_date,
        pms.product_category AS product_category,
        sms.store_region AS store_region,
        SUM(sts.quantity) AS total_quantity,
        SUM(sts.sale_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY
        sts.sale_date,
        pms.product_category,
        sms.store_region
    """
)

aggregated_sales_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/aggregated_sales_silver.csv"
)

job.commit()
