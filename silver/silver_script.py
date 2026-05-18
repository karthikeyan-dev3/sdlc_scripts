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
# Read source tables (Bronze)
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
# Create temp views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS transaction_date,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            COALESCE(CAST(stb.quantity AS INT), 0) AS quantity_sold,
            COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) AS total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND stb.store_id IS NOT NULL
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_amount
    FROM ranked
    WHERE rn = 1
      AND quantity_sold >= 0
      AND total_sales_amount >= 0
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
    WITH ranked AS (
        SELECT
            pb.product_id AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS product_category,
            CAST(pb.price AS DOUBLE) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY CAST(pb.is_active AS BOOLEAN) DESC
            ) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    )
    SELECT
        product_id,
        product_name,
        product_category,
        price
    FROM ranked
    WHERE rn = 1
      AND price >= 0
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
    WITH ranked AS (
        SELECT
            sb.store_id AS store_id,
            TRIM(sb.store_name) AS store_name,
            TRIM(CONCAT(sb.city, ', ', sb.state)) AS store_location,
            UPPER(TRIM(sb.state)) AS store_region,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.open_date DESC
            ) AS rn
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND sb.store_name IS NOT NULL
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
        sts.transaction_date AS date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_sales_amount) AS total_sales_revenue
    FROM sales_transactions_silver sts
    WHERE sts.transaction_date IS NOT NULL
      AND sts.product_id IS NOT NULL
      AND sts.store_id IS NOT NULL
    GROUP BY
        sts.transaction_date,
        sts.product_id,
        sts.store_id
    """
)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()