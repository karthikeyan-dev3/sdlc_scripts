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

# ============================================================
# Read Source Tables (S3)
# ============================================================
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

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

# ============================================================
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS date) AS sale_date,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            CAST(stb.quantity AS int) AS quantity_sold,
            CAST(stb.sale_amount AS double) AS total_sale_amount,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.store_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND CAST(stb.quantity AS int) > 0
          AND CAST(stb.sale_amount AS double) >= 0
    ),
    dedup AS (
        SELECT
            transaction_id,
            sale_date,
            product_id,
            store_id,
            quantity_sold,
            total_sale_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        sale_date,
        product_id,
        store_id,
        quantity_sold,
        total_sale_amount
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

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            pb.product_id AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            TRIM(pb.brand) AS brand
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND pb.is_active = true
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_id
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM dedup
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
# Target: silver.stores_silver
# ============================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sb.store_id AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            sb.state AS region
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            location,
            region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY store_id
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        region
    FROM dedup
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
# Target: silver.aggregated_sales_daily_silver
# ============================================================
aggregated_sales_daily_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        CAST(SUM(sts.quantity_sold) AS bigint) AS total_quantity_sold,
        CAST(SUM(sts.total_sale_amount) AS double) AS total_revenue,
        sts.sale_date AS aggregation_date
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.sale_date
    """
)
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

(
    aggregated_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_daily_silver.csv")
)

job.commit()