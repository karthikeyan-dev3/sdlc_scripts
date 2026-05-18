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

# -------------------------------
# 1) Read source tables (Bronze)
# -------------------------------
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

# -----------------------------------------
# 2) products_silver (dedup + active only)
# -----------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(pb.product_id)  AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category)     AS category,
            CAST(pb.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY TRIM(pb.product_id)
            ) AS rn
        FROM products_bronze pb
        WHERE COALESCE(CAST(pb.is_active AS BOOLEAN), FALSE) = TRUE
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM ranked
    WHERE rn = 1
    """
)
products_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/products_silver.csv"
)
products_silver_df.createOrReplaceTempView("products_silver")

# -----------------------------------------
# 3) stores_silver (dedup + location format)
# -----------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(sb.store_id)    AS store_id,
            TRIM(sb.store_name)  AS store_name,
            TRIM(sb.city)        AS city,
            TRIM(sb.state)       AS state,
            TRIM(sb.store_type)  AS store_type,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY TRIM(sb.store_id)
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        CONCAT(city, ', ', state) AS location,
        store_type
    FROM ranked
    WHERE rn = 1
    """
)
stores_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/stores_silver.csv"
)
stores_silver_df.createOrReplaceTempView("stores_silver")

# ---------------------------------------------------------
# 4) sales_transactions_silver (dedup by latest transaction_time)
# ---------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            TRIM(stb.product_id)     AS product_id,
            TRIM(stb.store_id)       AS store_id,
            CAST(stb.quantity AS INT)        AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE)  AS total_revenue,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            stb.transaction_time AS transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN products_silver ps
            ON TRIM(stb.product_id) = ps.product_id
        INNER JOIN stores_silver ss
            ON TRIM(stb.store_id) = ss.store_id
        WHERE CAST(stb.quantity AS INT) > 0
          AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        quantity_sold,
        total_revenue,
        sale_date
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ---------------------------------------------------------
# 5) daily_store_category_sales_silver (daily aggregates)
# ---------------------------------------------------------
daily_store_category_sales_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        ps.category  AS category,
        sts.sale_date AS report_date,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_revenue) AS total_revenue
    FROM sales_transactions_silver sts
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        sts.store_id,
        ps.category,
        sts.sale_date
    """
)
daily_store_category_sales_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/daily_store_category_sales_silver.csv"
)
daily_store_category_sales_silver_df.createOrReplaceTempView("daily_store_category_sales_silver")

# ---------------------------------------------------------
# 6) product_revenue_silver (product-level revenue)
# ---------------------------------------------------------
product_revenue_silver_df = spark.sql(
    """
    SELECT
        sts.product_id AS product_id,
        SUM(sts.total_revenue) AS total_revenue
    FROM sales_transactions_silver sts
    GROUP BY
        sts.product_id
    """
)
product_revenue_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/product_revenue_silver.csv"
)
product_revenue_silver_df.createOrReplaceTempView("product_revenue_silver")

# ---------------------------------------------------------
# 7) store_revenue_silver (store-level revenue)
# ---------------------------------------------------------
store_revenue_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        SUM(sts.total_revenue) AS total_revenue
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id
    """
)
store_revenue_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"{TARGET_PATH}/store_revenue_silver.csv"
)
store_revenue_silver_df.createOrReplaceTempView("store_revenue_silver")

job.commit()