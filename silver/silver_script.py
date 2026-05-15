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

# -------------------------
# Read Source Tables (Bronze)
# -------------------------
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

products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver.products_silver
# 1) Read source tables
# 2) Create temp views
# 3) SQL transformation
# 4) Write output
# ============================================================
products_silver_df = spark.sql(
    """
    SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        CAST(pb.price AS float) AS price
    FROM products_bronze pb
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
    SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        CONCAT(sb.city, ', ', sb.state) AS location,
        sb.store_type AS store_type
    FROM stores_bronze sb
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
# Target: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    SELECT
        stb.transaction_id AS transaction_id,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.transaction_time AS date) AS sale_date,
        COALESCE(CAST(stb.sale_amount AS double), CAST(stb.quantity AS double) * CAST(ps.price AS double)) AS total_revenue
    FROM sales_transactions_bronze stb
    INNER JOIN products_silver ps
        ON stb.product_id = ps.product_id
    INNER JOIN stores_silver ss
        ON stb.store_id = ss.store_id
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
# Target: silver.daily_sales_summary_silver
# ============================================================
daily_sales_summary_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sts.sale_date AS sale_date,
            sts.transaction_id AS transaction_id,
            sts.product_id AS product_id,
            sts.store_id AS store_id,
            sts.total_revenue AS total_revenue,
            ps.product_name AS product_name,
            ss.store_name AS store_name
        FROM sales_transactions_silver sts
        INNER JOIN products_silver ps
            ON sts.product_id = ps.product_id
        INNER JOIN stores_silver ss
            ON sts.store_id = ss.store_id
    ),
    daily_totals AS (
        SELECT
            sale_date,
            SUM(total_revenue) AS total_revenue,
            COUNT(DISTINCT transaction_id) AS total_transactions
        FROM base
        GROUP BY sale_date
    ),
    product_ranked AS (
        SELECT
            sale_date,
            FIRST_VALUE(product_id) OVER (PARTITION BY sale_date ORDER BY SUM(total_revenue) DESC) AS top_product_id,
            FIRST_VALUE(product_name) OVER (PARTITION BY sale_date ORDER BY SUM(total_revenue) DESC) AS top_product_name
        FROM base
        GROUP BY sale_date, product_id, product_name
    ),
    store_ranked AS (
        SELECT
            sale_date,
            FIRST_VALUE(store_id) OVER (PARTITION BY sale_date ORDER BY SUM(total_revenue) DESC) AS top_store_id,
            FIRST_VALUE(store_name) OVER (PARTITION BY sale_date ORDER BY SUM(total_revenue) DESC) AS top_store_name
        FROM base
        GROUP BY sale_date, store_id, store_name
    )
    SELECT
        dt.sale_date AS date,
        dt.total_revenue AS total_revenue,
        dt.total_transactions AS total_transactions,
        pr.top_product_id AS top_product_id,
        pr.top_product_name AS top_product_name,
        sr.top_store_id AS top_store_id,
        sr.top_store_name AS top_store_name
    FROM daily_totals dt
    INNER JOIN (
        SELECT DISTINCT sale_date, top_product_id, top_product_name
        FROM product_ranked
    ) pr
        ON dt.sale_date = pr.sale_date
    INNER JOIN (
        SELECT DISTINCT sale_date, top_store_id, top_store_name
        FROM store_ranked
    ) sr
        ON dt.sale_date = sr.sale_date
    """
)
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

(
    daily_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_summary_silver.csv")
)

job.commit()