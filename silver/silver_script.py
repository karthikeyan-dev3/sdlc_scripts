
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables (Bronze)
# ----------------------------
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

# ----------------------------
# 2) Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------
# 3) Transform + 4) Write: products_silver
# ----------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pb.product_name) AS STRING) AS product_name,
        CAST(TRIM(pb.category) AS STRING) AS category,
        CAST(TRIM(pb.brand) AS STRING) AS brand,
        CAST(pb.price AS DOUBLE) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_id)
        ) AS rn
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      CAST(price AS DOUBLE) AS price
    FROM base
    WHERE rn = 1
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

# ----------------------------
# 3) Transform + 4) Write: stores_silver
# ----------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sb.store_name) AS STRING) AS store_name,
        CONCAT(CAST(TRIM(sb.city) AS STRING), ', ', CAST(TRIM(sb.state) AS STRING)) AS location,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY TRIM(sb.store_id)
        ) AS rn
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    )
    SELECT
      store_id,
      store_name,
      location
    FROM base
    WHERE rn = 1
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

# ----------------------------
# 3) Transform + 4) Write: sales_transactions_silver
# ----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
        CAST(TRIM(stb.product_id) AS STRING) AS product_id,
        CAST(TRIM(stb.store_id) AS STRING) AS store_id,
        DATE(stb.transaction_time) AS transaction_date,
        CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
        CAST(stb.quantity AS INT) AS quantity_sold,
        stb.transaction_time AS transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
      LEFT JOIN stores_silver ss
        ON TRIM(stb.store_id) = ss.store_id
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_date,
      sales_amount,
      quantity_sold
    FROM joined
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

# ----------------------------
# 3) Transform + 4) Write: aggregated_sales_daily_silver
# ----------------------------
aggregated_sales_daily_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      sts.transaction_date AS date,
      SUM(sts.sales_amount) AS total_sales_amount,
      SUM(sts.quantity_sold) AS total_quantity_sold
    FROM sales_transactions_silver sts
    WHERE sts.transaction_date IS NOT NULL
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.transaction_date
    """
)

(
    aggregated_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_daily_silver.csv")
)

aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

# ----------------------------
# 3) Transform + 4) Write: daily_refresh_log_silver
# ----------------------------
daily_refresh_log_silver_df = spark.sql(
    """
    SELECT
      CURRENT_DATE AS refresh_date,
      COUNT(sts.transaction_id) AS row_count,
      'SUCCESS' AS status
    FROM sales_transactions_silver sts
    """
)

(
    daily_refresh_log_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_refresh_log_silver.csv")
)