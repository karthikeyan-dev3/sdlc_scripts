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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables (bronze) + create temp views
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) product_details_silver (pds)
# ------------------------------------------------------------
product_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.product_id) AS string) AS product_id,
        CAST(TRIM(pb.product_name) AS string) AS product_name,
        CAST(TRIM(pb.category) AS string) AS category,
        CAST(TRIM(pb.brand) AS string) AS brand,
        CAST(pb.price AS double) AS price
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      CAST(price AS float) AS price
    FROM dedup
    WHERE rn = 1
    """
)
product_details_silver_df.createOrReplaceTempView("product_details_silver")

(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/product_details_silver.csv")
)

# ------------------------------------------------------------
# 3) store_details_silver (sds)
# ------------------------------------------------------------
store_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.store_id) AS string) AS store_id,
        CAST(TRIM(sb.store_name) AS string) AS store_name,
        CAST(TRIM(sb.city) AS string) AS city,
        CAST(TRIM(sb.state) AS string) AS state,
        CAST(sb.open_date AS date) AS open_date
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        open_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      CAST(COALESCE(city, '') || ', ' || COALESCE(state, '') AS string) AS location,
      CAST(NULL AS string) AS manager,
      open_date
    FROM dedup
    WHERE rn = 1
    """
)
store_details_silver_df.createOrReplaceTempView("store_details_silver")

(
    store_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/store_details_silver.csv")
)

# ------------------------------------------------------------
# 4) sales_transactions_silver (sts)
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS string) AS transaction_id,
        CAST(stb.transaction_time AS timestamp) AS transaction_time,
        CAST(stb.transaction_time AS date) AS date,
        CAST(TRIM(stb.product_id) AS string) AS product_id,
        CAST(TRIM(stb.store_id) AS string) AS store_id,
        CAST(stb.sale_amount AS double) AS sales_amount,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(NULL AS string) AS promo_code
      FROM sales_transactions_bronze stb
      LEFT JOIN product_details_silver pds
        ON stb.product_id = pds.product_id
      LEFT JOIN store_details_silver sds
        ON stb.store_id = sds.store_id
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
    ),
    dedup AS (
      SELECT
        transaction_id,
        date,
        product_id,
        store_id,
        sales_amount,
        quantity_sold,
        promo_code,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      date,
      product_id,
      store_id,
      sales_amount,
      quantity_sold,
      promo_code
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
    .save(TARGET_PATH + "/sales_transactions_silver.csv")
)

# ------------------------------------------------------------
# 5) aggregated_sales_summary_silver (ass)
# ------------------------------------------------------------
aggregated_sales_summary_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      sts.date AS date,
      SUM(sts.sales_amount) AS total_sales_amount,
      SUM(sts.quantity_sold) AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.date
    """
)

(
    aggregated_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/aggregated_sales_summary_silver.csv")
)

job.commit()