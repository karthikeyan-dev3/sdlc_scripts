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

# -------------------------
# 1) Read source tables
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

# -------------------------
# 2) Create temp views
# -------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------
# 3) Transform: product_master_silver
# -------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(pb.product_id)) AS product_id,
        TRIM(pb.product_name)      AS product_name,
        TRIM(pb.category)          AS category,
        TRIM(pb.brand)             AS brand
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL AND TRIM(pb.product_id) <> ''
    ),
    ranked AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand
    FROM ranked
    WHERE rn = 1
    """
)

# -------------------------
# 4) Save: product_master_silver (single CSV file at TARGET_PATH)
# -------------------------
(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# Create temp view for downstream joins
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# -------------------------
# 3) Transform: store_master_silver
# -------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(sb.store_id)) AS store_id,
        TRIM(sb.store_name)      AS store_name,
        sb.state                 AS region,
        TRIM(sb.store_type)      AS store_type
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL AND TRIM(sb.store_id) <> ''
    ),
    ranked AS (
      SELECT
        store_id,
        store_name,
        region,
        store_type,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN store_type IS NOT NULL AND TRIM(store_type) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN region IS NOT NULL AND TRIM(region) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      region,
      store_type
    FROM ranked
    WHERE rn = 1
    """
)

# -------------------------
# 4) Save: store_master_silver (single CSV file at TARGET_PATH)
# -------------------------
(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# Create temp view for downstream joins
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# -------------------------
# 3) Transform: sales_transactions_silver
# -------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        UPPER(TRIM(stb.transaction_id)) AS transaction_id,
        pms.product_id                 AS product_id,
        sms.store_id                   AS store_id,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
        CAST(stb.quantity AS INT)      AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_revenue
      FROM sales_transactions_bronze stb
      INNER JOIN product_master_silver pms
        ON UPPER(TRIM(stb.product_id)) = pms.product_id
      INNER JOIN store_master_silver sms
        ON UPPER(TRIM(stb.store_id)) = sms.store_id
      WHERE stb.transaction_id IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    ),
    ranked AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_time,
        transaction_date,
        quantity_sold,
        total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY
            transaction_time DESC,
            total_revenue DESC
        ) AS rn
      FROM joined
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_time,
      transaction_date,
      quantity_sold,
      total_revenue
    FROM ranked
    WHERE rn = 1
    """
)

# -------------------------
# 4) Save: sales_transactions_silver (single CSV file at TARGET_PATH)
# -------------------------
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()