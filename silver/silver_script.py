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

# ------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# Create temp views
# ------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# Target: silver_product_master
# Source: bronze.products_bronze pb
# ============================================================
silver_product_master_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        CAST(pb.price AS DOUBLE) AS price,
        (
          CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END +
          CASE WHEN TRIM(pb.category) IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END +
          CASE WHEN pb.price IS NOT NULL AND CAST(pb.price AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END
        ) AS completeness_score
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
        AND (pb.price IS NULL OR CAST(pb.price AS DOUBLE) >= 0)
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY completeness_score DESC
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      price
    FROM dedup
    WHERE rn = 1
    """
)

(
    silver_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_product_master.csv")
)

silver_product_master_df.createOrReplaceTempView("silver_product_master")

# ============================================================
# Target: silver_store_master
# Source: bronze.stores_bronze sb
# ============================================================
silver_store_master_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.store_type) AS store_type,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        (
          CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END +
          CASE WHEN TRIM(sb.store_type) IS NOT NULL AND TRIM(sb.store_type) <> '' THEN 1 ELSE 0 END +
          CASE WHEN TRIM(sb.city) IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END +
          CASE WHEN TRIM(sb.state) IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END
        ) AS completeness_score
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        store_type,
        location,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY completeness_score DESC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      store_type,
      location
    FROM dedup
    WHERE rn = 1
    """
)

(
    silver_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_store_master.csv")
)

silver_store_master_df.createOrReplaceTempView("silver_store_master")

# ============================================================
# Target: silver_sales_transactions
# Source: bronze.sales_transactions_bronze stb
# Join: LEFT JOIN silver masters to enforce referential integrity
# ============================================================
silver_sales_transactions_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.transaction_time AS DATE) AS sale_date,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
      FROM sales_transactions_bronze stb
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
        AND TRIM(stb.product_id) IS NOT NULL
        AND TRIM(stb.product_id) <> ''
        AND TRIM(stb.store_id) IS NOT NULL
        AND TRIM(stb.store_id) <> ''
        AND (stb.quantity IS NULL OR CAST(stb.quantity AS INT) > 0)
    ),
    dedup AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        quantity_sold,
        sale_date,
        transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    ),
    filtered AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        quantity_sold,
        sale_date
      FROM dedup
      WHERE rn = 1
    )
    SELECT
      f.transaction_id,
      f.product_id,
      f.store_id,
      f.quantity_sold,
      f.sale_date
    FROM filtered f
    LEFT JOIN silver_product_master spm
      ON f.product_id = spm.product_id
    LEFT JOIN silver_store_master ssm
      ON f.store_id = ssm.store_id
    WHERE spm.product_id IS NOT NULL
      AND ssm.store_id IS NOT NULL
    """
)

(
    silver_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sales_transactions.csv")
)

silver_sales_transactions_df.createOrReplaceTempView("silver_sales_transactions")

# ============================================================
# Target: silver_daily_sales_agg
# Source: silver.silver_sales_transactions sst joined to masters
# ============================================================
silver_daily_sales_agg_df = spark.sql(
    """
    SELECT
      sst.product_id AS product_id,
      sst.store_id AS store_id,
      sst.sale_date AS sale_date,
      SUM(sst.quantity_sold) AS total_quantity_sold,
      SUM(sst.quantity_sold * spm.price) AS total_revenue
    FROM silver_sales_transactions sst
    INNER JOIN silver_product_master spm
      ON sst.product_id = spm.product_id
    INNER JOIN silver_store_master ssm
      ON sst.store_id = ssm.store_id
    WHERE sst.quantity_sold IS NOT NULL
      AND sst.quantity_sold > 0
      AND spm.price IS NOT NULL
      AND spm.price >= 0
    GROUP BY
      sst.product_id,
      sst.store_id,
      sst.sale_date
    """
)

(
    silver_daily_sales_agg_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_daily_sales_agg.csv")
)

job.commit()