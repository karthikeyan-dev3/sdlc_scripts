import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ------------------------------------------------------------
# TABLE: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(stb.store_id AS STRING) AS store_id,
        CAST(stb.product_id AS STRING) AS product_id,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.quantity AS INT) AS quantity
      FROM sales_transactions_bronze stb
    ),
    valid AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_time,
        sale_amount,
        quantity
      FROM base
      WHERE transaction_id IS NOT NULL
        AND store_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity >= 0
        AND sale_amount >= 0
    ),
    dedup AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_time,
        sale_amount,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
      FROM valid
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      CAST(transaction_time AS DATE) AS transaction_date,
      sale_amount AS revenue,
      quantity AS quantity_sold
    FROM dedup
    WHERE rn = 1
    """
)

sales_transactions_silver_output = f"{TARGET_PATH}/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_transactions_silver_output)
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------
# TABLE: silver.products_silver
# ------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(pb.product_id AS STRING) AS product_id,
        TRIM(CAST(pb.product_name AS STRING)) AS product_name,
        TRIM(CAST(pb.category AS STRING)) AS category
      FROM products_bronze pb
    ),
    filtered AS (
      SELECT
        product_id,
        product_name,
        category
      FROM base
      WHERE product_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM filtered
    )
    SELECT
      product_id,
      product_name,
      category
    FROM dedup
    WHERE rn = 1
    """
)

products_silver_output = f"{TARGET_PATH}/products_silver.csv"
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(products_silver_output)
)

products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------
# TABLE: silver.stores_silver
# ------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(sb.store_id AS STRING) AS store_id,
        TRIM(CAST(sb.store_name AS STRING)) AS store_name
      FROM stores_bronze sb
    ),
    filtered AS (
      SELECT
        store_id,
        store_name
      FROM base
      WHERE store_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM filtered
    )
    SELECT
      store_id,
      store_name
    FROM dedup
    WHERE rn = 1
    """
)

stores_silver_output = f"{TARGET_PATH}/stores_silver.csv"
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(stores_silver_output)
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------
# TABLE: silver.data_quality_metrics_silver
# ------------------------------------------------------------
data_quality_metrics_silver_df = spark.sql(
    """
    WITH bronze_cast AS (
      SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(stb.store_id AS STRING) AS store_id,
        CAST(stb.product_id AS STRING) AS product_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
      FROM sales_transactions_bronze stb
    ),
    bronze_metrics AS (
      SELECT
        CAST(transaction_time AS DATE) AS run_date,
        COUNT(transaction_id) AS bronze_count,
        SUM(
          CASE
            WHEN transaction_id IS NULL
              OR store_id IS NULL
              OR product_id IS NULL
              OR quantity < 0
              OR sale_amount < 0
              OR transaction_time IS NULL
            THEN 1 ELSE 0
          END
        ) AS validation_errors
      FROM bronze_cast
      GROUP BY CAST(transaction_time AS DATE)
    ),
    silver_metrics AS (
      SELECT
        sts.transaction_date AS run_date,
        COUNT(sts.transaction_id) AS silver_count
      FROM sales_transactions_silver sts
      GROUP BY sts.transaction_date
    )
    SELECT
      COALESCE(bm.run_date, sm.run_date) AS run_date,
      (COALESCE(bm.bronze_count, 0) - COALESCE(sm.silver_count, 0)) AS duplicate_count,
      COALESCE(bm.validation_errors, 0) AS validation_errors,
      CAST(
        100 * (
          (COALESCE(bm.bronze_count, 0) - COALESCE(bm.validation_errors, 0))
          / NULLIF(COALESCE(bm.bronze_count, 0), 0)
        ) AS DOUBLE
      ) AS data_quality_score
    FROM bronze_metrics bm
    FULL OUTER JOIN silver_metrics sm
      ON bm.run_date = sm.run_date
    """
)

data_quality_metrics_silver_output = f"{TARGET_PATH}/data_quality_metrics_silver.csv"
(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(data_quality_metrics_silver_output)
)

job.commit()
