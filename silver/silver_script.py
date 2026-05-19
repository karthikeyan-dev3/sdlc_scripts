import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# --------------------------------------------------------------------
# SOURCE READS (Bronze)
# --------------------------------------------------------------------
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

# --------------------------------------------------------------------
# TARGET: products_silver
# --------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(pb.product_id)   AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category)     AS category,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY
            CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN TRIM(pb.category) IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM products_bronze pb
      WHERE pb.is_active = true
    )
    SELECT
      product_id,
      product_name,
      category
    FROM ranked
    WHERE rn = 1
    """
)
products_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/products_silver.csv"
)
products_silver_df.createOrReplaceTempView("products_silver")

# --------------------------------------------------------------------
# TARGET: stores_silver
# --------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(sb.store_id)   AS store_id,
        TRIM(sb.store_name) AS store_name,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY
            CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name
    FROM ranked
    WHERE rn = 1
    """
)
stores_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/stores_silver.csv"
)
stores_silver_df.createOrReplaceTempView("stores_silver")

# --------------------------------------------------------------------
# TARGET: sales_transactions_silver
# --------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        TRIM(stb.transaction_id)          AS transaction_id,
        ss.store_id                       AS store_id,
        ps.product_id                     AS product_id,
        CAST(stb.transaction_time AS DATE) AS date,
        CAST(stb.quantity AS INT)         AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE)   AS total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      INNER JOIN stores_silver ss
        ON TRIM(stb.store_id) = ss.store_id
      INNER JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
      WHERE CAST(stb.quantity AS INT) >= 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      date,
      quantity_sold,
      total_revenue
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)

job.commit()
