import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# Target: silver.product_details_silver
# -------------------------------------------------------------------
product_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CASE
          WHEN CAST(pb.price AS DOUBLE) >= 0 THEN CAST(CAST(pb.price AS DOUBLE) AS DECIMAL(12,2))
          ELSE NULL
        END AS price
      FROM products_bronze pb
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
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
      brand,
      price
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

# -------------------------------------------------------------------
# Target: silver.store_details_silver
# -------------------------------------------------------------------
store_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        CASE
          WHEN LOWER(TRIM(sb.store_type)) IN ('franchise','franchised') THEN 'franchise'
          WHEN LOWER(TRIM(sb.store_type)) IN ('company-owned','company owned','corporate') THEN 'company-owned'
          ELSE 'unknown'
        END AS store_type
      FROM stores_bronze sb
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        location,
        store_type,
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
      store_type
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

# -------------------------------------------------------------------
# Target: silver.sales_details_silver
# -------------------------------------------------------------------
sales_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CASE
          WHEN CAST(stb.quantity AS INT) > 0 THEN CAST(stb.quantity AS INT)
          ELSE NULL
        END AS quantity_sold,
        CASE
          WHEN CAST(stb.sale_amount AS DOUBLE) >= 0 THEN CAST(CAST(stb.sale_amount AS DOUBLE) AS DECIMAL(14,2))
          ELSE NULL
        END AS total_revenue
      FROM sales_transactions_bronze stb
      INNER JOIN store_details_silver sds
        ON TRIM(stb.store_id) = sds.store_id
      INNER JOIN product_details_silver pds
        ON TRIM(stb.product_id) = pds.product_id
    ),
    dedup AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_id
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      transaction_date,
      quantity_sold,
      total_revenue
    FROM dedup
    WHERE rn = 1
    """
)
sales_details_silver_df.createOrReplaceTempView("sales_details_silver")

(
    sales_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_details_silver.csv")
)

# -------------------------------------------------------------------
# Target: silver.sales_aggregations_silver
# -------------------------------------------------------------------
sales_aggregations_silver_df = spark.sql(
    """
    WITH product_day AS (
      SELECT
        sls.store_id AS store_id,
        sls.product_id AS product_id,
        sls.transaction_date AS reporting_date,
        SUM(sls.quantity_sold) AS total_quantity_sold,
        SUM(sls.total_revenue) OVER (PARTITION BY sls.store_id, sls.transaction_date) AS total_store_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY sls.store_id, sls.transaction_date
          ORDER BY
            SUM(sls.quantity_sold) DESC,
            SUM(sls.total_revenue) DESC,
            sls.product_id ASC
        ) AS top_rank
      FROM sales_details_silver sls
      INNER JOIN product_details_silver pds
        ON sls.product_id = pds.product_id
      GROUP BY
        sls.store_id,
        sls.product_id,
        sls.transaction_date
    ),
    ranked AS (
      SELECT
        store_id,
        product_id,
        reporting_date,
        total_quantity_sold,
        total_store_revenue,
        FIRST_VALUE(product_id) OVER (
          PARTITION BY store_id, reporting_date
          ORDER BY top_rank ASC
        ) AS top_selling_product
      FROM product_day
    )
    SELECT
      store_id,
      product_id,
      reporting_date,
      total_quantity_sold,
      total_store_revenue,
      top_selling_product
    FROM ranked
    """
)

(
    sales_aggregations_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/sales_aggregations_silver.csv")
)

job.commit()