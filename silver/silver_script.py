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

# -------------------------
# Read source tables (Bronze)
# -------------------------
product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")

store_information_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_bronze.{FILE_FORMAT}/")
)
store_information_bronze_df.createOrReplaceTempView("store_information_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------
# TARGET: product_details_silver
# -------------------------
product_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pdb.product_id)   AS product_id,
        TRIM(pdb.product_name) AS product_name,
        TRIM(pdb.category)     AS category,
        CAST(pdb.price AS DECIMAL) AS price
      FROM product_details_bronze pdb
      WHERE TRIM(pdb.product_id) IS NOT NULL
        AND TRIM(pdb.product_id) <> ''
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY price DESC
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
product_details_silver_df.createOrReplaceTempView("product_details_silver")

(
    product_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_details_silver.csv")
)

# -------------------------
# TARGET: store_information_silver
# -------------------------
store_information_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sib.store_id)   AS store_id,
        TRIM(sib.store_name) AS store_name,
        CONCAT(TRIM(sib.city), ', ', TRIM(sib.state)) AS location,
        TRIM(sib.city)  AS city_trim,
        TRIM(sib.state) AS state_trim
      FROM store_information_bronze sib
      WHERE TRIM(sib.store_id) IS NOT NULL
        AND TRIM(sib.store_id) <> ''
    ),
    scored AS (
      SELECT
        store_id,
        store_name,
        location,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END +
            CASE WHEN city_trim  IS NOT NULL AND city_trim  <> '' THEN 1 ELSE 0 END +
            CASE WHEN state_trim IS NOT NULL AND state_trim <> '' THEN 1 ELSE 0 END DESC,
            store_name DESC,
            location DESC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      location
    FROM scored
    WHERE rn = 1
    """
)
store_information_silver_df.createOrReplaceTempView("store_information_silver")

(
    store_information_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_information_silver.csv")
)

# -------------------------
# TARGET: sales_transactions_silver
# -------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id)     AS product_id,
        TRIM(stb.store_id)       AS store_id,
        CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS date,
        stb.quantity             AS quantity_sold,
        CAST(stb.sale_amount AS DECIMAL) AS total_revenue,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
      FROM sales_transactions_bronze stb
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
        AND TRIM(stb.product_id) IS NOT NULL
        AND TRIM(stb.product_id) <> ''
        AND TRIM(stb.store_id) IS NOT NULL
        AND TRIM(stb.store_id) <> ''
        AND stb.quantity IS NOT NULL
        AND stb.quantity > 0
    ),
    joined AS (
      SELECT
        b.transaction_id,
        b.product_id,
        b.store_id,
        b.date,
        b.quantity_sold,
        b.total_revenue,
        b.transaction_time_ts
      FROM base b
      INNER JOIN product_details_silver pds
        ON b.product_id = pds.product_id
      INNER JOIN store_information_silver sis
        ON b.store_id = sis.store_id
    ),
    dedup AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        date,
        quantity_sold,
        total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time_ts DESC
        ) AS rn
      FROM joined
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      date,
      quantity_sold,
      total_revenue
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

job.commit()