import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue_silver_job", {})

# ------------------------------------------------------------
# Read source tables from S3
# ------------------------------------------------------------
mpb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/master_product_bronze.{FILE_FORMAT}/")
)
mpb_df.createOrReplaceTempView("master_product_bronze")

msb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/master_store_bronze.{FILE_FORMAT}/")
)
msb_df.createOrReplaceTempView("master_store_bronze")

spb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_bronze.{FILE_FORMAT}/")
)
spb_df.createOrReplaceTempView("sales_performance_bronze")

# ------------------------------------------------------------
# Target: silver.master_product_silver
# Columns: product_id, product_name, category, brand
# ------------------------------------------------------------
master_product_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(mpb.product_id) AS product_id,
        TRIM(mpb.product_name) AS product_name,
        TRIM(mpb.category) AS category,
        TRIM(mpb.brand) AS brand,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(mpb.product_id)
          ORDER BY TRIM(mpb.product_id)
        ) AS rn
      FROM master_product_bronze mpb
      WHERE TRIM(mpb.product_id) IS NOT NULL
        AND TRIM(mpb.product_id) <> ''
    )
    SELECT
      CAST(product_id AS STRING) AS product_id,
      CAST(product_name AS STRING) AS product_name,
      CAST(category AS STRING) AS category,
      CAST(brand AS STRING) AS brand
    FROM base
    WHERE rn = 1
    """
)
master_product_silver_df.createOrReplaceTempView("master_product_silver")

(
    master_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/master_product_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.master_store_silver
# Columns: store_id, store_name, region, store_type
# Note: UDT maps region = msb.state (no additional mapping allowed)
# ------------------------------------------------------------
master_store_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(msb.store_id) AS store_id,
        TRIM(msb.store_name) AS store_name,
        TRIM(msb.state) AS region,
        TRIM(msb.store_type) AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(msb.store_id)
          ORDER BY TRIM(msb.store_id)
        ) AS rn
      FROM master_store_bronze msb
      WHERE TRIM(msb.store_id) IS NOT NULL
        AND TRIM(msb.store_id) <> ''
    )
    SELECT
      CAST(store_id AS STRING) AS store_id,
      CAST(store_name AS STRING) AS store_name,
      CAST(region AS STRING) AS region,
      CAST(store_type AS STRING) AS store_type
    FROM base
    WHERE rn = 1
    """
)
master_store_silver_df.createOrReplaceTempView("master_store_silver")

(
    master_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/master_store_silver.csv")
)

# ------------------------------------------------------------
# Target: silver.sales_performance_silver
# Columns: transaction_id, store_id, store_name, product_id, product_name, date, sales_amount, units_sold
# ------------------------------------------------------------
sales_performance_silver_df = spark.sql(
    """
    WITH filtered AS (
      SELECT
        TRIM(spb.transaction_id) AS transaction_id,
        TRIM(spb.store_id) AS store_id,
        TRIM(spb.product_id) AS product_id,
        spb.transaction_time AS transaction_time,
        spb.sale_amount AS sale_amount,
        spb.quantity AS quantity
      FROM sales_performance_bronze spb
      WHERE TRIM(spb.transaction_id) IS NOT NULL AND TRIM(spb.transaction_id) <> ''
        AND TRIM(spb.store_id) IS NOT NULL AND TRIM(spb.store_id) <> ''
        AND TRIM(spb.product_id) IS NOT NULL AND TRIM(spb.product_id) <> ''
        AND CAST(spb.quantity AS INT) > 0
        AND CAST(spb.sale_amount AS DOUBLE) >= 0
    ),
    dedup AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_time,
        sale_amount,
        quantity,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM filtered
    )
    SELECT
      CAST(d.transaction_id AS STRING) AS transaction_id,
      CAST(d.store_id AS STRING) AS store_id,
      CAST(mss.store_name AS STRING) AS store_name,
      CAST(d.product_id AS STRING) AS product_id,
      CAST(mps.product_name AS STRING) AS product_name,
      CAST(d.transaction_time AS DATE) AS date,
      CAST(d.sale_amount AS DOUBLE) AS sales_amount,
      CAST(d.quantity AS INT) AS units_sold
    FROM dedup d
    LEFT JOIN master_store_silver mss
      ON d.store_id = mss.store_id
    LEFT JOIN master_product_silver mps
      ON d.product_id = mps.product_id
    WHERE d.rn = 1
    """
)

(
    sales_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_performance_silver.csv")
)

job.commit()
