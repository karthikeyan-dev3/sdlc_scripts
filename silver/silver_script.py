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

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
pmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
pmb_df.createOrReplaceTempView("product_master_bronze")

smb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
smb_df.createOrReplaceTempView("store_master_bronze")

stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

# ---------------------------------------
# Target: silver.product_master_silver
# ---------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(pmb.product_id)) AS product_id,
        TRIM(pmb.product_name)      AS product_name,
        pmb.category                AS product_category,
        CAST(pmb.price AS DOUBLE)   AS product_price
      FROM product_master_bronze pmb
      WHERE pmb.is_active = true
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        product_category,
        product_price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY product_id
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      product_category,
      product_price
    FROM dedup
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ---------------------------------------
# Target: silver.store_master_silver
# ---------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(smb.store_id))                    AS store_id,
        TRIM(smb.store_name)                         AS store_name,
        CONCAT(TRIM(smb.city), ', ', TRIM(smb.state)) AS store_location,
        smb.city                                     AS city,
        smb.state                                    AS state,
        smb.store_type                               AS store_type,
        CAST(smb.open_date AS DATE)                  AS open_date
      FROM store_master_bronze smb
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        store_location,
        city,
        state,
        store_type,
        open_date,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY store_id
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      store_location,
      city,
      state,
      store_type,
      open_date
    FROM dedup
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ---------------------------------------------
# Target: silver.sales_transactions_silver
# ---------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        stb.transaction_id                 AS transaction_id,
        TRIM(UPPER(stb.store_id))          AS store_id,
        TRIM(UPPER(stb.product_id))        AS product_id,
        CAST(stb.quantity AS INT)          AS quantity,
        CAST(stb.sale_amount AS DOUBLE)    AS sale_amount,
        stb.transaction_time               AS transaction_time,
        CAST(stb.transaction_time AS DATE) AS transaction_date
      FROM sales_transactions_bronze stb
      WHERE CAST(stb.quantity AS INT) > 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    ),
    dedup AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        transaction_date,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      d.transaction_id,
      d.store_id,
      d.product_id,
      d.quantity,
      d.sale_amount,
      d.transaction_time,
      d.transaction_date
    FROM dedup d
    INNER JOIN store_master_silver sms
      ON d.store_id = sms.store_id
    INNER JOIN product_master_silver pms
      ON d.product_id = pms.product_id
    WHERE d.rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()