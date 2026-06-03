import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, to_json

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables (Bronze)
# ------------------------------------------------------------------------------------
product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)

store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------------
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------------
# 3) product_master_silver: Transform (dedup + cleanse + metadata JSON)
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(pmb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pmb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pmb.category) AS STRING) AS category,
            to_json(struct(pmb.brand, pmb.price)) AS metadata_attributes,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pmb.product_id)
                ORDER BY pmb.product_id
            ) AS rn
        FROM product_master_bronze pmb
        WHERE TRIM(pmb.product_id) IS NOT NULL
          AND TRIM(pmb.product_id) <> ''
          AND LOWER(TRIM(pmb.is_active)) = 'true'
    )
    SELECT
        product_id,
        product_name,
        category,
        metadata_attributes
    FROM ranked
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ------------------------------------------------------------------------------------
# 4) store_master_silver: Transform (dedup + cleanse + metadata JSON)
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(smb.store_id) AS STRING) AS store_id,
            CAST(TRIM(smb.store_name) AS STRING) AS store_name,
            CAST(TRIM(smb.city) AS STRING) AS city,
            CAST(TRIM(smb.store_type) AS STRING) AS store_type,
            to_json(struct(smb.state, smb.open_date)) AS metadata_attributes,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(smb.store_id)
                ORDER BY smb.store_id
            ) AS rn
        FROM store_master_bronze smb
        WHERE TRIM(smb.store_id) IS NOT NULL
          AND TRIM(smb.store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        city,
        store_type,
        metadata_attributes
    FROM ranked
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ------------------------------------------------------------------------------------
# 5) sales_transactions_silver: Transform (validate + dedup + conform to masters)
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(stb.transaction_time AS DATE) AS reporting_date,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN store_master_silver sms
            ON TRIM(stb.store_id) = sms.store_id
        INNER JOIN product_master_silver pms
            ON TRIM(stb.product_id) = pms.product_id
        WHERE TRIM(stb.transaction_id) IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
          AND TRIM(stb.store_id) IS NOT NULL
          AND TRIM(stb.store_id) <> ''
          AND TRIM(stb.product_id) IS NOT NULL
          AND TRIM(stb.product_id) <> ''
          AND CAST(stb.quantity AS INT) > 0
          AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        quantity,
        sale_amount,
        transaction_time,
        reporting_date
    FROM ranked
    WHERE rn = 1
    """
)

# ------------------------------------------------------------------------------------
# 6) Write each target table separately (SINGLE CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------------------------
(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()