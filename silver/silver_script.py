
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

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")

store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------
# Target: silver.product_master_silver
# -----------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pmb.product_id) AS product_id,
            TRIM(pmb.product_name) AS product_name,
            UPPER(TRIM(pmb.category)) AS category,
            UPPER(TRIM(pmb.brand)) AS brand,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pmb.product_id)
                ORDER BY
                    CASE WHEN TRIM(pmb.product_name) IS NOT NULL AND TRIM(pmb.product_name) <> '' THEN 1 ELSE 0 END
                    + CASE WHEN TRIM(pmb.category) IS NOT NULL AND TRIM(pmb.category) <> '' THEN 1 ELSE 0 END
                    + CASE WHEN TRIM(pmb.brand) IS NOT NULL AND TRIM(pmb.brand) <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM product_master_bronze pmb
        WHERE TRIM(pmb.product_id) IS NOT NULL
          AND TRIM(pmb.product_id) <> ''
          AND pmb.is_active = true
    )
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM base
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

# ---------------------------------
# Target: silver.store_master_silver
# ---------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(smb.store_id) AS store_id,
            TRIM(smb.store_name) AS store_name,
            smb.state AS region,
            UPPER(TRIM(smb.store_type)) AS store_type,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(smb.store_id)
                ORDER BY
                    CASE WHEN TRIM(smb.store_name) IS NOT NULL AND TRIM(smb.store_name) <> '' THEN 1 ELSE 0 END
                    + CASE WHEN smb.state IS NOT NULL AND TRIM(smb.state) <> '' THEN 1 ELSE 0 END
                    + CASE WHEN TRIM(smb.store_type) IS NOT NULL AND TRIM(smb.store_type) <> '' THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM store_master_bronze smb
        WHERE TRIM(smb.store_id) IS NOT NULL
          AND TRIM(smb.store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        region,
        store_type
    FROM base
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

# -----------------------------------------
# Target: silver.sales_transactions_silver
# -----------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.transaction_time AS date) AS transaction_date,
            stb.sale_amount AS sales_amount,
            stb.quantity AS quantity_sold,
            stb.transaction_time AS transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN product_master_silver pms
            ON TRIM(stb.product_id) = pms.product_id
        INNER JOIN store_master_silver sms
            ON TRIM(stb.store_id) = sms.store_id
        WHERE TRIM(stb.transaction_id) IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
          AND TRIM(stb.product_id) IS NOT NULL
          AND TRIM(stb.product_id) <> ''
          AND TRIM(stb.store_id) IS NOT NULL
          AND TRIM(stb.store_id) <> ''
          AND stb.quantity > 0
          AND stb.sale_amount >= 0
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        sales_amount,
        quantity_sold
    FROM joined
    WHERE rn = 1
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