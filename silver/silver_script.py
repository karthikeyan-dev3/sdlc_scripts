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
# 1) Read source tables from S3 (CSV) and create temp views
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
# 2) product_master_silver (pms)
# --------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING) AS product_category,
            CAST(TRIM(pb.brand) AS STRING) AS product_brand
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            product_category,
            product_brand,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_brand
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

# --------------------------------------------------------------------
# 3) store_master_silver (sms)
# --------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CONCAT(TRIM(sb.city), ',', TRIM(sb.state)) AS store_location,
            CAST(TRIM(sb.state) AS STRING) AS store_region
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            store_location,
            store_region,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
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

# --------------------------------------------------------------------
# 4) sales_transactions_silver (sts)
# --------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            DATE(stb.transaction_time) AS sales_date,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
            ROW_NUMBER() OVER (PARTITION BY TRIM(stb.transaction_id) ORDER BY DATE(stb.transaction_time), TRIM(stb.transaction_id)) AS rn
        FROM sales_transactions_bronze stb
        INNER JOIN store_master_silver sms
            ON TRIM(stb.store_id) = sms.store_id
        INNER JOIN product_master_silver pms
            ON TRIM(stb.product_id) = pms.product_id
        WHERE
            TRIM(stb.transaction_id) IS NOT NULL
            AND TRIM(stb.store_id) IS NOT NULL
            AND TRIM(stb.product_id) IS NOT NULL
            AND CAST(stb.quantity AS INT) > 0
            AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
        transaction_id,
        sales_date,
        store_id,
        product_id,
        quantity_sold,
        total_sales_amount
    FROM base
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

# --------------------------------------------------------------------
# 5) sales_aggregates_silver (sas)
# --------------------------------------------------------------------
sales_aggregates_silver_df = spark.sql(
    """
    SELECT
        sts.sales_date AS sales_date,
        sts.store_id AS store_id,
        pms.product_category AS product_category,
        CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
        CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    GROUP BY
        sts.sales_date,
        sts.store_id,
        pms.product_category
    """
)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_silver.csv")
)

# --------------------------------------------------------------------
# 6) data_quality_metrics_silver (dqms)
# --------------------------------------------------------------------
data_quality_metrics_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            DATE(stb.transaction_time) AS metric_date,
            stb.transaction_id AS transaction_id,
            stb.store_id AS store_id,
            stb.product_id AS product_id,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
            sts.transaction_id AS silver_transaction_id,
            CASE
                WHEN COUNT(stb.transaction_id) OVER (
                    PARTITION BY DATE(stb.transaction_time), stb.transaction_id
                ) > 1 THEN 1 ELSE 0
            END AS dup_flag,
            CASE
                WHEN stb.store_id IS NULL
                  OR stb.product_id IS NULL
                  OR CAST(stb.quantity AS INT) <= 0
                  OR CAST(stb.sale_amount AS DOUBLE) < 0
                  OR sts.transaction_id IS NULL
                THEN 1 ELSE 0
            END AS err_flag
        FROM sales_transactions_bronze stb
        LEFT JOIN sales_transactions_silver sts
            ON TRIM(stb.transaction_id) = sts.transaction_id
    )
    SELECT
        metric_date,
        CAST(COUNT(transaction_id) AS INT) AS total_transactions,
        CAST(SUM(dup_flag) AS INT) AS duplicates_count,
        CAST(SUM(err_flag) AS INT) AS errors_count
    FROM base
    GROUP BY metric_date
    """
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()