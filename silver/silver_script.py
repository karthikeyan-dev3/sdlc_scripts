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

# -------------------------
# 1) Read source tables (Bronze)
# -------------------------
bronze_products_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_products_raw.{FILE_FORMAT}/")
)

bronze_stores_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_stores_raw.{FILE_FORMAT}/")
)

bronze_sales_transactions_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/bronze_sales_transactions_raw.{FILE_FORMAT}/")
)

# -------------------------
# 2) Create temp views
# -------------------------
bronze_products_raw_df.createOrReplaceTempView("bronze_products_raw")
bronze_stores_raw_df.createOrReplaceTempView("bronze_stores_raw")
bronze_sales_transactions_raw_df.createOrReplaceTempView("bronze_sales_transactions_raw")

# -------------------------
# 3) Transform + 4) Write: silver_products
# -------------------------
silver_products_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(bpr.product_id)) AS product_id,
            COALESCE(TRIM(bpr.product_name), 'UNKNOWN') AS product_name,
            TRIM(UPPER(COALESCE(bpr.category, 'UNKNOWN'))) AS category,
            CASE WHEN bpr.price IS NULL OR CAST(bpr.price AS DOUBLE) < 0 THEN 0 ELSE CAST(bpr.price AS DOUBLE) END AS price,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(UPPER(bpr.product_id))
                ORDER BY
                    CASE WHEN bpr.product_name IS NULL OR TRIM(bpr.product_name) = '' THEN 0 ELSE 1 END
                    + CASE WHEN bpr.category IS NULL OR TRIM(bpr.category) = '' THEN 0 ELSE 1 END
                    + CASE WHEN bpr.price IS NULL THEN 0 ELSE 1 END DESC
            ) AS rn
        FROM bronze_products_raw bpr
        WHERE COALESCE(LOWER(TRIM(bpr.is_active)), 'false') = 'true'
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM base
    WHERE rn = 1
    """
)

(
    silver_products_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_products.csv")
)

silver_products_df.createOrReplaceTempView("silver_products")

# -------------------------
# 3) Transform + 4) Write: silver_stores
# -------------------------
silver_stores_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(bsr.store_id)) AS store_id,
            COALESCE(TRIM(bsr.store_name), 'UNKNOWN') AS store_name,
            CONCAT(
                COALESCE(TRIM(bsr.city), ''),
                ', ',
                TRIM(UPPER(COALESCE(bsr.state, '')))
            ) AS location,
            CAST(NULL AS STRING) AS manager,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(UPPER(bsr.store_id))
                ORDER BY
                    CASE WHEN bsr.store_name IS NULL OR TRIM(bsr.store_name) = '' THEN 0 ELSE 1 END
                    + CASE WHEN bsr.city IS NULL OR TRIM(bsr.city) = '' THEN 0 ELSE 1 END
                    + CASE WHEN bsr.state IS NULL OR TRIM(bsr.state) = '' THEN 0 ELSE 1 END DESC
            ) AS rn
        FROM bronze_stores_raw bsr
    )
    SELECT
        store_id,
        store_name,
        location,
        manager
    FROM base
    WHERE rn = 1
    """
)

(
    silver_stores_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_stores.csv")
)

silver_stores_df.createOrReplaceTempView("silver_stores")

# -------------------------
# 3) Transform + 4) Write: silver_sales_transactions
# -------------------------
silver_sales_transactions_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            bstr.transaction_id AS transaction_id,
            TRIM(UPPER(bstr.store_id)) AS store_id,
            TRIM(UPPER(bstr.product_id)) AS product_id,
            CAST(bstr.transaction_time AS DATE) AS sale_date,
            CASE
                WHEN bstr.quantity IS NULL OR CAST(bstr.quantity AS INT) < 0 THEN 0
                ELSE CAST(bstr.quantity AS INT)
            END AS quantity_sold,
            CASE
                WHEN bstr.sale_amount IS NULL OR CAST(bstr.sale_amount AS DOUBLE) < 0 THEN 0
                ELSE CAST(bstr.sale_amount AS DOUBLE)
            END AS total_revenue,
            bstr.transaction_time AS transaction_time
        FROM bronze_sales_transactions_raw bstr
        INNER JOIN silver_products sp
            ON TRIM(UPPER(bstr.product_id)) = sp.product_id
        INNER JOIN silver_stores ss
            ON TRIM(UPPER(bstr.store_id)) = ss.store_id
        WHERE bstr.transaction_id IS NOT NULL
    ),
    dedup AS (
        SELECT
            transaction_id,
            store_id,
            product_id,
            sale_date,
            quantity_sold,
            total_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM joined
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        sale_date,
        quantity_sold,
        total_revenue
    FROM dedup
    WHERE rn = 1
    """
)

(
    silver_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sales_transactions.csv")
)

silver_sales_transactions_df.createOrReplaceTempView("silver_sales_transactions")

# -------------------------
# 3) Transform + 4) Write: silver_daily_sales_summary
# -------------------------
silver_daily_sales_summary_df = spark.sql(
    """
    SELECT
        sst.sale_date AS date,
        SUM(sst.total_revenue) AS total_revenue,
        COUNT(DISTINCT sst.transaction_id) AS total_transactions,
        CASE
            WHEN COUNT(DISTINCT sst.transaction_id) = 0 THEN 0
            ELSE SUM(sst.total_revenue) / COUNT(DISTINCT sst.transaction_id)
        END AS average_ticket_size
    FROM silver_sales_transactions sst
    GROUP BY sst.sale_date
    """
)

(
    silver_daily_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_daily_sales_summary.csv")
)

job.commit()