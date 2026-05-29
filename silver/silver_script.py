import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# Read Source Tables (Bronze)
# =========================

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

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

# =========================
# Target: sales_transactions_silver
# =========================

sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.store_id AS STRING) AS store_id,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(stb.transaction_id AS STRING)
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        WHERE
            stb.transaction_id IS NOT NULL
            AND stb.store_id IS NOT NULL
            AND stb.product_id IS NOT NULL
            AND stb.transaction_time IS NOT NULL
            AND stb.quantity IS NOT NULL
            AND stb.sale_amount IS NOT NULL
    )
    SELECT
        transaction_id,
        store_id,
        product_id,
        sale_date,
        quantity_sold,
        total_revenue
    FROM ranked
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

# =========================
# Target: product_master_silver
# =========================

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(pmb.product_id AS STRING)) AS product_id,
            NULLIF(TRIM(CAST(pmb.product_name AS STRING)), '') AS product_name,
            NULLIF(TRIM(CAST(pmb.category AS STRING)), '') AS category,
            CAST(pmb.price AS DECIMAL(38, 18)) AS price,
            pmb.is_active AS is_active
        FROM product_master_bronze pmb
        WHERE pmb.product_id IS NOT NULL
    ),
    filtered AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            is_active
        FROM base
        WHERE
            (is_active = true OR is_active IS NULL)
            AND (price IS NULL OR price >= 0)
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY CASE WHEN is_active = true THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM filtered
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM ranked
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

# =========================
# Target: store_master_silver
# =========================

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(CAST(smb.store_id AS STRING)) AS store_id,
            CONCAT_WS(', ', smb.city, smb.state) AS store_location,
            NULLIF(TRIM(CAST(smb.store_name AS STRING)), '') AS store_name,
            CASE
                WHEN UPPER(TRIM(smb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
                WHEN UPPER(TRIM(smb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
                WHEN UPPER(TRIM(smb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
                WHEN UPPER(TRIM(smb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
                ELSE NULL
            END AS region
        FROM store_master_bronze smb
        WHERE smb.store_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            store_id,
            store_location,
            store_name,
            region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                    CASE WHEN store_location IS NOT NULL THEN 1 ELSE 0 END DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_location,
        store_name,
        region
    FROM ranked
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

# =========================
# Target: sales_aggregated_silver
# =========================

sales_aggregated_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.sale_date AS reporting_date,
        CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
        CAST(SUM(sts.total_revenue) AS DOUBLE) AS total_revenue,
        CASE
            WHEN SUM(sts.quantity_sold) = 0 THEN NULL
            ELSE CAST(SUM(sts.total_revenue) / SUM(sts.quantity_sold) AS DOUBLE)
        END AS average_price
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.sale_date
    """
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()
