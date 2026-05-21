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

# ----------------------------
# 1) Read source tables (S3)
# ----------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ---------------------------------------
# TARGET TABLE: product_master_silver
# ---------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(pb.product_id))       AS product_id,
            TRIM(pb.product_name)            AS product_name,
            TRIM(pb.category)                AS category,
            CAST(pb.price AS float)          AS price
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    ),
    dedup AS (
        SELECT
            product_id,
            product_name,
            category,
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
        price
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
# TARGET TABLE: store_master_silver
# ---------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(sb.store_id)) AS store_id,
            TRIM(sb.store_name)      AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            CASE TRIM(UPPER(sb.state))
                WHEN 'CT' THEN 'NORTHEAST'
                WHEN 'ME' THEN 'NORTHEAST'
                WHEN 'MA' THEN 'NORTHEAST'
                WHEN 'NH' THEN 'NORTHEAST'
                WHEN 'RI' THEN 'NORTHEAST'
                WHEN 'VT' THEN 'NORTHEAST'
                WHEN 'NJ' THEN 'NORTHEAST'
                WHEN 'NY' THEN 'NORTHEAST'
                WHEN 'PA' THEN 'NORTHEAST'
                WHEN 'IL' THEN 'MIDWEST'
                WHEN 'IN' THEN 'MIDWEST'
                WHEN 'MI' THEN 'MIDWEST'
                WHEN 'OH' THEN 'MIDWEST'
                WHEN 'WI' THEN 'MIDWEST'
                WHEN 'IA' THEN 'MIDWEST'
                WHEN 'KS' THEN 'MIDWEST'
                WHEN 'MN' THEN 'MIDWEST'
                WHEN 'MO' THEN 'MIDWEST'
                WHEN 'NE' THEN 'MIDWEST'
                WHEN 'ND' THEN 'MIDWEST'
                WHEN 'SD' THEN 'MIDWEST'
                WHEN 'DE' THEN 'SOUTH'
                WHEN 'FL' THEN 'SOUTH'
                WHEN 'GA' THEN 'SOUTH'
                WHEN 'MD' THEN 'SOUTH'
                WHEN 'NC' THEN 'SOUTH'
                WHEN 'SC' THEN 'SOUTH'
                WHEN 'VA' THEN 'SOUTH'
                WHEN 'DC' THEN 'SOUTH'
                WHEN 'WV' THEN 'SOUTH'
                WHEN 'AL' THEN 'SOUTH'
                WHEN 'KY' THEN 'SOUTH'
                WHEN 'MS' THEN 'SOUTH'
                WHEN 'TN' THEN 'SOUTH'
                WHEN 'AR' THEN 'SOUTH'
                WHEN 'LA' THEN 'SOUTH'
                WHEN 'OK' THEN 'SOUTH'
                WHEN 'TX' THEN 'SOUTH'
                WHEN 'AZ' THEN 'WEST'
                WHEN 'CO' THEN 'WEST'
                WHEN 'ID' THEN 'WEST'
                WHEN 'MT' THEN 'WEST'
                WHEN 'NV' THEN 'WEST'
                WHEN 'NM' THEN 'WEST'
                WHEN 'UT' THEN 'WEST'
                WHEN 'WY' THEN 'WEST'
                WHEN 'AK' THEN 'WEST'
                WHEN 'CA' THEN 'WEST'
                WHEN 'HI' THEN 'WEST'
                WHEN 'OR' THEN 'WEST'
                WHEN 'WA' THEN 'WEST'
                ELSE NULL
            END AS region
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
        SELECT
            store_id,
            store_name,
            location,
            region,
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
        region
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

# ---------------------------------------
# TARGET TABLE: sales_transactions_silver
# ---------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(stb.transaction_id))            AS transaction_id,
            TRIM(UPPER(stb.product_id))                AS product_id,
            TRIM(UPPER(stb.store_id))                  AS store_id,
            CAST(stb.transaction_time AS DATE)         AS sale_date,
            CAST(stb.quantity AS int)                  AS quantity_sold,
            CAST(stb.sale_amount AS double)            AS total_sales,
            stb.transaction_time                       AS transaction_time
        FROM sales_transactions_bronze stb
        LEFT JOIN product_master_silver pms
            ON TRIM(UPPER(stb.product_id)) = pms.product_id
        LEFT JOIN store_master_silver sms
            ON TRIM(UPPER(stb.store_id)) = sms.store_id
        WHERE stb.transaction_id IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
          AND stb.product_id IS NOT NULL
          AND TRIM(stb.product_id) <> ''
          AND stb.store_id IS NOT NULL
          AND TRIM(stb.store_id) <> ''
    ),
    cleaned AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            COALESCE(quantity_sold, 0) AS quantity_sold,
            COALESCE(total_sales, 0D)  AS total_sales,
            transaction_time
        FROM base
    ),
    dedup AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sale_date,
            quantity_sold,
            total_sales,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM cleaned
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sale_date,
        quantity_sold,
        total_sales
    FROM dedup
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
