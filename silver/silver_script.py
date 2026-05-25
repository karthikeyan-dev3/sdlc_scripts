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
# 2) product_master_silver
#    Columns: product_id, product_name, category, brand, price, is_active
#    Transformations: TRIM/UPPER keys; filter active; de-dup by product_id
# -------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(pb.product_id)) AS product_id,
            TRIM(pb.product_name)      AS product_name,
            TRIM(pb.category)          AS category,
            TRIM(pb.brand)             AS brand,
            CAST(pb.price AS double)   AS price,
            CAST(pb.is_active AS boolean) AS is_active
        FROM products_bronze pb
        WHERE CAST(pb.is_active AS boolean) = true
          AND TRIM(pb.product_id) IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            price,
            is_active,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN category     IS NULL OR TRIM(category)     = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN brand        IS NULL OR TRIM(brand)        = '' THEN 1 ELSE 0 END ASC,
                    COALESCE(price, -1.0) DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        is_active
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

# -------------------------------------------------------------------
# 3) store_master_silver
#    Columns: store_id, store_name, region, store_type, city, state, open_date
#    Transformations: derive region from state; standardize store_type; dedup
# -------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            UPPER(TRIM(sb.store_id)) AS store_id,
            TRIM(sb.store_name)      AS store_name,
            TRIM(sb.city)            AS city,
            UPPER(TRIM(sb.state))    AS state,
            UPPER(TRIM(sb.store_type)) AS store_type,
            CAST(sb.open_date AS date) AS open_date,
            CASE
                WHEN UPPER(TRIM(sb.state)) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
                WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
                WHEN UPPER(TRIM(sb.state)) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'SOUTH'
                WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
                ELSE NULL
            END AS region
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            region,
            store_type,
            city,
            state,
            open_date,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    CASE WHEN store_name IS NULL OR TRIM(store_name) = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN state      IS NULL OR TRIM(state)      = '' THEN 1 ELSE 0 END ASC,
                    CASE WHEN store_type IS NULL OR TRIM(store_type) = '' THEN 1 ELSE 0 END ASC,
                    open_date DESC
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        region,
        store_type,
        city,
        state,
        open_date
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

# -------------------------------------------------------------------
# 4) sales_transactions_silver
#    Columns: transaction_id, transaction_date, store_id, product_id,
#             quantity_sold, sales_amount, transaction_time
#    Transformations: trim keys; transaction_date=CAST(transaction_time AS date);
#                     rename qty/amount; non-negative; dedup by transaction_id
#                     join to conformed store/product masters
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(stb.transaction_id)                 AS transaction_id,
            CAST(stb.transaction_time AS timestamp)  AS transaction_time,
            CAST(stb.transaction_time AS date)       AS transaction_date,
            UPPER(TRIM(stb.store_id))                AS store_id,
            UPPER(TRIM(stb.product_id))              AS product_id,
            CAST(stb.quantity AS int)                AS quantity_sold,
            CAST(stb.sale_amount AS double)          AS sales_amount
        FROM sales_transactions_bronze stb
        WHERE TRIM(stb.transaction_id) IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
    ),
    filtered AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            CASE WHEN COALESCE(quantity_sold, 0) < 0 THEN 0 ELSE quantity_sold END AS quantity_sold,
            CASE WHEN COALESCE(sales_amount, 0.0) < 0 THEN 0.0 ELSE sales_amount END AS sales_amount,
            transaction_time
        FROM base
    ),
    dedup AS (
        SELECT
            transaction_id,
            transaction_date,
            store_id,
            product_id,
            quantity_sold,
            sales_amount,
            transaction_time,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM filtered
    ),
    valid_keys AS (
        SELECT
            d.transaction_id,
            d.transaction_date,
            d.store_id,
            d.product_id,
            d.quantity_sold,
            d.sales_amount,
            d.transaction_time
        FROM dedup d
        INNER JOIN store_master_silver sms
            ON d.store_id = sms.store_id
        INNER JOIN product_master_silver pms
            ON d.product_id = pms.product_id
        WHERE d.rn = 1
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount,
        transaction_time
    FROM valid_keys
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

# -------------------------------------------------------------------
# 5) daily_sales_agg_silver
#    Columns: store_id, product_id, date, total_sales, total_quantity, average_sales_price
# -------------------------------------------------------------------
daily_sales_agg_silver_df = spark.sql(
    """
    SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        sts.transaction_date AS date,
        SUM(sts.sales_amount) AS total_sales,
        SUM(sts.quantity_sold) AS total_quantity,
        CASE
            WHEN SUM(sts.quantity_sold) = 0 THEN NULL
            ELSE SUM(sts.sales_amount) / SUM(sts.quantity_sold)
        END AS average_sales_price
    FROM sales_transactions_silver sts
    GROUP BY
        sts.store_id,
        sts.product_id,
        sts.transaction_date
    """
)

(
    daily_sales_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_agg_silver.csv")
)

job.commit()