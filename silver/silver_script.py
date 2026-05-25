import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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
# 1) Read source tables (Bronze) and create temp views
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
# 2) product_details_silver
#    - TRIM product_id/product_name/category
#    - filter product_id not null
#    - dedup: prefer is_active=true; tie-breaker by highest non-null price/brand (as available)
# -------------------------------------------------------------------
product_details_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id)   AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category)     AS category,
            pb.is_active          AS is_active,
            pb.price              AS price,
            pb.brand              AS brand
        FROM products_bronze pb
        WHERE TRIM(pb.product_id) IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY
                    CASE WHEN COALESCE(is_active, false) = true THEN 1 ELSE 0 END DESC,
                    CASE WHEN price IS NULL THEN 0 ELSE 1 END DESC,
                    CAST(COALESCE(price, 0) AS DOUBLE) DESC,
                    CASE WHEN brand IS NULL OR TRIM(brand) = '' THEN 0 ELSE 1 END DESC,
                    TRIM(brand) DESC
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category
    FROM ranked
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

# -------------------------------------------------------------------
# 3) store_details_silver
#    - TRIM store_id/store_name/state/city
#    - filter store_id not null
#    - region derived from state (state-to-region mapping)
#    - dedup: prefer most complete store_name/state/city; tie-breaker by latest open_date
# -------------------------------------------------------------------
store_details_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id)   AS store_id,
            TRIM(sb.store_name) AS store_name,
            TRIM(sb.state)      AS state,
            TRIM(sb.city)       AS city,
            sb.open_date        AS open_date
        FROM stores_bronze sb
        WHERE TRIM(sb.store_id) IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    ),
    enriched AS (
        SELECT
            store_id,
            store_name,
            CASE
                WHEN UPPER(state) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
                WHEN UPPER(state) IN ('OH','MI','IN','IL','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
                WHEN UPPER(state) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'SOUTH'
                WHEN UPPER(state) IN ('ID','MT','WY','NV','UT','CO','AZ','NM','WA','OR','CA','AK','HI') THEN 'WEST'
                ELSE NULL
            END AS region,
            state,
            city,
            open_date,
            (CASE WHEN store_name IS NOT NULL AND store_name <> '' THEN 1 ELSE 0 END
             + CASE WHEN state IS NOT NULL AND state <> '' THEN 1 ELSE 0 END
             + CASE WHEN city IS NOT NULL AND city <> '' THEN 1 ELSE 0 END) AS completeness_score
        FROM base
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY
                    completeness_score DESC,
                    open_date DESC
            ) AS rn
        FROM enriched
    )
    SELECT
        store_id,
        store_name,
        region
    FROM ranked
    WHERE rn = 1
    """
)
store_details_silver_df.createOrReplaceTempView("store_details_silver")

(
    store_details_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_details_silver.csv")
)

# -------------------------------------------------------------------
# 4) sales_transactions_silver
#    - joins to conformed product/store keys
#    - sale_date = CAST(transaction_time AS DATE)
#    - quantity_sold = GREATEST(COALESCE(quantity,0),0)
#    - total_sales_value = GREATEST(COALESCE(sale_amount,0),0)
#    - dedup by transaction_id: latest transaction_time, then highest total_sales_value
#    - enforce non-null transaction_id/product_id/store_id
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            stb.product_id AS product_id,
            stb.store_id AS store_id,
            GREATEST(COALESCE(CAST(stb.quantity AS INT), 0), 0) AS quantity_sold,
            GREATEST(COALESCE(CAST(stb.sale_amount AS DOUBLE), 0), 0) AS total_sales_value,
            stb.transaction_time AS transaction_time
        FROM sales_transactions_bronze stb
        LEFT JOIN product_details_silver pds
            ON stb.product_id = pds.product_id
        LEFT JOIN store_details_silver sds
            ON stb.store_id = sds.store_id
        WHERE stb.transaction_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND stb.store_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            transaction_id,
            sale_date,
            product_id,
            store_id,
            quantity_sold,
            total_sales_value,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    transaction_time DESC,
                    total_sales_value DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        sale_date,
        product_id,
        store_id,
        quantity_sold,
        total_sales_value
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

# -------------------------------------------------------------------
# 5) aggregated_sales_silver
#    - aggregation_date = sale_date
#    - totals and average on measures
# -------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS aggregation_date,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_sales_value) AS total_sales_value,
        AVG(sts.total_sales_value) AS average_sales_value
    FROM sales_transactions_silver sts
    GROUP BY sts.sale_date
    """
)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
