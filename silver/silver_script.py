import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("glue_silver_build", sys.argv)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) products_silver
# ----------------------------
products_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(pb.product_id)   AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category)     AS category,
        CAST(pb.price AS float) AS price
    FROM products_bronze pb
    WHERE CAST(pb.price AS float) >= 0
      AND LOWER(TRIM(pb.is_active)) = 'true'
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END DESC
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
""")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ----------------------------
# 2) stores_silver
# ----------------------------
stores_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(sb.store_id)   AS store_id,
        TRIM(sb.store_name) AS store_name,
        CASE
            WHEN TRIM(sb.state) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
            WHEN TRIM(sb.state) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
            WHEN TRIM(sb.state) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
            WHEN TRIM(sb.state) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
            ELSE 'Unknown'
        END AS region
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY
                CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END DESC,
                CASE WHEN region IS NOT NULL AND TRIM(region) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    region
FROM dedup
WHERE rn = 1
""")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# Create temp views for downstream joins (read from the just-built dataframes)
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ----------------------------
# 3) sales_transactions_silver
# ----------------------------
sales_transactions_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
WITH joined AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.store_id)       AS store_id,
        TRIM(stb.product_id)     AS product_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        CAST(stb.quantity AS int) AS quantity_sold,
        COALESCE(CAST(stb.sale_amount AS double), (CAST(stb.quantity AS double) * CAST(ps.price AS double))) AS revenue,
        stb.transaction_time AS transaction_time
    FROM sales_transactions_bronze stb
    INNER JOIN stores_silver ss
        ON TRIM(stb.store_id) = ss.store_id
    INNER JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
),
filtered AS (
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        revenue,
        transaction_time
    FROM joined
    WHERE quantity_sold > 0
      AND revenue >= 0
),
dedup AS (
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
    FROM filtered
)
SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    revenue
FROM dedup
WHERE rn = 1
""")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ----------------------------
# 4) data_quality_metrics_silver
# ----------------------------
data_quality_metrics_silver_df = spark.sql("""
WITH base AS (
    SELECT
        sts.transaction_date AS date,
        sts.transaction_id,
        sts.store_id,
        sts.product_id,
        sts.quantity_sold,
        sts.revenue
    FROM sales_transactions_silver sts
),
with_dupes AS (
    SELECT
        date,
        transaction_id,
        store_id,
        product_id,
        quantity_sold,
        revenue,
        CASE WHEN COUNT(transaction_id) OVER (PARTITION BY transaction_id) > 1 THEN 1 ELSE 0 END AS is_duplicate
    FROM base
),
metrics AS (
    SELECT
        date,
        COUNT(transaction_id) AS total_records,
        SUM(
            CASE
                WHEN transaction_id IS NULL OR TRIM(transaction_id) = ''
                  OR store_id IS NULL OR TRIM(store_id) = ''
                  OR product_id IS NULL OR TRIM(product_id) = ''
                  OR date IS NULL
                  OR quantity_sold <= 0
                  OR revenue < 0
                THEN 1 ELSE 0
            END
        ) AS invalid_records,
        SUM(is_duplicate) AS duplicate_records,
        CONCAT(
            'missing_keys=',
            SUM(CASE WHEN transaction_id IS NULL OR TRIM(transaction_id) = '' OR store_id IS NULL OR TRIM(store_id) = '' OR product_id IS NULL OR TRIM(product_id) = '' THEN 1 ELSE 0 END),
            ';nonpositive_quantity=',
            SUM(CASE WHEN quantity_sold <= 0 THEN 1 ELSE 0 END),
            ';negative_revenue=',
            SUM(CASE WHEN revenue < 0 THEN 1 ELSE 0 END),
            ';missing_transaction_date=',
            SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END)
        ) AS validation_errors
    FROM with_dupes
    GROUP BY date
)
SELECT
    date,
    total_records,
    invalid_records,
    duplicate_records,
    validation_errors
FROM metrics
""")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()
