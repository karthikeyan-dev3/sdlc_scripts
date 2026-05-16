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

# ------------------------------------------------------------------------------
# Read source tables from S3 (Bronze)
# ------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# Target: silver.product_master_silver
# Source: bronze.products_bronze pb
# ------------------------------------------------------------------------------

product_master_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CAST(pb.price AS double) AS price,
        pb.is_active AS is_active
    FROM products_bronze pb
    WHERE TRIM(pb.product_id) IS NOT NULL
      AND pb.is_active = true
),
dedup AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                (CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN brand IS NOT NULL AND TRIM(brand) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) DESC
        ) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price
FROM dedup
WHERE rn = 1
""")
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.store_master_silver
# Source: bronze.stores_bronze sb
# ------------------------------------------------------------------------------

store_master_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.city) AS city,
        TRIM(sb.state) AS state,
        TRIM(sb.store_type) AS store_type
    FROM stores_bronze sb
    WHERE TRIM(sb.store_id) IS NOT NULL
),
dedup AS (
    SELECT
        store_id,
        store_name,
        COALESCE(city, '') AS city,
        COALESCE(state, '') AS state,
        store_type,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY
                (CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN city IS NOT NULL AND TRIM(city) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN state IS NOT NULL AND TRIM(state) <> '' THEN 1 ELSE 0 END) DESC,
                (CASE WHEN store_type IS NOT NULL AND TRIM(store_type) <> '' THEN 1 ELSE 0 END) DESC
        ) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    CASE
        WHEN TRIM(city) <> '' AND TRIM(state) <> '' THEN CONCAT(city, ', ', state)
        WHEN TRIM(city) <> '' AND TRIM(state) = '' THEN city
        WHEN TRIM(city) = '' AND TRIM(state) <> '' THEN state
        ELSE NULL
    END AS location,
    store_type AS store_size
FROM dedup
WHERE rn = 1
""")
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# Source: bronze.sales_transactions_bronze stb
#        LEFT JOIN silver.product_master_silver pms ON stb.product_id = pms.product_id
#        LEFT JOIN silver.store_master_silver sms ON stb.store_id = sms.store_id
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql("""
WITH base AS (
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CAST(stb.sale_amount AS double) AS sales_amount,
        CAST(stb.transaction_time AS date) AS sale_date,
        CAST(stb.quantity AS int) AS quantity,
        stb.transaction_time AS transaction_time
    FROM sales_transactions_bronze stb
    WHERE TRIM(stb.transaction_id) IS NOT NULL
),
validated AS (
    SELECT
        b.transaction_id,
        b.product_id,
        b.store_id,
        CASE WHEN b.sales_amount >= 0 THEN b.sales_amount ELSE NULL END AS sales_amount,
        b.sale_date,
        CASE WHEN b.quantity >= 0 THEN b.quantity ELSE NULL END AS quantity,
        b.transaction_time
    FROM base b
),
joined AS (
    SELECT
        v.transaction_id,
        v.product_id,
        v.store_id,
        v.sales_amount,
        v.sale_date,
        v.quantity,
        v.transaction_time,
        pms.product_id AS ref_product_id,
        sms.store_id AS ref_store_id
    FROM validated v
    LEFT JOIN product_master_silver pms
        ON v.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
        ON v.store_id = sms.store_id
),
filtered AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        sales_amount,
        sale_date,
        quantity,
        transaction_time
    FROM joined
    WHERE ref_product_id IS NOT NULL
      AND ref_store_id IS NOT NULL
),
dedup AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        sales_amount,
        sale_date,
        quantity,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_time DESC
        ) AS rn
    FROM filtered
)
SELECT
    transaction_id,
    product_id,
    store_id,
    sales_amount,
    sale_date,
    quantity
FROM dedup
WHERE rn = 1
""")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.sales_aggregates_daily_silver
# Source: silver.sales_transactions_silver sts
# ------------------------------------------------------------------------------

sales_aggregates_daily_silver_df = spark.sql("""
SELECT
    sts.sale_date AS date,
    sts.store_id AS store_id,
    SUM(sts.sales_amount) AS total_sales,
    SUM(sts.quantity) AS total_units_sold,
    SUM(sts.sales_amount) / NULLIF(COUNT(DISTINCT sts.transaction_id), 0) AS average_transaction_value
FROM sales_transactions_silver sts
GROUP BY
    sts.sale_date,
    sts.store_id
""")
sales_aggregates_daily_silver_df.createOrReplaceTempView("sales_aggregates_daily_silver")

(
    sales_aggregates_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_daily_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: silver.data_quality_metrics_silver
# Source: silver.sales_transactions_silver sts, silver.product_master_silver pms, silver.store_master_silver sms
# ------------------------------------------------------------------------------

data_quality_metrics_silver_df = spark.sql("""
WITH base AS (
    SELECT
        sts.sale_date AS date_measured,
        sts.transaction_id,
        sts.product_id,
        sts.store_id,
        sts.sales_amount,
        sts.quantity
    FROM sales_transactions_silver sts
),
daily AS (
    SELECT
        date_measured,
        COUNT(1) AS total_rows,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS missing_product_id_cnt,
        SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END) AS missing_store_id_cnt,
        SUM(CASE WHEN sales_amount IS NULL THEN 1 ELSE 0 END) AS missing_sales_amount_cnt,
        SUM(CASE WHEN sales_amount < 0 THEN 1 ELSE 0 END) AS invalid_negative_amount_cnt,
        SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS invalid_negative_quantity_cnt
    FROM base
    GROUP BY date_measured
),
dup AS (
    SELECT
        sale_date AS date_measured,
        SUM(CASE WHEN rn > 1 THEN 1 ELSE 0 END) AS duplicate_transaction_id_count
    FROM (
        SELECT
            sts.sale_date,
            sts.transaction_id,
            ROW_NUMBER() OVER (PARTITION BY sts.transaction_id ORDER BY sts.transaction_id) AS rn
        FROM sales_transactions_silver sts
    ) t
    GROUP BY sale_date
),
orphan_prod AS (
    SELECT
        sts.sale_date AS date_measured,
        SUM(CASE WHEN pms.product_id IS NULL THEN 1 ELSE 0 END) AS orphan_product_fk_count
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
        ON sts.product_id = pms.product_id
    GROUP BY sts.sale_date
),
orphan_store AS (
    SELECT
        sts.sale_date AS date_measured,
        SUM(CASE WHEN sms.store_id IS NULL THEN 1 ELSE 0 END) AS orphan_store_fk_count
    FROM sales_transactions_silver sts
    LEFT JOIN store_master_silver sms
        ON sts.store_id = sms.store_id
    GROUP BY sts.sale_date
),
metrics AS (
    SELECT
        d.date_measured,
        d.total_rows,
        d.missing_product_id_cnt,
        d.missing_store_id_cnt,
        d.missing_sales_amount_cnt,
        d.invalid_negative_amount_cnt,
        d.invalid_negative_quantity_cnt,
        COALESCE(dp.duplicate_transaction_id_count, 0) AS duplicate_transaction_id_count,
        COALESCE(op.orphan_product_fk_count, 0) AS orphan_product_fk_count,
        COALESCE(os.orphan_store_fk_count, 0) AS orphan_store_fk_count
    FROM daily d
    LEFT JOIN dup dp
        ON d.date_measured = dp.date_measured
    LEFT JOIN orphan_prod op
        ON d.date_measured = op.date_measured
    LEFT JOIN orphan_store os
        ON d.date_measured = os.date_measured
)
SELECT
    date_measured,
    'pct_missing_product_id' AS metric_name,
    (missing_product_id_cnt * 1.0) / NULLIF(total_rows, 0) AS metric_value
FROM metrics
""")
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()