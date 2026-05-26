import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3 (Bronze)
# -----------------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------------
# 2) product_details_silver (pds)
# Columns: product_id, product_name, category, brand
# Transformations: trim/upper(product_id); trim(product_name, category, brand); filter is_active=true;
# De-duplicate on product_id (deterministic rule implemented with row_number)
# -----------------------------------------------------------------------------------

product_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(pb.product_id)) AS product_id,
        TRIM(pb.product_name)      AS product_name,
        TRIM(pb.category)          AS category,
        TRIM(pb.brand)             AS brand
      FROM products_bronze pb
      WHERE pb.is_active = true
    ),
    ranked AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NULL OR product_name = '' THEN 1 ELSE 0 END ASC,
            CASE WHEN category IS NULL OR category = '' THEN 1 ELSE 0 END ASC,
            CASE WHEN brand IS NULL OR brand = '' THEN 1 ELSE 0 END ASC
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand
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

# -----------------------------------------------------------------------------------
# 3) store_details_silver (sds)
# Columns: store_id, store_name, city, state, store_type
# Transformations: trim/upper(store_id); trim(store_name, city, state, store_type); cast open_date to date;
# De-duplicate on store_id keeping latest by open_date
# -----------------------------------------------------------------------------------

store_details_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(sb.store_id)) AS store_id,
        TRIM(sb.store_name)      AS store_name,
        TRIM(sb.city)            AS city,
        TRIM(sb.state)           AS state,
        TRIM(sb.store_type)      AS store_type,
        CAST(sb.open_date AS DATE) AS open_date
      FROM stores_bronze sb
    ),
    ranked AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            open_date DESC,
            CASE WHEN store_name IS NULL OR store_name = '' THEN 1 ELSE 0 END ASC,
            CASE WHEN city IS NULL OR city = '' THEN 1 ELSE 0 END ASC,
            CASE WHEN state IS NULL OR state = '' THEN 1 ELSE 0 END ASC,
            CASE WHEN store_type IS NULL OR store_type = '' THEN 1 ELSE 0 END ASC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type
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

# -----------------------------------------------------------------------------------
# 4) sales_transactions_silver (sts)
# Columns: transaction_id, store_id, product_id, transaction_date, quantity_sold, total_revenue
# Transformations: trim/upper keys; transaction_date=cast(transaction_time as date);
# quantity_sold=coalesce(quantity,0); total_revenue=coalesce(sale_amount,0.0);
# filter invalid rows; deduplicate on transaction_id keeping latest by transaction_time
# Note: mapping includes joins to silver store_details_silver and product_details_silver for conformance
# -----------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(stb.transaction_id)) AS transaction_id,
        TRIM(UPPER(stb.store_id))       AS store_id,
        TRIM(UPPER(stb.product_id))     AS product_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        COALESCE(CAST(stb.quantity AS INT), 0) AS quantity_sold,
        COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) AS total_revenue,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
      FROM sales_transactions_bronze stb
      INNER JOIN store_details_silver sds
        ON sds.store_id = TRIM(UPPER(stb.store_id))
      INNER JOIN product_details_silver pds
        ON pds.product_id = TRIM(UPPER(stb.product_id))
      WHERE TRIM(UPPER(stb.transaction_id)) IS NOT NULL
        AND TRIM(UPPER(stb.transaction_id)) <> ''
        AND NOT (COALESCE(CAST(stb.quantity AS INT), 0) <= 0 AND COALESCE(CAST(stb.sale_amount AS DOUBLE), 0.0) = 0.0)
    ),
    ranked AS (
      SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time_ts DESC
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      transaction_date,
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

# -----------------------------------------------------------------------------------
# 5) store_revenue_daily_silver (srds)
# Columns: store_id, reporting_date, total_revenue, total_transactions
# Transformations: reporting_date=transaction_date; total_revenue=sum(total_revenue);
# total_transactions=count(distinct transaction_id)
# -----------------------------------------------------------------------------------

store_revenue_daily_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.transaction_date AS reporting_date,
      SUM(sts.total_revenue) AS total_revenue,
      COUNT(DISTINCT sts.transaction_id) AS total_transactions
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.transaction_date
    """
)
store_revenue_daily_silver_df.createOrReplaceTempView("store_revenue_daily_silver")

(
    store_revenue_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_revenue_daily_silver.csv")
)

# -----------------------------------------------------------------------------------
# 6) product_sales_daily_silver (psds)
# Columns: product_id, transaction_date, total_revenue, quantity_sold
# Transformations: total_revenue=sum(total_revenue); quantity_sold=sum(quantity_sold)
# -----------------------------------------------------------------------------------

product_sales_daily_silver_df = spark.sql(
    """
    SELECT
      sts.product_id AS product_id,
      sts.transaction_date AS transaction_date,
      SUM(sts.total_revenue) AS total_revenue,
      SUM(sts.quantity_sold) AS quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      sts.product_id,
      sts.transaction_date
    """
)
product_sales_daily_silver_df.createOrReplaceTempView("product_sales_daily_silver")

(
    product_sales_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_sales_daily_silver.csv")
)

# -----------------------------------------------------------------------------------
# 7) category_performance_daily_silver (cpds)
# Columns: category, transaction_date, category_revenue, category_quantity_sold
# Transformations: category_revenue=sum(psds.total_revenue); category_quantity_sold=sum(psds.quantity_sold)
# -----------------------------------------------------------------------------------

category_performance_daily_silver_df = spark.sql(
    """
    SELECT
      pds.category AS category,
      psds.transaction_date AS transaction_date,
      SUM(psds.total_revenue) AS category_revenue,
      SUM(psds.quantity_sold) AS category_quantity_sold
    FROM product_sales_daily_silver psds
    INNER JOIN product_details_silver pds
      ON pds.product_id = psds.product_id
    GROUP BY
      pds.category,
      psds.transaction_date
    """
)
category_performance_daily_silver_df.createOrReplaceTempView("category_performance_daily_silver")

(
    category_performance_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/category_performance_daily_silver.csv")
)

job.commit()
