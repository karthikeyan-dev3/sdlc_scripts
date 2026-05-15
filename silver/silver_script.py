import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------
# 1) Read source tables from S3
# -------------------------------------
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

# -------------------------------------
# 2) Create temp views
# -------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =====================================================
# TABLE: product_master_silver
# 1) Read source table(s): products_bronze
# 2) Temp view(s): products_bronze already created
# 3) SQL transformation
# 4) Save output
# =====================================================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        pb.price AS price
      FROM products_bronze pb
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
            CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 0 ELSE 1 END,
            CASE WHEN category IS NOT NULL AND TRIM(category) <> '' THEN 0 ELSE 1 END,
            CASE WHEN price IS NOT NULL THEN 0 ELSE 1 END
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
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_master_silver.csv")
)

# =====================================================
# TABLE: store_master_silver
# 1) Read source table(s): stores_bronze
# 2) Temp view(s): stores_bronze already created
# 3) SQL transformation
# 4) Save output
# =====================================================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        sb.city AS city,
        sb.state AS state
      FROM stores_bronze sb
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 0 ELSE 1 END,
            CASE WHEN city IS NOT NULL AND TRIM(city) <> '' THEN 0 ELSE 1 END,
            CASE WHEN state IS NOT NULL AND TRIM(state) <> '' THEN 0 ELSE 1 END
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      city || ', ' || state AS location,
      state AS region
    FROM dedup
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_master_silver.csv")
)

# =====================================================
# TABLE: sales_transactions_silver
# 1) Read source table(s): sales_transactions_bronze
# 2) Temp view(s): sales_transactions_bronze already created
# 3) SQL transformation (includes joins to product_master_silver and store_master_silver)
# 4) Save output
# =====================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        stb.transaction_id AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS sales_date,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        stb.quantity AS quantity_sold,
        stb.sale_amount AS sales_amount,
        pms.product_id AS p_product_id,
        sms.store_id AS s_store_id
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON stb.product_id = pms.product_id
      LEFT JOIN store_master_silver sms
        ON stb.store_id = sms.store_id
    ),
    filtered AS (
      SELECT
        transaction_id,
        sales_date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount
      FROM joined
      WHERE p_product_id IS NOT NULL
        AND s_store_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        transaction_id,
        sales_date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY sales_date DESC
        ) AS rn
      FROM filtered
    )
    SELECT
      transaction_id,
      sales_date,
      product_id,
      store_id,
      quantity_sold,
      sales_amount
    FROM dedup
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =====================================================
# TABLE: sales_aggregated_silver
# 1) Read source table(s): sales_transactions_silver (temp view)
# 2) Temp view(s): sales_transactions_silver already created
# 3) SQL transformation (aggregation)
# 4) Save output
# =====================================================
sales_aggregated_silver_df = spark.sql(
    """
    SELECT
      sts.sales_date AS date,
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      SUM(sts.quantity_sold) AS total_quantity_sold,
      SUM(sts.sales_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
      sts.sales_date,
      sts.store_id,
      sts.product_id
    """
)
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# =====================================================
# TABLE: data_quality_metrics_silver
# 1) Read source table(s): product_master_silver, store_master_silver, sales_transactions_silver + sales_transactions_bronze (for MAX(transaction_time))
# 2) Temp view(s): already created
# 3) SQL transformation (metrics)
# 4) Save output
# =====================================================
data_quality_metrics_silver_df = spark.sql(
    """
    SELECT
      'duplicate_transaction_id_count' AS metric_name,
      CAST(COUNT(sts.transaction_id) - COUNT(DISTINCT sts.transaction_id) AS DOUBLE) AS metric_value,
      MAX(stb.transaction_time) AS last_updated
    FROM product_master_silver pms
    CROSS JOIN store_master_silver sms
    CROSS JOIN sales_transactions_silver sts
    CROSS JOIN sales_transactions_bronze stb
    """
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)