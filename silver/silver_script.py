import sys
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =========================
# 1) READ SOURCE TABLES
# =========================
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

# =========================
# 2) CREATE TEMP VIEWS
# =========================
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# =========================
# 3) TRANSFORM + WRITE: product_master_silver
# =========================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS product_category,
        CAST(TRIM(pb.price) AS FLOAT) AS product_price
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        product_category,
        product_price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            CASE WHEN product_name IS NOT NULL AND TRIM(product_name) <> '' THEN 0 ELSE 1 END,
            CASE WHEN product_category IS NOT NULL AND TRIM(product_category) <> '' THEN 0 ELSE 1 END,
            CASE WHEN product_price IS NOT NULL THEN 0 ELSE 1 END
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      product_category,
      product_price
    FROM dedup
    WHERE rn = 1
    """
)

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# =========================
# 4) TRANSFORM + WRITE: store_master_silver
# =========================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
        TRIM(sb.state) AS store_region
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        store_location,
        store_region,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            CASE WHEN store_name IS NOT NULL AND TRIM(store_name) <> '' THEN 0 ELSE 1 END,
            CASE WHEN store_location IS NOT NULL AND TRIM(store_location) <> '' THEN 0 ELSE 1 END,
            CASE WHEN store_region IS NOT NULL AND TRIM(store_region) <> '' THEN 0 ELSE 1 END
        ) AS rn
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

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# =========================
# 5) TRANSFORM + WRITE: sales_transactions_silver
# =========================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(stb.transaction_id)) AS transaction_id,
        UPPER(TRIM(stb.product_id)) AS product_id,
        UPPER(TRIM(stb.store_id)) AS store_id,
        CAST(TRIM(stb.sale_amount) AS DOUBLE) AS sale_amount,
        CAST(TRIM(stb.quantity) AS INT) AS quantity,
        DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sale_date,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
      FROM sales_transactions_bronze stb
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
    ),
    validated AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        CASE WHEN sale_amount < 0 THEN NULL ELSE sale_amount END AS sale_amount,
        CASE WHEN quantity < 0 THEN NULL ELSE quantity END AS quantity,
        sale_date,
        transaction_time
      FROM base
    ),
    dedup AS (
      SELECT
        transaction_id,
        product_id,
        store_id,
        sale_amount,
        quantity,
        sale_date,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM validated
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      sale_amount,
      quantity,
      sale_date
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

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# =========================
# 6) TRANSFORM + WRITE: sales_enriched_silver
# =========================
sales_enriched_silver_df = spark.sql(
    """
    SELECT
      sts.transaction_id AS transaction_id,
      sts.product_id AS product_id,
      sts.store_id AS store_id,
      sts.sale_date AS sale_date,
      sts.sale_amount AS sale_amount,
      sts.quantity AS quantity_sold,
      pms.product_name AS product_name,
      pms.product_category AS product_category,
      sms.store_name AS store_name,
      sms.store_location AS store_location
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
      ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
      ON sts.store_id = sms.store_id
    """
)

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

# =========================
# 7) TRANSFORM + WRITE: daily_sales_agg_silver
# =========================
daily_sales_agg_silver_df = spark.sql(
    """
    WITH daily_product AS (
      SELECT
        sts.sale_date AS reporting_date,
        sts.product_id AS top_selling_product,
        SUM(sts.sale_amount) AS product_sales_amount,
        SUM(sts.quantity) AS product_quantity_sold
      FROM sales_transactions_silver sts
      GROUP BY sts.sale_date, sts.product_id
    ),
    daily_store AS (
      SELECT
        sts.sale_date AS reporting_date,
        sts.store_id AS top_selling_store,
        SUM(sts.sale_amount) AS store_sales_amount,
        SUM(sts.quantity) AS store_quantity_sold
      FROM sales_transactions_silver sts
      GROUP BY sts.sale_date, sts.store_id
    ),
    product_ranked AS (
      SELECT
        reporting_date,
        top_selling_product,
        product_sales_amount,
        product_quantity_sold,
        ROW_NUMBER() OVER (
          PARTITION BY reporting_date
          ORDER BY product_sales_amount DESC, product_quantity_sold DESC, top_selling_product ASC
        ) AS rn
      FROM daily_product
    ),
    store_ranked AS (
      SELECT
        reporting_date,
        top_selling_store,
        store_sales_amount,
        store_quantity_sold,
        ROW_NUMBER() OVER (
          PARTITION BY reporting_date
          ORDER BY store_sales_amount DESC, store_quantity_sold DESC, top_selling_store ASC
        ) AS rn
      FROM daily_store
    ),
    totals AS (
      SELECT
        sts.sale_date AS reporting_date,
        SUM(sts.sale_amount) AS total_sales_amount,
        SUM(sts.quantity) AS total_quantity_sold
      FROM sales_transactions_silver sts
      GROUP BY sts.sale_date
    )
    SELECT
      t.reporting_date AS reporting_date,
      t.total_sales_amount AS total_sales_amount,
      CAST(t.total_quantity_sold AS INT) AS total_quantity_sold,
      pr.top_selling_product AS top_selling_product,
      sr.top_selling_store AS top_selling_store
    FROM totals t
    LEFT JOIN product_ranked pr
      ON t.reporting_date = pr.reporting_date AND pr.rn = 1
    LEFT JOIN store_ranked sr
      ON t.reporting_date = sr.reporting_date AND sr.rn = 1
    """
)

(
    daily_sales_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_agg_silver.csv")
)