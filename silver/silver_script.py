import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables (Bronze) and create temp views
# ------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

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

# ------------------------------------------------------------------------------------
# 2) sales_transactions_silver (sts)
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(stb.transaction_id))                                     AS transaction_id,
        CAST(stb.transaction_time AS date)                                  AS transaction_date,
        TRIM(UPPER(stb.store_id))                                           AS store_id,
        TRIM(UPPER(stb.product_id))                                         AS product_id,
        CAST(stb.quantity AS int)                                           AS quantity_sold,
        CAST(stb.sale_amount AS decimal(18,2))                              AS revenue,
        stb.transaction_time                                                AS transaction_time
      FROM sales_transactions_bronze stb
      WHERE CAST(stb.quantity AS int) > 0
        AND CAST(stb.sale_amount AS decimal(18,2)) >= 0
    ),
    dedup AS (
      SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      transaction_date,
      store_id,
      product_id,
      quantity_sold,
      revenue
    FROM dedup
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

# ------------------------------------------------------------------------------------
# 3) product_master_silver (pms)
# ------------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(pb.product_id))                              AS product_id,
        TRIM(pb.product_name)                                   AS product_name,
        xxhash64(TRIM(pb.category))                              AS category_id,
        TRIM(pb.category)                                       AS category_name,
        TRIM(pb.brand)                                          AS brand,
        pb.is_active                                            AS is_active
      FROM products_bronze pb
      WHERE LOWER(TRIM(pb.is_active)) = 'true'
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category_id,
        category_name,
        brand,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      CAST(category_id AS string) AS category_id,
      category_name,
      brand
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

# ------------------------------------------------------------------------------------
# 4) store_master_silver (sms)
# ------------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(sb.store_id))                                           AS store_id,
        TRIM(sb.store_name)                                                AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state))                         AS location,
        CASE
          WHEN TRIM(sb.state) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
          WHEN TRIM(sb.state) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
          WHEN TRIM(sb.state) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
          WHEN TRIM(sb.state) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
          ELSE NULL
        END                                                                 AS region
      FROM stores_bronze sb
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        location,
        region,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
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

# ------------------------------------------------------------------------------------
# 5) sales_store_daily_silver (ssds)
# ------------------------------------------------------------------------------------
sales_store_daily_silver_df = spark.sql(
    """
    SELECT
      sts.store_id                                         AS store_id,
      sts.transaction_date                                 AS date,
      SUM(sts.revenue)                                     AS total_revenue,
      COUNT(DISTINCT sts.transaction_id)                    AS total_transactions,
      SUM(sts.quantity_sold)                                AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.transaction_date
    """
)
sales_store_daily_silver_df.createOrReplaceTempView("sales_store_daily_silver")

(
    sales_store_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_store_daily_silver.csv")
)

# ------------------------------------------------------------------------------------
# 6) sales_product_daily_silver (spds)
# ------------------------------------------------------------------------------------
sales_product_daily_silver_df = spark.sql(
    """
    SELECT
      sts.product_id                                       AS product_id,
      sts.transaction_date                                 AS date,
      SUM(sts.revenue)                                     AS total_revenue,
      SUM(sts.quantity_sold)                                AS total_sold,
      pms.category_id                                      AS category_id
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
      ON sts.product_id = pms.product_id
    GROUP BY
      sts.product_id,
      sts.transaction_date,
      pms.category_id
    """
)
sales_product_daily_silver_df.createOrReplaceTempView("sales_product_daily_silver")

(
    sales_product_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_product_daily_silver.csv")
)

# ------------------------------------------------------------------------------------
# 7) data_quality_silver (dqs)
# ------------------------------------------------------------------------------------
data_quality_silver_df = spark.sql(
    """
    WITH dup_bronze AS (
      SELECT
        TRIM(UPPER(stb.transaction_id)) AS transaction_id,
        COUNT(1) AS cnt
      FROM sales_transactions_bronze stb
      GROUP BY TRIM(UPPER(stb.transaction_id))
    )
    SELECT
      sts.transaction_id                                                                 AS record_id,
      'bronze.sales_transactions_bronze'                                                 AS source_table,
      CASE
        WHEN TRIM(sts.transaction_id) <> ''
         AND TRIM(sts.store_id) <> ''
         AND TRIM(sts.product_id) <> ''
         AND sts.quantity_sold > 0
         AND sts.revenue >= 0
         AND pms.product_id IS NOT NULL
         AND sms.store_id IS NOT NULL
         AND COALESCE(db.cnt, 0) <= 1
        THEN TRUE ELSE FALSE
      END                                                                                AS is_valid,
      CASE
        WHEN TRIM(sts.transaction_id) = '' THEN 'missing transaction_id'
        WHEN TRIM(sts.store_id) = '' THEN 'missing store_id'
        WHEN TRIM(sts.product_id) = '' THEN 'missing product_id'
        WHEN sts.quantity_sold <= 0 THEN 'invalid quantity'
        WHEN sts.revenue < 0 THEN 'invalid revenue'
        WHEN COALESCE(db.cnt, 0) > 1 THEN 'duplicate transaction_id in bronze'
        WHEN sms.store_id IS NULL THEN 'orphan store_id'
        WHEN pms.product_id IS NULL THEN 'orphan product_id'
        ELSE 'valid'
      END                                                                                AS error_description
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
      ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
      ON sts.store_id = sms.store_id
    LEFT JOIN dup_bronze db
      ON sts.transaction_id = db.transaction_id
    """
)
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

(
    data_quality_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_silver.csv")
)
