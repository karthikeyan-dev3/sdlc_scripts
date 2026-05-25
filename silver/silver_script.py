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

# =============================
# Read source tables (Bronze)
# =============================
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

# =========================================================
# Target: product_master_silver (pms) from products_bronze
# =========================================================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CAST(pb.price AS FLOAT) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_name) DESC
        ) AS rn
      FROM products_bronze pb
      WHERE
        TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
        AND COALESCE(LOWER(TRIM(pb.is_active)), 'false') = 'true'
        AND CAST(pb.price AS FLOAT) >= 0
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price
    FROM base
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

# ======================================================
# Target: store_master_silver (sms) from stores_bronze
# ======================================================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY DATE(CAST(sb.open_date AS DATE)) DESC, TRIM(sb.store_name) DESC
        ) AS rn
      FROM stores_bronze sb
      WHERE
        TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    )
    SELECT
      store_id,
      store_name,
      location
    FROM base
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

# ==========================================================================================
# Target: sales_transactions_silver (sts) from sales_transactions_bronze + product/store dims
# ==========================================================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      INNER JOIN product_master_silver pms
        ON TRIM(stb.product_id) = pms.product_id
      INNER JOIN store_master_silver sms
        ON TRIM(stb.store_id) = sms.store_id
      WHERE
        TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
        AND TRIM(stb.store_id) IS NOT NULL AND TRIM(stb.store_id) <> ''
        AND TRIM(stb.product_id) IS NOT NULL AND TRIM(stb.product_id) <> ''
        AND CAST(stb.quantity AS INT) > 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
      transaction_id,
      transaction_date,
      store_id,
      product_id,
      quantity_sold,
      total_sales_amount
    FROM base
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

# ==================================================================================
# Target: sales_daily_store_product_silver (sdsp) from sales_transactions_silver
# ==================================================================================
sales_daily_store_product_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      sts.transaction_date AS date,
      SUM(sts.quantity_sold) AS total_quantity_sold,
      SUM(sts.total_sales_amount) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.transaction_date
    """
)
sales_daily_store_product_silver_df.createOrReplaceTempView("sales_daily_store_product_silver")

(
    sales_daily_store_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_daily_store_product_silver.csv")
)

# ==========================================================================================
# Target: sales_performance_store_product_silver (spsp) from sales_transactions_silver
# ==========================================================================================
sales_performance_store_product_silver_df = spark.sql(
    """
    WITH weekly AS (
      SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        weekofyear(sts.transaction_date) AS iso_week,
        SUM(sts.quantity_sold) AS weekly_sales_volume,
        AVG(CASE WHEN sts.quantity_sold > 0 THEN sts.total_sales_amount / sts.quantity_sold END) AS average_unit_price_week
      FROM sales_transactions_silver sts
      GROUP BY
        sts.store_id,
        sts.product_id,
        weekofyear(sts.transaction_date)
    ),
    latest_week AS (
      SELECT
        store_id,
        product_id,
        weekly_sales_volume,
        average_unit_price_week,
        ROW_NUMBER() OVER (
          PARTITION BY store_id, product_id
          ORDER BY iso_week DESC
        ) AS rn
      FROM weekly
    ),
    monthly AS (
      SELECT
        sts.store_id AS store_id,
        sts.product_id AS product_id,
        month(sts.transaction_date) AS txn_month,
        SUM(sts.quantity_sold) AS monthly_sales_volume,
        AVG(CASE WHEN sts.quantity_sold > 0 THEN sts.total_sales_amount / sts.quantity_sold END) AS average_unit_price_month
      FROM sales_transactions_silver sts
      GROUP BY
        sts.store_id,
        sts.product_id,
        month(sts.transaction_date)
    ),
    latest_month AS (
      SELECT
        store_id,
        product_id,
        monthly_sales_volume,
        average_unit_price_month,
        ROW_NUMBER() OVER (
          PARTITION BY store_id, product_id
          ORDER BY txn_month DESC
        ) AS rn
      FROM monthly
    )
    SELECT
      w.store_id AS store_id,
      w.product_id AS product_id,
      w.weekly_sales_volume AS weekly_sales_volume,
      m.monthly_sales_volume AS monthly_sales_volume,
      COALESCE(w.average_unit_price_week, m.average_unit_price_month) AS average_unit_price
    FROM latest_week w
    INNER JOIN latest_month m
      ON w.store_id = m.store_id
     AND w.product_id = m.product_id
    WHERE w.rn = 1 AND m.rn = 1
    """
)
sales_performance_store_product_silver_df.createOrReplaceTempView("sales_performance_store_product_silver")

(
    sales_performance_store_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_performance_store_product_silver.csv")
)

job.commit()