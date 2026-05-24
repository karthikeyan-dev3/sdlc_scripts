import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ------------------------------------------------------------
# 1) Read source tables (Bronze)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 2) product_master_silver
# ------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(pb.product_id)) AS product_id,
        TRIM(pb.product_name)      AS product_name,
        TRIM(pb.category)          AS category,
        TRIM(pb.brand)             AS brand,
        CASE
          WHEN CAST(pb.price AS DECIMAL(18,2)) < 0 THEN NULL
          ELSE CAST(pb.price AS DECIMAL(18,2))
        END                        AS price,
        CASE
          WHEN pb.is_active = true THEN 'ACTIVE'
          ELSE 'INACTIVE'
        END                        AS status
      FROM products_bronze pb
    ),
    ranked AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        status,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY
            product_name DESC,
            category DESC,
            brand DESC,
            price DESC,
            status DESC
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price,
      status
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

# ------------------------------------------------------------
# 3) store_master_silver
# ------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        UPPER(TRIM(sb.store_id)) AS store_id,
        TRIM(sb.store_name)      AS store_name,
        TRIM(sb.city)            AS city,
        TRIM(sb.state)           AS state,
        TRIM(sb.store_type)      AS store_type,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        CASE
          WHEN TRIM(sb.state) IS NULL OR TRIM(sb.state) = '' THEN 'UNKNOWN'
          ELSE 'UNKNOWN'
        END                      AS region,
        CAST(sb.open_date AS DATE) AS open_date,
        CASE
          WHEN sb.open_date IS NOT NULL AND sb.open_date <= CURRENT_DATE THEN 'OPEN'
          ELSE 'UNKNOWN'
        END                      AS status
      FROM stores_bronze sb
    ),
    ranked AS (
      SELECT
        store_id,
        store_name,
        city,
        state,
        store_type,
        location,
        region,
        open_date,
        status,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY
            store_name DESC,
            city DESC,
            state DESC,
            store_type DESC,
            location DESC,
            region DESC,
            open_date DESC,
            status DESC
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      location,
      region,
      open_date,
      status
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

# ------------------------------------------------------------
# 4) sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH enriched AS (
      SELECT
        TRIM(stb.transaction_id)        AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS date,
        UPPER(TRIM(stb.product_id))     AS product_id,
        UPPER(TRIM(stb.store_id))       AS store_id,

        CASE
          WHEN CAST(stb.quantity AS INT) <= 0 THEN NULL
          ELSE CAST(stb.quantity AS INT)
        END                             AS quantity_sold,

        CASE
          WHEN CAST(stb.sale_amount AS DECIMAL(18,2)) < 0 THEN NULL
          ELSE CAST(stb.sale_amount AS DECIMAL(18,2))
        END                             AS sales_amount,

        CASE
          WHEN stb.sale_amount < 0 OR stb.quantity < 0 THEN 'RETURN'
          ELSE 'SALE'
        END                             AS transaction_type,

        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time_ts
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON UPPER(TRIM(stb.product_id)) = pms.product_id
      LEFT JOIN store_master_silver sms
        ON UPPER(TRIM(stb.store_id)) = sms.store_id
    ),
    ranked AS (
      SELECT
        transaction_id,
        date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount,
        transaction_type,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time_ts DESC
        ) AS rn
      FROM enriched
    )
    SELECT
      transaction_id,
      date,
      product_id,
      store_id,
      quantity_sold,
      sales_amount,
      transaction_type
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

# ------------------------------------------------------------
# 5) aggregated_sales_silver
# ------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      sms.region                                      AS region,
      sts.store_id                                    AS store_id,
      sts.product_id                                  AS product_id,
      pms.category                                    AS category,
      sts.date                                        AS date,
      SUM(
        CASE
          WHEN sts.transaction_type = 'RETURN' THEN -1 * sts.sales_amount
          ELSE sts.sales_amount
        END
      )                                               AS total_sales_amount,
      SUM(
        CASE
          WHEN sts.transaction_type = 'RETURN' THEN -1 * sts.quantity_sold
          ELSE sts.quantity_sold
        END
      )                                               AS total_quantity_sold
    FROM sales_transactions_silver sts
    INNER JOIN product_master_silver pms
      ON sts.product_id = pms.product_id
    INNER JOIN store_master_silver sms
      ON sts.store_id = sms.store_id
    WHERE
      sts.date IS NOT NULL
      AND sts.sales_amount IS NOT NULL
      AND sts.quantity_sold IS NOT NULL
    GROUP BY
      sms.region,
      sts.store_id,
      sts.product_id,
      pms.category,
      sts.date
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