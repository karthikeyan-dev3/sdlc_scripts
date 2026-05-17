import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# 1) Read source tables (Bronze)
# -----------------------------
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

# --------------------------------
# 2) products_silver (Transform SQL)
# --------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pb.product_name) AS STRING) AS product_name,
        CAST(TRIM(pb.category) AS STRING) AS category,
        CAST(TRIM(pb.brand) AS STRING) AS brand,
        CAST(pb.price AS DOUBLE) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY TRIM(pb.product_id)
        ) AS rn
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      CAST(CASE WHEN COALESCE(price, 0.0) < 0 THEN 0.0 ELSE COALESCE(price, 0.0) END AS DOUBLE) AS price
    FROM base
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

# -----------------------------
# 3) Write products_silver to S3
# -----------------------------
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------
# 4) stores_silver (Transform SQL)
# ------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sb.store_name) AS STRING) AS store_name,
        CAST(CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS STRING) AS location,
        CAST(TRIM(sb.store_type) AS STRING) AS store_type,
        CAST(sb.open_date AS DATE) AS open_date,
        CAST(NULL AS STRING) AS manager_id,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY TRIM(sb.store_id)
        ) AS rn
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    )
    SELECT
      store_id,
      store_name,
      location,
      store_type,
      open_date,
      manager_id
    FROM base
    WHERE rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

# ---------------------------
# 5) Write stores_silver to S3
# ---------------------------
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# -----------------------------------------
# 6) sales_transactions_silver (Transform SQL)
# -----------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
        CAST(TRIM(stb.product_id) AS STRING) AS product_id,
        CAST(TRIM(stb.store_id) AS STRING) AS store_id,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        CAST(stb.transaction_time AS DATE) AS sale_date,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_amount,
        CAST(NULL AS STRING) AS customer_id,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      INNER JOIN products_silver ps
        ON CAST(TRIM(stb.product_id) AS STRING) = CAST(TRIM(ps.product_id) AS STRING)
      INNER JOIN stores_silver ss
        ON CAST(TRIM(stb.store_id) AS STRING) = CAST(TRIM(ss.store_id) AS STRING)
      WHERE stb.transaction_id IS NOT NULL
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      sale_date,
      quantity_sold,
      total_amount,
      customer_id
    FROM base
    WHERE rn = 1
      AND quantity_sold > 0
      AND total_amount >= 0
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# --------------------------------------
# 7) Write sales_transactions_silver to S3
# --------------------------------------
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -----------------------------------
# 8) daily_sales_agg_silver (Transform SQL)
# -----------------------------------
daily_sales_agg_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        sts.sale_date AS aggregation_date,
        SUM(sts.total_amount) AS total_sales,
        COUNT(DISTINCT sts.transaction_id) AS total_transactions,
        SUM(sts.total_amount) / COUNT(DISTINCT sts.transaction_id) AS average_transaction_value
      FROM sales_transactions_silver sts
      INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
      GROUP BY sts.sale_date
    ),
    category_breakdown AS (
      SELECT
        sts.sale_date AS aggregation_date,
        TO_JSON(
          MAP_FROM_ENTRIES(
            COLLECT_LIST(
              NAMED_STRUCT(
                'key', ps.category,
                'value', SUM(sts.total_amount)
              )
            )
          )
        ) AS category_sales_breakdown
      FROM sales_transactions_silver sts
      INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
      GROUP BY sts.sale_date, ps.category
    ),
    category_breakdown_agg AS (
      SELECT
        aggregation_date,
        TO_JSON(
          MAP_FROM_ENTRIES(
            COLLECT_LIST(
              NAMED_STRUCT('key', k, 'value', v)
            )
          )
        ) AS category_sales_breakdown
      FROM (
        SELECT
          aggregation_date,
          CAST(get_json_object(category_sales_breakdown, '$') AS STRING) AS raw_json,
          ps_key_value.key AS k,
          ps_key_value.value AS v
        FROM (
          SELECT
            aggregation_date,
            category_sales_breakdown,
            EXPLODE(
              FROM_JSON(category_sales_breakdown, 'MAP<STRING,DOUBLE>')
            ) AS ps_key_value
          FROM category_breakdown
        ) t
      ) x
      GROUP BY aggregation_date
    ),
    store_rank_base AS (
      SELECT
        sts.sale_date AS aggregation_date,
        sts.store_id,
        SUM(sts.total_amount) AS store_total_sales
      FROM sales_transactions_silver sts
      GROUP BY sts.sale_date, sts.store_id
    ),
    store_rank AS (
      SELECT
        aggregation_date,
        TO_JSON(
          COLLECT_LIST(
            NAMED_STRUCT(
              'rank', rn,
              'store_id', store_id,
              'total_sales', store_total_sales
            )
          )
        ) AS store_sales_ranking
      FROM (
        SELECT
          aggregation_date,
          store_id,
          store_total_sales,
          ROW_NUMBER() OVER (
            PARTITION BY aggregation_date
            ORDER BY store_total_sales DESC
          ) AS rn
        FROM store_rank_base
      ) r
      GROUP BY aggregation_date
    )
    SELECT
      b.aggregation_date,
      CAST(b.total_sales AS DOUBLE) AS total_sales,
      CAST(b.total_transactions AS BIGINT) AS total_transactions,
      CAST(b.average_transaction_value AS DOUBLE) AS average_transaction_value,
      c.category_sales_breakdown,
      s.store_sales_ranking
    FROM base b
    LEFT JOIN category_breakdown_agg c
      ON b.aggregation_date = c.aggregation_date
    LEFT JOIN store_rank s
      ON b.aggregation_date = s.aggregation_date
    """
)
daily_sales_agg_silver_df.createOrReplaceTempView("daily_sales_agg_silver")

# -----------------------------------
# 9) Write daily_sales_agg_silver to S3
# -----------------------------------
(
    daily_sales_agg_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/daily_sales_agg_silver.csv")
)

job.commit()