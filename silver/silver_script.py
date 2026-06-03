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

# -----------------------------
# 1) Read source tables from S3
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
# 2) products_silver (ps) - SQL ETL
# --------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(pb.product_id)) AS product_id,
        TRIM(pb.product_name)      AS product_name,
        TRIM(pb.category)          AS category,
        CAST(pb.price AS DOUBLE)   AS price
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      category,
      price
    FROM (
      SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.product_id ORDER BY b.product_id) AS rn
      FROM base b
    ) d
    WHERE d.rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

products_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/products_silver.csv"
)

# ------------------------------
# 3) stores_silver (ss) - SQL ETL
# ------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(UPPER(sb.store_id)) AS store_id,
        TRIM(sb.store_name)      AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        sb.state                 AS region
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location,
      region
    FROM (
      SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.store_id ORDER BY b.store_id) AS rn
      FROM base b
    ) d
    WHERE d.rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

stores_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/stores_silver.csv"
)

# ------------------------------------------
# 4) sales_transactions_silver (sts) - SQL ETL
# ------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        stb.transaction_id                         AS transaction_id,
        ps.product_id                               AS product_id,
        ss.store_id                                 AS store_id,
        DATE(stb.transaction_time)                  AS transaction_date,
        COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) AS revenue,
        COALESCE(CAST(stb.quantity AS INT), 0)       AS quantity_sold,
        stb.transaction_time                        AS transaction_time
      FROM sales_transactions_bronze stb
      INNER JOIN products_silver ps
        ON ps.product_id = stb.product_id
      INNER JOIN stores_silver ss
        ON ss.store_id = stb.store_id
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      transaction_date,
      revenue,
      quantity_sold
    FROM (
      SELECT
        j.*,
        ROW_NUMBER() OVER (
          PARTITION BY j.transaction_id
          ORDER BY j.transaction_time DESC
        ) AS rn
      FROM joined j
    ) d
    WHERE d.rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

sales_transactions_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_transactions_silver.csv"
)

# ------------------------------------------------
# 5) store_daily_performance_silver (sdps) - SQL ETL
# ------------------------------------------------
store_daily_performance_silver_df = spark.sql(
    """
    SELECT
      sts.store_id                                      AS store_id,
      sts.transaction_date                              AS date,
      SUM(sts.revenue)                                  AS total_revenue,
      COUNT(DISTINCT sts.transaction_id)                AS transaction_count
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.transaction_date
    """
)
store_daily_performance_silver_df.createOrReplaceTempView("store_daily_performance_silver")

store_daily_performance_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/store_daily_performance_silver.csv"
)

# --------------------------------------------------
# 6) product_daily_performance_silver (pdps) - SQL ETL
# --------------------------------------------------
product_daily_performance_silver_df = spark.sql(
    """
    SELECT
      sts.product_id                                    AS product_id,
      sts.transaction_date                              AS date,
      SUM(sts.revenue)                                  AS total_revenue,
      SUM(sts.quantity_sold)                            AS quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      sts.product_id,
      sts.transaction_date
    """
)
product_daily_performance_silver_df.createOrReplaceTempView("product_daily_performance_silver")

product_daily_performance_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/product_daily_performance_silver.csv"
)

# ---------------------------------------------
# 7) aggregated_reports_silver (ars) - SQL ETL
# ---------------------------------------------
aggregated_reports_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        'daily_performance_report' AS report_name,
        current_timestamp()        AS generated_on,
        'store_daily'              AS aggregation_level
      FROM store_daily_performance_silver sdps

      UNION ALL

      SELECT
        'daily_performance_report' AS report_name,
        current_timestamp()        AS generated_on,
        'product_daily'            AS aggregation_level
      FROM product_daily_performance_silver pdps
    )
    SELECT
      CAST(hash(report_name, generated_on, aggregation_level) AS STRING) AS report_id,
      report_name,
      generated_on,
      aggregation_level
    FROM base
    """
)
aggregated_reports_silver_df.createOrReplaceTempView("aggregated_reports_silver")

aggregated_reports_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/aggregated_reports_silver.csv"
)

job.commit()