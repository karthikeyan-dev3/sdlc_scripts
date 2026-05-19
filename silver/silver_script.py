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

# ------------------------------------------------------------
# 1) Read source tables from S3 and create temp views
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
# 2) products_silver
# ------------------------------------------------------------

products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.is_active DESC, pb.product_name DESC
        ) AS rn
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
    )
    SELECT
      product_id,
      product_name,
      category
    FROM ranked
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# 3) stores_silver
# ------------------------------------------------------------

stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.open_date DESC, sb.store_name DESC
        ) AS rn
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    )
    SELECT
      store_id,
      store_name
    FROM ranked
    WHERE rn = 1
    """
)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# 4) sales_cleaned_silver
# ------------------------------------------------------------

sales_cleaned_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CASE
          WHEN COALESCE(CAST(stb.quantity AS INT), 0) < 0 THEN 0
          ELSE COALESCE(CAST(stb.quantity AS INT), 0)
        END AS quantity_sold,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CASE
          WHEN COALESCE(CAST(stb.sale_amount AS DOUBLE), 0) < 0 THEN 0
          ELSE COALESCE(CAST(stb.sale_amount AS DOUBLE), 0)
        END AS sale_amount,
        CASE
          WHEN stb.transaction_id IS NOT NULL
           AND stb.store_id IS NOT NULL
           AND stb.product_id IS NOT NULL
           AND stb.transaction_time IS NOT NULL
           AND COALESCE(CAST(stb.quantity AS INT), 0) >= 0
           AND ps.product_id IS NOT NULL
           AND ss.store_id IS NOT NULL
          THEN TRUE ELSE FALSE
        END AS cleaned,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN products_silver ps
        ON stb.product_id = ps.product_id
      LEFT JOIN stores_silver ss
        ON stb.store_id = ss.store_id
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      quantity_sold,
      transaction_date,
      sale_amount,
      cleaned
    FROM ranked
    WHERE rn = 1
    """
)
sales_cleaned_silver_df.createOrReplaceTempView("sales_cleaned_silver")

(
    sales_cleaned_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_cleaned_silver.csv")
)

# ------------------------------------------------------------
# 5) sales_enriched_silver
# ------------------------------------------------------------

sales_enriched_silver_df = spark.sql(
    """
    SELECT
      scs.transaction_id AS transaction_id,
      scs.store_id AS store_id,
      scs.product_id AS product_id,
      scs.quantity_sold AS quantity_sold,
      scs.transaction_date AS transaction_date,
      scs.sale_amount AS sale_amount,
      ps.product_name AS product_name,
      ss.store_name AS store_name,
      ps.category AS category
    FROM sales_cleaned_silver scs
    INNER JOIN products_silver ps
      ON scs.product_id = ps.product_id
    INNER JOIN stores_silver ss
      ON scs.store_id = ss.store_id
    WHERE scs.cleaned = TRUE
    """
)
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

(
    sales_enriched_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_enriched_silver.csv")
)

# ------------------------------------------------------------
# 6) store_transaction_summary_silver
# ------------------------------------------------------------

store_transaction_summary_silver_df = spark.sql(
    """
    SELECT
      scs.store_id AS store_id,
      ss.store_name AS store_name,
      SUM(scs.sale_amount) AS total_revenue,
      COUNT(DISTINCT scs.transaction_id) AS transaction_count
    FROM sales_cleaned_silver scs
    INNER JOIN stores_silver ss
      ON scs.store_id = ss.store_id
    WHERE scs.cleaned = TRUE
    GROUP BY scs.store_id, ss.store_name
    """
)
store_transaction_summary_silver_df.createOrReplaceTempView("store_transaction_summary_silver")

(
    store_transaction_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/store_transaction_summary_silver.csv")
)

# ------------------------------------------------------------
# 7) product_sales_summary_silver
# ------------------------------------------------------------

product_sales_summary_silver_df = spark.sql(
    """
    SELECT
      scs.product_id AS product_id,
      ps.product_name AS product_name,
      ps.category AS category,
      SUM(scs.sale_amount) AS revenue_contribution,
      SUM(scs.quantity_sold) AS units_sold
    FROM sales_cleaned_silver scs
    INNER JOIN products_silver ps
      ON scs.product_id = ps.product_id
    WHERE scs.cleaned = TRUE
    GROUP BY scs.product_id, ps.product_name, ps.category
    """
)
product_sales_summary_silver_df.createOrReplaceTempView("product_sales_summary_silver")

(
    product_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/product_sales_summary_silver.csv")
)

# ------------------------------------------------------------
# 8) aggregated_sales_silver
# ------------------------------------------------------------

aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      ses.transaction_date AS date,
      ses.store_id AS store_id,
      SUM(COALESCE(ses.sale_amount, 0)) AS total_revenue,
      COUNT(DISTINCT ses.transaction_id) AS total_transactions,
      COUNT(DISTINCT ses.product_id) AS unique_products_sold
    FROM sales_enriched_silver ses
    GROUP BY ses.transaction_date, ses.store_id
    """
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
