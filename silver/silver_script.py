import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
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

# ------------------------------------
# 2) sales_transactions_silver (sts)
# ------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        CAST(stb.quantity AS int) AS quantity_sold,
        CAST(stb.sale_amount AS double) AS total_revenue,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
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

# ------------------------------
# 3) product_master_silver (pms)
# ------------------------------
product_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pb.product_id AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM products_bronze pb
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
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ----------------------------
# 4) store_master_silver (sms)
# ----------------------------
store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        sb.store_id AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        CASE
          WHEN sb.state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
          WHEN sb.state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
          WHEN sb.state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
          WHEN sb.state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
          ELSE 'UNKNOWN'
        END AS region,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location,
      region
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

# --------------------------
# 5) data_quality_silver (dqs)
# --------------------------
data_quality_silver_df = spark.sql(
    """
    SELECT
      sts.transaction_id AS record_id,
      'sales_transactions_raw' AS source_system,
      CASE
        WHEN sts.transaction_id IS NULL
          OR sts.store_id IS NULL
          OR sts.product_id IS NULL
          OR sts.quantity_sold IS NULL
          OR sts.quantity_sold <= 0
          OR sts.total_revenue IS NULL
          OR sts.total_revenue < 0
          OR sts.transaction_date IS NULL
          OR pms.product_id IS NULL
          OR sms.store_id IS NULL
        THEN 'FAIL'
        ELSE 'PASS'
      END AS validation_status,
      TRIM(BOTH ',' FROM CONCAT(
        CASE WHEN sts.transaction_id IS NULL THEN 'missing_transaction_id,' ELSE '' END,
        CASE WHEN sts.store_id IS NULL THEN 'missing_store_id,' ELSE '' END,
        CASE WHEN sts.product_id IS NULL THEN 'missing_product_id,' ELSE '' END,
        CASE WHEN sts.quantity_sold IS NULL OR sts.quantity_sold <= 0 THEN 'invalid_quantity,' ELSE '' END,
        CASE WHEN sts.total_revenue IS NULL OR sts.total_revenue < 0 THEN 'invalid_revenue,' ELSE '' END,
        CASE WHEN sts.transaction_date IS NULL THEN 'invalid_transaction_date,' ELSE '' END,
        CASE WHEN pms.product_id IS NULL THEN 'orphan_product_id,' ELSE '' END,
        CASE WHEN sms.store_id IS NULL THEN 'orphan_store_id,' ELSE '' END
      )) AS issues_found
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms
      ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
      ON sts.store_id = sms.store_id
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

# --------------------------------
# 6) sales_aggregates_silver (sas)
# --------------------------------
sales_aggregates_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      sts.transaction_date AS date,
      SUM(sts.total_revenue) AS total_revenue,
      COUNT(DISTINCT sts.transaction_id) AS transaction_count,
      SUM(sts.quantity_sold) AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.transaction_date
    """
)
sales_aggregates_silver_df.createOrReplaceTempView("sales_aggregates_silver")

(
    sales_aggregates_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregates_silver.csv")
)

job.commit()