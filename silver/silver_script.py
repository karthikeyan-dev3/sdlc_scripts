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

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------------------------------------------------
# 3) product_master_silver
#    Output: product_id, product_name, category, price
# -------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(TRIM(pb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pb.product_name) AS STRING) AS product_name,
        CAST(TRIM(pb.category) AS STRING) AS category,
        CAST(pb.price AS FLOAT) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
          ORDER BY CAST(TRIM(pb.product_id) AS STRING) DESC
        ) AS rn
      FROM products_bronze pb
    )
    SELECT
      product_id,
      product_name,
      category,
      price
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

# -------------------------------------------------------------------
# 4) store_master_silver
#    Output: store_id, store_name, region, store_type
#    Region derived directly from state (per UDT mapping: sb.state -> region)
# -------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(TRIM(sb.store_name) AS STRING) AS store_name,
        CAST(TRIM(sb.state) AS STRING) AS region,
        CAST(TRIM(sb.store_type) AS STRING) AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(TRIM(sb.store_id) AS STRING)
          ORDER BY CAST(TRIM(sb.store_id) AS STRING) DESC
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      region,
      store_type
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

# -------------------------------------------------------------------
# 5) sales_transactions_silver
#    Output: sales_id, transaction_date, product_id, store_id, revenue, quantity_sold, category
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH joined AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING) AS sales_id,
        DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
        CAST(TRIM(pms.product_id) AS STRING) AS product_id,
        CAST(TRIM(sms.store_id) AS STRING) AS store_id,
        CAST(stb.sale_amount AS DOUBLE) AS revenue,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(TRIM(pms.category) AS STRING) AS category,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
      FROM sales_transactions_bronze stb
      INNER JOIN product_master_silver pms
        ON CAST(TRIM(stb.product_id) AS STRING) = CAST(TRIM(pms.product_id) AS STRING)
      INNER JOIN store_master_silver sms
        ON CAST(TRIM(stb.store_id) AS STRING) = CAST(TRIM(sms.store_id) AS STRING)
    ),
    ranked AS (
      SELECT
        sales_id,
        transaction_date,
        product_id,
        store_id,
        revenue,
        quantity_sold,
        category,
        ROW_NUMBER() OVER (
          PARTITION BY sales_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM joined
    )
    SELECT
      sales_id,
      transaction_date,
      product_id,
      store_id,
      revenue,
      quantity_sold,
      category
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

# -------------------------------------------------------------------
# 6) aggregated_sales_silver
#    Output: date, store_id, total_revenue, total_transactions, total_quantity_sold
# -------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      DATE(sts.transaction_date) AS date,
      CAST(TRIM(sts.store_id) AS STRING) AS store_id,
      CAST(SUM(sts.revenue) AS DOUBLE) AS total_revenue,
      CAST(COUNT(DISTINCT sts.sales_id) AS BIGINT) AS total_transactions,
      CAST(SUM(sts.quantity_sold) AS BIGINT) AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      DATE(sts.transaction_date),
      CAST(TRIM(sts.store_id) AS STRING)
    """
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()