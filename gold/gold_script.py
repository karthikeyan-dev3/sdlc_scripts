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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
store_master_silver_df.createOrReplaceTempView("store_master_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -------------------------------------------------------------------
# TARGET: gold_store_master
# -------------------------------------------------------------------
gold_store_master_df = spark.sql(
    """
    SELECT
      CAST(sm.store_id AS STRING) AS store_id,
      CAST(sm.store_location AS STRING) AS store_location
    FROM store_master_silver sm
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------------------------------------------
# TARGET: gold_product_master
# -------------------------------------------------------------------
gold_product_master_df = spark.sql(
    """
    SELECT
      CAST(pm.product_id AS STRING) AS product_id,
      CAST(pm.product_name AS STRING) AS product_name,
      CAST(pm.product_category AS STRING) AS product_category,
      CAST(pm.product_price AS DOUBLE) AS product_price
    FROM product_master_silver pm
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------------------------------------------
# TARGET: gold_sales_performance
# -------------------------------------------------------------------
gold_sales_performance_df = spark.sql(
    """
    SELECT
      CAST(st.store_id AS STRING) AS store_id,
      CAST(sm.store_name AS STRING) AS store_name,
      CAST(st.transaction_date AS DATE) AS transaction_date,
      CAST(SUM(CAST(st.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
      CAST(COUNT(st.transaction_id) AS BIGINT) AS total_transactions,
      CAST(SUM(CAST(st.quantity AS BIGINT)) AS BIGINT) AS total_quantity_sold
    FROM sales_transactions_silver st
    INNER JOIN store_master_silver sm
      ON st.store_id = sm.store_id
    GROUP BY
      st.store_id,
      sm.store_name,
      st.transaction_date
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# -------------------------------------------------------------------
# TARGET: gold_product_performance
# -------------------------------------------------------------------
gold_product_performance_df = spark.sql(
    """
    SELECT
      CAST(st.product_id AS STRING) AS product_id,
      CAST(pm.product_name AS STRING) AS product_name,
      CAST(pm.product_category AS STRING) AS product_category,
      CAST(SUM(CAST(st.sale_amount AS DOUBLE)) AS DOUBLE) AS revenue_contribution,
      CAST(SUM(CAST(st.quantity AS BIGINT)) AS BIGINT) AS units_sold
    FROM sales_transactions_silver st
    INNER JOIN product_master_silver pm
      ON st.product_id = pm.product_id
    GROUP BY
      st.product_id,
      pm.product_name,
      pm.product_category
    """
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

# -------------------------------------------------------------------
# TARGET: gold_sales_aggregate
# -------------------------------------------------------------------
gold_sales_aggregate_df = spark.sql(
    """
    SELECT
      CAST(st.transaction_date AS DATE) AS date,
      CAST(SUM(CAST(st.sale_amount AS DOUBLE)) AS DOUBLE) AS total_revenue,
      CAST(COUNT(st.transaction_id) AS BIGINT) AS total_transactions,
      CAST(SUM(CAST(st.quantity AS BIGINT)) AS BIGINT) AS total_units_sold
    FROM sales_transactions_silver st
    GROUP BY
      st.transaction_date
    """
)

(
    gold_sales_aggregate_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregate.csv")
)

job.commit()
