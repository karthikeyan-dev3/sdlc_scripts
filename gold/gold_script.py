
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
sparkContext = glueContext.sparkContext

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
sales_transactions_silver = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
products_silver = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
stores_silver = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
product_store_mapping_silver = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/product_store_mapping_silver.{FILE_FORMAT}/")

# Create temp views
sales_transactions_silver.createOrReplaceTempView("sales_transactions_silver")
products_silver.createOrReplaceTempView("products_silver")
stores_silver.createOrReplaceTempView("stores_silver")
product_store_mapping_silver.createOrReplaceTempView("product_store_mapping_silver")

# gold_sales transformation
gold_sales_query = """
SELECT
    sts.transaction_id AS transaction_id,
    sts.product_id AS product_id,
    sts.store_id AS store_id
FROM
    sales_transactions_silver sts
LEFT JOIN
    products_silver ps ON sts.product_id = ps.product_id
LEFT JOIN
    stores_silver ss ON sts.store_id = ss.store_id
"""
gold_sales_df = spark.sql(gold_sales_query)
gold_sales_df.write.mode('overwrite').csv(f"{TARGET_PATH}/gold_sales.csv", header=True)

# gold_store_performance transformation
gold_store_performance_query = """
SELECT
    ss.store_id AS store_id,
    ss.store_name AS store_name,
    ss.region AS region,
    SUM(sts.total_revenue) AS total_revenue,
    COUNT(DISTINCT sts.transaction_id) AS total_transactions,
    SUM(sts.quantity_sold) AS total_quantity_sold
FROM
    sales_transactions_silver sts
INNER JOIN
    stores_silver ss ON sts.store_id = ss.store_id
GROUP BY
    ss.store_id, ss.store_name, ss.region
"""
gold_store_performance_df = spark.sql(gold_store_performance_query)
gold_store_performance_df.write.mode('overwrite').csv(f"{TARGET_PATH}/gold_store_performance.csv", header=True)

# gold_product_performance transformation
gold_product_performance_query = """
SELECT
    ps.product_id AS product_id,
    ps.product_name AS product_name,
    ps.category AS category,
    SUM(sts.total_revenue) AS total_revenue,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    (SUM(sts.total_revenue) / SUM(SUM(sts.total_revenue)) OVER ()) * 100 AS revenue_contribution_percentage
FROM
    sales_transactions_silver sts
INNER JOIN
    products_silver ps ON sts.product_id = ps.product_id
GROUP BY
    ps.product_id, ps.product_name, ps.category
"""
gold_product_performance_df = spark.sql(gold_product_performance_query)
gold_product_performance_df.write.mode('overwrite').csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True)

# gold_product_store_mapping transformation
gold_product_store_mapping_query = """
SELECT DISTINCT
    psm.product_id AS product_id,
    psm.store_id AS store_id,
    ps.product_name AS product_name,
    ss.store_name AS store_name
FROM
    product_store_mapping_silver psm
INNER JOIN
    products_silver ps ON psm.product_id = ps.product_id
INNER JOIN
    stores_silver ss ON psm.store_id = ss.store_id
"""
gold_product_store_mapping_df = spark.sql(gold_product_store_mapping_query)
gold_product_store_mapping_df.write.mode('overwrite').csv(f"{TARGET_PATH}/gold_product_store_mapping.csv", header=True)
