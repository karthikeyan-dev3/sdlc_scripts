
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AWSGluePySpark").getOrCreate()
glueContext = GlueContext(spark)

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Read source tables
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
products_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
stores_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
product_store_mapping_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/product_store_mapping_silver.{FILE_FORMAT}/")
data_quality_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")

# Create temporary views
sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")
products_df.createOrReplaceTempView("products_silver")
stores_df.createOrReplaceTempView("stores_silver")
product_store_mapping_df.createOrReplaceTempView("product_store_mapping_silver")
data_quality_df.createOrReplaceTempView("data_quality_silver")

# Transformations and target table writes
# Table: gold_sales
gold_sales_query = """
SELECT
    transaction_id,
    store_id,
    product_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    CAST(quantity_sold AS INT) AS quantity_sold,
    CAST(revenue AS DOUBLE) AS revenue
FROM
    sales_transactions_silver
"""
gold_sales_df = spark.sql(gold_sales_query)
gold_sales_df.coalesce(1).write.mode('overwrite').csv(f"{TARGET_PATH}/gold_sales.csv", header=True)

# Table: gold_product_performance
gold_product_performance_query = """
SELECT
    ps.product_id,
    ps.product_name,
    ps.category,
    SUM(sts.revenue) AS total_revenue,
    SUM(sts.quantity_sold) AS units_sold
FROM
    products_silver ps
JOIN
    sales_transactions_silver sts
ON
    ps.product_id = sts.product_id
GROUP BY
    ps.product_id, ps.product_name, ps.category
"""
gold_product_performance_df = spark.sql(gold_product_performance_query)
gold_product_performance_df.coalesce(1).write.mode('overwrite').csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True)

# Table: gold_store_performance
gold_store_performance_query = """
SELECT
    ss.store_id,
    ss.store_name,
    ss.location,
    SUM(sts.revenue) AS total_revenue,
    COUNT(DISTINCT sts.transaction_id) AS total_transactions,
    SUM(sts.quantity_sold) AS total_quantity_sold
FROM
    stores_silver ss
JOIN
    sales_transactions_silver sts
ON
    ss.store_id = sts.store_id
GROUP BY
    ss.store_id, ss.store_name, ss.location
"""
gold_store_performance_df = spark.sql(gold_store_performance_query)
gold_store_performance_df.coalesce(1).write.mode('overwrite').csv(f"{TARGET_PATH}/gold_store_performance.csv", header=True)

# Table: gold_product_store_mapping
gold_product_store_mapping_query = """
SELECT
    psm.product_id,
    psm.store_id,
    CAST(psm.availability_date AS DATE) AS availability_date
FROM
    product_store_mapping_silver psm
"""
gold_product_store_mapping_df = spark.sql(gold_product_store_mapping_query)
gold_product_store_mapping_df.coalesce(1).write.mode('overwrite').csv(f"{TARGET_PATH}/gold_product_store_mapping.csv", header=True)

# Table: gold_data_quality
gold_data_quality_query = """
SELECT
    dqs.record_id,
    dqs.validation_status,
    dqs.error_type,
    CAST(dqs.last_updated AS TIMESTAMP) AS last_updated
FROM
    data_quality_silver dqs
"""
gold_data_quality_df = spark.sql(gold_data_quality_query)
gold_data_quality_df.coalesce(1).write.mode('overwrite').csv(f"{TARGET_PATH}/gold_data_quality.csv", header=True)
