```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, lag
from pyspark.sql.window import Window

# Initialize Glue Context and Spark Session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"

# Define file format
FILE_FORMAT = "csv"

# Table: gold_sales_performance
# Read the source tables
transactionsDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/transactions_silver.csv/")
storesDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/stores_silver.csv/")
returnsDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/returns_silver.csv/")

# Create temp views
transactionsDF.createOrReplaceTempView("transactions_silver")
storesDF.createOrReplaceTempView("stores_silver")
returnsDF.createOrReplaceTempView("returns_silver")

# SQL transformation
gold_sales_performance_query = """
SELECT 
    ts.store_id,
    ts.date,
    SUM(ts.total_sale_value) AS total_sales,
    AVG(ts.total_sale_value) AS average_sales,
    COUNT(DISTINCT ts.transaction_id) AS transactions_count,
    ss.store_name,
    COUNT(rs.transaction_id) AS return_count
FROM transactions_silver ts
LEFT JOIN stores_silver ss ON ts.store_id = ss.store_id
LEFT JOIN returns_silver rs ON ts.store_id = rs.store_id AND ts.date = rs.date
GROUP BY ts.store_id, ts.date, ss.store_name
"""

gold_sales_performance_df = spark.sql(gold_sales_performance_query)

# Write to target path
gold_sales_performance_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_performance.csv", header=True)

# Table: gold_product_performance
# Read the source tables
salesTransactionsDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_silver.csv/")
productsDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/products_silver.csv/")

# Create temp views
salesTransactionsDF.createOrReplaceTempView("sales_transactions_silver")
productsDF.createOrReplaceTempView("products_silver")

# SQL transformation
gold_product_performance_query = """
SELECT 
    sts.product_id,
    sts.date,
    ps.product_name,
    ps.category,
    SUM(sts.quantity_sold) AS units_sold,
    SUM(sts.sale_value) AS revenue,
    SUM(sts.sale_value) / SUM(sts.quantity_sold) AS average_price,
    COUNT(rs.transaction_id) AS return_count
FROM sales_transactions_silver sts
INNER JOIN products_silver ps ON sts.product_id = ps.product_id
LEFT JOIN returns_silver rs ON sts.product_id = rs.product_id AND sts.date = rs.date
GROUP BY sts.product_id, sts.date, ps.product_name, ps.category
"""

gold_product_performance_df = spark.sql(gold_product_performance_query)

# Write to target path
gold_product_performance_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True)

# Table: gold_unified_sales_data
# SQL transformation
gold_unified_sales_data_query = """
SELECT 
    sts.transaction_id,
    sts.date,
    sts.product_id,
    sts.store_id,
    sts.quantity_sold,
    sts.sale_value,
    ss.store_region,
    ps.category AS product_category
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss ON sts.store_id = ss.store_id
INNER JOIN products_silver ps ON sts.product_id = ps.product_id
"""

gold_unified_sales_data_df = spark.sql(gold_unified_sales_data_query)

# Write to target path
gold_unified_sales_data_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_unified_sales_data.csv", header=True)

# Table: gold_store_performance_tracking
# Read the source table
customerVisitsDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/customer_visits_silver.csv/")

# Create temp view
customerVisitsDF.createOrReplaceTempView("customer_visits_silver")

# SQL transformation
gold_store_performance_tracking_query = """
SELECT 
    cvs.store_id,
    cvs.date,
    SUM(cvs.entry_count) AS foot_traffic,
    COUNT(DISTINCT ts.transaction_id) / SUM(cvs.entry_count) AS conversion_rate,
    SUM(ts.total_quantity) / COUNT(DISTINCT ts.transaction_id) AS average_basket_size
FROM customer_visits_silver cvs
LEFT JOIN transactions_silver ts ON cvs.store_id = ts.store_id AND cvs.date = ts.date
GROUP BY cvs.store_id, cvs.date
"""

gold_store_performance_tracking_df = spark.sql(gold_store_performance_tracking_query)

# Write to target path
gold_store_performance_tracking_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_performance_tracking.csv", header=True)

# Table: gold_product_analysis
# Read the source table
inventoryDF = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/inventory_silver.csv/")

# Create temp view
inventoryDF.createOrReplaceTempView("inventory_silver")

# SQL transformation
gold_product_analysis_query = """
SELECT 
    sts.product_id,
    sts.date,
    AVG(sts.quantity_sold) AS average_daily_sales,
    (SUM(sts.quantity_sold) - LAG(SUM(sts.quantity_sold)) OVER (PARTITION BY sts.product_id ORDER BY sts.date)) / LAG(SUM(sts.quantity_sold)) OVER (PARTITION BY sts.product_id ORDER BY sts.date) AS sales_growth_rate,
    SUM(isv.stock_level) AS stock_level,
    SUM(CASE WHEN isv.change_type='BACKORDER' THEN 1 ELSE 0 END) / COUNT(isv.change_type) AS backorder_rate
FROM sales_transactions_silver sts
LEFT JOIN inventory_silver isv ON sts.product_id = isv.product_id AND sts.store_id = isv.store_id AND sts.date = isv.date
GROUP BY sts.product_id, sts.date
"""

gold_product_analysis_df = spark.sql(gold_product_analysis_query)

# Write to target path
gold_product_analysis_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_analysis.csv", header=True)
```