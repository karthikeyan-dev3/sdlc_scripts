```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read Source Tables
sales_transactions_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_silver.csv/")
product_master_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/product_master_silver.csv/")
store_master_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/store_master_silver.csv/")

# Create Temporary Views
sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")
product_master_df.createOrReplaceTempView("product_master_silver")
store_master_df.createOrReplaceTempView("store_master_silver")

# Transformations and Write to Target

# 1. Gold Sales - Fact-level unified sales dataset
gold_sales_df = spark.sql("""
SELECT
    sts.transaction_id,
    sts.store_id,
    sts.product_id,
    sts.sale_date,
    sts.quantity_sold,
    sts.total_revenue
FROM
    sales_transactions_silver sts
LEFT JOIN
    product_master_silver pms ON sts.product_id = pms.product_id
LEFT JOIN
    store_master_silver sms ON sts.store_id = sms.store_id
""")
gold_sales_df.repartition(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales.csv", header=True)

# 2. Gold Store Performance - Store-level aggregated performance table
gold_store_performance_df = spark.sql("""
SELECT
    sms.store_id,
    sms.store_name,
    _sum(sts.total_revenue) AS total_revenue,
    countDistinct(sts.transaction_id) AS transaction_count,
    _sum(sts.quantity_sold) AS quantities_sold
FROM
    sales_transactions_silver sts
INNER JOIN
    store_master_silver sms ON sts.store_id = sms.store_id
GROUP BY
    sms.store_id, sms.store_name
""")
gold_store_performance_df.repartition(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_performance.csv", header=True)

# 3. Gold Product Performance - Product-level aggregated performance table
gold_product_performance_df = spark.sql("""
SELECT
    pms.product_id,
    pms.product_name,
    pms.category,
    _sum(sts.total_revenue) AS revenue_contribution,
    _sum(sts.quantity_sold) AS quantities_sold
FROM
    sales_transactions_silver sts
INNER JOIN
    product_master_silver pms ON sts.product_id = pms.product_id
GROUP BY
    pms.product_id, pms.product_name, pms.category
""")
gold_product_performance_df.repartition(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True)

# 4. Gold Store Master - Gold conformed store dimension/master
gold_store_master_df = spark.sql("""
SELECT
    sms.store_id,
    sms.store_name,
    sms.location,
    sms.opening_date
FROM
    store_master_silver sms
""")
gold_store_master_df.repartition(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_master.csv", header=True)

# 5. Gold Product Master - Gold conformed product dimension/master
gold_product_master_df = spark.sql("""
SELECT
    pms.product_id,
    pms.product_name,
    pms.category,
    pms.supplier_id
FROM
    product_master_silver pms
""")
gold_product_master_df.repartition(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_master.csv", header=True)
```