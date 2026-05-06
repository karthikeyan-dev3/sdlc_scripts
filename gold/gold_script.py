```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

# Initialize SparkSession and GlueContext
args = getResolvedOptions(sys.argv, [])
spark = SparkSession.builder.appName("GoldTablesETL").getOrCreate()
glueContext = GlueContext(spark)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read and transform gold_sales_transactions
sales_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_silver.csv/")
sales_df.createOrReplaceTempView("sales_transactions_silver")

gold_sales_transactions_df = spark.sql("""
SELECT 
    transaction_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales_amount
FROM sales_transactions_silver
""")

gold_sales_transactions_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_transactions.csv", header=True)

# Read and transform gold_product_master
products_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_silver.csv/")
products_df.createOrReplaceTempView("products_silver")

gold_product_master_df = spark.sql("""
SELECT 
    product_id,
    TRIM(product_name) AS product_name,
    category,
    brand,
    CAST(price AS DOUBLE) AS price
FROM products_silver
""")

gold_product_master_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_master.csv", header=True)

# Read and transform gold_store_master
stores_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_silver.csv/")
stores_df.createOrReplaceTempView("stores_silver")

gold_store_master_df = spark.sql("""
SELECT
    store_id,
    TRIM(store_name) AS store_name,
    region,
    store_type
FROM stores_silver
""")

gold_store_master_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_master.csv", header=True)

# Read, join and transform gold_sales_aggregated
gold_sales_aggregated_df = spark.sql("""
SELECT 
    s.store_id,
    p.category,
    SUM(s.total_sales_amount) AS total_sales_amount,
    SUM(s.quantity_sold) AS total_quantity_sold,
    SUM(s.total_sales_amount) / COUNT(DISTINCT s.transaction_id) AS average_sales_per_transaction
FROM sales_transactions_silver s
LEFT JOIN stores_silver st ON s.store_id = st.store_id
LEFT JOIN products_silver p ON s.product_id = p.product_id
GROUP BY s.store_id, p.category
""")

gold_sales_aggregated_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_aggregated.csv", header=True)

# Read, join and transform gold_sales_standardized
gold_sales_standardized_df = spark.sql("""
SELECT 
    s.transaction_id,
    COALESCE(p.product_id, s.product_id) AS standardized_product_id,
    COALESCE(st.store_id, s.store_id) AS standardized_store_id,
    CAST(s.transaction_date AS DATE) AS standardized_date
FROM sales_transactions_silver s
LEFT JOIN products_silver p ON s.product_id = p.product_id
LEFT JOIN stores_silver st ON s.store_id = st.store_id
""")

gold_sales_standardized_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_standardized.csv", header=True)

# Stop the Spark session
spark.stop()
```