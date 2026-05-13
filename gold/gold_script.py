```python
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize GlueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Process for gold_sales
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")

gold_sales_sql = """
SELECT
    CAST(sales_id AS STRING) AS sales_id,
    CAST(product_id AS STRING) AS product_id,
    CAST(store_id AS STRING) AS store_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    CAST(quantity_sold AS INT) AS quantity_sold,
    CAST(sales_amount AS DOUBLE) AS sales_amount
FROM sales_transactions_silver
"""

gold_sales_df = spark.sql(gold_sales_sql)

gold_sales_df.coalesce(1).write \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_sales.csv", mode='overwrite')

# Process for gold_product
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("products_silver")

gold_product_sql = """
SELECT
    CAST(product_id AS STRING) AS product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    TRIM(brand) AS brand,
    CAST(list_price AS FLOAT) AS list_price
FROM products_silver
"""

gold_product_df = spark.sql(gold_product_sql)

gold_product_df.coalesce(1).write \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_product.csv", mode='overwrite')

# Process for gold_store
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("stores_silver")

gold_store_sql = """
SELECT
    CAST(store_id AS STRING) AS store_id,
    TRIM(store_name) AS store_name,
    TRIM(location) AS location,
    TRIM(store_type) AS store_type
FROM stores_silver
"""

gold_store_df = spark.sql(gold_store_sql)

gold_store_df.coalesce(1).write \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_store.csv", mode='overwrite')
```