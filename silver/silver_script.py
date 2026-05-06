from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum, countDistinct, upper, trim, date_format
from pyspark.sql.window import Window

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read and process stores_bronze
stores_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

store_silver_query = """
SELECT 
    store_id,
    TRIM(UPPER(store_name)) AS store_name,
    TRIM(UPPER(city)) AS city,
    TRIM(UPPER(store_type)) AS store_type
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_name) as rn
    FROM stores_bronze
) filtered
WHERE rn = 1
"""
store_silver_df = spark.sql(store_silver_query)
store_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/store_silver.csv")

# Read and process products_bronze
products_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_bronze_df.createOrReplaceTempView("products_bronze")

product_silver_query = """
SELECT 
    product_id,
    TRIM(UPPER(product_name)) AS product_name,
    TRIM(UPPER(category)) AS category
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) as rn
    FROM products_bronze
) filtered
WHERE rn = 1
"""
product_silver_df = spark.sql(product_silver_query)
product_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_silver.csv")

# Read and process sales_transactions_bronze
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transaction_silver_query = """
SELECT 
    transaction_id,
    store_id,
    product_id,
    DATE(transaction_time) AS transaction_date,
    sale_amount AS total_revenue,
    quantity AS quantity_sold
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time) as rn
    FROM sales_transactions_bronze
) filtered
WHERE rn = 1
"""
sales_transaction_silver_df = spark.sql(sales_transaction_silver_query)
sales_transaction_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transaction_silver.csv")

# Process daily_store_metrics_silver
sales_transaction_silver_df.createOrReplaceTempView("sales_transaction_silver")

daily_store_metrics_silver_query = """
SELECT 
    store_id,
    transaction_date,
    SUM(total_revenue) AS total_daily_revenue,
    COUNT(DISTINCT transaction_id) AS total_daily_transactions,
    SUM(quantity_sold) AS total_daily_quantity_sold
FROM sales_transaction_silver
GROUP BY store_id, transaction_date
"""
daily_store_metrics_silver_df = spark.sql(daily_store_metrics_silver_query)
daily_store_metrics_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/daily_store_metrics_silver.csv")

# Process product_sales_silver
product_silver_df.createOrReplaceTempView("product_silver")

product_sales_silver_query = """
SELECT 
    sts.product_id,
    sts.transaction_id,
    sts.transaction_date,
    sts.quantity_sold,
    sts.total_revenue AS revenue
FROM sales_transaction_silver sts
INNER JOIN product_silver ps ON sts.product_id = ps.product_id
"""
product_sales_silver_df = spark.sql(product_sales_silver_query)
product_sales_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_sales_silver.csv")