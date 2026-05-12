from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, trim, upper, coalesce
from pyspark.sql.window import Window

# Initialize SparkSession and GlueContext
spark = SparkSession.builder \
    .appName("GlueApp") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# Read and process products_bronze
products_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/products_bronze.csv")

products_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
SELECT product_id, 
       TRIM(UPPER(product_name)) AS product_name, 
       TRIM(UPPER(category)) AS category, 
       TRIM(UPPER(brand)) AS brand
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY is_active DESC, ingestion_time DESC) AS rn
    FROM products_bronze
    WHERE COALESCE(TRIM(product_id), '') <> ''
) tmp
WHERE rn = 1
""")

products_silver_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/products_silver.csv")

# Read and process stores_bronze
stores_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/stores_bronze.csv")

stores_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
SELECT store_id,
       TRIM(UPPER(store_name)) AS store_name,
       TRIM(UPPER(state)) AS region,
       TRIM(UPPER(store_type)) AS store_type
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY is_open DESC, updated_at DESC) AS rn
    FROM stores_bronze
    WHERE COALESCE(TRIM(store_id), '') <> ''
) tmp
WHERE rn = 1
""")

stores_silver_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/stores_silver.csv")

# Read and process sales_transactions_bronze
transactions_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/sales_transactions_bronze.csv")

transactions_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
SELECT transaction_id, 
       CAST(transaction_time AS DATE) AS transaction_date, 
       store_id,
       product_id,
       COALESCE(quantity, 0) AS quantity_sold,
       COALESCE(sale_amount, 0.0) AS sales_amount
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY recorded_at DESC) AS rn
    FROM sales_transactions_bronze
    WHERE COALESCE(TRIM(transaction_id), '') <> ''
) tmp
WHERE rn = 1 AND quantity_sold >= 0 AND sales_amount >= 0
""")

sales_transactions_silver_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv")

# Read and join data for sales_integrated_silver
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

sales_integrated_silver_df = spark.sql("""
SELECT sts.transaction_id,
       sts.transaction_date,
       sts.store_id,
       ss.store_name,
       ss.region,
       sts.product_id,
       ps.product_name,
       ps.category,
       ps.brand,
       sts.quantity_sold,
       sts.sales_amount
FROM sales_transactions_silver sts
LEFT JOIN stores_silver ss ON sts.store_id = ss.store_id
LEFT JOIN products_silver ps ON sts.product_id = ps.product_id
""")

sales_integrated_silver_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_integrated_silver.csv")