```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Initialize Spark and Glue context
spark = SparkSession.builder \
    .appName("GluePySparkJob") \
    .getOrCreate()

glue_context = GlueContext(spark.sparkContext)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Load source tables

# Read sales transactions table
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

# Read stores table
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

# Read products table
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")

# Create temporary views
sales_transactions_df.createOrReplaceTempView("sts")
stores_df.createOrReplaceTempView("ss")
products_df.createOrReplaceTempView("ps")

# Transformations for gold_sales_performance
gold_sales_performance_df = spark.sql("""
    SELECT 
        sts.transaction_id,
        sts.store_id,
        sts.product_id,
        sts.transaction_date,
        sts.quantity_sold,
        sts.total_sales,
        ss.region,
        ss.store_name,
        ps.product_name,
        ps.category,
        sts.total_sales / NULLIF(sts.quantity_sold, 0) AS store_performance_score
    FROM 
        sts
    LEFT JOIN 
        ss ON sts.store_id = ss.store_id
    LEFT JOIN 
        ps ON sts.product_id = ps.product_id
""")

# Write output for gold_sales_performance
gold_sales_performance_df.write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_sales_performance.csv")

# Transformations for gold_store_performance
gold_store_performance_df = spark.sql("""
    SELECT 
        sts.store_id,
        ss.region,
        ss.store_name,
        SUM(sts.total_sales) AS total_sales,
        COUNT(DISTINCT sts.transaction_id) AS total_transactions,
        SUM(sts.total_sales) / NULLIF(COUNT(DISTINCT sts.transaction_id), 0) AS average_transaction_value,
        SUM(sts.total_sales) / NULLIF(COUNT(DISTINCT sts.transaction_id), 0) AS performance_score
    FROM 
        sts
    LEFT JOIN 
        ss ON sts.store_id = ss.store_id
    GROUP BY 
        sts.store_id, ss.region, ss.store_name
""")

# Write output for gold_store_performance
gold_store_performance_df.write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_store_performance.csv")

# Transformations for gold_product_performance
gold_product_performance_df = spark.sql("""
    SELECT
        sts.product_id,
        ps.product_name,
        ps.category,
        SUM(sts.total_sales) AS total_sales,
        SUM(sts.quantity_sold) AS quantity_sold,
        SUM(sts.total_sales) / NULLIF(SUM(sts.quantity_sold), 0) AS average_price,
        SUM(sts.total_sales) / NULLIF(SUM(sts.quantity_sold), 0) AS performance_score
    FROM
        sts
    LEFT JOIN
        ps ON sts.product_id = ps.product_id
    GROUP BY
        sts.product_id, ps.product_name, ps.category
""")

# Write output for gold_product_performance
gold_product_performance_df.write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_product_performance.csv")
```
