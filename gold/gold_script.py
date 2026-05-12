import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# Initialize GlueContext and SparkSession
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark)

# Load sales data
sales_silver_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/silver/sales_silver.csv/")

# Load products data
products_silver_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/silver/products_silver.csv/")

# Load stores data
stores_silver_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/silver/stores_silver.csv/")

# Create temporary views
sales_silver_df.createOrReplaceTempView("sales_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# Transform and save gold_sales
gold_sales_df = spark.sql("""
    SELECT
        ss.transaction_id,
        ss.product_id,
        ss.store_id,
        ss.sale_date,
        ss.quantity_sold,
        ss.total_sales_amount
    FROM sales_silver ss
    JOIN products_silver ps ON ss.product_id = ps.product_id
    JOIN stores_silver sts ON ss.store_id = sts.store_id
""")
gold_sales_df.coalesce(1).write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales.csv", header=True, mode="overwrite")

# Transform and save gold_products
gold_products_df = spark.sql("""
    SELECT
        ps.product_id,
        ps.product_name,
        ps.category,
        ps.manufacturer
    FROM products_silver ps
""")
gold_products_df.coalesce(1).write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_products.csv", header=True, mode="overwrite")

# Transform and save gold_stores
gold_stores_df = spark.sql("""
    SELECT
        sts.store_id,
        sts.store_name,
        sts.location,
        sts.region
    FROM stores_silver sts
""")
gold_stores_df.coalesce(1).write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_stores.csv", header=True, mode="overwrite")

# Transform and save gold_aggregated_sales
gold_aggregated_sales_df = spark.sql("""
    SELECT
        ss.store_id,
        ss.product_id,
        ss.sale_date,
        SUM(ss.quantity_sold) AS total_quantity_sold,
        SUM(ss.total_sales_amount) AS total_revenue
    FROM sales_silver ss
    GROUP BY ss.store_id, ss.product_id, ss.sale_date
""")
gold_aggregated_sales_df.coalesce(1).write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_aggregated_sales.csv", header=True, mode="overwrite")

spark.stop()