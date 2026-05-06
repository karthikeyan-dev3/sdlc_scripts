from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

# Create Spark and Glue contexts
spark = SparkSession.builder \
    .appName("Glue ETL for Gold Tables") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Load source tables
daily_store_metrics_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/daily_store_metrics_silver.csv/")
store_silver_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/store_silver.csv/")
product_sales_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/product_sales_silver.csv/")
product_silver_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/product_silver.csv/")

# Create temp views
daily_store_metrics_df.createOrReplaceTempView("daily_store_metrics_silver")
store_silver_df.createOrReplaceTempView("store_silver")
product_sales_df.createOrReplaceTempView("product_sales_silver")
product_silver_df.createOrReplaceTempView("product_silver")

# Gold Sales Performance
gold_sales_performance_query = """
SELECT 
    dsms.store_id,
    ss.store_name,
    ss.city,
    ss.store_type,
    dsms.transaction_date,
    dsms.total_daily_revenue AS total_revenue,
    dsms.total_daily_transactions AS total_transactions,
    dsms.total_daily_quantity_sold AS total_quantity_sold
FROM daily_store_metrics_silver dsms
INNER JOIN store_silver ss ON dsms.store_id = ss.store_id
"""
gold_sales_performance_df = spark.sql(gold_sales_performance_query)
gold_sales_performance_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_sales_performance.csv", header=True, mode="overwrite")

# Gold Product Performance
gold_product_performance_query = """
WITH product_weekly_agg AS (
    SELECT 
        pss.product_id,
        ps.product_name,
        ps.category,
        weekofyear(pss.sale_date) AS week_number,
        SUM(pss.revenue) AS weekly_revenue,
        SUM(pss.quantity_sold) AS weekly_quantity_sold
    FROM product_sales_silver pss
    INNER JOIN product_silver ps ON pss.product_id = ps.product_id
    GROUP BY pss.product_id, ps.product_name, ps.category, weekofyear(pss.sale_date)
),
category_agg AS (
    SELECT 
        category,
        week_number,
        SUM(weekly_revenue) AS total_category_weekly_revenue
    FROM product_weekly_agg
    GROUP BY category, week_number
)
SELECT 
    pwa.product_id,
    pwa.product_name,
    pwa.category,
    pwa.week_number,
    pwa.weekly_revenue,
    pwa.weekly_quantity_sold,
    pwa.weekly_revenue / ca.total_category_weekly_revenue AS category_level_revenue_contrib
FROM product_weekly_agg pwa
INNER JOIN category_agg ca ON pwa.category = ca.category AND pwa.week_number = ca.week_number
"""
gold_product_performance_df = spark.sql(gold_product_performance_query)
gold_product_performance_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True, mode="overwrite")

# Gold Aggregated Store Performance
gold_aggregated_store_performance_query = """
SELECT 
    dsms.transaction_date AS date,
    ss.city,
    ss.store_type,
    AVG(dsms.total_daily_revenue) AS average_daily_revenue,
    AVG(dsms.total_daily_transactions) AS average_daily_transactions
FROM daily_store_metrics_silver dsms
INNER JOIN store_silver ss ON dsms.store_id = ss.store_id
GROUP BY dsms.transaction_date, ss.city, ss.store_type
"""
gold_aggregated_store_performance_df = spark.sql(gold_aggregated_store_performance_query)
gold_aggregated_store_performance_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_aggregated_store_performance.csv", header=True, mode="overwrite")