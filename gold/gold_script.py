import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Glue and Spark context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName("AWS Glue job").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
sales_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
products_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
stores_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

# Create temp views
sales_silver_df.createOrReplaceTempView("sales_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# gold_sales_insights table
gold_sales_insights_sql = """
SELECT 
    ss.transaction_id AS sale_id,
    ss.product_id,
    ss.store_id,
    CAST(ss.transaction_time AS date) AS sale_date,
    ss.sale_amount AS sales_amount,
    ss.quantity AS sales_units,
    ps.product_name,
    sts.store_name,
    NULL AS inventory_level,
    NULL AS stockout_flag
FROM sales_silver ss
LEFT JOIN products_silver ps ON ss.product_id = ps.product_id
LEFT JOIN stores_silver sts ON ss.store_id = sts.store_id
"""
gold_sales_insights_df = spark.sql(gold_sales_insights_sql)
gold_sales_insights_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_insights.csv", header=True)

# gold_store_performance table
gold_store_performance_sql = """
SELECT 
    ss.store_id,
    CAST(ss.transaction_time AS date) AS report_date,
    SUM(ss.sale_amount) AS total_sales,
    SUM(ss.quantity) AS total_units_sold,
    sts.region,
    NULL AS stockout_rate,
    NULL AS sales_velocity
FROM sales_silver ss
INNER JOIN stores_silver sts ON ss.store_id = sts.store_id
GROUP BY ss.store_id, CAST(ss.transaction_time AS date), sts.region
"""
gold_store_performance_df = spark.sql(gold_store_performance_sql)
gold_store_performance_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_performance.csv", header=True)

# gold_product_inventory table
gold_product_inventory_sql = """
SELECT 
    ps.product_id,
    ps.product_name,
    MAX(ss.transaction_time) AS last_stocked_date,
    NULL AS current_inventory_level,
    NULL AS predicted_stockout,
    NULL AS restock_required
FROM products_silver ps
LEFT JOIN sales_silver ss ON ps.product_id = ss.product_id
GROUP BY ps.product_id, ps.product_name
"""
gold_product_inventory_df = spark.sql(gold_product_inventory_sql)
gold_product_inventory_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_inventory.csv", header=True)

# Commit job
job.commit()