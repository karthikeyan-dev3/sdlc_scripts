from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

# Initialize GlueContext and SparkSession
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName("MyGlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")

sales_data_quality_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_data_quality_silver.{FILE_FORMAT}/")

# Create temp views
sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")
stores_df.createOrReplaceTempView("stores_silver")
products_df.createOrReplaceTempView("products_silver")
sales_data_quality_df.createOrReplaceTempView("sales_data_quality_silver")

# Transformations and saving gold_sales_transactions
gold_sales_transactions = spark.sql("""
    SELECT 
        sts.transaction_id,
        sts.store_id,
        sts.product_id,
        sts.transaction_date,
        sts.quantity_sold,
        sts.total_revenue
    FROM sales_transactions_silver sts
""")
gold_sales_transactions.write.csv(f"{TARGET_PATH}/gold_sales_transactions.csv", header=True, mode='overwrite')

# Transformations and saving gold_store_performance
gold_store_performance = spark.sql("""
    SELECT 
        sts.store_id,
        ss.store_name,
        ss.location,
        SUM(sts.total_revenue) AS total_revenue,
        COUNT(sts.transaction_id) AS transaction_count,
        SUM(sts.quantity_sold) AS quantity_sold
    FROM sales_transactions_silver sts
    JOIN stores_silver ss ON sts.store_id = ss.store_id
    GROUP BY sts.store_id, ss.store_name, ss.location
""")
gold_store_performance.write.csv(f"{TARGET_PATH}/gold_store_performance.csv", header=True, mode='overwrite')

# Transformations and saving gold_product_performance
gold_product_performance = spark.sql("""
    SELECT 
        sts.product_id,
        ps.product_name,
        ps.category,
        sts.store_id,
        SUM(sts.total_revenue) AS total_revenue,
        SUM(sts.quantity_sold) AS quantity_sold
    FROM sales_transactions_silver sts
    JOIN products_silver ps ON sts.product_id = ps.product_id
    GROUP BY sts.product_id, ps.product_name, ps.category, sts.store_id
""")
gold_product_performance.write.csv(f"{TARGET_PATH}/gold_product_performance.csv", header=True, mode='overwrite')

# Transformations and saving gold_sales_data_quality
gold_sales_data_quality = spark.sql("""
    SELECT 
        sdq.record_id,
        sdq.data_source,
        sdq.check_type,
        sdq.status,
        sdq.timestamp
    FROM sales_data_quality_silver sdq
""")
gold_sales_data_quality.write.csv(f"{TARGET_PATH}/gold_sales_data_quality.csv", header=True, mode='overwrite')

# Commit job
job.commit()