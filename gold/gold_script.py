from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

# Initialize Spark and Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read silver.sales_transactions_silver
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
sales_transactions_df.createOrReplaceTempView("sts")

# Read silver.products_silver
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
products_df.createOrReplaceTempView("ps")

# Read silver.stores_silver
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
stores_df.createOrReplaceTempView("ss")

# Transformation for gold_sales
gold_sales_df = spark.sql("""
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount,
        price_per_unit
    FROM sts
""")
gold_sales_df.write.mode('overwrite').csv(TARGET_PATH + "/gold_sales.csv", header=True)

# Transformation for gold_product_details
gold_product_details_df = spark.sql("""
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM ps
""")
gold_product_details_df.write.mode('overwrite').csv(TARGET_PATH + "/gold_product_details.csv", header=True)

# Transformation for gold_store_details
gold_store_details_df = spark.sql("""
    SELECT
        store_id,
        store_name,
        store_region,
        store_manager
    FROM ss
""")
gold_store_details_df.write.mode('overwrite').csv(TARGET_PATH + "/gold_store_details.csv", header=True)

# Transformation for gold_store_performance
gold_store_performance_df = spark.sql("""
    SELECT
        sts.store_id,
        sts.transaction_date AS date,
        SUM(sts.sales_amount) AS total_sales,
        COUNT(DISTINCT sts.transaction_id) AS total_transactions,
        SUM(sts.sales_amount) / COUNT(DISTINCT sts.transaction_id) AS average_transaction_value
    FROM sts
    INNER JOIN ss ON sts.store_id = ss.store_id
    GROUP BY sts.store_id, sts.transaction_date
""")
gold_store_performance_df.write.mode('overwrite').csv(TARGET_PATH + "/gold_store_performance.csv", header=True)

# Transformation for gold_product_performance
gold_product_performance_df = spark.sql("""
    SELECT
        sts.product_id,
        sts.transaction_date AS date,
        SUM(sts.sales_amount) AS total_sales,
        SUM(sts.quantity_sold) AS total_units_sold,
        SUM(sts.sales_amount) / SUM(sts.quantity_sold) AS average_price
    FROM sts
    INNER JOIN ps ON sts.product_id = ps.product_id
    GROUP BY sts.product_id, sts.transaction_date
""")
gold_product_performance_df.write.mode('overwrite').csv(TARGET_PATH + "/gold_product_performance.csv", header=True)