from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize GlueContext and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
sales_transactions_silver = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

store_information_silver = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")

product_details_silver = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")

store_sales_aggregation_silver = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_sales_aggregation_silver.{FILE_FORMAT}/")

product_sales_aggregation_silver = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_sales_aggregation_silver.{FILE_FORMAT}/")

# Create temp views
sales_transactions_silver.createOrReplaceTempView("sales_transactions_silver")
store_information_silver.createOrReplaceTempView("store_information_silver")
product_details_silver.createOrReplaceTempView("product_details_silver")
store_sales_aggregation_silver.createOrReplaceTempView("store_sales_aggregation_silver")
product_sales_aggregation_silver.createOrReplaceTempView("product_sales_aggregation_silver")

# Transform and save gold_sales
gold_sales_df = spark.sql("""
    SELECT 
        sts.transaction_id, 
        sts.store_id, 
        sts.product_id, 
        CAST(sts.transaction_date AS DATE) AS transaction_date, 
        sts.quantity_sold, 
        sts.revenue, 
        sis.region, 
        sis.store_name, 
        pds.product_name, 
        pds.product_category
    FROM 
        sales_transactions_silver sts
    INNER JOIN 
        store_information_silver sis ON sts.store_id = sis.store_id
    INNER JOIN 
        product_details_silver pds ON sts.product_id = pds.product_id
""")
gold_sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_sales.csv")

# Transform and save gold_store_performance
gold_store_performance_df = spark.sql("""
    SELECT 
        ssas.store_id, 
        ssas.store_name, 
        ssas.region, 
        ssas.total_revenue, 
        ssas.total_transactions, 
        ssas.average_transaction_value, 
        ssas.top_selling_product_id, 
        ssas.top_selling_product_name
    FROM 
        store_sales_aggregation_silver ssas
""")
gold_store_performance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_store_performance.csv")

# Transform and save gold_product_performance
gold_product_performance_df = spark.sql("""
    SELECT 
        psas.product_id, 
        psas.product_name, 
        psas.product_category, 
        psas.total_revenue, 
        psas.total_units_sold, 
        psas.average_price, 
        psas.top_selling_store_id, 
        psas.top_selling_store_name
    FROM 
        product_sales_aggregation_silver psas
""")
gold_product_performance_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_product_performance.csv")