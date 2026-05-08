from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, functions as F, Window

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Process gold_sales_transactions
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

sales_transactions_df.createOrReplaceTempView("sts")

sales_transactions_curated = spark.sql("""
    SELECT 
        transaction_id,
        product_id,
        store_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        quantity_sold,
        total_sales_amount,
        sales_channel
    FROM sts
""")

sales_transactions_curated.write.csv(f"{TARGET_PATH}/gold_sales_transactions.csv", header=True, mode="overwrite")

# Process gold_product_master
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("ps")

products_curated = spark.sql("""
    SELECT * FROM (
        SELECT 
            product_id,
            product_name,
            category,
            brand,
            price,
            availability_status,
            ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_id) as rn
        FROM ps
    ) WHERE rn = 1
""")

products_curated.write.csv(f"{TARGET_PATH}/gold_product_master.csv", header=True, mode="overwrite")

# Process gold_store_master
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("ss")

stores_curated = spark.sql("""
    SELECT * FROM (
        SELECT 
            store_id,
            store_name,
            location,
            store_type,
            CAST(opening_date AS DATE) AS opening_date,
            ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY store_id) as rn
        FROM ss
    ) WHERE rn = 1
""")

stores_curated.write.csv(f"{TARGET_PATH}/gold_store_master.csv", header=True, mode="overwrite")

# Process gold_sales_aggregates
sales_aggregates_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_aggregates_silver.{FILE_FORMAT}/")

sales_aggregates_df.createOrReplaceTempView("sas")

sales_aggregates_curated = spark.sql("""
    SELECT 
        CAST(date AS DATE) AS date,
        store_id,
        product_id,
        category,
        total_quantity_sold,
        total_sales_amount
    FROM sas
""")

sales_aggregates_curated.write.csv(f"{TARGET_PATH}/gold_sales_aggregates.csv", header=True, mode="overwrite")