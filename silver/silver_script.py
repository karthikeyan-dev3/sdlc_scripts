from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, upper, lower
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark and Glue contexts
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"

# Load sales_transactions_bronze table
sales_transactions_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.csv/")
sales_transactions_df.createOrReplaceTempView("stb")

# Transform sales_transactions
sales_transactions_silver = spark.sql("""
    SELECT transaction_id, store_id, product_id,
           CAST(transaction_time AS DATE) AS transaction_date,
           quantity AS quantity_sold, sale_amount AS total_revenue
    FROM (
        SELECT stb.*,
               ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
        FROM stb
        WHERE transaction_id IS NOT NULL AND transaction_id != ''
          AND store_id IS NOT NULL AND store_id != ''
          AND product_id IS NOT NULL AND product_id != ''
          AND quantity > 0 AND sale_amount >= 0
    ) tmp
    WHERE rn = 1
    """)

# Write sales_transactions_silver to target
sales_transactions_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Load product_master_bronze table
product_master_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/product_master_bronze.csv/")
product_master_df.createOrReplaceTempView("pmb")

# Transform product_master
product_master_silver = spark.sql("""
    SELECT product_id, product_name,
           category AS product_category, price AS list_price
    FROM (
        SELECT pmb.*,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC, category DESC) AS rn
        FROM pmb
        WHERE product_id IS NOT NULL AND product_id != ''
          AND price >= 0 AND is_active = 'true'
    ) tmp
    WHERE rn = 1
    """)

# Write product_master_silver to target
product_master_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/product_master_silver.csv", header=True)

# Load store_master_bronze table
store_master_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/store_master_bronze.csv/")
store_master_df.createOrReplaceTempView("smb")

# Transform store_master
store_master_silver = spark.sql("""
    SELECT store_id, TRIM(UPPER(store_name)) AS store_name,
           TRIM(UPPER(state)) AS region, NULL AS store_manager
    FROM (
        SELECT smb.*,
               ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY state DESC) AS rn
        FROM smb
        WHERE store_id IS NOT NULL AND store_id != ''
          AND open_date <= CURRENT_DATE()
    ) tmp
    WHERE rn = 1
    """)

# Write store_master_silver to target
store_master_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/store_master_silver.csv", header=True)