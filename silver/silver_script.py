
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.transforms import *

# Initialize Spark and Glue Context
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(sc)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read products_bronze table
products_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_df.createOrReplaceTempView("products_bronze")

# Transforming and writing to product_master_silver
product_master_silver_df = spark.sql("""
    SELECT 
        pb.product_id,
        TRIM(UPPER(pb.product_name)) AS product_name,
        TRIM(UPPER(pb.category)) AS category,
        TRIM(UPPER(pb.brand)) AS brand,
        CASE 
            WHEN pb.price < 0 THEN 0.0
            ELSE pb.price
        END AS price
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS row_num
        FROM products_bronze
    ) pb
    WHERE pb.row_num = 1 AND pb.is_active = true
""")
product_master_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/product_master_silver.csv", header=True)

# Read stores_bronze table
stores_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_df.createOrReplaceTempView("stores_bronze")

# Transforming and writing to store_master_silver
store_master_silver_df = spark.sql("""
    SELECT 
        sb.store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        CASE 
            WHEN sb.state IN ('NY', 'NJ', 'CT') THEN 'Northeast'
            WHEN sb.state IN ('CA', 'NV') THEN 'West'
            WHEN sb.state IN ('TX', 'OK') THEN 'South'
            ELSE 'Other'
        END AS region
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY updated_at DESC) AS row_num
        FROM stores_bronze
    ) sb
    WHERE sb.row_num = 1
""")
store_master_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/store_master_silver.csv", header=True)

# Read sales_transactions_bronze table
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_df.createOrReplaceTempView("sales_transactions_bronze")

# Transforming and writing to sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
    SELECT 
        CAST(stb.transaction_id AS VARCHAR(10)) AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        CAST(stb.product_id AS VARCHAR(10)) AS product_id,
        CAST(stb.store_id AS VARCHAR(10)) AS store_id,
        stb.quantity AS quantity_sold,
        stb.sale_amount AS sales_amount
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS row_num
        FROM sales_transactions_bronze
    ) stb
    WHERE 
        stb.row_num = 1 AND 
        stb.quantity > 0 AND 
        stb.sale_amount >= 0 AND 
        stb.transaction_id IS NOT NULL AND 
        stb.product_id IS NOT NULL AND 
        stb.store_id IS NOT NULL
""")
sales_transactions_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Read the transformed sales_transactions_silver
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# Transforming and writing to sales_aggregated_daily_silver
sales_aggregated_daily_silver_df = spark.sql("""
    SELECT 
        sts.transaction_date AS date,
        sts.product_id,
        sts.store_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.sales_amount) AS total_sales_amount,
        CASE 
            WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.sales_amount) / SUM(sts.quantity_sold)
            ELSE NULL
        END AS average_price
    FROM sales_transactions_silver sts
    GROUP BY sts.transaction_date, sts.product_id, sts.store_id
""")
sales_aggregated_daily_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/sales_aggregated_daily_silver.csv", header=True)
