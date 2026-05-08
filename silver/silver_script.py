from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, sum as _sum, concat, trim, when, count, coalesce

# Initialize Spark session and Glue context
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark)

# Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and process transactions_bronze
transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")

transactions_df.createOrReplaceTempView("transactions_bronze")

sales_transactions_silver = spark.sql("""
    SELECT sts.transaction_id,
           CAST(sts.transaction_time AS DATE) AS transaction_date,
           sts.store_id,
           sts.product_id,
           sts.quantity AS quantity_sold,
           sts.sale_amount AS total_sales_amount
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
        FROM transactions_bronze
        WHERE transaction_id IS NOT NULL AND quantity >= 0 AND sale_amount >= 0 AND transaction_time IS NOT NULL
    ) sts
    WHERE rn = 1
""")

sales_transactions_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv")

# Read and process products_bronze
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("products_bronze")

product_master_silver = spark.sql("""
    SELECT pms.product_id,
           TRIM(pms.product_name) AS product_name,
           TRIM(pms.category) AS category,
           TRIM(pms.brand) AS brand
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY is_active DESC, ingestion_time DESC) AS rn
        FROM products_bronze
        WHERE is_active = 'true'
    ) pms
    WHERE rn = 1
""")

product_master_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/product_master_silver.csv")

# Read and process stores_bronze
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("stores_bronze")

store_master_silver = spark.sql("""
    SELECT sms.store_id,
           TRIM(sms.store_name) AS store_name,
           CONCAT(TRIM(sms.city), ', ', TRIM(sms.state)) AS location,
           TRIM(sms.store_type) AS store_type
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY open_date DESC, ingestion_time DESC) AS rn
        FROM stores_bronze
    ) sms
    WHERE rn = 1
""")

store_master_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/store_master_silver.csv")

# Read sales_transactions_silver for aggregations
sales_transactions_agg_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{TARGET_PATH}/sales_transactions_silver.csv")

sales_transactions_agg_df.createOrReplaceTempView("sales_transactions_silver")

sales_aggregated_silver = spark.sql("""
    SELECT sts.transaction_date AS date,
           sts.store_id,
           sts.product_id,
           SUM(sts.quantity_sold) AS total_quantity_sold,
           SUM(sts.total_sales_amount) AS total_revenue
    FROM sales_transactions_silver sts
    GROUP BY sts.transaction_date, sts.store_id, sts.product_id
""")

sales_aggregated_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_aggregated_silver.csv")

# Create report metadata
sales_transactions_silver.createOrReplaceTempView("sales_transactions_silver")
product_master_silver.createOrReplaceTempView("product_master_silver")
store_master_silver.createOrReplaceTempView("store_master_silver")

report_metadata_silver = spark.sql("""
    SELECT sts.transaction_date AS report_date,
           CASE WHEN COUNT(sts.transaction_id) > 0 THEN 'SUCCESS' ELSE 'FAILED' END AS data_refresh_status,
           100.0 * SUM(CASE WHEN sts.store_id IS NOT NULL AND sts.product_id IS NOT NULL AND
                            sms.store_id IS NOT NULL AND pms.product_id IS NOT NULL THEN 1 ELSE 0 END) / 
           COUNT(sts.transaction_id) AS accuracy_percentage
    FROM sales_transactions_silver sts
    LEFT JOIN product_master_silver pms ON sts.product_id = pms.product_id
    LEFT JOIN store_master_silver sms ON sts.store_id = sms.store_id
    GROUP BY sts.transaction_date
""")

report_metadata_silver.write.mode("overwrite").csv(f"{TARGET_PATH}/report_metadata_silver.csv")