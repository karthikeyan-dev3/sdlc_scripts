from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, expr, current_timestamp, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("GlueJob") \
    .getOrCreate()

glueContext = GlueContext(spark)
sc = spark.sparkContext

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read products_bronze table
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

# Create temp view for products
products_df.createOrReplaceTempView("pb")

# Transform products_silver
products_silver_df = spark.sql("""
SELECT 
    pb.product_id,
    TRIM(UPPER(pb.product_name)) AS product_name,
    TRIM(UPPER(pb.category)) AS category,
    TRIM(UPPER(pb.brand)) AS brand,
    pb.price,
    pb.is_active
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY current_timestamp DESC) AS rn
    FROM pb
) as subquery
WHERE rn = 1 AND pb.is_active = 'true' AND pb.product_id IS NOT NULL
""")

# Write products_silver as a single CSV file
products_silver_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/products_silver.csv")

# Read stores_bronze table
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

# Create temp view for stores
stores_df.createOrReplaceTempView("sb")

# Transform stores_silver
stores_silver_df = spark.sql("""
SELECT 
    sb.store_id,
    TRIM(UPPER(sb.store_name)) AS store_name,
    TRIM(UPPER(sb.city)) AS city,
    TRIM(UPPER(sb.state)) AS state,
    TRIM(UPPER(sb.store_type)) AS store_type,
    sb.open_date,
    CONCAT(TRIM(UPPER(sb.city)), ', ', TRIM(UPPER(sb.state))) AS location 
FROM (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY current_timestamp DESC) AS rn
    FROM sb
) as subquery
WHERE rn = 1 AND sb.store_id IS NOT NULL
""")

# Write stores_silver as a single CSV file
stores_silver_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/stores_silver.csv")

# Read sales_transactions_bronze table
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

# Create temp views for processing
sales_transactions_df.createOrReplaceTempView("stb")
products_silver_df.createOrReplaceTempView("ps")
stores_silver_df.createOrReplaceTempView("ss")

# Transform sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
SELECT 
    stb.transaction_id,
    stb.store_id,
    stb.product_id,
    stb.transaction_time,
    DATE(stb.transaction_time) AS transaction_date,
    stb.quantity AS quantity_sold,
    stb.sale_amount AS total_revenue
FROM (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
    FROM stb INNER JOIN ps ON stb.product_id = ps.product_id 
    INNER JOIN ss ON stb.store_id = ss.store_id
) as subquery
WHERE rn = 1 AND quantity_sold > 0 AND total_revenue >= 0
""")

# Write sales_transactions_silver as a single CSV file
sales_transactions_silver_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/sales_transactions_silver.csv")

# Transform sales_data_quality_silver
sales_data_quality_silver_df = spark.sql("""
SELECT 
    stb.transaction_id AS record_id,
    'sales_transactions_raw' AS data_source,
    CASE 
        WHEN stb.transaction_id IS NULL THEN 'missing_transaction_id' 
        WHEN stb.store_id IS NULL THEN 'missing_store_id' 
        WHEN stb.product_id IS NULL THEN 'missing_product_id' 
        WHEN ps.product_id IS NULL THEN 'orphan_product_id' 
        WHEN ss.store_id IS NULL THEN 'orphan_store_id' 
        WHEN stb.sale_amount < 0 THEN 'negative_sale_amount' 
        WHEN stb.quantity <= 0 THEN 'non_positive_quantity' 
        WHEN stb.transaction_time IS NULL THEN 'invalid_transaction_time' 
        ELSE 'passed_all_checks' 
    END AS check_type,
    CASE 
        WHEN stb.transaction_id IS NULL 
            OR stb.store_id IS NULL 
            OR stb.product_id IS NULL 
            OR ps.product_id IS NULL 
            OR ss.store_id IS NULL 
            OR stb.sale_amount < 0 
            OR stb.quantity <= 0 
            OR stb.transaction_time IS NULL THEN 'failed' 
        ELSE 'passed' 
    END AS status,
    current_timestamp AS timestamp
FROM stb
LEFT JOIN ps ON stb.product_id = ps.product_id
LEFT JOIN ss ON stb.store_id = ss.store_id
""")

# Write sales_data_quality_silver as a single CSV file
sales_data_quality_silver_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/sales_data_quality_silver.csv")