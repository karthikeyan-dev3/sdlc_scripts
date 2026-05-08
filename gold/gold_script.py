from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Initialize SparkSession and GlueContext
spark = SparkSession.builder.appName("GoldLayerTransformation").getOrCreate()
glueContext = GlueContext(spark)

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read and process sales_transactions_silver
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")

sales_transactions_df.createOrReplaceTempView("sts")

sales_transactions_gold_df = spark.sql("""
SELECT 
    transaction_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    COALESCE(store_id, '') AS store_id,
    COALESCE(product_id, '') AS product_id,
    quantity_sold,
    total_sales_amount
FROM (
    SELECT 
        sts.*,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_id) as row_num
    FROM sts
) tmp
WHERE row_num = 1
""")

sales_transactions_gold_df.write \
    .mode("overwrite") \
    .csv(TARGET_PATH + "/sales_transactions_gold.csv", header=True)

# Read and process product_master_silver
product_master_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")

product_master_df.createOrReplaceTempView("pms")

product_master_gold_df = spark.sql("""
SELECT 
    product_id,
    COALESCE(product_name, '') AS product_name,
    COALESCE(category, '') AS category,
    COALESCE(brand, '') AS brand
FROM pms
""")

product_master_gold_df.write \
    .mode("overwrite") \
    .csv(TARGET_PATH + "/product_master_gold.csv", header=True)

# Read and process store_master_silver
store_master_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")

store_master_df.createOrReplaceTempView("sms")

store_master_gold_df = spark.sql("""
SELECT 
    store_id,
    COALESCE(store_name, '') AS store_name,
    COALESCE(location, '') AS location,
    COALESCE(store_type, '') AS store_type
FROM sms
""")

store_master_gold_df.write \
    .mode("overwrite") \
    .csv(TARGET_PATH + "/store_master_gold.csv", header=True)

# Read and process sales_aggregated_silver
sales_aggregated_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")

sales_aggregated_df.createOrReplaceTempView("sas")

sales_aggregated_gold_df = spark.sql("""
SELECT 
    CAST(date AS DATE) AS date,
    COALESCE(store_id, '') AS store_id,
    COALESCE(product_id, '') AS product_id,
    total_quantity_sold,
    total_revenue
FROM sas
""")

sales_aggregated_gold_df.write \
    .mode("overwrite") \
    .csv(TARGET_PATH + "/sales_aggregated_gold.csv", header=True)

# Read and process report_metadata_silver
report_metadata_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/report_metadata_silver.{FILE_FORMAT}/")

report_metadata_df.createOrReplaceTempView("rms")

report_metadata_gold_df = spark.sql("""
SELECT 
    CAST(report_date AS DATE) AS report_date,
    data_refresh_status,
    accuracy_percentage,
    COALESCE(user_adoption_rate, 0.0) AS user_adoption_rate
FROM rms
""")

report_metadata_gold_df.write \
    .mode("overwrite") \
    .csv(TARGET_PATH + "/report_metadata_gold.csv", header=True)