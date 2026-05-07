```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, concat_ws, coalesce, row_number, sum as _sum, countDistinct
from pyspark.sql.window import Window

# Initialize Glue context and Spark session
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()

# Constants
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Reading the source tables

# Sales Transactions Bronze
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

# Product Master Bronze
product_master_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true") \
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")

# Store Master Bronze
store_master_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true") \
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")

# Temp Views
sales_transactions_bronze_df.createOrReplaceTempView("stb")
product_master_bronze_df.createOrReplaceTempView("pmb")
store_master_bronze_df.createOrReplaceTempView("smb")

# Sales Transactions Silver
sales_transactions_silver_df = spark.sql("""
    SELECT 
        stb.transaction_id,
        DATE(stb.transaction_time) as sales_date,
        stb.product_id,
        stb.store_id,
        stb.quantity as quantity_sold,
        stb.sale_amount as total_sales_amount
    FROM stb
    LEFT JOIN pmb ON stb.product_id = pmb.product_id
    LEFT JOIN smb ON stb.store_id = smb.store_id
    WHERE stb.transaction_id IS NOT NULL 
      AND stb.product_id IS NOT NULL 
      AND stb.store_id IS NOT NULL
      AND stb.quantity > 0 
      AND stb.sale_amount > 0
""")
sales_transactions_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Product Master Silver
product_master_silver_df = spark.sql("""
    SELECT 
        TRIM(pmb.product_id) as product_id,
        UPPER(TRIM(pmb.product_name)) as product_name,
        UPPER(TRIM(pmb.category)) as category,
        pmb.price
    FROM pmb
    WHERE pmb.is_active = 'true'
""")
product_master_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_master_silver.csv", header=True)

# Store Master Silver
store_master_silver_df = spark.sql("""
    SELECT 
        TRIM(smb.store_id) as store_id,
        UPPER(TRIM(smb.store_name)) as store_name,
        CONCAT_WS(', ', COALESCE(smb.city, ''), COALESCE(smb.state, '')) as store_location,
        smb.open_date
    FROM smb
""")
store_master_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/store_master_silver.csv", header=True)

# Using dummy region mapping as region cannot be hardcoded without specific logic
# Assuming some logic such as `CASE smb.state ... END AS region`

# Sales Transactions Silver for Aggregation
sales_transactions_silver_df.createOrReplaceTempView("sts")

# Product Master Silver for Aggregation
product_master_silver_df.createOrReplaceTempView("pms")

# Sales Aggregate Silver
sales_aggregate_silver_df = spark.sql("""
    SELECT 
        sts.sales_date as date,
        sts.store_id,
        pms.category,
        SUM(sts.total_sales_amount) as total_sales_amount,
        COUNT(DISTINCT sts.transaction_id) as total_transactions,
        'daily_store_category' as aggregated_level
    FROM sts
    INNER JOIN pms ON sts.product_id = pms.product_id
    GROUP BY sts.sales_date, sts.store_id, pms.category
""")
sales_aggregate_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_aggregate_silver.csv", header=True)
```