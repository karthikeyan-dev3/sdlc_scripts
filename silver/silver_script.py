```python
import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

# Initialize GlueContext and Spark session
args = getResolvedOptions(sys.argv, [])
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark)
sc = glueContext.sparkContext

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# Load source data frames
sales_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_event_bronze.{FILE_FORMAT}/")
event_metadata_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/event_metadata_bronze.{FILE_FORMAT}/")
payment_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/payment_event_bronze.{FILE_FORMAT}/")
inventory_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/inventory_event_bronze.{FILE_FORMAT}/")
footfall_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/footfall_event_bronze.{FILE_FORMAT}/")

# Create temporary views for transformations
sales_event_df.createOrReplaceTempView("sales_event_bronze")
event_metadata_df.createOrReplaceTempView("event_metadata_bronze")
payment_event_df.createOrReplaceTempView("payment_event_bronze")
inventory_event_df.createOrReplaceTempView("inventory_event_bronze")
footfall_event_df.createOrReplaceTempView("footfall_event_bronze")

# Transform and write sales_transactions_silver
sales_transactions_query = """
SELECT 
    seb.transaction_id AS transaction_id,
    seb.store_id AS store_id,
    seb.product_id AS product_id,
    CAST(emb.event_timestamp AS date) AS transaction_date,
    COALESCE(seb.quantity, 0) AS quantity_sold,
    COALESCE(seb.total_amount, (COALESCE(seb.quantity, 0) * COALESCE(seb.unit_price, 0) - COALESCE(seb.discount, 0))) AS revenue
FROM sales_event_bronze seb
INNER JOIN event_metadata_bronze emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
LEFT JOIN payment_event_bronze peb ON peb.transaction_id = seb.transaction_id AND peb.payment_id = seb.payment_id
WHERE emb.is_deleted = false
  AND seb.event_action IN ('SALE','COMPLETED')
  AND seb.transaction_id IS NOT NULL
  AND seb.store_id IS NOT NULL
  AND seb.product_id IS NOT NULL
"""

sales_transactions_df = spark.sql(sales_transactions_query)
sales_transactions_df.write.csv(f"{TARGET_PATH}/sales_transactions_silver.csv", mode="overwrite", header=True)

# Transform and write product_master_silver
product_master_query = """
WITH ranked_products AS (
    SELECT 
        seb.product_id,
        seb.product_name,
        seb.category,
        seb.unit_price,
        emb.event_timestamp,
        ROW_NUMBER() OVER(PARTITION BY seb.product_id ORDER BY emb.event_timestamp DESC, seb.ingestion_timestamp DESC) as rn
    FROM sales_event_bronze seb
    INNER JOIN event_metadata_bronze emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
    WHERE emb.is_deleted = false
      AND seb.product_id IS NOT NULL
)
SELECT 
    product_id,
    TRIM(product_name) AS product_name,
    LOWER(category) AS category,
    unit_price AS price
FROM ranked_products
WHERE rn = 1
"""

product_master_df = spark.sql(product_master_query)
product_master_df.write.csv(f"{TARGET_PATH}/product_master_silver.csv", mode="overwrite", header=True)

# Transform and write store_master_silver
store_master_query = """
WITH all_stores AS (
    SELECT seb.store_id
    FROM sales_event_bronze seb
    INNER JOIN event_metadata_bronze emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
    WHERE emb.is_deleted = false
    UNION
    SELECT ieb.store_id
    FROM inventory_event_bronze ieb
    INNER JOIN event_metadata_bronze emb2 ON emb2.event_id = ieb.inventory_event_id AND emb2.event_type = 'inventory_event'
    WHERE emb2.is_deleted = false
    UNION
    SELECT feb.store_id
    FROM footfall_event_bronze feb
    INNER JOIN event_metadata_bronze emb3 ON emb3.event_id = feb.footfall_event_id AND emb3.event_type = 'footfall_event'
    WHERE emb3.is_deleted = false
)
SELECT DISTINCT 
    store_id,
    CAST(NULL AS STRING) AS store_name,
    CAST(NULL AS STRING) AS region,
    CAST(NULL AS STRING) AS store_type
FROM all_stores
"""

store_master_df = spark.sql(store_master_query)
store_master_df.write.csv(f"{TARGET_PATH}/store_master_silver.csv", mode="overwrite", header=True)
```