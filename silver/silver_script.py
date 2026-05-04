```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, current_date, date_add
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# 1. sales_transactions_silver
# Read source tables
sales_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_event_bronze.csv/")
event_metadata_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/event_metadata_bronze.csv/")
payment_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/payment_event_bronze.csv/")

# Create temp views
sales_event_df.createOrReplaceTempView("seb")
event_metadata_df.createOrReplaceTempView("emb")
payment_event_df.createOrReplaceTempView("peb")

# SQL query
sales_transactions_query = """
SELECT 
    seb.transaction_id,
    seb.store_id,
    seb.product_id,
    CAST(emb.event_timestamp AS DATE) AS transaction_date,
    seb.quantity AS quantity_sold,
    seb.total_amount AS sales_amount
FROM 
    seb
INNER JOIN 
    emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
LEFT JOIN 
    peb ON peb.transaction_id = seb.transaction_id
WHERE 
    emb.is_deleted = false 
    AND seb.event_action = 'sale'
"""

sales_transactions_silver_df = spark.sql(sales_transactions_query)

# Write to target
sales_transactions_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv")

# 2. cleaned_sales_silver
# Read source table
sales_transactions_silver_df.createOrReplaceTempView("sts")

# SQL query
cleaned_sales_query = """
SELECT 
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    sales_amount,
    CURRENT_DATE AS data_cleaned_date
FROM (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY transaction_id, store_id, product_id, transaction_date ORDER BY event_timestamp DESC) as rn
    FROM sts 
) 
WHERE rn = 1
AND transaction_id IS NOT NULL
AND store_id IS NOT NULL
AND product_id IS NOT NULL
AND quantity_sold > 0
AND sales_amount >= 0
"""

cleaned_sales_silver_df = spark.sql(cleaned_sales_query)

# Write to target
cleaned_sales_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/cleaned_sales_silver.csv")

# 3. product_master_silver
# SQL query
product_master_query = """
SELECT 
    seb.product_id, 
    seb.product_name, 
    seb.category, 
    seb.unit_price as price, 
    NULL as supplier_id,
    CASE WHEN MAX(emb.event_timestamp) >= date_add(current_date(), -90) THEN 'Y' ELSE 'N' END as active_status
FROM 
    seb
INNER JOIN 
    emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
GROUP BY 
    seb.product_id, seb.product_name, seb.category, seb.unit_price
"""

product_master_silver_df = spark.sql(product_master_query)

# Write to target
product_master_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/product_master_silver.csv")

# 4. store_master_silver
# SQL query
store_master_query = """
SELECT 
    seb.store_id,
    NULL as store_name,
    NULL as region,
    NULL as store_type,
    CASE WHEN MAX(emb.event_timestamp) >= date_add(current_date(), -30) THEN 'OPEN' ELSE 'UNKNOWN' END as operational_status
FROM 
    seb
INNER JOIN 
    emb ON emb.event_id = seb.transaction_id AND emb.event_type = 'sales_event'
LEFT JOIN 
    feb ON feb.store_id = seb.store_id
GROUP BY 
    seb.store_id
"""

store_master_silver_df = spark.sql(store_master_query)

# Write to target
store_master_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/store_master_silver.csv")

# 5. sales_daily_aggregated_silver
# Read source table
cleaned_sales_silver_df.createOrReplaceTempView("css")

# SQL query
sales_daily_aggregated_query = """
SELECT 
    store_id, 
    product_id, 
    transaction_date AS aggregation_date, 
    SUM(sales_amount) AS total_sales_amount, 
    SUM(quantity_sold) AS total_quantity_sold
FROM 
    css
GROUP BY 
    store_id, product_id, transaction_date
"""

sales_daily_aggregated_silver_df = spark.sql(sales_daily_aggregated_query)

# Write to target
sales_daily_aggregated_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/sales_daily_aggregated_silver.csv")
```