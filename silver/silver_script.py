```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, row_number, coalesce, trim, upper, lower
from pyspark.sql.window import Window

# Initialize Spark and Glue contexts
spark = SparkSession.builder.appName("AWS Glue PySpark Script").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Read sales_event_bronze and create a temporary view
sales_event_df = spark.read.option("header", "true") \
    .csv(f"s3://sdlc-agent-bucket/engineering-agent/bronze/sales_event_bronze.csv/")
sales_event_df.createOrReplaceTempView("sales_event_bronze")

# Generate sales_transactions_silver table
sales_transactions_query = """
SELECT
    seb.transaction_id,
    seb.store_id,
    seb.product_id,
    CAST(seb.event_timestamp AS DATE) AS sale_date,
    seb.quantity AS quantity_sold,
    COALESCE(seb.total_amount, (seb.quantity * seb.unit_price) - COALESCE(seb.discount, 0)) AS total_revenue
FROM
    sales_event_bronze seb
WHERE
    seb.event_type = 'sales'
    AND seb.is_deleted = false
    AND ROW_NUMBER() OVER (PARTITION BY seb.transaction_id, seb.product_id, seb.store_id ORDER BY seb.event_timestamp DESC, seb.ingestion_timestamp DESC) = 1
"""
sales_transactions_df = spark.sql(sales_transactions_query)

# Write sales_transactions_silver table
sales_transactions_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv", header=True)

# Generate product_master_silver table
product_master_query = """
SELECT
    seb.product_id,
    TRIM(seb.product_name) AS product_name,
    LOWER(TRIM(seb.category)) AS category
FROM
    sales_event_bronze seb
WHERE
    seb.event_type = 'sales'
    AND ROW_NUMBER() OVER (PARTITION BY seb.product_id ORDER BY seb.event_timestamp DESC, seb.ingestion_timestamp DESC) = 1
"""
product_master_df = spark.sql(product_master_query)

# Write product_master_silver table
product_master_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv", header=True)

# Read inventory_event_bronze and footfall_event_bronze and create temporary views
inventory_event_df = spark.read.option("header", "true") \
    .csv(f"s3://sdlc-agent-bucket/engineering-agent/bronze/inventory_event_bronze.csv/")
inventory_event_df.createOrReplaceTempView("inventory_event_bronze")

footfall_event_df = spark.read.option("header", "true") \
    .csv(f"s3://sdlc-agent-bucket/engineering-agent/bronze/footfall_event_bronze.csv/")
footfall_event_df.createOrReplaceTempView("footfall_event_bronze")

# Generate store_master_silver table
store_master_query = """
SELECT DISTINCT
    feb.store_id
FROM
    footfall_event_bronze feb
UNION
SELECT DISTINCT
    ieb.store_id
FROM
    inventory_event_bronze ieb
UNION
SELECT DISTINCT
    seb.store_id
FROM
    sales_event_bronze seb
"""
store_master_df = spark.sql(store_master_query)

# Write store_master_silver table
store_master_df.write.mode("overwrite").csv(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv", header=True)
```