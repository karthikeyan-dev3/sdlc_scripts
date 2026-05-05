```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, coalesce, trim, upper, lower, unix_timestamp
from pyspark.sql.window import Window

# Initialize Spark and Glue Context
spark = SparkSession.builder \
    .appName("ETL Job") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Source file paths
sales_event_bronze_path = f"s3://sdlc-agent-bucket/engineering-agent/bronze/sales_event_bronze.csv/"
payment_event_bronze_path = f"s3://sdlc-agent-bucket/engineering-agent/bronze/payment_event_bronze.csv/"

# Read source tables
sales_event_df = spark.read.format("csv").option("header", "true").load(sales_event_bronze_path)
payment_event_df = spark.read.format("csv").option("header", "true").load(payment_event_bronze_path)

# Create temp views
sales_event_df.createOrReplaceTempView("seb")
payment_event_df.createOrReplaceTempView("peb")

# Transformation for sales_transaction_silver
sales_transaction_query = """
SELECT 
    seb.transaction_id AS sales_id,
    seb.product_id,
    seb.store_id,
    CAST(seb.event_timestamp AS DATE) AS sale_date,
    seb.quantity AS quantity_sold,
    CAST(seb.total_amount AS DECIMAL(18,2)) AS sale_amount,
    peb.payment_mode,
    peb.payment_provider,
    peb.payment_status,
    peb.currency,
    peb.payment_amount
FROM 
    seb 
LEFT JOIN 
    peb 
ON 
    seb.transaction_id = peb.transaction_id 
    AND seb.payment_id = peb.payment_id
WHERE 
    seb.is_deleted = false
"""

sales_transaction_df = spark.sql(sales_transaction_query)
sales_transaction_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transaction_silver.csv", header=True)

# Transformation for product_silver
product_query = """
SELECT 
    seb.product_id,
    TRIM(UPPER(seb.product_name)) AS product_name,
    COALESCE(NULLIF(TRIM(seb.category), ''), 'UNKNOWN') AS category,
    CAST(seb.unit_price AS DECIMAL(18,2)) AS price
FROM 
    (SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_timestamp DESC, ingress_timestamp DESC) as rn 
    FROM 
        seb 
    WHERE 
        seb.is_deleted = false) 
WHERE 
    rn = 1
"""

product_df = spark.sql(product_query)
product_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/product_silver.csv", header=True)

# Transformation for store_silver
store_query = """
SELECT 
    seb.store_id,
    'UNKNOWN' AS store_name,
    'UNKNOWN' AS location,
    'UNKNOWN' AS region,
    'UNKNOWN' AS store_type
FROM 
    (SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY event_timestamp DESC, ingress_timestamp DESC) as rn 
    FROM 
        seb 
    WHERE 
        seb.is_deleted = false) 
WHERE 
    rn = 1
"""

store_df = spark.sql(store_query)
store_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/store_silver.csv", header=True)
```