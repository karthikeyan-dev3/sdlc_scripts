```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

# Initialize contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Load pos_sales_event_bronze data
pos_sales_event_bronze_df = spark.read.option("header", "true") \
    .csv(f"{SOURCE_PATH}/pos_sales_event_bronze.{FILE_FORMAT}/")

# Create temp views
pos_sales_event_bronze_df.createOrReplaceTempView("pseb")

# Transform and write sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
    SELECT 
        pseb.transaction_id,
        CAST(pseb.event_timestamp AS DATE) AS transaction_date,
        pseb.store_id,
        pseb.product_id,
        pseb.quantity AS quantity_sold,
        pseb.total_amount AS sales_amount
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pseb.transaction_id, pseb.store_id, pseb.product_id, CAST(pseb.event_timestamp AS DATE)
                ORDER BY pseb.event_timestamp DESC, pseb.ingestion_timestamp DESC
            ) AS rn
        FROM pseb
        WHERE pseb.event_type = 'sales'
          AND pseb.is_deleted = FALSE
          AND pseb.event_action <> 'DELETE'
    ) t
    WHERE rn = 1
""")
sales_transactions_silver_df.write.csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True, mode="overwrite")

# Transform and write product_master_silver
product_master_silver_df = spark.sql("""
    SELECT 
        pseb.product_id,
        pseb.product_name,
        pseb.category,
        COALESCE(NULLIF(pseb.brand, ''), 'UNKNOWN') AS brand,
        pseb.unit_price AS price
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pseb.product_id
                ORDER BY pseb.event_timestamp DESC, pseb.ingestion_timestamp DESC
            ) AS rn
        FROM pseb
        WHERE pseb.event_type = 'sales'
          AND pseb.is_deleted = FALSE
          AND pseb.product_id IS NOT NULL
          AND pseb.product_name IS NOT NULL
          AND pseb.category IS NOT NULL
          AND pseb.unit_price IS NOT NULL
    ) t
    WHERE rn = 1
""")
product_master_silver_df.write.csv(f"{TARGET_PATH}/product_master_silver.csv", header=True, mode="overwrite")
```