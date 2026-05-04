```python
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

# Initialize Glue Context and Spark Session
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session

# Load data from S3 bucket
pos_sales_event_bronze_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/pos_sales_event_bronze.csv/")

# Create Temporary View
pos_sales_event_bronze_df.createOrReplaceTempView("pseb")

# Process sales_transactions_silver
sales_transactions_df = spark.sql("""
    SELECT 
        sts.transaction_id, 
        sts.store_id, 
        sts.product_id, 
        CAST(sts.event_timestamp AS DATE) AS sale_date, 
        sts.quantity AS quantity_sold, 
        sts.total_amount AS revenue
    FROM 
    (
        SELECT 
            pseb.transaction_id, 
            pseb.store_id, 
            pseb.product_id,
            pseb.event_timestamp, 
            pseb.quantity, 
            pseb.total_amount,
            pseb.event_action,
            ROW_NUMBER() OVER (PARTITION BY pseb.transaction_id, pseb.store_id, pseb.product_id ORDER BY pseb.ingestion_timestamp DESC) as rn
        FROM 
            pseb 
        WHERE 
            pseb.event_type = 'sales' AND 
            pseb.is_deleted = false
    ) as sts 
    WHERE
        sts.rn = 1
    AND
        (sts.event_action IN ('INSERT', 'UPDATE') OR (sts.event_action IN ('CANCEL', 'DELETE') AND sts.event_action IS NULL))
""")
sales_transactions_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv", header=True)

# Process stores_silver
stores_df = spark.sql("""
    SELECT DISTINCT
        pseb.store_id,
        NULL AS store_name,
        NULL AS store_location
    FROM 
        pseb
    WHERE 
        pseb.event_type = 'sales' AND 
        pseb.is_deleted = false
""")
stores_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/stores_silver.csv", header=True)

# Process products_silver
products_df = spark.sql("""
    SELECT 
        product_id,
        FIRST(product_name, TRUE) AS product_name,
        FIRST(category, TRUE) AS product_category
    FROM 
    (
        SELECT 
            pseb.product_id,
            pseb.product_name,
            pseb.category,
            ROW_NUMBER() OVER (PARTITION BY pseb.product_id ORDER BY pseb.ingestion_timestamp DESC) as rn
        FROM 
            pseb
        WHERE 
            pseb.event_type = 'sales' AND 
            pseb.is_deleted = false
    ) as ps
    WHERE ps.rn = 1
    GROUP BY ps.product_id
""")
products_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/products_silver.csv", header=True)

# TODO: Implement data quality logs handling
```