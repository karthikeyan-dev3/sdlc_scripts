```python
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.logger import GlueLogger
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = GlueLogger()

# Source and Target Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read Source Table: sales_event_bronze
sales_event_bronze = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/sales_event_bronze.csv/")
sales_event_bronze.createOrReplaceTempView("seb")

# Transform and Write: products_silver
products_silver_df = spark.sql("""
    SELECT 
        product_id,
        NULLIF(TRIM(product_name), '') AS product_name,
        NULLIF(TRIM(category), '') AS category,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_timestamp DESC, ingestion_timestamp DESC) as rn
    FROM seb
    WHERE is_deleted = false
""")

products_silver_dedup = products_silver_df.filter("rn = 1").drop("rn")
products_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/products_silver.csv", header=True)

# Transform and Write: stores_silver
stores_silver_df = spark.sql("""
    SELECT 
        store_id,
        NULL AS store_name,
        NULL AS store_region,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY event_timestamp DESC, ingestion_timestamp DESC) as rn
    FROM seb
    WHERE is_deleted = false
""")

stores_silver_dedup = stores_silver_df.filter("rn = 1").drop("rn")
stores_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Transform and Write: sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
    SELECT 
        transaction_id,
        product_id,
        store_id,
        CAST(event_timestamp AS DATE) AS date,
        quantity AS quantity_sold,
        unit_price,
        COALESCE(discount, 0) AS discount,
        total_amount AS sale_value,
        payment_id,
        order_id,
        terminal_id,
        cashier_id,
        ROW_NUMBER() OVER (PARTITION BY transaction_id, product_id, store_id, event_timestamp ORDER BY ingestion_timestamp DESC) as rn
    FROM seb
    WHERE is_deleted = false 
      AND UPPER(event_action) IN ('SALE', 'SELL', 'PURCHASE', 'COMPLETED_SALE')
""")

sales_transactions_silver_dedup = sales_transactions_silver_df.filter("rn = 1").drop("rn")
sales_transactions_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Transform and Write: returns_silver
returns_silver_df = spark.sql("""
    SELECT 
        transaction_id,
        product_id,
        store_id,
        CAST(event_timestamp AS DATE) AS date,
        quantity AS return_quantity,
        total_amount AS return_value,
        ROW_NUMBER() OVER (PARTITION BY transaction_id, product_id, store_id, event_timestamp ORDER BY ingestion_timestamp DESC) as rn
    FROM seb
    WHERE is_deleted = false 
      AND UPPER(event_action) IN ('RETURN', 'REFUND')
""")

returns_silver_dedup = returns_silver_df.filter("rn = 1").drop("rn")
returns_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/returns_silver.csv", header=True)

# Read Source Table: footfall_event_bronze
footfall_event_bronze = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/footfall_event_bronze.csv/")
footfall_event_bronze.createOrReplaceTempView("feb")

# Transform and Write: customer_visits_silver
customer_visits_silver_df = spark.sql("""
    SELECT 
        footfall_event_id,
        CAST(event_timestamp AS DATE) AS date,
        entry_count,
        exit_count,
        store_id,
        ROW_NUMBER() OVER (PARTITION BY footfall_event_id ORDER BY ingestion_timestamp DESC) as rn
    FROM feb
    WHERE is_deleted = false
""")

customer_visits_silver_dedup = customer_visits_silver_df.filter("rn = 1").drop("rn")
customer_visits_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/customer_visits_silver.csv", header=True)

# Read Source Table: inventory_event_bronze
inventory_event_bronze = spark.read.option("header", "true").csv(f"{SOURCE_PATH}/inventory_event_bronze.csv/")
inventory_event_bronze.createOrReplaceTempView("ieb")

# Transform and Write: inventory_silver
inventory_silver_df = spark.sql("""
    SELECT 
        product_id,
        store_id,
        CAST(event_timestamp AS DATE) AS date,
        current_stock AS stock_level,
        quantity_changed,
        UPPER(TRIM(change_type)) AS change_type,
        warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id, CAST(event_timestamp AS DATE) ORDER BY event_timestamp DESC, ingestion_timestamp DESC) as rn
    FROM ieb
    WHERE is_deleted = false
""")

inventory_silver_dedup = inventory_silver_df.filter("rn = 1").drop("rn")
inventory_silver_dedup.write.mode("overwrite").csv(f"{TARGET_PATH}/inventory_silver.csv", header=True)

# Read Source from Silver: sales_transactions_silver
sales_transactions_silver = spark.read.option("header", "true").csv(f"{TARGET_PATH}/sales_transactions_silver.csv/")
sales_transactions_silver.createOrReplaceTempView("sts")

# Transform and Write: transactions_silver
transactions_silver_df = spark.sql("""
    SELECT 
        transaction_id,
        MIN(date) AS date,
        ANY_VALUE(store_id) AS store_id,
        SUM(sale_value) AS total_sale_value,
        SUM(quantity_sold) AS total_quantity,
        ANY_VALUE(payment_id) AS payment_id
    FROM sts
    GROUP BY transaction_id
""")

transactions_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/transactions_silver.csv", header=True)
```