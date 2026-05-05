```python
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, concat, lit, upper, trim, coalesce

# Initialize Glue and Spark context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read source table
sales_event_df = spark.read.format("csv").option("header", True).load(f"{SOURCE_PATH}/sales_event_bronze.csv/")
sales_event_df.createOrReplaceTempView("sales_event_bronze")

# Product Catalog Silver Table
product_catalog_sql = """
SELECT 
    seb.product_id,
    TRIM(seb.product_name) AS product_name,
    UPPER(TRIM(seb.category)) AS category,
    UPPER(TRIM(seb.sub_category)) AS subcategory,
    COALESCE(
        FIRST_VALUE(seb.unit_price) OVER (PARTITION BY seb.product_id ORDER BY 
            CASE WHEN seb.unit_price > 0 THEN 1 ELSE 2 END, 
            COUNT(*) OVER (PARTITION BY seb.product_id, seb.unit_price) DESC),
        MAX(seb.unit_price) OVER (PARTITION BY seb.product_id)
    ) AS price
FROM sales_event_bronze seb
WHERE seb.product_id IS NOT NULL
GROUP BY seb.product_id, seb.product_name, seb.category, seb.sub_category
"""

product_catalog_df = spark.sql(product_catalog_sql)
product_catalog_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_catalog_silver.csv", header=True)

# Store Details Silver Table
store_details_sql = """
SELECT 
    seb.store_id,
    CONCAT('STORE_', seb.store_id) AS store_name,
    'UNKNOWN' AS city,
    'UNKNOWN' AS store_type,
    'UNKNOWN' AS region
FROM sales_event_bronze seb
WHERE seb.store_id IS NOT NULL
GROUP BY seb.store_id
"""

store_details_df = spark.sql(store_details_sql)
store_details_df.write.mode("overwrite").csv(f"{TARGET_PATH}/store_details_silver.csv", header=True)

# Sales Transactions Silver Table
product_catalog_df.createOrReplaceTempView("product_catalog_silver")
store_details_df.createOrReplaceTempView("store_details_silver")

sales_transactions_sql = """
SELECT 
    seb.transaction_id,
    seb.store_id,
    seb.product_id,
    seb.transaction_id AS sale_date,
    COALESCE(seb.quantity, 0) AS quantity_sold,
    COALESCE(
        seb.total_amount, 
        (COALESCE(seb.quantity, 0) * COALESCE(seb.unit_price, pcs.price, 0)) - COALESCE(seb.discount, 0)
    ) AS revenue,
    sds.city,
    sds.store_type
FROM sales_event_bronze seb
LEFT JOIN product_catalog_silver pcs ON seb.product_id = pcs.product_id
LEFT JOIN store_details_silver sds ON seb.store_id = sds.store_id
WHERE UPPER(TRIM(seb.event_action)) = 'SALE'
"""

sales_transactions_df = spark.sql(sales_transactions_sql)
sales_transactions_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)
```
