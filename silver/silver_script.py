```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, upper, lower, lit, row_number
from pyspark.sql.window import Window

## Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

## Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## Sales Silver Table
# Read source tables
sales_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
payment_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/payment_bronze.{FILE_FORMAT}/")

# Create temp views
sales_bronze_df.createOrReplaceTempView("sb")
payment_bronze_df.createOrReplaceTempView("pb")

# Write SQL query for transformation
sales_silver_query = """
SELECT
  sb.transaction_id,
  sb.store_id,
  sb.product_id AS sku_id,
  sb.quantity AS quantity_sold,
  sb.transaction_date,
  sb.unit_price AS price_per_unit,
  sb.total_amount AS sales_revenue
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY sb.transaction_id, sb.store_id, sb.product_id ORDER BY sb.event_timestamp DESC) as rn
  FROM sb
  LEFT JOIN pb ON sb.payment_id = pb.payment_id AND sb.transaction_id = pb.transaction_id
  WHERE (pb.payment_status = 'successful' OR pb.payment_status IS NULL)
    AND (sb.event_action = 'completed')
) as filtered_sales
WHERE rn = 1
"""

# Execute query and write to target path
sales_silver_df = spark.sql(sales_silver_query)
sales_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_silver.csv", header=True)

## Inventory Silver Table
# Read source table
inventory_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/inventory_bronze.{FILE_FORMAT}/")

# Create temp view
inventory_bronze_df.createOrReplaceTempView("ib")

# Write SQL query for transformation
inventory_silver_query = """
SELECT
  ib.store_id,
  ib.product_id AS sku_id,
  GREATEST(ib.current_stock, 0) AS stock_on_hand
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY ib.store_id, ib.product_id ORDER BY ib.event_timestamp DESC) as rn
  FROM ib
) as filtered_inventory
WHERE rn = 1
"""

# Execute query and write to target path
inventory_silver_df = spark.sql(inventory_silver_query)
inventory_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/inventory_silver.csv", header=True)

job.commit()
```