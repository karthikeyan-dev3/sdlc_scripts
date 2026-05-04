```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()
job = Job(glueContext)

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read and process for gold_sales
sales_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/sales_silver.csv/")
sales_df.createOrReplaceTempView("ss")

gold_sales_sql = """
SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    CAST(store_id AS STRING) AS store_id,
    CAST(sku_id AS STRING) AS sku_id,
    CAST(quantity_sold AS INTEGER) AS quantity_sold,
    CAST(transaction_date AS STRING) AS transaction_date,
    CAST(price_per_unit AS DECIMAL(18,2)) AS price_per_unit,
    CAST(sales_revenue AS DECIMAL(18,2)) AS sales_revenue
FROM ss
"""

gold_sales_df = spark.sql(gold_sales_sql)
gold_sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_sales.csv")

# Read and process for gold_inventory
inventory_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/inventory_silver.csv/")
inventory_df.createOrReplaceTempView("is")

demand_forecast_df = spark.read.format("csv").option("header", "true").load(f"{TARGET_PATH}/gold_demand_forecast_metrics.csv/")
demand_forecast_df.createOrReplaceTempView("gdfm")

gold_inventory_sql = """
SELECT
    is.store_id,
    is.sku_id,
    is.stock_on_hand,
    COALESCE(gdfm.demand_forecast, 0) AS demand_forecast
FROM is
LEFT JOIN gdfm
ON is.store_id = gdfm.store_id AND is.sku_id = gdfm.sku_id
"""

gold_inventory_df = spark.sql(gold_inventory_sql)
gold_inventory_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_inventory.csv")

# Read and process for gold_demand_forecast_metrics
gold_demand_forecast_metrics_sql = """
SELECT
    ss.store_id,
    ss.sku_id,
    DATE(ss.transaction_date) AS forecast_date,
    SUM(ss.quantity_sold) AS actual_quantity_sold
FROM ss
GROUP BY ss.store_id, ss.sku_id, DATE(ss.transaction_date)
"""

gold_demand_forecast_metrics_df = spark.sql(gold_demand_forecast_metrics_sql)
gold_demand_forecast_metrics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/gold_demand_forecast_metrics.csv")

# Finish job
job.commit()
```