import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum as _sum, coalesce, trim, upper, lower
from pyspark.sql.window import Window

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define paths and file format
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read source tables
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
products_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
stores_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

# Create temp views
sales_transactions_df.createOrReplaceTempView("stb")
products_df.createOrReplaceTempView("pb")
stores_df.createOrReplaceTempView("sb")

# Inventory Usage Transformation
inventory_usage_sql = """
    SELECT 
        stb.product_id AS ingredient_id,
        CAST(stb.transaction_time AS date) AS date,
        stb.store_id AS location_id,
        SUM(COALESCE(stb.quantity, 0)) AS quantity_used,
        0 AS waste_quantity
    FROM stb
    INNER JOIN pb ON stb.product_id = pb.product_id AND pb.is_active = true
    INNER JOIN sb ON stb.store_id = sb.store_id
    WHERE stb.product_id IS NOT NULL
      AND trim(stb.product_id) <> ''
      AND stb.store_id IS NOT NULL
      AND trim(stb.store_id) <> ''
    GROUP BY stb.product_id, CAST(stb.transaction_time AS date), stb.store_id
"""

inventory_usage_df = spark.sql(inventory_usage_sql)

# Write Inventory Usage Output
inventory_usage_df.repartition(1).write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/inventory_usage_silver.csv")

# Supplier Performance Placeholder
supplier_performance_sql = """
    SELECT 
        NULL AS supplier_id,
        CAST(NULL AS date) AS delivery_date,
        CAST(NULL AS double) AS quantity_delivered,
        CAST(NULL AS boolean) AS on_time_delivery
    WHERE 1=0
"""

supplier_performance_df = spark.sql(supplier_performance_sql)

# Write Supplier Performance Output
supplier_performance_df.repartition(1).write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/supplier_performance_silver.csv")

# Spoilage Trends Placeholder
spoilage_trends_sql = """
    SELECT 
        NULL AS ingredient_id,
        CAST(NULL AS date) AS spoilage_date,
        CAST(NULL AS double) AS waste_quantity,
        CAST(NULL AS varchar(255)) AS reason_for_spoilage
    WHERE 1=0
"""

spoilage_trends_df = spark.sql(spoilage_trends_sql)

# Write Spoilage Trends Output
spoilage_trends_df.repartition(1).write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/spoilage_trends_silver.csv")

# Commit job
job.commit()