from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Function to load data from S3 into a DataFrame
def load_table(table_name):
    return spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}{table_name}.{FILE_FORMAT}/")

# Load source tables
product_silver_df = load_table("product_silver")
sales_inventory_daily_silver_df = load_table("sales_inventory_daily_silver")
kpi_daily_product_silver_df = load_table("kpi_daily_product_silver")

# Create temporary views
product_silver_df.createOrReplaceTempView("ps")
sales_inventory_daily_silver_df.createOrReplaceTempView("sids")
kpi_daily_product_silver_df.createOrReplaceTempView("kdps")

# Transformations for gold_dim_product
gold_dim_product_df = spark.sql("""
    SELECT 
        ps.product_id AS product_id,
        ps.product_name AS product_name,
        ps.sku AS sku
    FROM 
        ps
""")

# Write gold_dim_product to S3
gold_dim_product_df.coalesce(1).write.csv(f"{TARGET_PATH}gold_dim_product.csv", header=True, mode="overwrite")

# Transformations for gold_fact_sales_inventory
gold_fact_sales_inventory_df = spark.sql("""
    SELECT 
        sids.store_id AS store_id,
        sids.product_id AS product_id,
        sids.date AS date,
        sids.sales_qty AS sales_qty,
        sids.inventory_qty AS inventory_qty,
        sids.stockout_flag AS stockout_flag
    FROM 
        sids
""")

# Write gold_fact_sales_inventory to S3
gold_fact_sales_inventory_df.coalesce(1).write.csv(f"{TARGET_PATH}gold_fact_sales_inventory.csv", header=True, mode="overwrite")

# Transformations for gold_kpis
gold_kpis_df = spark.sql("""
    SELECT 
        kdps.date AS date,
        kdps.product_id AS product_id,
        kdps.stockout_rate AS stockout_rate,
        kdps.sales_velocity AS sales_velocity,
        kdps.revenue_leakage_index AS revenue_leakage_index
    FROM 
        kdps
""")

# Write gold_kpis to S3
gold_kpis_df.coalesce(1).write.csv(f"{TARGET_PATH}gold_kpis.csv", header=True, mode="overwrite")