```python
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Glue context and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Function to read data from S3
def read_source_data(table_name):
    return spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/{table_name}.{FILE_FORMAT}/")

# gold_sales_transactions transformation
cleaned_sales_df = read_source_data("cleaned_sales_silver")
cleaned_sales_df.createOrReplaceTempView("css")

gold_sales_transactions_df = spark.sql("""
    SELECT 
        css.transaction_id AS transaction_id,
        css.store_id AS store_id,
        css.product_id AS product_id,
        CAST(css.transaction_date AS DATE) AS transaction_date,
        css.quantity_sold AS quantity_sold,
        css.sales_amount AS sales_amount
    FROM css
""")

gold_sales_transactions_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_transactions.csv", header=True)

# gold_product_master transformation
product_master_df = read_source_data("product_master_silver")
product_master_df.createOrReplaceTempView("pms")

gold_product_master_df = spark.sql("""
    SELECT 
        pms.product_id AS product_id,
        pms.product_name AS product_name,
        pms.category AS category,
        pms.price AS price,
        COALESCE(pms.supplier_id, 'Unknown') AS supplier_id,
        pms.active_status AS active_status
    FROM pms
""")

gold_product_master_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_master.csv", header=True)

# gold_store_master transformation
store_master_df = read_source_data("store_master_silver")
store_master_df.createOrReplaceTempView("sms")

gold_store_master_df = spark.sql("""
    SELECT 
        sms.store_id AS store_id,
        sms.store_name AS store_name,
        sms.region AS region,
        sms.store_type AS store_type,
        sms.operational_status AS operational_status
    FROM sms
""")

gold_store_master_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_master.csv", header=True)

# gold_sales_aggregated transformation
sales_daily_aggregated_df = read_source_data("sales_daily_aggregated_silver")
sales_daily_aggregated_df.createOrReplaceTempView("sdas")

gold_sales_aggregated_df = spark.sql("""
    SELECT 
        sdas.aggregation_date AS aggregation_date,
        sdas.store_id AS store_id,
        sdas.product_id AS product_id,
        sdas.total_sales_amount AS total_sales_amount,
        sdas.total_quantity_sold AS total_quantity_sold
    FROM sdas
""")

gold_sales_aggregated_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_aggregated.csv", header=True)

# gold_cleaned_sales transformation
gold_cleaned_sales_df = spark.sql("""
    SELECT 
        css.transaction_id AS transaction_id,
        css.store_id AS store_id,
        css.product_id AS product_id,
        CAST(css.transaction_date AS DATE) AS transaction_date,
        css.quantity_sold AS quantity_sold,
        css.sales_amount AS sales_amount
    FROM css
""")

gold_cleaned_sales_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/gold_cleaned_sales.csv", header=True)
```