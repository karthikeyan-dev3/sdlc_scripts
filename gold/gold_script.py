```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark and Glue Contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
sts = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
pcs = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/product_catalog_silver.{FILE_FORMAT}/")
sds = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")

# Create temporary views
sts.createOrReplaceTempView("sales_transactions_silver")
pcs.createOrReplaceTempView("product_catalog_silver")
sds.createOrReplaceTempView("store_details_silver")

# Transform and write the gold_sales table
gold_sales_df = spark.sql("""
SELECT
    CAST(sts.transaction_id AS STRING) AS transaction_id,
    CAST(sts.store_id AS STRING) AS store_id,
    CAST(sts.product_id AS STRING) AS product_id,
    CAST(sts.sale_date AS STRING) AS sale_date,
    CAST(sts.quantity_sold AS INT) AS quantity_sold,
    CAST(sts.revenue AS DECIMAL(18, 2)) AS revenue,
    CAST(sds.city AS STRING) AS city,
    CAST(sds.store_type AS STRING) AS store_type
FROM
    sales_transactions_silver sts
LEFT JOIN
    product_catalog_silver pcs ON sts.product_id = pcs.product_id
LEFT JOIN
    store_details_silver sds ON sts.store_id = sds.store_id
""")
gold_sales_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_sales.csv", header=True, mode="overwrite")

# Transform and write the gold_products table
gold_products_df = spark.sql("""
SELECT
    CAST(pcs.product_id AS STRING) AS product_id,
    CAST(pcs.product_name AS STRING) AS product_name,
    CAST(pcs.category AS STRING) AS category,
    CAST(pcs.subcategory AS STRING) AS subcategory,
    CAST(pcs.price AS DECIMAL(18, 2)) AS price
FROM
    product_catalog_silver pcs
""")
gold_products_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_products.csv", header=True, mode="overwrite")

# Transform and write the gold_stores table
gold_stores_df = spark.sql("""
SELECT
    CAST(sds.store_id AS STRING) AS store_id,
    CAST(sds.store_name AS STRING) AS store_name,
    CAST(sds.city AS STRING) AS city,
    CAST(sds.store_type AS STRING) AS store_type,
    CAST(sds.region AS STRING) AS region
FROM
    store_details_silver sds
""")
gold_stores_df.coalesce(1).write.csv(f"{TARGET_PATH}/gold_stores.csv", header=True, mode="overwrite")
```
