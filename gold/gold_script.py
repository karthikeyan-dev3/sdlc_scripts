```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Setup
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended CSV read options (adjust if your silver CSV uses different settings)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "false",
}

# -----------------------------------------------------------------------------------
# SOURCE READS + TEMP VIEWS
# -----------------------------------------------------------------------------------

# silver.stores_silver (alias: ss)
df_stores_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
df_stores_silver.createOrReplaceTempView("stores_silver")

# silver.products_silver (alias: ps)
df_products_silver = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
df_products_silver.createOrReplaceTempView("products_silver")

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_dim_store
# -----------------------------------------------------------------------------------
# Transformations (EXACT per UDT):
#   gds.store_id=ss.store_id
#   gds.store_name=ss.store_name
#   gds.region=ss.region

df_gold_dim_store = spark.sql("""
SELECT
    CAST(ss.store_id AS STRING)   AS store_id,
    CAST(ss.store_name AS STRING) AS store_name,
    CAST(ss.region AS STRING)     AS region
FROM stores_silver ss
""")

(
    df_gold_dim_store.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold.gold_dim_product
# -----------------------------------------------------------------------------------
# Transformations (EXACT per UDT):
#   gdp.product_id=ps.product_id
#   gdp.product_name=ps.product_name
#   gdp.standard_category=ps.standard_category

df_gold_dim_product = spark.sql("""
SELECT
    CAST(ps.product_id AS STRING)         AS product_id,
    CAST(ps.product_name AS STRING)       AS product_name,
    CAST(ps.standard_category AS STRING)  AS standard_category
FROM products_silver ps
""")

(
    df_gold_dim_product.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)
```