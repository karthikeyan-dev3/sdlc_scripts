```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue / Spark setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# SOURCE READS + TEMP VIEWS
# (Path format is STRICT: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

# ------------------------------------------------------------------------------------
# TARGET TABLE: stores_silver
# - Apply UDT transformations exactly:
#     ss.store_id=sb.store_id
#     ss.store_name=sb.store_name
#     ss.region=sb.state
# - Include dedup with ROW_NUMBER (latest cannot be determined from UDT, so stable pick)
# ------------------------------------------------------------------------------------
stores_silver_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(TRIM(sb.store_id) AS STRING)      AS store_id,
        CAST(TRIM(sb.store_name) AS STRING)    AS store_name,
        CAST(TRIM(sb.state) AS STRING)         AS region
    FROM stores_bronze sb
),
dedup AS (
    SELECT
        store_id,
        store_name,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY store_id
        ) AS rn
    FROM base
)
SELECT
    store_id,
    store_name,
    region
FROM dedup
WHERE rn = 1
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------------------------------
# TARGET TABLE: products_silver
# - Apply UDT transformations exactly:
#     ps.product_id=pb.product_id
#     ps.product_name=pb.product_name
#     ps.standard_category=pb.category
# - Include dedup with ROW_NUMBER (latest cannot be determined from UDT, so stable pick)
# ------------------------------------------------------------------------------------
products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(TRIM(pb.product_id) AS STRING)     AS product_id,
        CAST(TRIM(pb.product_name) AS STRING)   AS product_name,
        CAST(TRIM(pb.category) AS STRING)       AS standard_category
    FROM products_bronze pb
),
dedup AS (
    SELECT
        product_id,
        product_name,
        standard_category,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS rn
    FROM base
)
SELECT
    product_id,
    product_name,
    standard_category
FROM dedup
WHERE rn = 1
""")

# Write SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/products_silver.csv")
)

job.commit()
```