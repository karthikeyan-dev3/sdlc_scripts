```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue / Spark setup
# -----------------------------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -----------------------------------------------------------------------------------
# Parameters (as provided)
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source table(s) from S3
#    SOURCE READING RULE: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# -----------------------------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")

# -----------------------------------------------------------------------------------
# 3) Transform using Spark SQL (clean + dedup with ROW_NUMBER)
#    Target: silver.products_silver
#    Output columns: product_id, product_name, product_category
# -----------------------------------------------------------------------------------
products_silver_df = spark.sql("""
WITH base AS (
    SELECT
        CAST(TRIM(pb.product_id) AS STRING)          AS product_id,
        CAST(TRIM(pb.product_name) AS STRING)        AS product_name,
        CAST(TRIM(pb.category) AS STRING)            AS product_category,

        ROW_NUMBER() OVER (
            PARTITION BY CAST(TRIM(pb.product_id) AS STRING)
            ORDER BY CAST(TRIM(pb.product_id) AS STRING) DESC
        ) AS rn
    FROM products_bronze pb
    WHERE COALESCE(TRIM(pb.product_id), '') <> ''
),
dedup AS (
    SELECT
        product_id,
        product_name,
        product_category
    FROM base
    WHERE rn = 1
)
SELECT
    product_id,
    product_name,
    product_category
FROM dedup
""")

# -----------------------------------------------------------------------------------
# 4) Save output
#    OUTPUT WRITING RULE: single CSV file directly under TARGET_PATH
#    Path must be: TARGET_PATH + "/" + target_table.csv
# -----------------------------------------------------------------------------------
products_silver_output_path = f"{TARGET_PATH}/products_silver.csv"

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(products_silver_output_path)
)
```