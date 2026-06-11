```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = Glueext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/src/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# ---------------------------------------
# Read source tables from S3 + temp views
# ---------------------------------------
products_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_raw.{FILE_FORMAT}/")
)
products_raw_df.createOrReplaceTempView("products_raw")

stores_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_raw.{FILE_FORMAT}/")
)
stores_raw_df.createOrReplaceTempView("stores_raw")

sales_transactions_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_raw.{FILE_FORMAT}/")
)
sales_transactions_raw_df.createOrReplaceTempView("sales_transactions_raw")

# -------------------------
# bronze.products_bronze
# -------------------------
products_bronze_df = spark.sql(
    """
    SELECT
        CAST(pr.product_id AS STRING)      AS product_id,
        CAST(pr.product_name AS STRING)    AS product_name,
        CAST(pr.category AS STRING)        AS category,
        CAST(pr.brand AS STRING)           AS brand,
        CAST(pr.price AS FLOAT)            AS price,
        CAST(pr.is_active AS BOOLEAN)      AS is_active
    FROM products_raw pr
    """
)

(
    products_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_bronze.csv")
)

# -------------------------
# bronze.stores_bronze
# -------------------------
stores_bronze_df = spark.sql(
    """
    SELECT
        CAST(sr.store_id AS STRING)        AS store_id,
        CAST(sr.store_name AS STRING)      AS store_name,
        CAST(sr.city AS STRING)            AS city,
        CAST(sr.state AS STRING)           AS state,
        CAST(sr.store_type AS STRING)      AS store_type,
        CAST(sr.open_date AS DATE)         AS open_date
    FROM stores_raw sr
    """
)

(
    stores_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_bronze.csv")
)

# --------------------------------------
# bronze.sales_transactions_bronze
# --------------------------------------
sales_transactions_bronze_df = spark.sql(
    """
    SELECT
        CAST(str.transaction_id AS STRING)     AS transaction_id,
        CAST(str.store_id AS STRING)           AS store_id,
        CAST(str.product_id AS STRING)         AS product_id,
        CAST(str.quantity AS INT)              AS quantity,
        CAST(str.sale_amount AS DOUBLE)        AS sale_amount,
        CAST(str.transaction_time AS TIMESTAMP) AS transaction_time
    FROM sales_transactions_raw str
    """
)

(
    sales_transactions_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_bronze.csv")
)

job.commit()
```