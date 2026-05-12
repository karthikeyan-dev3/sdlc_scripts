```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# AWS Glue Setup
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ===================================================================================
# SOURCE READS (Silver)
# ===================================================================================

# silver.silver_dim_product (sdp)
sdp_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_dim_product.{FILE_FORMAT}/")
)
sdp_df.createOrReplaceTempView("sdp")

# silver.silver_dim_store (sds)
sds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_dim_store.{FILE_FORMAT}/")
)
sds_df.createOrReplaceTempView("sds")

# silver.silver_fact_sales_transaction (sfst)
sfst_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/silver_fact_sales_transaction.{FILE_FORMAT}/")
)
sfst_df.createOrReplaceTempView("sfst")

# ===================================================================================
# TARGET: gold.gold_dim_product
# Mapping: silver.silver_dim_product sdp
# ===================================================================================

gold_dim_product_sql = """
SELECT
  CAST(sdp.product_id AS STRING)   AS product_id,
  CAST(sdp.product_name AS STRING) AS product_name,
  CAST(sdp.category AS STRING)     AS category,
  CAST(sdp.brand AS STRING)        AS brand,
  CAST(sdp.price AS DECIMAL(38,10)) AS price,
  CAST(sdp.is_active AS BOOLEAN)   AS is_active
FROM sdp
QUALIFY ROW_NUMBER() OVER (PARTITION BY sdp.product_id ORDER BY sdp.product_id) = 1
"""
gold_dim_product_df = spark.sql(gold_dim_product_sql)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

gold_dim_product_df.createOrReplaceTempView("gdp")

# ===================================================================================
# TARGET: gold.gold_dim_store
# Mapping: silver.silver_dim_store sds
# ===================================================================================

gold_dim_store_sql = """
SELECT
  CAST(sds.store_id AS STRING)     AS store_id,
  CAST(sds.store_name AS STRING)   AS store_name,
  CAST(sds.city AS STRING)         AS city,
  CAST(sds.state AS STRING)        AS state,
  CAST(sds.store_type AS STRING)   AS store_type,
  DATE(CAST(sds.open_date AS DATE)) AS open_date
FROM sds
QUALIFY ROW_NUMBER() OVER (PARTITION BY sds.store_id ORDER BY sds.store_id) = 1
"""
gold_dim_store_df = spark.sql(gold_dim_store_sql)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

gold_dim_store_df.createOrReplaceTempView("gds")

# ===================================================================================
# TARGET: gold.gold_fact_sales_transaction
# Mapping:
# silver.silver_fact_sales_transaction sfst
#   INNER JOIN gold.gold_dim_store gds ON sfst.store_id = gds.store_id
#   INNER JOIN gold.gold_dim_product gdp ON sfst.product_id = gdp.product_id
# ===================================================================================

gold_fact_sales_transaction_sql = """
SELECT
  CAST(sfst.transaction_id AS STRING)       AS transaction_id,
  CAST(sfst.transaction_time AS TIMESTAMP)  AS transaction_time,
  DATE(CAST(sfst.transaction_date AS DATE)) AS transaction_date,
  CAST(sfst.store_id AS STRING)             AS store_id,
  CAST(sfst.product_id AS STRING)           AS product_id,
  CAST(sfst.quantity AS INT)                AS quantity,
  CAST(sfst.sale_amount AS DECIMAL(38,10))  AS sale_amount
FROM sfst
INNER JOIN gds
  ON CAST(sfst.store_id AS STRING) = CAST(gds.store_id AS STRING)
INNER JOIN gdp
  ON CAST(sfst.product_id AS STRING) = CAST(gdp.product_id AS STRING)
QUALIFY ROW_NUMBER() OVER (PARTITION BY sfst.transaction_id ORDER BY sfst.transaction_id) = 1
"""
gold_fact_sales_transaction_df = spark.sql(gold_fact_sales_transaction_sql)

(
    gold_fact_sales_transaction_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_fact_sales_transaction.csv")
)

job.commit()
```