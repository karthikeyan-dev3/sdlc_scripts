```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Glue Job Init
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters (as provided)
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended options for CSV I/O (adjust if your headers/delimiters differ)
CSV_READ_OPTIONS = {
    "header": "true",
    "inferSchema": "true",
    "mode": "PERMISSIVE",
}
CSV_WRITE_OPTIONS = {
    "header": "true",
}

# ====================================================================================
# TABLE: gold.gold_dim_product
# ====================================================================================

# 1) Read source tables from S3
df_sdp = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_dim_product.{FILE_FORMAT}/")
)

# 2) Create temp views
df_sdp.createOrReplaceTempView("silver_dim_product")

# 3) SQL transformation (EXACT mapping: direct select)
df_gold_dim_product = spark.sql(
    """
    SELECT
        CAST(sdp.product_id   AS STRING)  AS product_id,
        CAST(sdp.product_name AS STRING)  AS product_name,
        CAST(sdp.category     AS STRING)  AS category,
        CAST(sdp.brand        AS STRING)  AS brand,
        CAST(sdp.price        AS DECIMAL(38,10)) AS price,
        CAST(sdp.is_active    AS BOOLEAN) AS is_active
    FROM silver_dim_product sdp
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_dim_product.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# ====================================================================================
# TABLE: gold.gold_dim_store
# ====================================================================================

# 1) Read source tables from S3
df_sds = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_dim_store.{FILE_FORMAT}/")
)

# 2) Create temp views
df_sds.createOrReplaceTempView("silver_dim_store")

# 3) SQL transformation (EXACT mapping: direct select)
df_gold_dim_store = spark.sql(
    """
    SELECT
        CAST(sds.store_id    AS STRING) AS store_id,
        CAST(sds.store_name  AS STRING) AS store_name,
        CAST(sds.city        AS STRING) AS city,
        CAST(sds.state       AS STRING) AS state,
        CAST(sds.store_type  AS STRING) AS store_type,
        CAST(sds.open_date   AS DATE)   AS open_date
    FROM silver_dim_store sds
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_dim_store.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# ====================================================================================
# TABLE: gold.gold_fact_sales_transaction
# ====================================================================================

# 1) Read source tables from S3
df_sfst = (
    spark.read.format(FILE_FORMAT)
    .options(**CSV_READ_OPTIONS)
    .load(f"{SOURCE_PATH}/silver_fact_sales_transaction.{FILE_FORMAT}/")
)

# 2) Create temp views
df_sfst.createOrReplaceTempView("silver_fact_sales_transaction")
df_gold_dim_store.createOrReplaceTempView("gold_dim_store")
df_gold_dim_product.createOrReplaceTempView("gold_dim_product")

# 3) SQL transformation (EXACT mapping: join to conformed dimensions)
df_gold_fact_sales_transaction = spark.sql(
    """
    SELECT
        CAST(sfst.transaction_id   AS STRING)     AS transaction_id,
        CAST(sfst.transaction_time AS TIMESTAMP)  AS transaction_time,
        CAST(sfst.transaction_date AS DATE)       AS transaction_date,
        CAST(gds.store_id          AS STRING)     AS store_id,
        CAST(gdp.product_id        AS STRING)     AS product_id,
        CAST(sfst.quantity         AS INT)        AS quantity,
        CAST(sfst.sale_amount      AS DECIMAL(38,10)) AS sale_amount
    FROM silver_fact_sales_transaction sfst
    INNER JOIN gold_dim_store   gds ON sfst.store_id   = gds.store_id
    INNER JOIN gold_dim_product gdp ON sfst.product_id = gdp.product_id
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_fact_sales_transaction.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .options(**CSV_WRITE_OPTIONS)
    .save(f"{TARGET_PATH}/gold_fact_sales_transaction.csv")
)

job.commit()
```