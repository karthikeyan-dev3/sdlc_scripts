```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# Job setup (AWS Glue)
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Parameters
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Recommended for deterministic single-file writes (best-effort)
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

# ====================================================================================
# TARGET TABLE: gold.gold_customer_order  (alias: gco)
# Source: silver.customer_order_silver (alias: cos)
# ====================================================================================

# 1) Read source table from S3
df_customer_order_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
df_customer_order_silver.createOrReplaceTempView("customer_order_silver")

# 3) Transform using Spark SQL (apply exact mappings)
df_gold_customer_order = spark.sql(
    """
    SELECT
        CAST(cos.order_id AS STRING)                             AS order_id,
        DATE(cos.order_date)                                     AS order_date,
        CAST(cos.customer_id AS STRING)                          AS customer_id,
        CAST(cos.order_status AS STRING)                         AS order_status,
        CAST(cos.order_total_amount AS DECIMAL(38, 18))          AS order_total_amount,
        CAST(cos.currency_code AS STRING)                        AS currency_code,
        CAST(cos.source_system AS STRING)                        AS source_system,
        DATE(cos.ingestion_date)                                 AS ingestion_date
    FROM customer_order_silver cos
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_order.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order.csv")
)

# ====================================================================================
# TARGET TABLE: gold.gold_customer_order_line  (alias: gcol)
# Source: silver.customer_order_line_silver (alias: cols)
# ====================================================================================

# 1) Read source table from S3
df_customer_order_line_silver = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_order_line_silver.{FILE_FORMAT}/")
)

# 2) Create temp view
df_customer_order_line_silver.createOrReplaceTempView("customer_order_line_silver")

# 3) Transform using Spark SQL (apply exact mappings)
df_gold_customer_order_line = spark.sql(
    """
    SELECT
        CAST(cols.order_id AS STRING)                            AS order_id,
        CAST(cols.order_line_id AS INT)                          AS order_line_id,
        CAST(cols.product_id AS STRING)                          AS product_id,
        CAST(cols.quantity AS INT)                               AS quantity,
        CAST(cols.unit_price_amount AS DECIMAL(38, 18))          AS unit_price_amount,
        CAST(cols.line_total_amount AS DECIMAL(38, 18))          AS line_total_amount,
        CAST(cols.currency_code AS STRING)                       AS currency_code,
        DATE(cols.ingestion_date)                                AS ingestion_date
    FROM customer_order_line_silver cols
    """
)

# 4) Save output as a SINGLE CSV file directly under TARGET_PATH
(
    df_gold_customer_order_line.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_customer_order_line.csv")
)

job.commit()
```