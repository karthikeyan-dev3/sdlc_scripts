import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------
# Read Source Tables from S3
# -----------------------------
customer_orders_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders.{FILE_FORMAT}/")
)

customer_orders_dq_summary_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/customer_orders_dq_summary.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
customer_orders_src_df.createOrReplaceTempView("customer_orders")
customer_orders_dq_summary_src_df.createOrReplaceTempView("customer_orders_dq_summary")

# -----------------------------
# Target Table: gold_customer_orders
# -----------------------------
gold_customer_orders_df = spark.sql("""
SELECT
  CAST(sco.order_id AS STRING)                 AS order_id,
  CAST(sco.order_date AS DATE)                 AS order_date,
  CAST(sco.customer_id AS STRING)              AS customer_id,
  CAST(sco.product_id AS STRING)               AS product_id,
  CAST(sco.quantity AS INT)                    AS quantity,
  CAST(sco.unit_price AS DECIMAL(38, 10))      AS unit_price,
  CAST(sco.order_amount AS DECIMAL(38, 10))    AS order_amount,
  CAST(sco.order_status AS STRING)             AS order_status,
  CAST(sco.source_file_date AS DATE)           AS source_file_date,
  CAST(sco.load_timestamp AS TIMESTAMP)        AS load_timestamp
FROM customer_orders sco
""")

gold_customer_orders_output_path = f"{TARGET_PATH}/gold_customer_orders.csv"
(
    gold_customer_orders_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_output_path)
)

# -----------------------------
# Target Table: gold_customer_orders_dq_summary
# -----------------------------
gold_customer_orders_dq_summary_df = spark.sql("""
SELECT
  CAST(scodq.process_date AS DATE)                    AS process_date,
  CAST(scodq.total_records AS INT)                    AS total_records,
  CAST(scodq.valid_records AS INT)                    AS valid_records,
  CAST(scodq.invalid_records AS INT)                  AS invalid_records,
  CAST(scodq.data_quality_score_pct AS DECIMAL(38, 10)) AS data_quality_score_pct,
  CAST(scodq.ingestion_latency_minutes AS INT)        AS ingestion_latency_minutes,
  CAST(scodq.loaded_to_redshift_timestamp AS TIMESTAMP) AS loaded_to_redshift_timestamp
FROM customer_orders_dq_summary scodq
""")

gold_customer_orders_dq_summary_output_path = f"{TARGET_PATH}/gold_customer_orders_dq_summary.csv"
(
    gold_customer_orders_dq_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_customer_orders_dq_summary_output_path)
)

job.commit()