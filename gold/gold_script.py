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

# ----------------------------
# 1) Read source tables from S3
# ----------------------------
sqs_messages_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_messages_silver.{FILE_FORMAT}/")
)

sqs_events_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_events_silver.{FILE_FORMAT}/")
)

sqs_errors_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_errors_silver.{FILE_FORMAT}/")
)

data_governance_policies_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_governance_policies_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sqs_messages_silver_df.createOrReplaceTempView("sqs_messages_silver")
sqs_events_silver_df.createOrReplaceTempView("sqs_events_silver")
sqs_errors_silver_df.createOrReplaceTempView("sqs_errors_silver")
data_governance_policies_silver_df.createOrReplaceTempView("data_governance_policies_silver")

# ----------------------------
# 3) Transformations using Spark SQL
# ----------------------------

# gold.gold_sqs_messages
gold_sqs_messages_df = spark.sql("""
SELECT
  CAST(sms.message_id AS STRING)        AS message_id,
  CAST(sms.timestamp AS TIMESTAMP)      AS timestamp,
  CAST(sms.payload AS STRING)           AS payload,
  CAST(sms.validation_status AS STRING) AS validation_status
FROM sqs_messages_silver sms
""")

# gold.gold_sqs_monitoring
gold_sqs_monitoring_df = spark.sql("""
SELECT
  CAST(ses.event_id AS STRING)      AS event_id,
  CAST(ses.message_id AS STRING)    AS message_id,
  CAST(ses.event_type AS STRING)    AS event_type,
  CAST(ses.timestamp AS TIMESTAMP)  AS timestamp
FROM sqs_events_silver ses
INNER JOIN sqs_messages_silver sms
  ON ses.message_id = sms.message_id
""")

# gold.gold_error_handling
gold_error_handling_df = spark.sql("""
SELECT
  CAST(ser.error_id AS STRING)             AS error_id,
  CAST(ser.message_id AS STRING)           AS message_id,
  CAST(ser.error_description AS STRING)    AS error_description,
  CAST(ser.resolution_time AS TIMESTAMP)   AS resolution_time
FROM sqs_errors_silver ser
LEFT JOIN sqs_messages_silver sms
  ON ser.message_id = sms.message_id
""")

# gold.gold_data_governance_policies
gold_data_governance_policies_df = spark.sql("""
SELECT
  CAST(dgps.policy_id AS STRING)      AS policy_id,
  CAST(dgps.policy_name AS STRING)    AS policy_name,
  CAST(dgps.description AS STRING)    AS description,
  CAST(dgps.status AS STRING)         AS status
FROM data_governance_policies_silver dgps
""")

# ----------------------------
# 4) Save outputs (single CSV file per target table directly under TARGET_PATH)
# ----------------------------
(
    gold_sqs_messages_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sqs_messages.csv")
)

(
    gold_sqs_monitoring_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sqs_monitoring.csv")
)

(
    gold_error_handling_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_error_handling.csv")
)

(
    gold_data_governance_policies_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_governance_policies.csv")
)

job.commit()