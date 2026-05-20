import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Read Source Tables (S3) + Create Temp Views
# -----------------------------------------------------------------------------------
sqs_messages_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_messages_raw.{FILE_FORMAT}/")
)
sqs_messages_raw_df.createOrReplaceTempView("sqs_messages_raw")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sqs_event_log_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_event_log_raw.{FILE_FORMAT}/")
)
sqs_event_log_raw_df.createOrReplaceTempView("sqs_event_log_raw")

sqs_error_log_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sqs_error_log_raw.{FILE_FORMAT}/")
)
sqs_error_log_raw_df.createOrReplaceTempView("sqs_error_log_raw")

data_governance_policies_raw_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_governance_policies_raw.{FILE_FORMAT}/")
)
data_governance_policies_raw_df.createOrReplaceTempView("data_governance_policies_raw")

# -----------------------------------------------------------------------------------
# Target: silver.sqs_messages_silver
# mapping_details: sqs_messages_raw smr
#   LEFT JOIN bronze.sales_transactions_bronze stb ON smr.transaction_id = stb.transaction_id
#   LEFT JOIN bronze.products_bronze pb ON smr.product_id = pb.product_id
#   LEFT JOIN bronze.stores_bronze sb ON smr.store_id = sb.store_id
# -----------------------------------------------------------------------------------
sqs_messages_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            smr.message_id AS message_id,
            CAST(smr.timestamp AS TIMESTAMP) AS timestamp,
            smr.payload AS payload,
            smr.validation_status AS validation_status,
            ROW_NUMBER() OVER (
                PARTITION BY smr.message_id
                ORDER BY CAST(smr.timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM sqs_messages_raw smr
        LEFT JOIN sales_transactions_bronze stb
            ON smr.transaction_id = stb.transaction_id
        LEFT JOIN products_bronze pb
            ON smr.product_id = pb.product_id
        LEFT JOIN stores_bronze sb
            ON smr.store_id = sb.store_id
    )
    SELECT
        message_id,
        timestamp,
        payload,
        validation_status
    FROM base
    WHERE rn = 1
    """
)
sqs_messages_silver_df.createOrReplaceTempView("sqs_messages_silver")

(
    sqs_messages_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sqs_messages_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sqs_events_silver
# mapping_details: sqs_event_log_raw sel INNER JOIN silver.sqs_messages_silver sms ON sel.message_id = sms.message_id
# -----------------------------------------------------------------------------------
sqs_events_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sel.event_id AS event_id,
            sel.message_id AS message_id,
            sel.event_type AS event_type,
            CAST(sel.timestamp AS TIMESTAMP) AS timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY sel.event_id
                ORDER BY CAST(sel.timestamp AS TIMESTAMP) DESC
            ) AS rn
        FROM sqs_event_log_raw sel
        INNER JOIN sqs_messages_silver sms
            ON sel.message_id = sms.message_id
    )
    SELECT
        event_id,
        message_id,
        event_type,
        timestamp
    FROM base
    WHERE rn = 1
    """
)

(
    sqs_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sqs_events_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.sqs_errors_silver
# mapping_details: sqs_error_log_raw erl LEFT JOIN silver.sqs_messages_silver sms ON erl.message_id = sms.message_id
# -----------------------------------------------------------------------------------
sqs_errors_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            erl.error_id AS error_id,
            erl.message_id AS message_id,
            erl.error_description AS error_description,
            CAST(erl.resolution_time AS TIMESTAMP) AS resolution_time,
            ROW_NUMBER() OVER (
                PARTITION BY erl.error_id
                ORDER BY CAST(erl.resolution_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sqs_error_log_raw erl
        LEFT JOIN sqs_messages_silver sms
            ON erl.message_id = sms.message_id
    )
    SELECT
        error_id,
        message_id,
        error_description,
        resolution_time
    FROM base
    WHERE rn = 1
    """
)

(
    sqs_errors_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sqs_errors_silver.csv")
)

# -----------------------------------------------------------------------------------
# Target: silver.data_governance_policies_silver
# mapping_details: data_governance_policies_raw dgpr
# -----------------------------------------------------------------------------------
data_governance_policies_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            dgpr.policy_id AS policy_id,
            dgpr.policy_name AS policy_name,
            dgpr.description AS description,
            dgpr.status AS status,
            ROW_NUMBER() OVER (
                PARTITION BY dgpr.policy_id
                ORDER BY dgpr.policy_id
            ) AS rn
        FROM data_governance_policies_raw dgpr
    )
    SELECT
        policy_id,
        policy_name,
        description,
        status
    FROM base
    WHERE rn = 1
    """
)

(
    data_governance_policies_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_governance_policies_silver.csv")
)