import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# Source: bronze.spend_data_bronze
# --------------------------------------------------------------------
spend_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/spend_data_bronze.{FILE_FORMAT}/")
)
spend_data_bronze_df.createOrReplaceTempView("spend_data_bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("DROP VIEW IF EXISTS bronze.spend_data_bronze")
spark.sql("CREATE VIEW bronze.spend_data_bronze AS SELECT * FROM spend_data_bronze")

# --------------------------------------------------------------------
# Target: silver.recipient_profile_silver
# --------------------------------------------------------------------
recipient_profile_silver_df = spark.sql(
    """
SELECT DISTINCT
  TRIM(CUSTOMER_MASTERID) AS CUSTOMER_MASTERID,
  TRIM(COMPANY_PROFILEID) AS COMPANY_PROFILEID,
  TRIM(RECIPIENT_CATEGORY) AS RECIPIENT_CATEGORY,
  TRIM(ORGANIZATION_NAME) AS ORGANIZATION_NAME,
  TRIM(LAST_NAME) AS LAST_NAME,
  TRIM(FIRST_NAME) AS FIRST_NAME,
  TRIM(MIDDLE_NAME) AS MIDDLE_NAME,
  TRIM(ADDRESS_1) AS ADDRESS_1,
  TRIM(ADDRESS_2) AS ADDRESS_2,
  TRIM(CITY) AS CITY,
  TRIM(PROVINCE) AS PROVINCE,
  POSTAL_CODE AS POSTAL_CODE,
  TRIM(COUNTRY) AS COUNTRY,
  TRIM(PROFILE_TYPE) AS PROFILE_TYPE,
  TRIM(SPECIALTY) AS SPECIALTY,
  STATE_LICENSE_NUMBER AS STATE_LICENSE_NUMBER,
  TRIM(LICENSE_STATE) AS LICENSE_STATE,
  NPI_NUMBER AS NPI_NUMBER,
  TAX_ID_NUM AS TAX_ID_NUM,
  TRIM(RECIPIENT_IDENTIFIER_COUNTRY) AS RECIPIENT_IDENTIFIER_COUNTRY,
  TRIM(RECIPIENT_IDENTIFIER_TYPE) AS RECIPIENT_IDENTIFIER_TYPE,
  RECIPIENT_IDENTIFIER_VALUE AS RECIPIENT_IDENTIFIER_VALUE,
  TRANSACTION_CONSENT AS TRANSACTION_CONSENT,
  TRIM(CUSTOMER_SOURCESYSTEM) AS CUSTOMER_SOURCESYSTEM,
  TRIM(PAYEE_NAME) AS PAYEE_NAME,
  TRIM(PAYEE_TYPE) AS PAYEE_TYPE
FROM bronze.spend_data_bronze
WHERE
  (CUSTOMER_MASTERID IS NOT NULL OR COMPANY_PROFILEID IS NOT NULL OR NPI_NUMBER IS NOT NULL OR TAX_ID_NUM IS NOT NULL OR RECIPIENT_IDENTIFIER_VALUE IS NOT NULL)
"""
)

(
    recipient_profile_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/recipient_profile_silver.csv")
)

# --------------------------------------------------------------------
# Target: silver.transfer_of_value_silver
# --------------------------------------------------------------------
transfer_of_value_silver_df = spark.sql(
    """
SELECT DISTINCT
  TRIM(CUSTOMER_SOURCESYSTEM) AS CUSTOMER_SOURCESYSTEM,
  TRIM(COMPANY_TRANSACTIONID) AS COMPANY_TRANSACTIONID,
  TRANSACTION_DATE AS TRANSACTION_DATE,
  TRIM(COMPANY_PROFILEID) AS COMPANY_PROFILEID,
  TRIM(CUSTOMER_MASTERID) AS CUSTOMER_MASTERID,
  TRIM(RECIPIENT_CATEGORY) AS RECIPIENT_CATEGORY,
  TRIM(PURPOSE) AS PURPOSE,
  TRIM(SECONDARY_PURPOSE) AS SECONDARY_PURPOSE,
  TRIM(FORM) AS FORM,
  TOTAL_AMOUNT AS TOTAL_AMOUNT,
  TRIM(CURRENCY) AS CURRENCY,
  TOTAL_NUMBER_OF_RECIPIENTS AS TOTAL_NUMBER_OF_RECIPIENTS,
  NUMBER_OF_COMPANY_REPRESENTATIVES AS NUMBER_OF_COMPANY_REPRESENTATIVES,
  NUMBER_OF_NONPROFESSIONAL_RECIPIENTS AS NUMBER_OF_NONPROFESSIONAL_RECIPIENTS,
  NUMBER_OF_NOSHOWS AS NUMBER_OF_NOSHOWS,
  TRIM(COMPANY_SALES_REPID) AS COMPANY_SALES_REPID,
  TRIM(TRANSACTION_INITIATOR_FIRSTNAME) AS TRANSACTION_INITIATOR_FIRSTNAME,
  TRIM(TRANSACTION_INITIATOR_LASTNAME) AS TRANSACTION_INITIATOR_LASTNAME,
  TRIM(PRODUCT) AS PRODUCT,
  TRIM(PRODUCT_2) AS PRODUCT_2,
  INDIRECT_PAYMENT AS INDIRECT_PAYMENT,
  TRIM(PAYEE_NAME) AS PAYEE_NAME,
  TRIM(PAYEE_TYPE) AS PAYEE_TYPE,
  TRIM(COUNTRY) AS COUNTRY,
  TRIM(PROVINCE) AS PROVINCE,
  TRIM(CITY) AS CITY,
  POSTAL_CODE AS POSTAL_CODE,
  TRANSACTION_CONSENT AS TRANSACTION_CONSENT,
  TRIM(COMPANY_EVENT_ID) AS COMPANY_EVENT_ID
FROM bronze.spend_data_bronze
WHERE COMPANY_TRANSACTIONID IS NOT NULL
"""
)

(
    transfer_of_value_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transfer_of_value_silver.csv")
)

# --------------------------------------------------------------------
# Target: silver.event_silver
# --------------------------------------------------------------------
event_silver_df = spark.sql(
    """
SELECT DISTINCT
  TRIM(COMPANY_EVENT_ID) AS COMPANY_EVENT_ID,
  TRIM(ENGAGEMENT_TYPE) AS ENGAGEMENT_TYPE,
  TRIM(ENGAGEMENT_NAME) AS ENGAGEMENT_NAME,
  ENGAGEMENT_START_DATE AS ENGAGEMENT_START_DATE,
  ENGAGEMENT_END_DATE AS ENGAGEMENT_END_DATE,
  TRIM(ENGAGEMENT_DESCRIPTION) AS ENGAGEMENT_DESCRIPTION,
  TRIM(VENUE_CITY) AS VENUE_CITY,
  TRIM(VENUE_PROVINCE) AS VENUE_PROVINCE,
  TRIM(VENUE_COUNTRY) AS VENUE_COUNTRY,
  VENUE_POSTALCODE AS VENUE_POSTALCODE,
  TRIM(COMPANY_PROFILEID) AS COMPANY_PROFILEID
FROM bronze.spend_data_bronze
WHERE COMPANY_EVENT_ID IS NOT NULL
"""
)

(
    event_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/event_silver.csv")
)

# --------------------------------------------------------------------
# Target: silver.material_distribution_silver
# --------------------------------------------------------------------
material_distribution_silver_df = spark.sql(
    """
SELECT DISTINCT
  TRIM(CUSTOMER_SOURCESYSTEM) AS CUSTOMER_SOURCESYSTEM,
  TRIM(COMPANY_TRANSACTIONID) AS COMPANY_TRANSACTIONID,
  TRANSACTION_DATE AS TRANSACTION_DATE,
  TRIM(MATERIAL_NAME) AS MATERIAL_NAME,
  MATERIAL_QTY AS MATERIAL_QTY,
  TRIM(CURRENCY) AS CURRENCY,
  TOTAL_AMOUNT AS TOTAL_AMOUNT,
  TRIM(COMPANY_PROFILEID) AS COMPANY_PROFILEID,
  TRIM(CUSTOMER_MASTERID) AS CUSTOMER_MASTERID,
  NPI_NUMBER AS NPI_NUMBER,
  TAX_ID_NUM AS TAX_ID_NUM,
  RECIPIENT_IDENTIFIER_VALUE AS RECIPIENT_IDENTIFIER_VALUE,
  TRIM(COUNTRY) AS COUNTRY
FROM bronze.spend_data_bronze
WHERE MATERIAL_NAME IS NOT NULL OR MATERIAL_QTY IS NOT NULL
"""
)

(
    material_distribution_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/material_distribution_silver.csv")
)

job.commit()
