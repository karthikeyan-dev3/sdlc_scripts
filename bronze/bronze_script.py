import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/spend_data/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# -------------------------
# Source Read(s) + Temp View(s)
# -------------------------
spend_data_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/spend_data.{FILE_FORMAT}/")
)
spend_data_df.createOrReplaceTempView("spend_data")

# -------------------------
# Target: bronze.spend_data_bronze
# -------------------------
spend_data_bronze_df = spark.sql(
    """
    SELECT
        sd.CUSTOMER_MASTERID AS CUSTOMER_MASTERID,
        sd.COMPANY_PROFILEID AS COMPANY_PROFILEID,
        sd.RECIPIENT_CATEGORY AS RECIPIENT_CATEGORY,
        sd.ORGANIZATION_NAME AS ORGANIZATION_NAME,
        sd.LAST_NAME AS LAST_NAME,
        sd.FIRST_NAME AS FIRST_NAME,
        sd.MIDDLE_NAME AS MIDDLE_NAME,
        sd.ADDRESS_1 AS ADDRESS_1,
        sd.ADDRESS_2 AS ADDRESS_2,
        sd.CITY AS CITY,
        sd.PROVINCE AS PROVINCE,
        CAST(sd.POSTAL_CODE AS INT) AS POSTAL_CODE,
        sd.COUNTRY AS COUNTRY,
        sd.PROFILE_TYPE AS PROFILE_TYPE,
        sd.SPECIALTY AS SPECIALTY,
        CAST(sd.STATE_LICENSE_NUMBER AS INT) AS STATE_LICENSE_NUMBER,
        sd.LICENSE_STATE AS LICENSE_STATE,
        CAST(sd.NPI_NUMBER AS INT) AS NPI_NUMBER,
        CAST(sd.TAX_ID_NUM AS INT) AS TAX_ID_NUM,
        sd.RECIPIENT_IDENTIFIER_COUNTRY AS RECIPIENT_IDENTIFIER_COUNTRY,
        sd.RECIPIENT_IDENTIFIER_TYPE AS RECIPIENT_IDENTIFIER_TYPE,
        CAST(sd.RECIPIENT_IDENTIFIER_VALUE AS INT) AS RECIPIENT_IDENTIFIER_VALUE,
        CAST(sd.TRANSACTION_CONSENT AS BOOLEAN) AS TRANSACTION_CONSENT,
        sd.CUSTOMER_SOURCESYSTEM AS CUSTOMER_SOURCESYSTEM,
        sd.COMPANY_TRANSACTIONID AS COMPANY_TRANSACTIONID,
        CAST(sd.TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,
        sd.PURPOSE AS PURPOSE,
        sd.SECONDARY_PURPOSE AS SECONDARY_PURPOSE,
        sd.FORM AS FORM,
        CAST(sd.TOTAL_AMOUNT AS FLOAT) AS TOTAL_AMOUNT,
        sd.CURRENCY AS CURRENCY,
        CAST(sd.TOTAL_NUMBER_OF_RECIPIENTS AS INT) AS TOTAL_NUMBER_OF_RECIPIENTS,
        CAST(sd.NUMBER_OF_COMPANY_REPRESENTATIVES AS INT) AS NUMBER_OF_COMPANY_REPRESENTATIVES,
        CAST(sd.NUMBER_OF_NONPROFESSIONAL_RECIPIENTS AS INT) AS NUMBER_OF_NONPROFESSIONAL_RECIPIENTS,
        CAST(sd.NUMBER_OF_NOSHOWS AS INT) AS NUMBER_OF_NOSHOWS,
        sd.COMPANY_SALES_REPID AS COMPANY_SALES_REPID,
        sd.TRANSACTION_INITIATOR_FIRSTNAME AS TRANSACTION_INITIATOR_FIRSTNAME,
        sd.TRANSACTION_INITIATOR_LASTNAME AS TRANSACTION_INITIATOR_LASTNAME,
        sd.PRODUCT AS PRODUCT,
        sd.PRODUCT_2 AS PRODUCT_2,
        CAST(sd.INDIRECT_PAYMENT AS BOOLEAN) AS INDIRECT_PAYMENT,
        sd.PAYEE_NAME AS PAYEE_NAME,
        sd.PAYEE_TYPE AS PAYEE_TYPE,
        sd.MATERIAL_NAME AS MATERIAL_NAME,
        CAST(sd.MATERIAL_QTY AS INT) AS MATERIAL_QTY,
        sd.COMPANY_EVENT_ID AS COMPANY_EVENT_ID,
        sd.ENGAGEMENT_TYPE AS ENGAGEMENT_TYPE,
        sd.ENGAGEMENT_NAME AS ENGAGEMENT_NAME,
        CAST(sd.ENGAGEMENT_START_DATE AS DATE) AS ENGAGEMENT_START_DATE,
        CAST(sd.ENGAGEMENT_END_DATE AS DATE) AS ENGAGEMENT_END_DATE,
        sd.ENGAGEMENT_DESCRIPTION AS ENGAGEMENT_DESCRIPTION,
        sd.VENUE_CITY AS VENUE_CITY,
        sd.VENUE_PROVINCE AS VENUE_PROVINCE,
        sd.VENUE_COUNTRY AS VENUE_COUNTRY,
        CAST(sd.VENUE_POSTALCODE AS INT) AS VENUE_POSTALCODE
    FROM spend_data sd
    """
)

spend_data_bronze_output_path = f"{TARGET_PATH}/spend_data_bronze.csv"
(
    spend_data_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(spend_data_bronze_output_path)
)

job.commit()