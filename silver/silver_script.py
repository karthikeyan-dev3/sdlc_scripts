import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -----------------------------
# Read sources (S3)
# -----------------------------
sdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/spend_data_bronze.{FILE_FORMAT}/")
)
sdb_df.createOrReplaceTempView("spend_data_bronze")

# =============================================================================
# Table: recipient_profile_silver
# =============================================================================
recipient_profile_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            sdb.CUSTOMER_MASTERID AS customer_master_id,
            sdb.COMPANY_PROFILEID AS company_profile_id,
            CAST(sdb.NPI_NUMBER AS STRING) AS npi_number,
            CAST(sdb.TAX_ID_NUM AS STRING) AS tax_id_num,
            sdb.RECIPIENT_IDENTIFIER_TYPE AS recipient_identifier_type,
            CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS STRING) AS recipient_identifier_value,
            COALESCE(sdb.RECIPIENT_CATEGORY, sdb.PROFILE_TYPE, sdb.PAYEE_TYPE) AS recipient_type,
            COALESCE(sdb.COUNTRY, sdb.RECIPIENT_IDENTIFIER_COUNTRY) AS country_code,
            COALESCE(sdb.PROVINCE, sdb.LICENSE_STATE) AS state_province_code,
            sdb.CITY AS city,
            CAST(sdb.POSTAL_CODE AS STRING) AS postal_code,
            COALESCE(
                sdb.ORGANIZATION_NAME,
                TRIM(CONCAT(COALESCE(sdb.FIRST_NAME,''), ' ', COALESCE(sdb.MIDDLE_NAME,''), ' ', COALESCE(sdb.LAST_NAME,'')))
            ) AS recipient_name,
            ROW_NUMBER() OVER (
                PARTITION BY
                    sdb.COMPANY_PROFILEID,
                    sdb.CUSTOMER_MASTERID,
                    sdb.NPI_NUMBER,
                    sdb.TAX_ID_NUM,
                    sdb.RECIPIENT_IDENTIFIER_TYPE,
                    sdb.RECIPIENT_IDENTIFIER_VALUE,
                    sdb.ORGANIZATION_NAME,
                    sdb.FIRST_NAME,
                    sdb.MIDDLE_NAME,
                    sdb.LAST_NAME,
                    sdb.ADDRESS_1,
                    sdb.CITY,
                    sdb.PROVINCE,
                    sdb.POSTAL_CODE,
                    sdb.COUNTRY
                ORDER BY
                    sdb.TRANSACTION_DATE DESC
            ) AS rn
        FROM spend_data_bronze sdb
    )
    SELECT
        customer_master_id,
        company_profile_id,
        npi_number,
        tax_id_num,
        recipient_identifier_type,
        recipient_identifier_value,
        recipient_type,
        country_code,
        state_province_code,
        city,
        postal_code,
        recipient_name
    FROM base
    WHERE rn = 1
    """
)
recipient_profile_silver_df.createOrReplaceTempView("recipient_profile_silver")

(
    recipient_profile_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/recipient_profile_silver.csv")
)

# =============================================================================
# Table: transfer_of_value_silver
# =============================================================================
transfer_of_value_silver_df = spark.sql(
    """
    SELECT
        CAST(hash(CONCAT(sdb.CUSTOMER_SOURCESYSTEM,'|',sdb.COMPANY_TRANSACTIONID)) AS STRING) AS tov_id,
        sdb.CUSTOMER_SOURCESYSTEM AS source_system,
        sdb.COMPANY_TRANSACTIONID AS source_transaction_id,
        sdb.TRANSACTION_DATE AS transaction_date,
        sdb.TRANSACTION_DATE AS posting_date,
        sdb.TRANSACTION_DATE AS payment_date,
        sdb.COMPANY_PROFILEID AS company_profile_id,
        rps.country_code AS recipient_country_code,
        rps.state_province_code AS recipient_state_province_code,
        rps.recipient_type AS recipient_type,
        COALESCE(sdb.PURPOSE, sdb.SECONDARY_PURPOSE, sdb.FORM) AS description,
        sdb.CURRENCY AS currency_code,
        sdb.TOTAL_AMOUNT AS amount_local,
        CAST(COALESCE(sdb.TOTAL_NUMBER_OF_RECIPIENTS, 1) AS INT) AS quantity
    FROM spend_data_bronze sdb
    LEFT JOIN recipient_profile_silver rps
        ON (
            (sdb.COMPANY_PROFILEID IS NOT NULL AND sdb.COMPANY_PROFILEID = rps.company_profile_id)
            OR (sdb.CUSTOMER_MASTERID IS NOT NULL AND sdb.CUSTOMER_MASTERID = rps.customer_master_id)
            OR (sdb.NPI_NUMBER IS NOT NULL AND CAST(sdb.NPI_NUMBER AS STRING) = rps.npi_number)
            OR (sdb.TAX_ID_NUM IS NOT NULL AND CAST(sdb.TAX_ID_NUM AS STRING) = rps.tax_id_num)
            OR (
                sdb.RECIPIENT_IDENTIFIER_TYPE IS NOT NULL
                AND sdb.RECIPIENT_IDENTIFIER_VALUE IS NOT NULL
                AND sdb.RECIPIENT_IDENTIFIER_TYPE = rps.recipient_identifier_type
                AND CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS STRING) = rps.recipient_identifier_value
            )
        )
    """
)
transfer_of_value_silver_df.createOrReplaceTempView("transfer_of_value_silver")

(
    transfer_of_value_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/transfer_of_value_silver.csv")
)

# =============================================================================
# Table: recipient_consent_silver
# =============================================================================
recipient_consent_silver_df = spark.sql(
    """
    WITH consent_events AS (
        SELECT
            CAST(
                hash(
                    CONCAT(
                        COALESCE(rps.company_profile_id,''),'|',
                        COALESCE(rps.customer_master_id,''),'|',
                        COALESCE(rps.npi_number,''),'|',
                        COALESCE(rps.tax_id_num,''),'|',
                        COALESCE(rps.recipient_identifier_type,''),'|',
                        COALESCE(rps.recipient_identifier_value,''),'|',
                        'DISCLOSURE','|',
                        'TRANSFER_OF_VALUE','|',
                        COALESCE(rps.country_code,''),'|',
                        CAST(sdb.TRANSACTION_DATE AS STRING)
                    )
                ) AS STRING
            ) AS consent_id,
            rps.country_code AS country_code,
            sdb.TRANSACTION_DATE AS consent_captured_date,
            sdb.CUSTOMER_SOURCESYSTEM AS consent_source,
            CASE
                WHEN sdb.TRANSACTION_CONSENT = true THEN 'GRANTED'
                WHEN sdb.TRANSACTION_CONSENT = false THEN 'DENIED'
                ELSE 'UNKNOWN'
            END AS consent_status,
            ROW_NUMBER() OVER (
                PARTITION BY
                    COALESCE(rps.company_profile_id,''),
                    COALESCE(rps.customer_master_id,''),
                    COALESCE(rps.npi_number,''),
                    COALESCE(rps.tax_id_num,''),
                    COALESCE(rps.recipient_identifier_type,''),
                    COALESCE(rps.recipient_identifier_value,''),
                    COALESCE(rps.country_code,'')
                ORDER BY sdb.TRANSACTION_DATE DESC
            ) AS rn
        FROM spend_data_bronze sdb
        INNER JOIN recipient_profile_silver rps
            ON (
                (sdb.COMPANY_PROFILEID IS NOT NULL AND sdb.COMPANY_PROFILEID = rps.company_profile_id)
                OR (sdb.CUSTOMER_MASTERID IS NOT NULL AND sdb.CUSTOMER_MASTERID = rps.customer_master_id)
                OR (sdb.NPI_NUMBER IS NOT NULL AND CAST(sdb.NPI_NUMBER AS STRING) = rps.npi_number)
                OR (sdb.TAX_ID_NUM IS NOT NULL AND CAST(sdb.TAX_ID_NUM AS STRING) = rps.tax_id_num)
                OR (
                    sdb.RECIPIENT_IDENTIFIER_TYPE IS NOT NULL
                    AND sdb.RECIPIENT_IDENTIFIER_VALUE IS NOT NULL
                    AND sdb.RECIPIENT_IDENTIFIER_TYPE = rps.recipient_identifier_type
                    AND CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS STRING) = rps.recipient_identifier_value
                )
            )
    )
    SELECT
        consent_id,
        country_code,
        consent_captured_date,
        consent_source,
        consent_status
    FROM consent_events
    WHERE rn = 1
    """
)
recipient_consent_silver_df.createOrReplaceTempView("recipient_consent_silver")

(
    recipient_consent_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/recipient_consent_silver.csv")
)

# =============================================================================
# Table: event_silver
# =============================================================================
event_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(
                CASE
                    WHEN sdb.COMPANY_EVENT_ID IS NOT NULL
                        THEN hash(CONCAT(sdb.CUSTOMER_SOURCESYSTEM,'|',sdb.COMPANY_EVENT_ID))
                    ELSE hash(CONCAT(sdb.CUSTOMER_SOURCESYSTEM,'|',sdb.ENGAGEMENT_NAME,'|',CAST(sdb.ENGAGEMENT_START_DATE AS STRING)))
                END AS STRING
            ) AS event_id,
            COALESCE(sdb.ENGAGEMENT_NAME, sdb.COMPANY_EVENT_ID) AS event_name,
            sdb.ENGAGEMENT_TYPE AS event_type,
            sdb.ENGAGEMENT_START_DATE AS event_start_date,
            sdb.ENGAGEMENT_END_DATE AS event_end_date,
            sdb.VENUE_COUNTRY AS event_country_code,
            sdb.VENUE_PROVINCE AS event_state_province_code,
            sdb.VENUE_CITY AS event_city,
            sdb.COMPANY_PROFILEID AS organizing_company_profile_id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CASE
                        WHEN sdb.COMPANY_EVENT_ID IS NOT NULL
                            THEN hash(CONCAT(sdb.CUSTOMER_SOURCESYSTEM,'|',sdb.COMPANY_EVENT_ID))
                        ELSE hash(CONCAT(sdb.CUSTOMER_SOURCESYSTEM,'|',sdb.ENGAGEMENT_NAME,'|',CAST(sdb.ENGAGEMENT_START_DATE AS STRING)))
                    END
                ORDER BY sdb.TRANSACTION_DATE DESC
            ) AS rn
        FROM spend_data_bronze sdb
    )
    SELECT
        event_id,
        event_name,
        event_type,
        event_start_date,
        event_end_date,
        event_country_code,
        event_state_province_code,
        event_city,
        organizing_company_profile_id
    FROM base
    WHERE rn = 1
    """
)
event_silver_df.createOrReplaceTempView("event_silver")

(
    event_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/event_silver.csv")
)

# =============================================================================
# Table: event_spend_allocation_silver
# =============================================================================
event_spend_allocation_silver_df = spark.sql(
    """
    SELECT
        CAST(hash(CONCAT(es.event_id,'|',tovs.tov_id)) AS STRING) AS allocation_id,
        es.event_id AS event_id,
        tovs.tov_id AS tov_id,
        CASE
            WHEN COALESCE(sdb.TOTAL_NUMBER_OF_RECIPIENTS,0) > 0 THEN 'EQUAL_SPLIT_BY_RECIPIENT_COUNT'
            ELSE 'FULL_TO_PRIMARY_RECIPIENT'
        END AS allocation_method,
        CAST(COALESCE(sdb.TOTAL_NUMBER_OF_RECIPIENTS, 1) AS INT) AS allocation_basis_value,
        CASE
            WHEN COALESCE(sdb.TOTAL_NUMBER_OF_RECIPIENTS,0) > 0 THEN (sdb.TOTAL_AMOUNT / sdb.TOTAL_NUMBER_OF_RECIPIENTS)
            ELSE sdb.TOTAL_AMOUNT
        END AS allocated_amount_usd,
        sdb.TRANSACTION_DATE AS allocation_date
    FROM transfer_of_value_silver tovs
    INNER JOIN spend_data_bronze sdb
        ON (tovs.source_system = sdb.CUSTOMER_SOURCESYSTEM AND tovs.source_transaction_id = sdb.COMPANY_TRANSACTIONID)
    INNER JOIN event_silver es
        ON (
            (sdb.COMPANY_EVENT_ID IS NOT NULL AND es.event_name = sdb.COMPANY_EVENT_ID)
            OR (sdb.COMPANY_EVENT_ID IS NULL AND sdb.ENGAGEMENT_NAME IS NOT NULL AND es.event_name = sdb.ENGAGEMENT_NAME AND es.event_start_date = sdb.ENGAGEMENT_START_DATE)
        )
    """
)
event_spend_allocation_silver_df.createOrReplaceTempView("event_spend_allocation_silver")

(
    event_spend_allocation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/event_spend_allocation_silver.csv")
)

# =============================================================================
# Table: material_master_silver
# =============================================================================
material_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(
                hash(CONCAT(UPPER(TRIM(COALESCE(sdb.MATERIAL_NAME,sdb.PRODUCT,sdb.PRODUCT_2))),'|',sdb.COUNTRY)) AS STRING
            ) AS material_id,
            COALESCE(sdb.MATERIAL_NAME, sdb.PRODUCT, sdb.PRODUCT_2) AS material_name,
            CASE WHEN sdb.MATERIAL_NAME IS NOT NULL THEN 'IN_KIND' ELSE 'PRODUCT' END AS material_type,
            sdb.COUNTRY AS country_code,
            ROW_NUMBER() OVER (
                PARTITION BY hash(CONCAT(UPPER(TRIM(COALESCE(sdb.MATERIAL_NAME,sdb.PRODUCT,sdb.PRODUCT_2))),'|',sdb.COUNTRY))
                ORDER BY sdb.TRANSACTION_DATE DESC
            ) AS rn
        FROM spend_data_bronze sdb
        WHERE COALESCE(sdb.MATERIAL_NAME, sdb.PRODUCT, sdb.PRODUCT_2) IS NOT NULL
    )
    SELECT
        material_id,
        material_name,
        material_type,
        country_code
    FROM base
    WHERE rn = 1
    """
)
material_master_silver_df.createOrReplaceTempView("material_master_silver")

(
    material_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/material_master_silver.csv")
)

# =============================================================================
# Table: material_distribution_silver
# =============================================================================
material_distribution_silver_df = spark.sql(
    """
    SELECT
        CAST(hash(CONCAT(tovs.tov_id,'|',mms.material_id)) AS STRING) AS distribution_id,
        tovs.tov_id AS tov_id,
        mms.material_id AS material_id,
        sdb.TRANSACTION_DATE AS distribution_date,
        sdb.COUNTRY AS country_code,
        CAST(COALESCE(sdb.MATERIAL_QTY, 1) AS INT) AS quantity,
        COALESCE(sdb.FORM, sdb.PURPOSE) AS distribution_channel
    FROM transfer_of_value_silver tovs
    INNER JOIN spend_data_bronze sdb
        ON (tovs.source_system = sdb.CUSTOMER_SOURCESYSTEM AND tovs.source_transaction_id = sdb.COMPANY_TRANSACTIONID)
    LEFT JOIN material_master_silver mms
        ON (
            UPPER(TRIM(COALESCE(sdb.MATERIAL_NAME,sdb.PRODUCT,sdb.PRODUCT_2))) = UPPER(TRIM(mms.material_name))
            AND mms.country_code = sdb.COUNTRY
        )
    """
)
material_distribution_silver_df.createOrReplaceTempView("material_distribution_silver")

(
    material_distribution_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/material_distribution_silver.csv")
)

# =============================================================================
# Table: hcp_profile_silver
# =============================================================================
hcp_profile_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            rps.npi_number AS npi_number,
            sdb.SPECIALTY AS hcp_specialty_description,
            CAST(hash(UPPER(TRIM(sdb.SPECIALTY))) AS STRING) AS hcp_specialty_code,
            COALESCE(sdb.RECIPIENT_IDENTIFIER_COUNTRY, sdb.COUNTRY) AS license_country_code,
            COALESCE(sdb.LICENSE_STATE, sdb.PROVINCE) AS license_state_province_code,
            (sdb.NPI_NUMBER IS NOT NULL AND sdb.ADDRESS_1 IS NOT NULL AND sdb.CITY IS NOT NULL AND sdb.COUNTRY IS NOT NULL) AS active_practice_flag,
            ROW_NUMBER() OVER (
                PARTITION BY rps.npi_number
                ORDER BY sdb.TRANSACTION_DATE DESC
            ) AS rn
        FROM spend_data_bronze sdb
        INNER JOIN recipient_profile_silver rps
            ON (
                (sdb.COMPANY_PROFILEID IS NOT NULL AND sdb.COMPANY_PROFILEID = rps.company_profile_id)
                OR (sdb.CUSTOMER_MASTERID IS NOT NULL AND sdb.CUSTOMER_MASTERID = rps.customer_master_id)
                OR (sdb.NPI_NUMBER IS NOT NULL AND CAST(sdb.NPI_NUMBER AS STRING) = rps.npi_number)
                OR (sdb.TAX_ID_NUM IS NOT NULL AND CAST(sdb.TAX_ID_NUM AS STRING) = rps.tax_id_num)
                OR (
                    sdb.RECIPIENT_IDENTIFIER_TYPE IS NOT NULL
                    AND sdb.RECIPIENT_IDENTIFIER_VALUE IS NOT NULL
                    AND sdb.RECIPIENT_IDENTIFIER_TYPE = rps.recipient_identifier_type
                    AND CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS STRING) = rps.recipient_identifier_value
                )
            )
        WHERE rps.npi_number IS NOT NULL
    )
    SELECT
        npi_number,
        hcp_specialty_description,
        hcp_specialty_code,
        license_country_code,
        license_state_province_code,
        active_practice_flag
    FROM base
    WHERE rn = 1
    """
)
hcp_profile_silver_df.createOrReplaceTempView("hcp_profile_silver")

(
    hcp_profile_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/hcp_profile_silver.csv")
)

# =============================================================================
# Table: cross_border_payment_silver
# =============================================================================
cross_border_payment_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(hash(tovs.tov_id) AS STRING) AS cross_border_id,
            tovs.tov_id AS tov_id,
            tovs.payer_country_code AS payer_country_code,
            tovs.recipient_country_code AS recipient_country_code,
            (
                tovs.payer_country_code IS NOT NULL
                AND tovs.recipient_country_code IS NOT NULL
                AND tovs.payer_country_code <> tovs.recipient_country_code
            ) AS cross_border_flag,
            CASE
                WHEN (
                    tovs.payer_country_code IS NOT NULL
                    AND tovs.recipient_country_code IS NOT NULL
                    AND tovs.payer_country_code <> tovs.recipient_country_code
                )
                THEN 'PAYER_TO_RECIPIENT_COUNTRY_MISMATCH'
                ELSE NULL
            END AS cross_border_type,
            ROW_NUMBER() OVER (PARTITION BY tovs.tov_id ORDER BY tovs.tov_id) AS rn
        FROM transfer_of_value_silver tovs
    )
    SELECT
        cross_border_id,
        tov_id,
        payer_country_code,
        recipient_country_code,
        cross_border_flag,
        cross_border_type
    FROM base
    WHERE rn = 1
    """
)

(
    cross_border_payment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/cross_border_payment_silver.csv")
)

job.commit()
