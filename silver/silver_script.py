import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -----------------------------
# 1) Read source tables (S3)
# -----------------------------
sdb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/spend_data_bronze.{FILE_FORMAT}/")
)
sdb_df.createOrReplaceTempView("spend_data_bronze")

# -----------------------------
# recipient_identity_silver
# -----------------------------
recipient_identity_silver_sql = """
WITH base AS (
  SELECT
    COALESCE(NULLIF(UPPER(TRIM(sdb.RECIPIENT_CATEGORY)),''), NULLIF(UPPER(TRIM(sdb.PROFILE_TYPE)),'')) AS recipient_type,
    NULLIF(TRIM(sdb.COMPANY_PROFILEID),'') AS company_profile_id,
    NULLIF(TRIM(sdb.CUSTOMER_MASTERID),'') AS customer_master_id,
    CAST(NULLIF(TRIM(CAST(sdb.NPI_NUMBER AS VARCHAR)), '') AS VARCHAR) AS npi_number,
    CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR) AS tax_id_num,
    CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR) AS recipient_identifier_value,
    COALESCE(
      NULLIF(TRIM(sdb.ORGANIZATION_NAME),''),
      NULLIF(TRIM(CONCAT_WS(' ', sdb.FIRST_NAME, sdb.MIDDLE_NAME, sdb.LAST_NAME)),''),
      NULLIF(TRIM(sdb.PAYEE_NAME),'')
    ) AS recipient_name,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS country_code,
    UPPER(NULLIF(TRIM(sdb.PROVINCE),'')) AS state_province_code,
    NULLIF(TRIM(sdb.CITY),'') AS city,
    CAST(NULLIF(TRIM(CAST(sdb.POSTAL_CODE AS VARCHAR)), '') AS VARCHAR) AS postal_code,
    TRUE AS active_flag,
    sdb.TRANSACTION_DATE AS effective_start_date,
    DATE '9999-12-31' AS effective_end_date,
    ROW_NUMBER() OVER (
      PARTITION BY
        NULLIF(TRIM(sdb.COMPANY_PROFILEID),''),
        NULLIF(TRIM(sdb.CUSTOMER_MASTERID),''),
        CAST(NULLIF(TRIM(CAST(sdb.NPI_NUMBER AS VARCHAR)), '') AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR),
        COALESCE(
          NULLIF(TRIM(sdb.ORGANIZATION_NAME),''),
          NULLIF(TRIM(CONCAT_WS(' ', sdb.FIRST_NAME, sdb.MIDDLE_NAME, sdb.LAST_NAME)),''),
          NULLIF(TRIM(sdb.PAYEE_NAME),'')
        ),
        UPPER(NULLIF(TRIM(sdb.COUNTRY),'')),
        UPPER(NULLIF(TRIM(sdb.PROVINCE),'')),
        NULLIF(TRIM(sdb.CITY),''),
        CAST(NULLIF(TRIM(CAST(sdb.POSTAL_CODE AS VARCHAR)), '') AS VARCHAR)
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM spend_data_bronze sdb
)
SELECT
  recipient_type,
  company_profile_id,
  customer_master_id,
  npi_number,
  tax_id_num,
  recipient_identifier_value,
  recipient_name,
  country_code,
  state_province_code,
  city,
  postal_code,
  active_flag,
  effective_start_date,
  effective_end_date
FROM base
WHERE rn = 1
"""
recipient_identity_silver_df = spark.sql(recipient_identity_silver_sql)
recipient_identity_silver_df.createOrReplaceTempView("recipient_identity_silver")

recipient_identity_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/recipient_identity_silver.csv"
)

# -----------------------------
# transfer_of_value_silver
# -----------------------------
transfer_of_value_silver_sql = """
WITH joined AS (
  SELECT
    sdb.*,
    ris.company_profile_id AS ris_company_profile_id,
    ris.customer_master_id AS ris_customer_master_id,
    ris.npi_number AS ris_npi_number,
    ris.tax_id_num AS ris_tax_id_num,
    ris.recipient_identifier_value AS ris_recipient_identifier_value
  FROM spend_data_bronze sdb
  LEFT JOIN recipient_identity_silver ris
    ON COALESCE(NULLIF(TRIM(sdb.COMPANY_PROFILEID),''),'~') = COALESCE(ris.company_profile_id,'~')
   AND COALESCE(NULLIF(TRIM(sdb.CUSTOMER_MASTERID),''),'~') = COALESCE(ris.customer_master_id,'~')
   AND COALESCE(CAST(sdb.NPI_NUMBER AS VARCHAR),'~') = COALESCE(ris.npi_number,'~')
   AND COALESCE(CAST(sdb.TAX_ID_NUM AS VARCHAR),'~') = COALESCE(ris.tax_id_num,'~')
   AND COALESCE(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR),'~') = COALESCE(ris.recipient_identifier_value,'~')
),
dedup AS (
  SELECT
    SHA2(CONCAT_WS('||',
      NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),''),
      NULLIF(TRIM(sdb.COMPANY_TRANSACTIONID),'')
    ), 256) AS tov_id,
    NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),'') AS source_system,
    NULLIF(TRIM(sdb.COMPANY_TRANSACTIONID),'') AS source_transaction_id,
    sdb.TRANSACTION_DATE AS transaction_date,
    CAST(NULL AS DATE) AS posting_date,
    CAST(NULL AS DATE) AS payment_date,
    NULLIF(TRIM(sdb.COMPANY_PROFILEID),'') AS company_profile_id,
    CAST(NULL AS VARCHAR) AS payer_entity_id,
    NULLIF(TRIM(sdb.CUSTOMER_MASTERID),'') AS customer_master_id,
    CAST(NULLIF(TRIM(CAST(sdb.NPI_NUMBER AS VARCHAR)), '') AS VARCHAR) AS npi_number,
    CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR) AS tax_id_num,
    CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR) AS recipient_identifier_value,
    COALESCE(
      NULLIF(TRIM(sdb.ORGANIZATION_NAME),''),
      NULLIF(TRIM(CONCAT_WS(' ', sdb.FIRST_NAME, sdb.MIDDLE_NAME, sdb.LAST_NAME)),''),
      NULLIF(TRIM(sdb.PAYEE_NAME),'')
    ) AS recipient_name,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS recipient_country_code,
    UPPER(NULLIF(TRIM(sdb.PROVINCE),'')) AS recipient_state_province_code,
    NULLIF(TRIM(sdb.CITY),'') AS recipient_city,
    CAST(NULLIF(TRIM(CAST(sdb.POSTAL_CODE AS VARCHAR)), '') AS VARCHAR) AS recipient_postal_code,
    COALESCE(NULLIF(UPPER(TRIM(sdb.RECIPIENT_CATEGORY)),''), NULLIF(UPPER(TRIM(sdb.PROFILE_TYPE)),'')) AS recipient_type,
    NULLIF(TRIM(sdb.PURPOSE),'') AS tov_category,
    NULLIF(TRIM(sdb.SECONDARY_PURPOSE),'') AS tov_subcategory,
    COALESCE(NULLIF(TRIM(sdb.FORM),''), NULLIF(TRIM(sdb.PURPOSE),'')) AS description,
    UPPER(NULLIF(TRIM(sdb.CURRENCY),'')) AS currency_code,
    sdb.TOTAL_AMOUNT AS amount_local,
    CAST(NULL AS DOUBLE) AS amount_usd,
    CAST(NULL AS DOUBLE) AS quantity,
    NULLIF(TRIM(sdb.COMPANY_EVENT_ID),'') AS event_id,
    NULLIF(TRIM(sdb.MATERIAL_NAME),'') AS material_id,
    CAST(NULL AS VARCHAR) AS contract_id,
    CAST(NULL AS VARCHAR) AS invoice_id,
    CAST(NULL AS VARCHAR) AS po_number,
    CAST(NULL AS VARCHAR) AS payment_method,
    CAST(NULL AS VARCHAR) AS cross_border_flag,
    CURRENT_TIMESTAMP() AS created_ts,
    CURRENT_TIMESTAMP() AS updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY
        NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),''),
        NULLIF(TRIM(sdb.COMPANY_TRANSACTIONID),'')
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM joined sdb
)
SELECT
  tov_id,
  source_system,
  source_transaction_id,
  transaction_date,
  posting_date,
  payment_date,
  company_profile_id,
  payer_entity_id,
  customer_master_id,
  npi_number,
  tax_id_num,
  recipient_identifier_value,
  recipient_name,
  recipient_country_code,
  recipient_state_province_code,
  recipient_city,
  recipient_postal_code,
  recipient_type,
  tov_category,
  tov_subcategory,
  description,
  currency_code,
  amount_local,
  amount_usd,
  quantity,
  event_id,
  material_id,
  contract_id,
  invoice_id,
  po_number,
  payment_method,
  cross_border_flag,
  created_ts,
  updated_ts
FROM dedup
WHERE rn = 1
"""
transfer_of_value_silver_df = spark.sql(transfer_of_value_silver_sql)
transfer_of_value_silver_df.createOrReplaceTempView("transfer_of_value_silver")

transfer_of_value_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/transfer_of_value_silver.csv"
)

# -----------------------------
# recipient_consent_silver
# -----------------------------
recipient_consent_silver_sql = """
WITH base AS (
  SELECT
    SHA2(CONCAT_WS('||',
      NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),''),
      NULLIF(TRIM(sdb.COMPANY_TRANSACTIONID),''),
      'TRANSACTION_CONSENT'
    ), 256) AS consent_id,
    NULLIF(TRIM(sdb.COMPANY_PROFILEID),'') AS company_profile_id,
    NULLIF(TRIM(sdb.CUSTOMER_MASTERID),'') AS customer_master_id,
    CAST(NULLIF(TRIM(CAST(sdb.NPI_NUMBER AS VARCHAR)), '') AS VARCHAR) AS npi_number,
    CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR) AS tax_id_num,
    CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR) AS recipient_identifier_value,
    'DISCLOSURE_CONSENT' AS consent_type,
    'TRANSACTION' AS consent_scope,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS country_code,
    sdb.TRANSACTION_DATE AS effective_start_date,
    DATE '9999-12-31' AS effective_end_date,
    CASE
      WHEN sdb.TRANSACTION_CONSENT = TRUE THEN 'CONSENTED'
      WHEN sdb.TRANSACTION_CONSENT = FALSE THEN 'DECLINED'
      ELSE 'UNKNOWN'
    END AS consent_status,
    sdb.TRANSACTION_DATE AS consent_captured_date,
    NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),'') AS consent_source,
    CAST(NULL AS VARCHAR) AS consent_document_reference,
    CURRENT_TIMESTAMP() AS last_validated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY
        NULLIF(TRIM(sdb.COMPANY_PROFILEID),''),
        NULLIF(TRIM(sdb.CUSTOMER_MASTERID),''),
        CAST(NULLIF(TRIM(CAST(sdb.NPI_NUMBER AS VARCHAR)), '') AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR),
        UPPER(NULLIF(TRIM(sdb.COUNTRY),'')),
        sdb.TRANSACTION_DATE
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM spend_data_bronze sdb
)
SELECT
  consent_id,
  company_profile_id,
  customer_master_id,
  npi_number,
  tax_id_num,
  recipient_identifier_value,
  consent_type,
  consent_scope,
  country_code,
  effective_start_date,
  effective_end_date,
  consent_status,
  consent_captured_date,
  consent_source,
  consent_document_reference,
  last_validated_ts
FROM base
WHERE rn = 1
"""
recipient_consent_silver_df = spark.sql(recipient_consent_silver_sql)
recipient_consent_silver_df.createOrReplaceTempView("recipient_consent_silver")

recipient_consent_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/recipient_consent_silver.csv"
)

# -----------------------------
# event_silver
# -----------------------------
event_silver_sql = """
WITH base AS (
  SELECT
    SHA2(CONCAT_WS('||',
      NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),''),
      NULLIF(TRIM(sdb.COMPANY_EVENT_ID),'')
    ), 256) AS event_id,
    NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),'') AS source_system,
    NULLIF(TRIM(sdb.COMPANY_EVENT_ID),'') AS company_event_id,
    NULLIF(TRIM(sdb.ENGAGEMENT_NAME),'') AS event_name,
    NULLIF(TRIM(sdb.ENGAGEMENT_TYPE),'') AS event_type,
    sdb.ENGAGEMENT_START_DATE AS event_start_date,
    sdb.ENGAGEMENT_END_DATE AS event_end_date,
    UPPER(NULLIF(TRIM(sdb.VENUE_COUNTRY),'')) AS event_country_code,
    UPPER(NULLIF(TRIM(sdb.VENUE_PROVINCE),'')) AS event_state_province_code,
    NULLIF(TRIM(sdb.VENUE_CITY),'') AS event_city,
    NULLIF(TRIM(sdb.COMPANY_PROFILEID),'') AS organizing_company_profile_id,
    CAST(NULL AS VARCHAR) AS event_owner_org,
    CASE
      WHEN sdb.ENGAGEMENT_END_DATE IS NOT NULL AND sdb.ENGAGEMENT_END_DATE < CURRENT_DATE THEN 'COMPLETED'
      ELSE 'PLANNED_OR_ACTIVE'
    END AS event_status,
    CURRENT_TIMESTAMP() AS created_ts,
    CURRENT_TIMESTAMP() AS updated_ts,
    ROW_NUMBER() OVER (
      PARTITION BY
        NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),''),
        NULLIF(TRIM(sdb.COMPANY_EVENT_ID),'')
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM spend_data_bronze sdb
)
SELECT
  event_id,
  company_event_id,
  source_system,
  event_name,
  event_type,
  event_start_date,
  event_end_date,
  event_country_code,
  event_state_province_code,
  event_city,
  organizing_company_profile_id,
  event_owner_org,
  event_status,
  created_ts,
  updated_ts
FROM base
WHERE rn = 1
"""
event_silver_df = spark.sql(event_silver_sql)
event_silver_df.createOrReplaceTempView("event_silver")

event_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/event_silver.csv"
)

# -----------------------------
# material_silver
# -----------------------------
material_silver_sql = """
WITH base AS (
  SELECT
    SHA2(CONCAT_WS('||', UPPER(TRIM(sdb.MATERIAL_NAME))), 256) AS material_id,
    NULLIF(TRIM(sdb.MATERIAL_NAME),'') AS material_name,
    CAST(NULL AS VARCHAR) AS material_type,
    CAST(NULL AS BOOLEAN) AS regulated_flag,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS country_code,
    CAST(NULL AS DOUBLE) AS unit_value_usd,
    MIN(sdb.TRANSACTION_DATE) OVER (PARTITION BY NULLIF(TRIM(sdb.MATERIAL_NAME),'')) AS effective_start_date,
    DATE '9999-12-31' AS effective_end_date
  FROM spend_data_bronze sdb
)
SELECT DISTINCT
  material_id,
  material_name,
  material_type,
  regulated_flag,
  country_code,
  unit_value_usd,
  effective_start_date,
  effective_end_date
FROM base
WHERE material_name IS NOT NULL
"""
material_silver_df = spark.sql(material_silver_sql)
material_silver_df.createOrReplaceTempView("material_silver")

material_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/material_silver.csv"
)

# -----------------------------
# material_distribution_silver
# -----------------------------
material_distribution_silver_sql = """
WITH base AS (
  SELECT
    SHA2(CONCAT_WS('||', tovs.tov_id, ms.material_id), 256) AS distribution_id,
    tovs.tov_id AS tov_id,
    ms.material_id AS material_id,
    sdb.TRANSACTION_DATE AS distribution_date,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS country_code,
    sdb.MATERIAL_QTY AS quantity,
    CAST(NULL AS DOUBLE) AS unit_value_usd,
    CAST(NULL AS DOUBLE) AS total_value_usd,
    CAST(NULL AS VARCHAR) AS distribution_channel,
    CAST(NULL AS VARCHAR) AS compliance_flag,
    CAST(NULL AS VARCHAR) AS noncompliance_reason_code,
    ROW_NUMBER() OVER (
      PARTITION BY tovs.tov_id, ms.material_id
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM spend_data_bronze sdb
  INNER JOIN transfer_of_value_silver tovs
    ON NULLIF(TRIM(sdb.CUSTOMER_SOURCESYSTEM),'') = tovs.source_system
   AND NULLIF(TRIM(sdb.COMPANY_TRANSACTIONID),'') = tovs.source_transaction_id
  LEFT JOIN material_silver ms
    ON UPPER(TRIM(sdb.MATERIAL_NAME)) = UPPER(TRIM(ms.material_name))
  WHERE (sdb.MATERIAL_NAME IS NOT NULL OR sdb.MATERIAL_QTY IS NOT NULL)
)
SELECT
  distribution_id,
  tov_id,
  material_id,
  distribution_date,
  country_code,
  quantity,
  unit_value_usd,
  total_value_usd,
  distribution_channel,
  compliance_flag,
  noncompliance_reason_code
FROM base
WHERE rn = 1
"""
material_distribution_silver_df = spark.sql(material_distribution_silver_sql)
material_distribution_silver_df.createOrReplaceTempView("material_distribution_silver")

material_distribution_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/material_distribution_silver.csv"
)

# -----------------------------
# hcp_profile_silver
# -----------------------------
hcp_profile_silver_sql = """
WITH base AS (
  SELECT
    NULLIF(TRIM(sdb.COMPANY_PROFILEID),'') AS company_profile_id,
    NULLIF(TRIM(sdb.CUSTOMER_MASTERID),'') AS customer_master_id,
    CAST(sdb.NPI_NUMBER AS VARCHAR) AS npi_number,
    CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR) AS tax_id_num,
    CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR) AS recipient_identifier_value,
    CAST(NULL AS VARCHAR) AS hcp_specialty_code,
    NULLIF(TRIM(sdb.SPECIALTY),'') AS hcp_specialty_description,
    UPPER(NULLIF(TRIM(sdb.COUNTRY),'')) AS license_country_code,
    UPPER(NULLIF(TRIM(sdb.LICENSE_STATE),'')) AS license_state_province_code,
    CAST(NULL AS BOOLEAN) AS active_practice_flag,
    CAST(NULL AS VARCHAR) AS primary_affiliated_hco_id,
    ROW_NUMBER() OVER (
      PARTITION BY
        NULLIF(TRIM(sdb.COMPANY_PROFILEID),''),
        NULLIF(TRIM(sdb.CUSTOMER_MASTERID),''),
        CAST(sdb.NPI_NUMBER AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.TAX_ID_NUM AS VARCHAR)), '') AS VARCHAR),
        CAST(NULLIF(TRIM(CAST(sdb.RECIPIENT_IDENTIFIER_VALUE AS VARCHAR)), '') AS VARCHAR)
      ORDER BY sdb.TRANSACTION_DATE DESC
    ) AS rn
  FROM spend_data_bronze sdb
  WHERE
    UPPER(TRIM(sdb.RECIPIENT_CATEGORY)) IN ('HCP','HEALTHCARE PROFESSIONAL')
    OR sdb.NPI_NUMBER IS NOT NULL
)
SELECT
  company_profile_id,
  customer_master_id,
  npi_number,
  tax_id_num,
  recipient_identifier_value,
  hcp_specialty_code,
  hcp_specialty_description,
  license_country_code,
  license_state_province_code,
  active_practice_flag,
  primary_affiliated_hco_id
FROM base
WHERE rn = 1
"""
hcp_profile_silver_df = spark.sql(hcp_profile_silver_sql)
hcp_profile_silver_df.createOrReplaceTempView("hcp_profile_silver")

hcp_profile_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/hcp_profile_silver.csv"
)

job.commit()
