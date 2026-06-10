import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# =========================
# 1) Read source tables
# =========================
ris_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/recipient_identity_silver.{FILE_FORMAT}/")
)
tovs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transfer_of_value_silver.{FILE_FORMAT}/")
)
rcs_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/recipient_consent_silver.{FILE_FORMAT}/")
)
es_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/event_silver.{FILE_FORMAT}/")
)
ms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/material_silver.{FILE_FORMAT}/")
)
mds_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/material_distribution_silver.{FILE_FORMAT}/")
)
hcps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/hcp_profile_silver.{FILE_FORMAT}/")
)

# =========================
# 2) Create temp views
# =========================
ris_df.createOrReplaceTempView("recipient_identity_silver")
tovs_df.createOrReplaceTempView("transfer_of_value_silver")
rcs_df.createOrReplaceTempView("recipient_consent_silver")
es_df.createOrReplaceTempView("event_silver")
ms_df.createOrReplaceTempView("material_silver")
mds_df.createOrReplaceTempView("material_distribution_silver")
hcps_df.createOrReplaceTempView("hcp_profile_silver")

# ============================================================
# TARGET: gold_recipient_master
# ============================================================
gold_recipient_master_df = spark.sql(
    """
    SELECT
      sha2(
        concat_ws('||',
          COALESCE(CAST(ris.company_profile_id AS STRING), '~'),
          COALESCE(CAST(ris.customer_master_id AS STRING), '~'),
          COALESCE(CAST(ris.npi_number AS STRING), '~'),
          COALESCE(CAST(ris.tax_id_num AS STRING), '~'),
          COALESCE(CAST(ris.recipient_identifier_value AS STRING), '~')
        ),
        256
      ) AS recipient_id,
      CAST(ris.recipient_type AS STRING) AS recipient_type,
      CAST(ris.company_profile_id AS STRING) AS company_profile_id,
      CAST(ris.customer_master_id AS STRING) AS customer_master_id,
      CAST(ris.npi_number AS STRING) AS npi_number,
      CAST(ris.tax_id_num AS STRING) AS tax_id_num,
      CAST(ris.recipient_identifier_value AS STRING) AS recipient_identifier_value,
      CAST(ris.recipient_name AS STRING) AS recipient_name,
      CAST(ris.country_code AS STRING) AS country_code,
      CAST(ris.state_province_code AS STRING) AS state_province_code,
      CAST(ris.city AS STRING) AS city,
      CAST(ris.postal_code AS STRING) AS postal_code,
      CAST(ris.active_flag AS BOOLEAN) AS active_flag,
      concat_ws(',', collect_set(CAST(ris.recipient_identifier_value AS STRING))) AS mastered_from_recipient_ids,
      CAST(
        CASE
          WHEN ris.npi_number IS NOT NULL THEN 0.95
          WHEN ris.tax_id_num IS NOT NULL THEN 0.90
          WHEN ris.recipient_identifier_value IS NOT NULL THEN 0.80
          ELSE 0.60
        END AS FLOAT
      ) AS mastering_confidence_score,
      CAST(
        CASE
          WHEN ris.npi_number IS NOT NULL THEN 'IDQ_NPI_V1'
          WHEN ris.tax_id_num IS NOT NULL THEN 'IDQ_TAX_V1'
          WHEN ris.recipient_identifier_value IS NOT NULL THEN 'IDQ_RECIPIENT_IDENTIFIER_V1'
          ELSE 'IDQ_NAME_ADDRESS_V1'
        END AS STRING
      ) AS mastering_rule_version,
      MIN(CAST(ris.effective_start_date AS DATE)) AS effective_start_date,
      MAX(CAST(ris.effective_end_date AS DATE)) AS effective_end_date
    FROM recipient_identity_silver ris
    GROUP BY
      ris.recipient_type,
      ris.company_profile_id,
      ris.customer_master_id,
      ris.npi_number,
      ris.tax_id_num,
      ris.recipient_identifier_value,
      ris.recipient_name,
      ris.country_code,
      ris.state_province_code,
      ris.city,
      ris.postal_code,
      ris.active_flag
    """
)
gold_recipient_master_df.createOrReplaceTempView("gold_recipient_master")

(
    gold_recipient_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_recipient_master.csv")
)

# ============================================================
# TARGET: gold_transfer_of_value_fact
# ============================================================
# NOTE: Added payer_country_code and payer_entity_country_code to support downstream
# gold_cross_border_payment without changing business logic.
gold_transfer_of_value_fact_df = spark.sql(
    """
    SELECT
      CAST(tovs.tov_id AS STRING) AS tov_id,
      CAST(tovs.source_system AS STRING) AS source_system,
      CAST(tovs.source_transaction_id AS STRING) AS source_transaction_id,
      CAST(tovs.transaction_date AS DATE) AS transaction_date,
      CAST(tovs.company_profile_id AS STRING) AS company_profile_id,
      CAST(grm.recipient_id AS STRING) AS recipient_id,
      CAST(tovs.payer_country_code AS STRING) AS payer_country_code,
      CAST(tovs.payer_entity_country_code AS STRING) AS payer_entity_country_code,
      CAST(tovs.recipient_country_code AS STRING) AS recipient_country_code,
      CAST(tovs.recipient_state_province_code AS STRING) AS recipient_state_province_code,
      CAST(tovs.recipient_type AS STRING) AS recipient_type,
      CAST(tovs.tov_category AS STRING) AS tov_category,
      CAST(tovs.tov_subcategory AS STRING) AS tov_subcategory,
      CAST(tovs.description AS STRING) AS description,
      CAST(tovs.currency_code AS STRING) AS currency_code,
      CAST(tovs.amount_local AS FLOAT) AS amount_local,
      CAST(tovs.quantity AS INT) AS quantity,
      CAST(tovs.event_id AS STRING) AS event_id,
      CAST(tovs.material_id AS STRING) AS material_id,
      CAST(
        CASE
          WHEN COALESCE(tovs.payer_country_code, tovs.payer_entity_country_code) IS NULL
            OR tovs.recipient_country_code IS NULL THEN NULL
          WHEN COALESCE(tovs.payer_country_code, tovs.payer_entity_country_code) <> tovs.recipient_country_code THEN TRUE
          ELSE FALSE
        END AS BOOLEAN
      ) AS cross_border_flag,
      CAST(tovs.created_ts AS TIMESTAMP) AS created_ts,
      CAST(tovs.updated_ts AS TIMESTAMP) AS updated_ts
    FROM transfer_of_value_silver tovs
    LEFT JOIN gold_recipient_master grm
      ON COALESCE(tovs.company_profile_id,'~') = COALESCE(grm.company_profile_id,'~')
     AND COALESCE(tovs.customer_master_id,'~') = COALESCE(grm.customer_master_id,'~')
     AND COALESCE(tovs.npi_number,'~') = COALESCE(grm.npi_number,'~')
     AND COALESCE(tovs.tax_id_num,'~') = COALESCE(grm.tax_id_num,'~')
     AND COALESCE(tovs.recipient_identifier_value,'~') = COALESCE(grm.recipient_identifier_value,'~')
    """
)
gold_transfer_of_value_fact_df.createOrReplaceTempView("gold_transfer_of_value_fact")

(
    gold_transfer_of_value_fact_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_transfer_of_value_fact.csv")
)

# ============================================================
# TARGET: gold_recipient_consent
# ============================================================
gold_recipient_consent_df = spark.sql(
    """
    SELECT
      CAST(rcs.consent_id AS STRING) AS consent_id,
      CAST(grm.recipient_id AS STRING) AS recipient_id,
      CAST(rcs.consent_type AS STRING) AS consent_type,
      CAST(rcs.consent_scope AS STRING) AS consent_scope,
      CAST(rcs.country_code AS STRING) AS country_code,
      CAST(rcs.effective_start_date AS DATE) AS effective_start_date,
      CAST(rcs.effective_end_date AS DATE) AS effective_end_date,
      CAST(rcs.consent_status AS STRING) AS consent_status,
      CAST(rcs.consent_captured_date AS DATE) AS consent_captured_date,
      CAST(rcs.consent_source AS STRING) AS consent_source,
      CAST(rcs.consent_document_reference AS STRING) AS consent_document_reference,
      CAST(rcs.last_validated_ts AS TIMESTAMP) AS last_validated_ts
    FROM recipient_consent_silver rcs
    LEFT JOIN gold_recipient_master grm
      ON COALESCE(rcs.company_profile_id,'~') = COALESCE(grm.company_profile_id,'~')
     AND COALESCE(rcs.customer_master_id,'~') = COALESCE(grm.customer_master_id,'~')
     AND COALESCE(rcs.npi_number,'~') = COALESCE(grm.npi_number,'~')
     AND COALESCE(rcs.tax_id_num,'~') = COALESCE(grm.tax_id_num,'~')
     AND COALESCE(rcs.recipient_identifier_value,'~') = COALESCE(grm.recipient_identifier_value,'~')
    """
)

(
    gold_recipient_consent_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_recipient_consent.csv")
)

# ============================================================
# TARGET: gold_event_master
# ============================================================
gold_event_master_df = spark.sql(
    """
    SELECT
      CAST(es.event_id AS STRING) AS event_id,
      CAST(es.event_name AS STRING) AS event_name,
      CAST(es.event_type AS STRING) AS event_type,
      CAST(es.event_start_date AS DATE) AS event_start_date,
      CAST(es.event_end_date AS DATE) AS event_end_date,
      CAST(es.event_country_code AS STRING) AS event_country_code,
      CAST(es.event_state_province_code AS STRING) AS event_state_province_code,
      CAST(es.event_city AS STRING) AS event_city,
      CAST(es.organizing_company_profile_id AS STRING) AS organizing_company_profile_id,
      CAST(es.event_status AS STRING) AS event_status,
      CAST(es.created_ts AS TIMESTAMP) AS created_ts,
      CAST(es.updated_ts AS TIMESTAMP) AS updated_ts
    FROM event_silver es
    """
)
gold_event_master_df.createOrReplaceTempView("gold_event_master")

(
    gold_event_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_event_master.csv")
)

# ============================================================
# TARGET: gold_event_spend_allocation
# ============================================================
gold_event_spend_allocation_df = spark.sql(
    """
    SELECT
      sha2(
        concat_ws('||',
          CAST(es.event_id AS STRING),
          CAST(tovs.tov_id AS STRING),
          CAST(gtvf.recipient_id AS STRING)
        ),
        256
      ) AS allocation_id,
      CAST(es.event_id AS STRING) AS event_id,
      CAST(tovs.tov_id AS STRING) AS tov_id,
      CAST(gtvf.recipient_id AS STRING) AS recipient_id,
      CAST(tovs.transaction_date AS DATE) AS allocation_date
    FROM transfer_of_value_silver tovs
    INNER JOIN event_silver es
      ON tovs.event_id = es.event_id
    LEFT JOIN gold_transfer_of_value_fact gtvf
      ON tovs.tov_id = gtvf.tov_id
    """
)

(
    gold_event_spend_allocation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_event_spend_allocation.csv")
)

# ============================================================
# TARGET: gold_event_lifecycle_compliance
# ============================================================
gold_event_lifecycle_compliance_df = spark.sql(
    """
    SELECT
      CAST(es.event_id AS STRING) AS event_id,
      CAST(
        CASE
          WHEN es.event_status IS NOT NULL THEN es.event_status
          WHEN es.event_start_date IS NOT NULL AND es.event_end_date IS NOT NULL THEN 'COMPLETED'
          WHEN es.event_start_date IS NOT NULL THEN 'ACTIVE'
          ELSE 'PLANNED'
        END AS STRING
      ) AS lifecycle_stage,
      CAST(es.event_start_date AS DATE) AS stage_start_date,
      CAST(es.event_end_date AS DATE) AS stage_end_date
    FROM event_silver es
    """
)

(
    gold_event_lifecycle_compliance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_event_lifecycle_compliance.csv")
)

# ============================================================
# TARGET: gold_material_master
# ============================================================
gold_material_master_df = spark.sql(
    """
    SELECT
      CAST(ms.material_id AS STRING) AS material_id,
      CAST(ms.material_name AS STRING) AS material_name,
      CAST(ms.country_code AS STRING) AS country_code,
      CAST(ms.effective_start_date AS DATE) AS effective_start_date,
      CAST(ms.effective_end_date AS DATE) AS effective_end_date
    FROM material_silver ms
    """
)
gold_material_master_df.createOrReplaceTempView("gold_material_master")

(
    gold_material_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_material_master.csv")
)

# ============================================================
# TARGET: gold_material_distribution_fact
# ============================================================
gold_material_distribution_fact_df = spark.sql(
    """
    SELECT
      CAST(mds.distribution_id AS STRING) AS distribution_id,
      CAST(mds.tov_id AS STRING) AS tov_id,
      CAST(mds.material_id AS STRING) AS material_id,
      CAST(gtvf.recipient_id AS STRING) AS recipient_id,
      CAST(mds.distribution_date AS DATE) AS distribution_date,
      CAST(mds.country_code AS STRING) AS country_code,
      CAST(mds.quantity AS INT) AS quantity
    FROM material_distribution_silver mds
    INNER JOIN gold_transfer_of_value_fact gtvf
      ON mds.tov_id = gtvf.tov_id
    INNER JOIN gold_material_master gmm
      ON mds.material_id = gmm.material_id
    """
)

(
    gold_material_distribution_fact_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_material_distribution_fact.csv")
)

# ============================================================
# TARGET: gold_hcp_profile
# ============================================================
gold_hcp_profile_df = spark.sql(
    """
    SELECT
      CAST(grm.recipient_id AS STRING) AS recipient_id,
      CAST(hcps.npi_number AS STRING) AS npi_number,
      CAST(hcps.hcp_specialty_description AS STRING) AS hcp_specialty_description,
      CAST(hcps.license_country_code AS STRING) AS license_country_code,
      CAST(hcps.license_state_province_code AS STRING) AS license_state_province_code
    FROM hcp_profile_silver hcps
    LEFT JOIN gold_recipient_master grm
      ON COALESCE(hcps.company_profile_id,'~') = COALESCE(grm.company_profile_id,'~')
     AND COALESCE(hcps.customer_master_id,'~') = COALESCE(grm.customer_master_id,'~')
     AND COALESCE(hcps.npi_number,'~') = COALESCE(grm.npi_number,'~')
     AND COALESCE(hcps.tax_id_num,'~') = COALESCE(grm.tax_id_num,'~')
     AND COALESCE(hcps.recipient_identifier_value,'~') = COALESCE(grm.recipient_identifier_value,'~')
    """
)

(
    gold_hcp_profile_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_hcp_profile.csv")
)

# ============================================================
# TARGET: gold_cross_border_payment
# ============================================================
gold_cross_border_payment_df = spark.sql(
    """
    SELECT
      sha2(concat_ws('||', CAST(gtvf.tov_id AS STRING)), 256) AS cross_border_id,
      CAST(gtvf.tov_id AS STRING) AS tov_id,
      CAST(gtvf.payer_country_code AS STRING) AS payer_country_code,
      CAST(gtvf.recipient_country_code AS STRING) AS recipient_country_code,
      CAST(
        CASE
          WHEN gtvf.payer_country_code IS NOT NULL
           AND gtvf.recipient_country_code IS NOT NULL
           AND gtvf.payer_country_code <> gtvf.recipient_country_code THEN 'PAYER_COUNTRY'
          WHEN gtvf.payer_entity_country_code IS NOT NULL
           AND gtvf.recipient_country_code IS NOT NULL
           AND gtvf.payer_entity_country_code <> gtvf.recipient_country_code THEN 'PAYER_ENTITY_COUNTRY'
          ELSE 'DOMESTIC_OR_UNKNOWN'
        END AS STRING
      ) AS cross_border_type,
      CAST(gtvf.cross_border_flag AS BOOLEAN) AS cross_border_flag
    FROM gold_transfer_of_value_fact gtvf
    """
)

(
    gold_cross_border_payment_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cross_border_payment.csv")
)

job.commit()
