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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# Read Source Tables (Silver)
# ------------------------------------------------------------------------------

recipient_profile_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/recipient_profile_silver.{FILE_FORMAT}/")
)
recipient_profile_silver_df.createOrReplaceTempView("recipient_profile_silver")

transfer_of_value_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/transfer_of_value_silver.{FILE_FORMAT}/")
)
transfer_of_value_silver_df.createOrReplaceTempView("transfer_of_value_silver")

recipient_consent_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/recipient_consent_silver.{FILE_FORMAT}/")
)
recipient_consent_silver_df.createOrReplaceTempView("recipient_consent_silver")

event_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/event_silver.{FILE_FORMAT}/")
)
event_silver_df.createOrReplaceTempView("event_silver")

event_spend_allocation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/event_spend_allocation_silver.{FILE_FORMAT}/")
)
event_spend_allocation_silver_df.createOrReplaceTempView("event_spend_allocation_silver")

material_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/material_master_silver.{FILE_FORMAT}/")
)
material_master_silver_df.createOrReplaceTempView("material_master_silver")

material_distribution_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/material_distribution_silver.{FILE_FORMAT}/")
)
material_distribution_silver_df.createOrReplaceTempView("material_distribution_silver")

hcp_profile_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/hcp_profile_silver.{FILE_FORMAT}/")
)
hcp_profile_silver_df.createOrReplaceTempView("hcp_profile_silver")

cross_border_payment_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/cross_border_payment_silver.{FILE_FORMAT}/")
)
cross_border_payment_silver_df.createOrReplaceTempView("cross_border_payment_silver")

# ------------------------------------------------------------------------------
# Target: gold.gold_recipient_master
# ------------------------------------------------------------------------------

gold_recipient_master_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(rps.recipient_type AS STRING) AS recipient_type,
        CAST(rps.company_profile_id AS STRING) AS company_profile_id,
        CAST(rps.customer_master_id AS STRING) AS customer_master_id,
        CAST(rps.npi_number AS STRING) AS npi_number,
        CAST(rps.tax_id_num AS STRING) AS tax_id_num,
        CAST(rps.recipient_identifier_value AS STRING) AS recipient_identifier_value,
        CAST(rps.recipient_name AS STRING) AS recipient_name,
        CAST(rps.country_code AS STRING) AS country_code,
        CAST(rps.state_province_code AS STRING) AS state_province_code,
        CAST(rps.city AS STRING) AS city,
        CAST(rps.postal_code AS STRING) AS postal_code
      FROM recipient_profile_silver rps
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY
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
            recipient_type
          ORDER BY
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
            recipient_type
        ) AS rn
      FROM base
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
      postal_code
    FROM ranked
    WHERE rn = 1
    """
)

(
    gold_recipient_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_recipient_master.csv")
)

gold_recipient_master_df.createOrReplaceTempView("gold_recipient_master")

# ------------------------------------------------------------------------------
# Target: gold.gold_transfer_of_value_fact
# ------------------------------------------------------------------------------

gold_transfer_of_value_fact_df = spark.sql(
    """
    SELECT
      CAST(tovs.tov_id AS STRING) AS tov_id,
      CAST(tovs.source_system AS STRING) AS source_system,
      CAST(tovs.source_transaction_id AS STRING) AS source_transaction_id,
      DATE(tovs.transaction_date) AS transaction_date,
      DATE(tovs.posting_date) AS posting_date,
      DATE(tovs.payment_date) AS payment_date,
      CAST(tovs.company_profile_id AS STRING) AS company_profile_id,
      CAST(tovs.recipient_country_code AS STRING) AS recipient_country_code,
      CAST(tovs.recipient_state_province_code AS STRING) AS recipient_state_province_code,
      CAST(tovs.recipient_type AS STRING) AS recipient_type,
      CAST(tovs.description AS STRING) AS description,
      CAST(tovs.currency_code AS STRING) AS currency_code,
      CAST(tovs.amount_local AS FLOAT) AS amount_local,
      CAST(tovs.quantity AS INT) AS quantity,
      CAST(tovs.cross_border_flag AS BOOLEAN) AS cross_border_flag
    FROM transfer_of_value_silver tovs
    LEFT JOIN gold_recipient_master grm
      ON (
           (tovs.company_profile_id IS NOT NULL AND tovs.company_profile_id = grm.company_profile_id)
        OR (tovs.recipient_type IS NOT NULL
            AND tovs.recipient_type = grm.recipient_type
            AND tovs.recipient_country_code = grm.country_code
            AND tovs.recipient_state_province_code = grm.state_province_code)
        OR (tovs.source_system IS NOT NULL AND tovs.source_transaction_id IS NOT NULL)
      )
    """
)

(
    gold_transfer_of_value_fact_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_transfer_of_value_fact.csv")
)

gold_transfer_of_value_fact_df.createOrReplaceTempView("gold_transfer_of_value_fact")

# ------------------------------------------------------------------------------
# Target: gold.gold_recipient_consent
# ------------------------------------------------------------------------------

gold_recipient_consent_df = spark.sql(
    """
    SELECT
      CAST(rcs.consent_id AS STRING) AS consent_id,
      CAST(rcs.country_code AS STRING) AS country_code,
      CAST(rcs.consent_status AS STRING) AS consent_status,
      DATE(rcs.consent_captured_date) AS consent_captured_date,
      CAST(rcs.consent_source AS STRING) AS consent_source
    FROM recipient_consent_silver rcs
    INNER JOIN gold_recipient_master grm
      ON (
        (rcs.country_code = grm.country_code)
        AND (grm.company_profile_id IS NOT NULL AND rcs.consent_id IS NOT NULL)
      )
    """
)

(
    gold_recipient_consent_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_recipient_consent.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_event_master
# ------------------------------------------------------------------------------

gold_event_master_df = spark.sql(
    """
    SELECT
      CAST(es.event_id AS STRING) AS event_id,
      CAST(es.event_name AS STRING) AS event_name,
      CAST(es.event_type AS STRING) AS event_type,
      DATE(es.event_start_date) AS event_start_date,
      DATE(es.event_end_date) AS event_end_date,
      CAST(es.event_country_code AS STRING) AS event_country_code,
      CAST(es.event_state_province_code AS STRING) AS event_state_province_code,
      CAST(es.event_city AS STRING) AS event_city,
      CAST(es.organizing_company_profile_id AS STRING) AS organizing_company_profile_id
    FROM event_silver es
    """
)

(
    gold_event_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_event_master.csv")
)

gold_event_master_df.createOrReplaceTempView("gold_event_master")

# ------------------------------------------------------------------------------
# Target: gold.gold_event_spend_allocation
# ------------------------------------------------------------------------------

gold_event_spend_allocation_df = spark.sql(
    """
    SELECT
      CAST(esas.allocation_id AS STRING) AS allocation_id,
      CAST(esas.event_id AS STRING) AS event_id,
      CAST(esas.tov_id AS STRING) AS tov_id,
      CAST(esas.allocation_method AS STRING) AS allocation_method,
      CAST(esas.allocation_basis_value AS INT) AS allocation_basis_value,
      CAST(esas.allocated_amount_usd AS FLOAT) AS allocated_amount_usd,
      DATE(esas.allocation_date) AS allocation_date
    FROM event_spend_allocation_silver esas
    INNER JOIN gold_transfer_of_value_fact gtov
      ON esas.tov_id = gtov.tov_id
    INNER JOIN gold_event_master gem
      ON esas.event_id = gem.event_id
    """
)

(
    gold_event_spend_allocation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_event_spend_allocation.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_material_master
# ------------------------------------------------------------------------------

gold_material_master_df = spark.sql(
    """
    SELECT
      CAST(mms.material_id AS STRING) AS material_id,
      CAST(mms.material_name AS STRING) AS material_name,
      CAST(mms.material_type AS STRING) AS material_type,
      CAST(mms.country_code AS STRING) AS country_code
    FROM material_master_silver mms
    """
)

(
    gold_material_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_material_master.csv")
)

gold_material_master_df.createOrReplaceTempView("gold_material_master")

# ------------------------------------------------------------------------------
# Target: gold.gold_material_distribution_fact
# ------------------------------------------------------------------------------

gold_material_distribution_fact_df = spark.sql(
    """
    SELECT
      CAST(mds.distribution_id AS STRING) AS distribution_id,
      CAST(mds.tov_id AS STRING) AS tov_id,
      CAST(mds.material_id AS STRING) AS material_id,
      DATE(mds.distribution_date) AS distribution_date,
      CAST(mds.country_code AS STRING) AS country_code,
      CAST(mds.quantity AS INT) AS quantity,
      CAST(mds.distribution_channel AS STRING) AS distribution_channel
    FROM material_distribution_silver mds
    INNER JOIN gold_transfer_of_value_fact gtov
      ON mds.tov_id = gtov.tov_id
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

# ------------------------------------------------------------------------------
# Target: gold.gold_hcp_profile
# ------------------------------------------------------------------------------

gold_hcp_profile_df = spark.sql(
    """
    SELECT
      CAST(hcps.npi_number AS STRING) AS npi_number,
      CAST(hcps.hcp_specialty_code AS STRING) AS hcp_specialty_code,
      CAST(hcps.hcp_specialty_description AS STRING) AS hcp_specialty_description,
      CAST(hcps.license_country_code AS STRING) AS license_country_code,
      CAST(hcps.license_state_province_code AS STRING) AS license_state_province_code,
      CAST(hcps.active_practice_flag AS BOOLEAN) AS active_practice_flag
    FROM hcp_profile_silver hcps
    INNER JOIN gold_recipient_master grm
      ON (
           (hcps.npi_number IS NOT NULL AND hcps.npi_number = grm.npi_number)
        OR (hcps.npi_number IS NULL AND grm.recipient_type = 'HCP')
      )
    """
)

(
    gold_hcp_profile_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_hcp_profile.csv")
)

# ------------------------------------------------------------------------------
# Target: gold.gold_cross_border_payment
# ------------------------------------------------------------------------------

gold_cross_border_payment_df = spark.sql(
    """
    SELECT
      CAST(cbps.cross_border_id AS STRING) AS cross_border_id,
      CAST(cbps.tov_id AS STRING) AS tov_id,
      CAST(cbps.payer_country_code AS STRING) AS payer_country_code,
      CAST(cbps.recipient_country_code AS STRING) AS recipient_country_code,
      CAST(cbps.cross_border_type AS STRING) AS cross_border_type,
      CAST(cbps.cross_border_flag AS BOOLEAN) AS cross_border_flag
    FROM cross_border_payment_silver cbps
    INNER JOIN gold_transfer_of_value_fact gtov
      ON cbps.tov_id = gtov.tov_id
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
