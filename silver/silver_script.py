import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# Source Reads (S3) + Temp Views
# --------------------------------------------------------------------------------------

patient_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_bronze.{FILE_FORMAT}/")
)
patient_data_bronze_df.createOrReplaceTempView("patient_data_bronze")

healthcare_events_adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/healthcare_events_adverse_events_bronze.{FILE_FORMAT}/")
)
healthcare_events_adverse_events_bronze_df.createOrReplaceTempView("healthcare_events_adverse_events_bronze")

healthcare_events_clinical_visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/healthcare_events_clinical_visits_bronze.{FILE_FORMAT}/")
)
healthcare_events_clinical_visits_bronze_df.createOrReplaceTempView("healthcare_events_clinical_visits_bronze")

healthcare_events_drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/healthcare_events_drug_administration_bronze.{FILE_FORMAT}/")
)
healthcare_events_drug_administration_bronze_df.createOrReplaceTempView("healthcare_events_drug_administration_bronze")

healthcare_events_lab_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/healthcare_events_lab_results_bronze.{FILE_FORMAT}/")
)
healthcare_events_lab_results_bronze_df.createOrReplaceTempView("healthcare_events_lab_results_bronze")

# --------------------------------------------------------------------------------------
# Target: silver.patient_data_silver (pds)
# --------------------------------------------------------------------------------------

patient_data_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pdb.patient_id AS STRING) AS patient_id,
            CAST(pdb.patient_name AS STRING) AS patient_name,
            CAST(pdb.date_of_birth AS DATE) AS date_of_birth,
            CAST(pdb.gender AS STRING) AS gender,
            CAST(pdb.patient_id AS STRING) AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pdb.patient_id AS STRING)
                ORDER BY CAST(pdb.patient_id AS STRING)
            ) AS rn
        FROM patient_data_bronze pdb
    )
    SELECT
        patient_id,
        patient_name,
        date_of_birth,
        gender,
        standardized_patient_identifier
    FROM base
    WHERE rn = 1
    """
)

(
    patient_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_data_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.healthcare_events_silver (hes)
# NOTE: UDT provides mappings only from adverse events (aeb.*) into target columns below.
#       Other UNION ALL sources are included as-is in the union staging, but only mapped
#       columns are selected for the final output.
# --------------------------------------------------------------------------------------

healthcare_events_silver_df = spark.sql(
    """
    WITH unioned AS (
        SELECT
            aeb.patient_id AS aeb_patient_id,
            aeb.event_start_date AS aeb_event_start_date,
            aeb.event_type AS aeb_event_type,
            aeb.severity AS aeb_severity,
            aeb.reported_by AS aeb_reported_by
        FROM healthcare_events_adverse_events_bronze aeb

        UNION ALL

        SELECT
            NULL AS aeb_patient_id,
            NULL AS aeb_event_start_date,
            NULL AS aeb_event_type,
            NULL AS aeb_severity,
            NULL AS aeb_reported_by
        FROM healthcare_events_clinical_visits_bronze cvb

        UNION ALL

        SELECT
            NULL AS aeb_patient_id,
            NULL AS aeb_event_start_date,
            NULL AS aeb_event_type,
            NULL AS aeb_severity,
            NULL AS aeb_reported_by
        FROM healthcare_events_drug_administration_bronze dab

        UNION ALL

        SELECT
            NULL AS aeb_patient_id,
            NULL AS aeb_event_start_date,
            NULL AS aeb_event_type,
            NULL AS aeb_severity,
            NULL AS aeb_reported_by
        FROM healthcare_events_lab_results_bronze lrb
    ),
    mapped AS (
        SELECT
            CAST(aeb_patient_id AS STRING) AS patient_id,
            CAST(aeb_event_start_date AS TIMESTAMP) AS event_date,
            CAST(aeb_event_type AS STRING) AS event_type,
            CAST(aeb_severity AS STRING) AS event_description,
            CAST(aeb_reported_by AS STRING) AS healthcare_provider,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CAST(aeb_patient_id AS STRING),
                    CAST(aeb_event_start_date AS TIMESTAMP),
                    CAST(aeb_event_type AS STRING),
                    CAST(aeb_severity AS STRING),
                    CAST(aeb_reported_by AS STRING)
                ORDER BY
                    CAST(aeb_patient_id AS STRING)
            ) AS rn
        FROM unioned
    )
    SELECT
        patient_id,
        event_date,
        event_type,
        event_description,
        healthcare_provider
    FROM mapped
    WHERE rn = 1
    """
)

(
    healthcare_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/healthcare_events_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.clinical_trial_data_silver (ctds)
# --------------------------------------------------------------------------------------

clinical_trial_data_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            CAST(pdb.patient_id AS STRING) AS patient_id,
            CAST(pdb.trial_id AS STRING) AS trial_id,
            CAST(pdb.enrollment_date AS TIMESTAMP) AS enrollment_date,
            CAST(pdb.consent_status AS STRING) AS trial_status,
            CAST(cvb.visit_type AS STRING) AS results,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CAST(pdb.patient_id AS STRING),
                    CAST(pdb.trial_id AS STRING),
                    CAST(pdb.enrollment_date AS TIMESTAMP),
                    CAST(pdb.consent_status AS STRING),
                    CAST(cvb.visit_type AS STRING)
                ORDER BY
                    CAST(pdb.patient_id AS STRING)
            ) AS rn
        FROM patient_data_bronze pdb
        LEFT JOIN healthcare_events_clinical_visits_bronze cvb
            ON pdb.patient_id = cvb.patient_id
           AND pdb.trial_id = cvb.trial_id
    )
    SELECT
        patient_id,
        trial_id,
        enrollment_date,
        trial_status,
        results
    FROM joined
    WHERE rn = 1
    """
)

(
    clinical_trial_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trial_data_silver.csv")
)

job.commit()
