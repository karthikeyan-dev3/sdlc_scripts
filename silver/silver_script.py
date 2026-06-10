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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -----------------------------------------------------------------------------------
# 1) Read Source Tables (Bronze) from S3
# -----------------------------------------------------------------------------------
peb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
cvb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_visit_bronze.{FILE_FORMAT}/")
)
lrb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_results_bronze.{FILE_FORMAT}/")
)
dab_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
aeb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
wmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/wearable_monitoring_bronze.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create Temp Views
# -----------------------------------------------------------------------------------
peb_df.createOrReplaceTempView("patient_enrollment_bronze")
cvb_df.createOrReplaceTempView("clinical_visit_bronze")
lrb_df.createOrReplaceTempView("lab_results_bronze")
dab_df.createOrReplaceTempView("drug_administration_bronze")
aeb_df.createOrReplaceTempView("adverse_events_bronze")
wmb_df.createOrReplaceTempView("wearable_monitoring_bronze")

# -----------------------------------------------------------------------------------
# TARGET TABLE: patient_identity_xref_silver
# -----------------------------------------------------------------------------------
patient_identity_xref_silver_df = spark.sql(
    """
    WITH unioned AS (
        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM patient_enrollment_bronze

        UNION ALL

        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM clinical_visit_bronze

        UNION ALL

        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM lab_results_bronze

        UNION ALL

        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM drug_administration_bronze

        UNION ALL

        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM adverse_events_bronze

        UNION ALL

        SELECT
            source_system,
            patient_id,
            updated_at,
            ingestion_ts
        FROM wearable_monitoring_bronze
    ),
    ranked AS (
        SELECT
            source_system,
            patient_id,
            SHA2(CONCAT(COALESCE(source_system,'NA'),'|',COALESCE(patient_id,'NA')),256) AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY source_system, patient_id
                ORDER BY COALESCE(updated_at, ingestion_ts) DESC
            ) AS rn
        FROM unioned
    )
    SELECT
        source_system,
        patient_id,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)
patient_identity_xref_silver_df.createOrReplaceTempView("patient_identity_xref_silver")

(
    patient_identity_xref_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/patient_identity_xref_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: patient_enrollment_silver
# -----------------------------------------------------------------------------------
patient_enrollment_silver_df = spark.sql(
    """
    SELECT
        peb.patient_id AS patient_id,
        CAST(peb.enrollment_date AS TIMESTAMP) AS enrollment_date,
        peb.trial_id AS study_id,
        to_json(named_struct(
            'patient_name', NULLIF(TRIM(peb.patient_name),''),
            'gender', UPPER(NULLIF(TRIM(peb.gender),'')),
            'date_of_birth', CAST(peb.date_of_birth AS DATE),
            'country', NULLIF(TRIM(peb.country),''),
            'site_id', NULLIF(TRIM(peb.site_id),''),
            'consent_status', NULLIF(TRIM(peb.consent_status),'')
        )) AS demographics,
        pix.standardized_patient_identifier AS standardized_patient_identifier
    FROM patient_enrollment_bronze peb
    LEFT JOIN patient_identity_xref_silver pix
        ON peb.source_system = pix.source_system
       AND peb.patient_id = pix.patient_id
    """
)

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/patient_enrollment_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: clinical_visits_silver
# -----------------------------------------------------------------------------------
clinical_visits_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            cvb.patient_id AS patient_id,
            CAST(cvb.visit_date AS TIMESTAMP) AS visit_date,
            NULLIF(TRIM(cvb.visit_type),'') AS visit_type,
            pix.standardized_patient_identifier AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY cvb.visit_id
                ORDER BY COALESCE(cvb.updated_at, cvb.ingestion_ts) DESC
            ) AS rn
        FROM clinical_visit_bronze cvb
        LEFT JOIN patient_identity_xref_silver pix
            ON cvb.source_system = pix.source_system
           AND cvb.patient_id = pix.patient_id
    )
    SELECT
        patient_id,
        visit_date,
        visit_type,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)

(
    clinical_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/clinical_visits_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: laboratory_tests_silver
# -----------------------------------------------------------------------------------
laboratory_tests_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            lrb.patient_id AS patient_id,
            CAST(lrb.test_date AS TIMESTAMP) AS test_date,
            NULLIF(TRIM(lrb.test_name),'') AS test_type,
            CAST(lrb.test_result AS DOUBLE) AS test_result,
            pix.standardized_patient_identifier AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY lrb.lab_result_id
                ORDER BY COALESCE(lrb.updated_at, lrb.ingestion_ts) DESC
            ) AS rn
        FROM lab_results_bronze lrb
        LEFT JOIN patient_identity_xref_silver pix
            ON lrb.patient_id = pix.patient_id
    )
    SELECT
        patient_id,
        test_date,
        test_type,
        test_result,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)

(
    laboratory_tests_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/laboratory_tests_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: drug_administration_silver
# -----------------------------------------------------------------------------------
drug_administration_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            dab.patient_id AS patient_id,
            CAST(dab.administration_date AS TIMESTAMP) AS administration_date,
            NULLIF(TRIM(dab.drug_code),'') AS drug_name,
            CAST(dab.dosage_mg AS DOUBLE) AS dosage,
            pix.standardized_patient_identifier AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY dab.administration_id
                ORDER BY COALESCE(dab.updated_at, dab.ingestion_ts) DESC
            ) AS rn
        FROM drug_administration_bronze dab
        LEFT JOIN patient_identity_xref_silver pix
            ON dab.patient_id = pix.patient_id
    )
    SELECT
        patient_id,
        administration_date,
        drug_name,
        dosage,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)

(
    drug_administration_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/drug_administration_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: adverse_events_silver
# -----------------------------------------------------------------------------------
adverse_events_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            aeb.patient_id AS patient_id,
            CAST(aeb.event_start_date AS TIMESTAMP) AS event_date,
            NULLIF(TRIM(aeb.event_type),'') AS event_type,
            NULLIF(TRIM(aeb.severity),'') AS severity,
            pix.standardized_patient_identifier AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY aeb.event_id
                ORDER BY COALESCE(aeb.updated_at, aeb.ingestion_ts) DESC
            ) AS rn
        FROM adverse_events_bronze aeb
        LEFT JOIN patient_identity_xref_silver pix
            ON aeb.patient_id = pix.patient_id
    )
    SELECT
        patient_id,
        event_date,
        event_type,
        severity,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/adverse_events_silver.csv")
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: wearable_device_data_silver
# -----------------------------------------------------------------------------------
wearable_device_data_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            wmb.patient_id AS patient_id,
            CAST(wmb.recorded_timestamp AS TIMESTAMP) AS data_timestamp,
            NULLIF(TRIM(wmb.device_type),'') AS device_type,
            CAST(wmb.heart_rate AS DOUBLE) AS heart_rate,
            CAST(wmb.step_count AS DOUBLE) AS activity_level,
            pix.standardized_patient_identifier AS standardized_patient_identifier,
            ROW_NUMBER() OVER (
                PARTITION BY wmb.device_record_id
                ORDER BY COALESCE(wmb.updated_at, wmb.ingestion_ts) DESC
            ) AS rn
        FROM wearable_monitoring_bronze wmb
        LEFT JOIN patient_identity_xref_silver pix
            ON wmb.patient_id = pix.patient_id
    )
    SELECT
        patient_id,
        data_timestamp,
        device_type,
        heart_rate,
        activity_level,
        standardized_patient_identifier
    FROM ranked
    WHERE rn = 1
    """
)

(
    wearable_device_data_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/wearable_device_data_silver.csv")
)

job.commit()
