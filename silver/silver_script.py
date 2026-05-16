import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ----------------------------
# Read Source Tables (Bronze)
# ----------------------------
adverse_events_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/adverse_events_bronze.{FILE_FORMAT}/")
)
adverse_events_bronze_df.createOrReplaceTempView("adverse_events_bronze")

drug_administration_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/drug_administration_bronze.{FILE_FORMAT}/")
)
drug_administration_bronze_df.createOrReplaceTempView("drug_administration_bronze")

# ==========================================
# Target: silver.adverse_events_silver
# ==========================================
adverse_events_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(UPPER(aeb.event_id)) AS adverse_event_id,
            NULLIF(TRIM(aeb.event_type), '') AS description,
            CASE
                WHEN UPPER(TRIM(aeb.severity)) IN ('FATAL','DEATH') THEN 'FATAL'
                WHEN UPPER(TRIM(aeb.severity)) IN ('LIFE THREATENING','LIFE-THREATENING','LIFE_THREATENING') THEN 'LIFE_THREATENING'
                WHEN UPPER(TRIM(aeb.severity)) IN ('SEVERE','SERIOUS') THEN 'SEVERE'
                WHEN UPPER(TRIM(aeb.severity)) IN ('MODERATE','MEDIUM') THEN 'MODERATE'
                WHEN UPPER(TRIM(aeb.severity)) IN ('MILD','LOW') THEN 'MILD'
                ELSE 'UNKNOWN'
            END AS severity_classification,
            aeb.event_start_date AS event_start_date,
            aeb.event_end_date AS event_end_date,
            aeb.reported_by AS reported_by
        FROM adverse_events_bronze aeb
    ),
    ranked AS (
        SELECT
            adverse_event_id,
            description,
            severity_classification,
            ROW_NUMBER() OVER (
                PARTITION BY adverse_event_id
                ORDER BY event_start_date DESC, event_end_date DESC, reported_by ASC
            ) AS rn
        FROM base
    )
    SELECT
        adverse_event_id,
        description,
        severity_classification
    FROM ranked
    WHERE rn = 1
    """
)
adverse_events_silver_df.createOrReplaceTempView("adverse_events_silver")

(
    adverse_events_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/adverse_events_silver.csv")
)

# ==========================================
# Target: silver.drug_safety_analysis_silver
# ==========================================
drug_safety_analysis_silver_df = spark.sql(
    """
    WITH joined AS (
        SELECT
            TRIM(UPPER(dab.patient_id)) AS patient_id,
            NULLIF(TRIM(UPPER(dab.drug_code)), '') AS drug_id,
            dab.administration_date AS administration_date,
            CASE WHEN CAST(dab.dosage_mg AS DOUBLE) < 0 THEN NULL ELSE CAST(dab.dosage_mg AS DOUBLE) END AS dosage_amount,
            TRIM(UPPER(aeb.event_id)) AS adverse_event_id,
            COALESCE(aes.severity_classification, 'UNKNOWN') AS event_severity,
            COALESCE(CAST(aeb.hospitalization_required AS BOOLEAN), false) AS hospitalization_indicator,
            CASE
                WHEN COALESCE(CAST(aeb.hospitalization_required AS BOOLEAN), false) = true
                  OR UPPER(TRIM(aeb.outcome)) IN ('DEATH','FATAL')
                  OR UPPER(TRIM(aeb.severity)) IN ('SEVERE','SERIOUS','LIFE THREATENING','LIFE-THREATENING','LIFE_THREATENING','FATAL')
                THEN 1 ELSE 0
            END AS sae_count,
            CASE
                WHEN aeb.event_id IS NULL THEN false
                WHEN COALESCE(CAST(aeb.related_to_drug AS BOOLEAN), false) = true THEN true
                ELSE true
            END AS drug_event_correlation,
            CASE
                WHEN (
                    CASE
                        WHEN COALESCE(CAST(aeb.hospitalization_required AS BOOLEAN), false) = true
                          OR UPPER(TRIM(aeb.outcome)) IN ('DEATH','FATAL')
                          OR UPPER(TRIM(aeb.severity)) IN ('SEVERE','SERIOUS','LIFE THREATENING','LIFE-THREATENING','LIFE_THREATENING','FATAL')
                        THEN 1 ELSE 0
                    END
                ) = 1 THEN true ELSE false
            END AS high_risk_patient_indicator,
            aeb.event_start_date AS event_start_date
        FROM drug_administration_bronze dab
        LEFT JOIN adverse_events_bronze aeb
            ON dab.patient_id = aeb.patient_id
           AND aeb.event_start_date BETWEEN dab.administration_date AND (dab.administration_date + INTERVAL '30' DAY)
        LEFT JOIN adverse_events_silver aes
            ON TRIM(UPPER(aeb.event_id)) = aes.adverse_event_id
    ),
    ranked AS (
        SELECT
            patient_id,
            drug_id,
            administration_date,
            dosage_amount,
            adverse_event_id,
            event_severity,
            hospitalization_indicator,
            sae_count,
            drug_event_correlation,
            high_risk_patient_indicator,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id, drug_id, administration_date, adverse_event_id
                ORDER BY
                    CASE event_severity
                        WHEN 'FATAL' THEN 6
                        WHEN 'LIFE_THREATENING' THEN 5
                        WHEN 'SEVERE' THEN 4
                        WHEN 'MODERATE' THEN 3
                        WHEN 'MILD' THEN 2
                        ELSE 1
                    END DESC,
                    event_start_date ASC
            ) AS rn
        FROM joined
    )
    SELECT
        patient_id,
        drug_id,
        dosage_amount,
        adverse_event_id,
        event_severity,
        hospitalization_indicator,
        sae_count,
        drug_event_correlation,
        high_risk_patient_indicator
    FROM ranked
    WHERE rn = 1
    """
)

(
    drug_safety_analysis_silver_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .format("csv")
    .save(f"{TARGET_PATH}/drug_safety_analysis_silver.csv")
)

job.commit()
