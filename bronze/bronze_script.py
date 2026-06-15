import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/genomics/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# =========================
# patient_bronze
# =========================
patient_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_5000.{FILE_FORMAT}/")
)
patient_src_df.createOrReplaceTempView("patient_data_5000")

patient_bronze_df = spark.sql(
    """
    SELECT
        CAST(p.patient_id AS STRING)            AS patient_id,
        CAST(p.first_name AS STRING)            AS first_name,
        CAST(p.last_name AS STRING)             AS last_name,
        CAST(p.gender AS STRING)                AS gender,
        DATE(CAST(p.date_of_birth AS STRING))   AS date_of_birth,
        CAST(p.blood_group AS STRING)           AS blood_group,
        CAST(p.ethnicity AS STRING)             AS ethnicity,
        CAST(p.contact_number AS STRING)        AS contact_number,
        CAST(p.email AS STRING)                 AS email,
        CAST(p.address AS STRING)               AS address,
        CAST(p.city AS STRING)                  AS city,
        CAST(p.state AS STRING)                 AS state,
        CAST(p.country AS STRING)               AS country,
        CAST(p.diagnosis AS STRING)             AS diagnosis,
        DATE(CAST(p.registration_date AS STRING)) AS registration_date
    FROM patient_data_5000 p
    """
)

(
    patient_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_bronze.csv")
)

# =========================
# variant_bronze
# =========================
variant_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_5000.{FILE_FORMAT}/")
)
variant_src_df.createOrReplaceTempView("genomic_variants_5000")

variant_bronze_df = spark.sql(
    """
    SELECT
        CAST(v.variant_id AS STRING)                 AS variant_id,
        CAST(v.patient_id AS STRING)                 AS patient_id,
        CAST(v.run_id AS STRING)                     AS run_id,
        CAST(v.chromosome AS STRING)                 AS chromosome,
        CAST(v.gene_name AS STRING)                  AS gene_name,
        CAST(v.variant_type AS STRING)               AS variant_type,
        CAST(v.mutation AS STRING)                   AS mutation,
        CAST(v.genomic_position AS INT)              AS genomic_position,
        CAST(v.reference_allele AS STRING)           AS reference_allele,
        CAST(v.alternate_allele AS STRING)           AS alternate_allele,
        CAST(v.clinical_significance AS STRING)      AS clinical_significance,
        CAST(v.pathogenicity_score AS FLOAT)         AS pathogenicity_score,
        DATE(CAST(v.detected_date AS STRING))        AS detected_date,
        CAST(v.validation_status AS STRING)          AS validation_status,
        CAST(v.reporting_lab AS STRING)              AS reporting_lab
    FROM genomic_variants_5000 v
    """
)

(
    variant_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/variant_bronze.csv")
)

# =========================
# sequencing_run_bronze
# =========================
runs_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_5000.{FILE_FORMAT}/")
)
runs_src_df.createOrReplaceTempView("genomics_sequencing_runs_5000")

sequencing_run_bronze_df = spark.sql(
    """
    SELECT
        CAST(r.run_id AS STRING)                       AS run_id,
        CAST(r.patient_id AS STRING)                   AS patient_id,
        CAST(r.sample_id AS STRING)                    AS sample_id,
        CAST(r.sequencing_platform AS STRING)          AS sequencing_platform,
        DATE(CAST(r.run_date AS STRING))               AS run_date,
        CAST(r.technician_name AS STRING)              AS technician_name,
        CAST(r.read_length AS INT)                     AS read_length,
        CAST(r.coverage_depth AS DOUBLE)               AS coverage_depth,
        CAST(r.raw_data_size_gb AS DOUBLE)             AS raw_data_size_gb,
        CAST(r.quality_score AS DOUBLE)                AS quality_score,
        CAST(r.alignment_rate AS DOUBLE)               AS alignment_rate,
        CAST(r.reference_genome AS STRING)             AS reference_genome,
        CAST(r.sequencing_center AS STRING)            AS sequencing_center,
        CAST(r.processing_status AS STRING)            AS processing_status,
        CAST(r.upload_timestamp AS TIMESTAMP)          AS upload_timestamp
    FROM genomics_sequencing_runs_5000 r
    """
)

(
    sequencing_run_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_run_bronze.csv")
)

# =========================
# lab_result_bronze
# =========================
lab_src_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_5000.{FILE_FORMAT}/")
)
lab_src_df.createOrReplaceTempView("lab_test_results_5000")

lab_result_bronze_df = spark.sql(
    """
    SELECT
        CAST(t.result_id AS STRING)                  AS result_id,
        CAST(t.patient_id AS STRING)                 AS patient_id,
        CAST(t.sample_id AS STRING)                  AS sample_id,
        CAST(t.test_name AS STRING)                  AS test_name,
        CAST(t.biomarker AS STRING)                  AS biomarker,
        CAST(t.test_result AS STRING)                AS test_result,
        CAST(t.unit AS STRING)                       AS unit,
        CAST(t.reference_range AS STRING)            AS reference_range,
        CAST(t.interpretation AS STRING)             AS interpretation,
        CAST(t.performed_by AS STRING)               AS performed_by,
        CAST(t.lab_name AS STRING)                   AS lab_name,
        DATE(CAST(t.collection_date AS STRING))      AS collection_date,
        DATE(CAST(t.result_date AS STRING))          AS result_date,
        CAST(t.approval_status AS STRING)            AS approval_status,
        CAST(t.remarks AS STRING)                    AS remarks
    FROM lab_test_results_5000 t
    """
)

(
    lab_result_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_result_bronze.csv")
)