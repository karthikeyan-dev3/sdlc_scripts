import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------
# Read source tables (S3) + Temp Views
# ------------------------------------------------------------------
sp_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient.{FILE_FORMAT}/")
)
sp_df.createOrReplaceTempView("silver_patient")

ssr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sequencing_run.{FILE_FORMAT}/")
)
ssr_df.createOrReplaceTempView("silver_sequencing_run")

sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_patient_sample.{FILE_FORMAT}/")
)
sps_df.createOrReplaceTempView("silver_patient_sample")

sssr_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sample_sequencing_run.{FILE_FORMAT}/")
)
sssr_df.createOrReplaceTempView("silver_sample_sequencing_run")

sgv_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_genomic_variant.{FILE_FORMAT}/")
)
sgv_df.createOrReplaceTempView("silver_genomic_variant")

ssvo_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sample_variant_observation.{FILE_FORMAT}/")
)
ssvo_df.createOrReplaceTempView("silver_sample_variant_observation")

# ------------------------------------------------------------------
# Target: gold_patient
# ------------------------------------------------------------------
gold_patient_df = spark.sql(
    """
    SELECT
        CAST(sp.patient_id AS STRING)            AS patient_id,
        CAST(sp.mrn AS STRING)                   AS mrn,
        CAST(sp.first_name AS STRING)            AS first_name,
        CAST(sp.last_name AS STRING)             AS last_name,
        CAST(sp.date_of_birth AS DATE)           AS date_of_birth,
        CAST(sp.age_years AS INT)                AS age_years,
        CAST(sp.gender AS STRING)                AS gender,
        CAST(sp.phone_number AS STRING)          AS phone_number,
        CAST(sp.email AS STRING)                 AS email,
        CAST(sp.address_line1 AS STRING)         AS address_line1,
        CAST(sp.address_line2 AS STRING)         AS address_line2,
        CAST(sp.city AS STRING)                  AS city,
        CAST(sp.state AS STRING)                 AS state,
        CAST(sp.postal_code AS STRING)           AS postal_code,
        CAST(sp.country AS STRING)               AS country,
        CAST(sp.record_effective_ts AS TIMESTAMP) AS record_effective_ts,
        CAST(sp.record_end_ts AS TIMESTAMP)      AS record_end_ts,
        CAST(sp.is_current_record AS STRING)     AS is_current_record
    FROM silver_patient sp
    """
)

(
    gold_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient.csv")
)

# ------------------------------------------------------------------
# Target: gold_sequencing_run
# ------------------------------------------------------------------
gold_sequencing_run_df = spark.sql(
    """
    SELECT
        CAST(ssr.sequencing_run_id AS STRING)     AS sequencing_run_id,
        CAST(ssr.run_accession AS STRING)         AS run_accession,
        CAST(ssr.run_date AS DATE)                AS run_date,
        CAST(ssr.platform AS STRING)              AS platform,
        CAST(ssr.instrument_id AS STRING)         AS instrument_id,
        CAST(ssr.lab_name AS STRING)              AS lab_name,
        CAST(ssr.sequencing_method AS STRING)     AS sequencing_method,
        CAST(ssr.panel_or_assay AS STRING)        AS panel_or_assay,
        CAST(ssr.read_length_bp AS INT)           AS read_length_bp,
        CAST(ssr.coverage_mean AS DOUBLE)         AS coverage_mean,
        CAST(ssr.qc_status AS STRING)             AS qc_status,
        CAST(ssr.run_result_summary AS STRING)    AS run_result_summary,
        CAST(ssr.record_effective_ts AS TIMESTAMP) AS record_effective_ts,
        CAST(ssr.record_end_ts AS TIMESTAMP)      AS record_end_ts,
        CAST(ssr.is_current_record AS STRING)     AS is_current_record
    FROM silver_sequencing_run ssr
    """
)

(
    gold_sequencing_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sequencing_run.csv")
)

# ------------------------------------------------------------------
# Target: gold_patient_sample
# ------------------------------------------------------------------
gold_patient_sample_df = spark.sql(
    """
    SELECT
        CAST(sps.sample_id AS STRING)             AS sample_id,
        CAST(sps.sample_accession AS STRING)      AS sample_accession,
        CAST(sps.patient_id AS STRING)            AS patient_id,
        CAST(sps.sample_type AS STRING)           AS sample_type,
        CAST(sps.collection_date AS DATE)         AS collection_date,
        CAST(sps.received_date AS DATE)           AS received_date,
        CAST(sps.anatomical_site AS STRING)       AS anatomical_site,
        CAST(sps.specimen_source AS STRING)       AS specimen_source,
        CAST(sps.processing_lab AS STRING)        AS processing_lab,
        CAST(sps.chain_of_custody_id AS STRING)   AS chain_of_custody_id,
        CAST(sps.record_effective_ts AS TIMESTAMP) AS record_effective_ts,
        CAST(sps.record_end_ts AS TIMESTAMP)      AS record_end_ts,
        CAST(sps.is_current_record AS STRING)     AS is_current_record
    FROM silver_patient_sample sps
    INNER JOIN silver_patient sp
        ON sps.patient_id = sp.patient_id
    """
)

(
    gold_patient_sample_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_patient_sample.csv")
)

# ------------------------------------------------------------------
# Target: gold_sample_sequencing_run
# ------------------------------------------------------------------
gold_sample_sequencing_run_df = spark.sql(
    """
    SELECT
        CAST(sssr.sample_id AS STRING)                 AS sample_id,
        CAST(sssr.sequencing_run_id AS STRING)         AS sequencing_run_id,
        CAST(sssr.library_id AS STRING)                AS library_id,
        CAST(sssr.lane_id AS STRING)                   AS lane_id,
        CAST(sssr.alignment_reference_build AS STRING) AS alignment_reference_build,
        CAST(sssr.pipeline_version AS STRING)          AS pipeline_version,
        CAST(sssr.analysis_date AS DATE)               AS analysis_date,
        CAST(sssr.qc_status AS STRING)                 AS qc_status,
        CAST(sssr.record_effective_ts AS TIMESTAMP)     AS record_effective_ts,
        CAST(sssr.record_end_ts AS TIMESTAMP)          AS record_end_ts,
        CAST(sssr.is_current_record AS STRING)         AS is_current_record
    FROM silver_sample_sequencing_run sssr
    INNER JOIN silver_patient_sample sps
        ON sssr.sample_id = sps.sample_id
    INNER JOIN silver_sequencing_run ssr
        ON sssr.sequencing_run_id = ssr.sequencing_run_id
    """
)

(
    gold_sample_sequencing_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sample_sequencing_run.csv")
)

# ------------------------------------------------------------------
# Target: gold_genomic_variant
# ------------------------------------------------------------------
gold_genomic_variant_df = spark.sql(
    """
    SELECT
        CAST(sgv.variant_id AS STRING)             AS variant_id,
        CAST(sgv.variant_key AS STRING)            AS variant_key,
        CAST(sgv.chromosome AS STRING)             AS chromosome,
        CAST(sgv.position AS INT)                  AS position,
        CAST(sgv.reference_allele AS STRING)       AS reference_allele,
        CAST(sgv.alternate_allele AS STRING)       AS alternate_allele,
        CAST(sgv.variant_type AS STRING)           AS variant_type,
        CAST(sgv.gene_symbol AS STRING)            AS gene_symbol,
        CAST(sgv.transcript_id AS STRING)          AS transcript_id,
        CAST(sgv.hgvs_c AS STRING)                 AS hgvs_c,
        CAST(sgv.hgvs_p AS STRING)                 AS hgvs_p,
        CAST(sgv.clinical_significance AS STRING)  AS clinical_significance,
        CAST(sgv.dbsnp_id AS STRING)               AS dbsnp_id
    FROM silver_genomic_variant sgv
    """
)

(
    gold_genomic_variant_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_genomic_variant.csv")
)

# ------------------------------------------------------------------
# Target: gold_sample_variant_observation
# ------------------------------------------------------------------
gold_sample_variant_observation_df = spark.sql(
    """
    SELECT
        CAST(ssvo.sample_id AS STRING)              AS sample_id,
        CAST(ssvo.variant_id AS STRING)             AS variant_id,
        CAST(ssvo.sequencing_run_id AS STRING)      AS sequencing_run_id,
        CAST(ssvo.genotype AS STRING)               AS genotype,
        CAST(ssvo.allele_fraction AS FLOAT)         AS allele_fraction,
        CAST(ssvo.read_depth AS INT)                AS read_depth,
        CAST(ssvo.variant_quality AS DOUBLE)        AS variant_quality,
        CAST(ssvo.filter_status AS STRING)          AS filter_status,
        CAST(ssvo.caller_name AS STRING)            AS caller_name,
        CAST(ssvo.caller_version AS STRING)         AS caller_version,
        CAST(ssvo.detected_date AS DATE)            AS detected_date,
        CAST(ssvo.record_effective_ts AS TIMESTAMP)  AS record_effective_ts,
        CAST(ssvo.record_end_ts AS TIMESTAMP)       AS record_end_ts,
        CAST(ssvo.is_current_record AS STRING)      AS is_current_record
    FROM silver_sample_variant_observation ssvo
    INNER JOIN silver_patient_sample sps
        ON ssvo.sample_id = sps.sample_id
    INNER JOIN silver_sequencing_run ssr
        ON ssvo.sequencing_run_id = ssr.sequencing_run_id
    INNER JOIN silver_genomic_variant sgv
        ON ssvo.variant_id = sgv.variant_id
    """
)

(
    gold_sample_variant_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sample_variant_observation.csv")
)
