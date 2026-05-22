import sys
from awsglue.transforms import *
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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------
# Read source tables from S3
# -------------------------------------
patient_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_bronze.{FILE_FORMAT}/")
)
patient_data_bronze_df.createOrReplaceTempView("patient_data_bronze")

genomics_sequencing_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_bronze.{FILE_FORMAT}/")
)
genomics_sequencing_runs_bronze_df.createOrReplaceTempView("genomics_sequencing_runs_bronze")

lab_test_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_bronze.{FILE_FORMAT}/")
)
lab_test_results_bronze_df.createOrReplaceTempView("lab_test_results_bronze")

genomic_variants_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_bronze.{FILE_FORMAT}/")
)
genomic_variants_bronze_df.createOrReplaceTempView("genomic_variants_bronze")

# -------------------------------------
# Target: silver_patient (sp)
# Source: bronze.patient_data_bronze pdb
# -------------------------------------
silver_patient_df = spark.sql("""
SELECT
  pdb.patient_id AS patient_id,
  pdb.patient_id AS mrn,
  pdb.first_name AS first_name,
  pdb.last_name AS last_name,
  CAST(pdb.date_of_birth AS date) AS date_of_birth,
  CAST(months_between(current_date(), CAST(pdb.date_of_birth AS date)) / 12 AS int) AS age_years,
  pdb.gender AS gender,
  pdb.contact_number AS phone_number,
  pdb.email AS email,
  pdb.address AS address_line1,
  pdb.address AS address_line2,
  pdb.city AS city,
  pdb.state AS state,
  pdb.address AS postal_code,
  pdb.country AS country,
  CAST(pdb.registration_date AS timestamp) AS record_effective_ts,
  CAST(pdb.registration_date AS timestamp) AS record_end_ts,
  CAST('true' AS string) AS is_current_record
FROM patient_data_bronze pdb
""")

(
    silver_patient_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_patient.csv")
)

# -------------------------------------
# Target: silver_sequencing_run (ssr)
# Source: bronze.genomics_sequencing_runs_bronze gsrb
#        LEFT JOIN bronze.lab_test_results_bronze ltrb ON gsrb.run_id = ltrb.sample_id
# -------------------------------------
silver_sequencing_run_df = spark.sql("""
SELECT
  gsrb.run_id AS sequencing_run_id,
  gsrb.run_id AS run_accession,
  CAST(gsrb.run_date AS date) AS run_date,
  gsrb.sequencing_platform AS platform,
  gsrb.technician_name AS instrument_id,
  ltrb.lab_name AS lab_name,
  gsrb.sequencing_platform AS sequencing_method,
  ltrb.test_name AS panel_or_assay,
  CAST(gsrb.read_length AS int) AS read_length_bp,
  CAST(gsrb.coverage_depth AS double) AS coverage_mean,
  gsrb.processing_status AS qc_status,
  ltrb.test_result AS run_result_summary,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_effective_ts,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_end_ts,
  CAST('true' AS string) AS is_current_record
FROM genomics_sequencing_runs_bronze gsrb
LEFT JOIN lab_test_results_bronze ltrb
  ON gsrb.run_id = ltrb.sample_id
""")

(
    silver_sequencing_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sequencing_run.csv")
)

# -------------------------------------
# Target: silver_patient_sample (sps)
# Source: bronze.genomics_sequencing_runs_bronze gsrb
#        LEFT JOIN bronze.lab_test_results_bronze ltrb ON gsrb.sample_id = ltrb.sample_id
# -------------------------------------
silver_patient_sample_df = spark.sql("""
SELECT
  gsrb.sample_id AS sample_id,
  gsrb.sample_id AS sample_accession,
  gsrb.patient_id AS patient_id,
  ltrb.test_name AS sample_type,
  CAST(ltrb.collection_date AS date) AS collection_date,
  CAST(ltrb.collection_date AS date) AS received_date,
  ltrb.biomarker AS anatomical_site,
  ltrb.biomarker AS specimen_source,
  ltrb.lab_name AS processing_lab,
  ltrb.result_id AS chain_of_custody_id,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_effective_ts,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_end_ts,
  CAST('true' AS string) AS is_current_record
FROM genomics_sequencing_runs_bronze gsrb
LEFT JOIN lab_test_results_bronze ltrb
  ON gsrb.sample_id = ltrb.sample_id
""")

(
    silver_patient_sample_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_patient_sample.csv")
)

# -------------------------------------
# Target: silver_sample_sequencing_run (sssr)
# Source: bronze.genomics_sequencing_runs_bronze gsrb
# -------------------------------------
silver_sample_sequencing_run_df = spark.sql("""
SELECT
  gsrb.sample_id AS sample_id,
  gsrb.run_id AS sequencing_run_id,
  gsrb.run_id AS library_id,
  gsrb.run_id AS lane_id,
  gsrb.reference_genome AS alignment_reference_build,
  gsrb.processing_status AS pipeline_version,
  CAST(gsrb.run_date AS date) AS analysis_date,
  gsrb.processing_status AS qc_status,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_effective_ts,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_end_ts,
  CAST('true' AS string) AS is_current_record
FROM genomics_sequencing_runs_bronze gsrb
""")

(
    silver_sample_sequencing_run_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sample_sequencing_run.csv")
)

# -------------------------------------
# Target: silver_genomic_variant (sgv)
# Source: bronze.genomic_variants_bronze gvb
# -------------------------------------
silver_genomic_variant_df = spark.sql("""
SELECT
  gvb.variant_id AS variant_id,
  gvb.variant_id AS variant_key,
  gvb.chromosome AS chromosome,
  CAST(gvb.genomic_position AS int) AS position,
  gvb.reference_allele AS reference_allele,
  gvb.alternate_allele AS alternate_allele,
  gvb.variant_type AS variant_type,
  gvb.gene_name AS gene_symbol,
  gvb.mutation AS transcript_id,
  gvb.mutation AS hgvs_c,
  gvb.mutation AS hgvs_p,
  gvb.clinical_significance AS clinical_significance,
  gvb.variant_id AS dbsnp_id
FROM genomic_variants_bronze gvb
""")

(
    silver_genomic_variant_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_genomic_variant.csv")
)

# -------------------------------------
# Target: silver_sample_variant_observation (ssvo)
# Source: bronze.genomic_variants_bronze gvb
#        LEFT JOIN bronze.genomics_sequencing_runs_bronze gsrb ON gvb.run_id = gsrb.run_id
# -------------------------------------
silver_sample_variant_observation_df = spark.sql("""
SELECT
  gsrb.sample_id AS sample_id,
  gvb.variant_id AS variant_id,
  gsrb.run_id AS sequencing_run_id,
  gvb.validation_status AS genotype,
  CAST(gvb.pathogenicity_score AS float) AS allele_fraction,
  CAST(gvb.genomic_position AS int) AS read_depth,
  CAST(gsrb.quality_score AS double) AS variant_quality,
  gsrb.processing_status AS filter_status,
  gsrb.technician_name AS caller_name,
  gsrb.processing_status AS caller_version,
  CAST(gvb.detected_date AS date) AS detected_date,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_effective_ts,
  CAST(gsrb.upload_timestamp AS timestamp) AS record_end_ts,
  CAST('true' AS string) AS is_current_record
FROM genomic_variants_bronze gvb
LEFT JOIN genomics_sequencing_runs_bronze gsrb
  ON gvb.run_id = gsrb.run_id
""")

(
    silver_sample_variant_observation_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/silver_sample_variant_observation.csv")
)

job.commit()