import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/genomics/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")

# -------------------------------------------------------------------
# Source: patient_data_5000 (p)
# Target: patient_genomics_profile_bronze
# -------------------------------------------------------------------
patient_data_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_5000.{FILE_FORMAT}/")
)
patient_data_5000_df.createOrReplaceTempView("patient_data_5000")

patient_genomics_profile_bronze_df = spark.sql("""
SELECT
  CAST(p.patient_id AS STRING)            AS patient_id,
  CAST(p.first_name AS STRING)            AS first_name,
  CAST(p.last_name AS STRING)             AS last_name,
  CAST(p.gender AS STRING)                AS gender,
  DATE(p.date_of_birth)                   AS date_of_birth,
  CAST(p.blood_group AS STRING)           AS blood_group,
  CAST(p.ethnicity AS STRING)             AS ethnicity,
  CAST(p.contact_number AS STRING)        AS contact_number,
  CAST(p.email AS STRING)                 AS email,
  CAST(p.address AS STRING)               AS address,
  CAST(p.city AS STRING)                  AS city,
  CAST(p.state AS STRING)                 AS state,
  CAST(p.country AS STRING)               AS country,
  CAST(p.diagnosis AS STRING)             AS diagnosis,
  DATE(p.registration_date)               AS registration_date
FROM patient_data_5000 p
""")

(
    patient_genomics_profile_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_genomics_profile_bronze.csv")
)

# -------------------------------------------------------------------
# Source: genomics_sequencing_runs_5000 (r)
# Target: sequencing_run_performance_daily_bronze
# -------------------------------------------------------------------
genomics_sequencing_runs_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_5000.{FILE_FORMAT}/")
)
genomics_sequencing_runs_5000_df.createOrReplaceTempView("genomics_sequencing_runs_5000")

sequencing_run_performance_daily_bronze_df = spark.sql("""
SELECT
  CAST(r.run_id AS STRING)                AS run_id,
  CAST(r.patient_id AS STRING)            AS patient_id,
  CAST(r.sample_id AS STRING)             AS sample_id,
  CAST(r.sequencing_platform AS STRING)   AS sequencing_platform,
  DATE(r.run_date)                        AS run_date,
  CAST(r.technician_name AS STRING)       AS technician_name,
  CAST(r.read_length AS INT)              AS read_length,
  CAST(r.coverage_depth AS DOUBLE)        AS coverage_depth,
  CAST(r.raw_data_size_gb AS DOUBLE)      AS raw_data_size_gb,
  CAST(r.quality_score AS DOUBLE)         AS quality_score,
  CAST(r.alignment_rate AS DOUBLE)        AS alignment_rate,
  CAST(r.reference_genome AS STRING)      AS reference_genome,
  CAST(r.sequencing_center AS STRING)     AS sequencing_center,
  CAST(r.processing_status AS STRING)     AS processing_status,
  CAST(r.upload_timestamp AS TIMESTAMP)   AS upload_timestamp
FROM genomics_sequencing_runs_5000 r
""")

(
    sequencing_run_performance_daily_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_run_performance_daily_bronze.csv")
)

# -------------------------------------------------------------------
# Source: genomic_variants_5000 (v)
# Target: patient_variant_fact_bronze
# -------------------------------------------------------------------
genomic_variants_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_5000.{FILE_FORMAT}/")
)
genomic_variants_5000_df.createOrReplaceTempView("genomic_variants_5000")

patient_variant_fact_bronze_df = spark.sql("""
SELECT
  CAST(v.variant_id AS STRING)                AS variant_id,
  CAST(v.patient_id AS STRING)                AS patient_id,
  CAST(v.run_id AS STRING)                    AS run_id,
  CAST(v.chromosome AS STRING)                AS chromosome,
  CAST(v.gene_name AS STRING)                 AS gene_name,
  CAST(v.variant_type AS STRING)              AS variant_type,
  CAST(v.mutation AS STRING)                  AS mutation,
  CAST(v.genomic_position AS INT)             AS genomic_position,
  CAST(v.reference_allele AS STRING)          AS reference_allele,
  CAST(v.alternate_allele AS STRING)          AS alternate_allele,
  CAST(v.clinical_significance AS STRING)     AS clinical_significance,
  CAST(v.pathogenicity_score AS FLOAT)        AS pathogenicity_score,
  DATE(v.detected_date)                       AS detected_date,
  CAST(v.validation_status AS STRING)         AS validation_status,
  CAST(v.reporting_lab AS STRING)             AS reporting_lab
FROM genomic_variants_5000 v
""")

(
    patient_variant_fact_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_variant_fact_bronze.csv")
)

# -------------------------------------------------------------------
# Source: lab_test_results_5000 (l)
# Target: lab_results_trend_bronze
# -------------------------------------------------------------------
lab_test_results_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_5000.{FILE_FORMAT}/")
)
lab_test_results_5000_df.createOrReplaceTempView("lab_test_results_5000")

lab_results_trend_bronze_df = spark.sql("""
SELECT
  CAST(l.result_id AS STRING)            AS result_id,
  CAST(l.patient_id AS STRING)           AS patient_id,
  CAST(l.sample_id AS STRING)            AS sample_id,
  CAST(l.test_name AS STRING)            AS test_name,
  CAST(l.biomarker AS STRING)            AS biomarker,
  CAST(l.test_result AS STRING)          AS test_result,
  CAST(l.unit AS STRING)                 AS unit,
  CAST(l.reference_range AS STRING)      AS reference_range,
  CAST(l.interpretation AS STRING)       AS interpretation,
  CAST(l.performed_by AS STRING)         AS performed_by,
  CAST(l.lab_name AS STRING)             AS lab_name,
  DATE(l.collection_date)                AS collection_date,
  DATE(l.result_date)                    AS result_date,
  CAST(l.approval_status AS STRING)      AS approval_status,
  CAST(l.remarks AS STRING)              AS remarks
FROM lab_test_results_5000 l
""")

(
    lab_results_trend_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_results_trend_bronze.csv")
)

job.commit()
