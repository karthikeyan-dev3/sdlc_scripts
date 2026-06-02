import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/genomics/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------
genomics_sequencing_runs_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_5000.{FILE_FORMAT}/")
)
lab_test_results_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_5000.{FILE_FORMAT}/")
)
patient_data_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_5000.{FILE_FORMAT}/")
)
genomic_variants_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_5000.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
genomics_sequencing_runs_5000_df.createOrReplaceTempView("genomics_sequencing_runs_5000")
lab_test_results_5000_df.createOrReplaceTempView("lab_test_results_5000")
patient_data_5000_df.createOrReplaceTempView("patient_data_5000")
genomic_variants_5000_df.createOrReplaceTempView("genomic_variants_5000")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------------------

# sequencing_runs_bronze (source: genomics_sequencing_runs_5000 s)
sequencing_runs_bronze_df = spark.sql("""
SELECT
  CAST(s.run_id AS STRING)                 AS run_id,
  CAST(s.patient_id AS STRING)             AS patient_id,
  CAST(s.sample_id AS STRING)              AS sample_id,
  CAST(s.sequencing_platform AS STRING)    AS sequencing_platform,
  CAST(s.run_date AS DATE)                 AS run_date,
  CAST(s.processing_status AS STRING)      AS processing_status,
  CAST(s.upload_timestamp AS TIMESTAMP)    AS upload_timestamp
FROM genomics_sequencing_runs_5000 s
""")

# lab_work_items_bronze (source: lab_test_results_5000 l)
lab_work_items_bronze_df = spark.sql("""
SELECT
  CAST(l.result_id AS STRING)         AS result_id,
  CAST(l.patient_id AS STRING)        AS patient_id,
  CAST(l.sample_id AS STRING)         AS sample_id,
  CAST(l.test_name AS STRING)         AS test_name,
  CAST(l.biomarker AS STRING)         AS biomarker,
  CAST(l.test_result AS STRING)       AS test_result,
  CAST(l.unit AS STRING)              AS unit,
  CAST(l.collection_date AS DATE)     AS collection_date,
  CAST(l.result_date AS DATE)         AS result_date
FROM lab_test_results_5000 l
""")

# pending_approvals_bronze (source: lab_test_results_5000 l)
pending_approvals_bronze_df = spark.sql("""
SELECT
  CAST(l.approval_status AS STRING)   AS approval_status,
  CAST(l.result_id AS STRING)         AS result_id,
  CAST(l.patient_id AS STRING)        AS patient_id,
  CAST(l.result_date AS DATE)         AS result_date
FROM lab_test_results_5000 l
""")

# patient_diagnostic_results_bronze (source: patient_data_5000 p)
patient_diagnostic_results_bronze_df = spark.sql("""
SELECT
  CAST(p.patient_id AS STRING)            AS patient_id,
  CAST(p.diagnosis AS STRING)             AS diagnosis,
  CAST(p.registration_date AS DATE)       AS registration_date
FROM patient_data_5000 p
""")

# pathogenic_variant_alerts_bronze (source: genomic_variants_5000 v)
pathogenic_variant_alerts_bronze_df = spark.sql("""
SELECT
  CAST(v.variant_id AS STRING)                 AS variant_id,
  CAST(v.patient_id AS STRING)                 AS patient_id,
  CAST(v.run_id AS STRING)                     AS run_id,
  CAST(v.gene_name AS STRING)                  AS gene_name,
  CAST(v.variant_type AS STRING)               AS variant_type,
  CAST(v.clinical_significance AS STRING)      AS clinical_significance,
  CAST(v.pathogenicity_score AS FLOAT)         AS pathogenicity_score,
  CAST(v.detected_date AS DATE)                AS detected_date,
  CAST(v.validation_status AS STRING)          AS validation_status,
  CAST(v.reporting_lab AS STRING)              AS reporting_lab
FROM genomic_variants_5000 v
""")

# ------------------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------------------
(
    sequencing_runs_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_runs_bronze.csv")
)

(
    lab_work_items_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_work_items_bronze.csv")
)

(
    pending_approvals_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pending_approvals_bronze.csv")
)

(
    patient_diagnostic_results_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_diagnostic_results_bronze.csv")
)

(
    pathogenic_variant_alerts_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/pathogenic_variant_alerts_bronze.csv")
)

job.commit()