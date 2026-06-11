import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Read Sources (Bronze)
# -----------------------------
patient_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_bronze.{FILE_FORMAT}/")
)
sequencing_run_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sequencing_run_bronze.{FILE_FORMAT}/")
)
lab_result_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_result_bronze.{FILE_FORMAT}/")
)
genomic_variant_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variant_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# Create Temp Views
# -----------------------------
patient_bronze_df.createOrReplaceTempView("patient_bronze")
sequencing_run_bronze_df.createOrReplaceTempView("sequencing_run_bronze")
lab_result_bronze_df.createOrReplaceTempView("lab_result_bronze")
genomic_variant_bronze_df.createOrReplaceTempView("genomic_variant_bronze")

# =============================
# patient_silver
# =============================
patient_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    pb.patient_id AS patient_id,
    TRIM(pb.first_name) AS first_name,
    TRIM(pb.last_name) AS last_name,
    UPPER(TRIM(pb.gender)) AS gender,
    CAST(pb.date_of_birth AS date) AS date_of_birth,
    UPPER(TRIM(pb.blood_group)) AS blood_group,
    TRIM(pb.ethnicity) AS ethnicity,
    TRIM(pb.contact_number) AS contact_number,
    LOWER(TRIM(pb.email)) AS email,
    TRIM(pb.address) AS address,
    TRIM(pb.city) AS city,
    UPPER(TRIM(pb.state)) AS state,
    UPPER(TRIM(pb.country)) AS country,
    TRIM(pb.diagnosis) AS diagnosis,
    CAST(pb.registration_date AS date) AS registration_date,
    ROW_NUMBER() OVER (
      PARTITION BY pb.patient_id
      ORDER BY pb.registration_date DESC NULLS LAST
    ) AS rn
  FROM patient_bronze pb
)
SELECT
  patient_id,
  first_name,
  last_name,
  gender,
  date_of_birth,
  blood_group,
  ethnicity,
  contact_number,
  email,
  address,
  city,
  state,
  country,
  diagnosis,
  registration_date
FROM ranked
WHERE rn = 1
""")

patient_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/patient_silver.csv"
)

# =============================
# specimen_silver
# =============================
specimen_silver_df = spark.sql("""
WITH unioned AS (
  SELECT
    srb.sample_id AS sample_id,
    srb.patient_id AS patient_id,
    CAST(NULL AS varchar(255)) AS sequencing_platform,
    CAST(NULL AS date) AS run_date,
    CAST(NULL AS varchar(255)) AS technician_name,
    CAST(NULL AS int) AS read_length,
    CAST(NULL AS double) AS coverage_depth,
    CAST(NULL AS double) AS raw_data_size_gb,
    CAST(NULL AS double) AS quality_score,
    CAST(NULL AS double) AS alignment_rate,
    CAST(NULL AS varchar(255)) AS reference_genome,
    CAST(NULL AS varchar(255)) AS sequencing_center,
    CAST(NULL AS varchar(255)) AS processing_status,
    CAST(NULL AS timestamp) AS upload_timestamp
  FROM sequencing_run_bronze srb
  WHERE 1 = 0

  UNION ALL

  SELECT
    lrb.sample_id AS sample_id,
    lrb.patient_id AS patient_id,
    CAST(NULL AS varchar(255)) AS sequencing_platform,
    CAST(NULL AS date) AS run_date,
    CAST(NULL AS varchar(255)) AS technician_name,
    CAST(NULL AS int) AS read_length,
    CAST(NULL AS double) AS coverage_depth,
    CAST(NULL AS double) AS raw_data_size_gb,
    CAST(NULL AS double) AS quality_score,
    CAST(NULL AS double) AS alignment_rate,
    CAST(NULL AS varchar(255)) AS reference_genome,
    CAST(NULL AS varchar(255)) AS sequencing_center,
    CAST(NULL AS varchar(255)) AS processing_status,
    CAST(NULL AS timestamp) AS upload_timestamp
  FROM lab_result_bronze lrb
  WHERE 1 = 0
),
ranked AS (
  SELECT
    sample_id,
    patient_id,
    sequencing_platform,
    run_date,
    technician_name,
    read_length,
    coverage_depth,
    raw_data_size_gb,
    quality_score,
    alignment_rate,
    reference_genome,
    sequencing_center,
    processing_status,
    upload_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY sample_id
      ORDER BY patient_id DESC
    ) AS rn
  FROM unioned
)
SELECT
  sample_id,
  patient_id,
  sequencing_platform,
  run_date,
  technician_name,
  read_length,
  coverage_depth,
  raw_data_size_gb,
  quality_score,
  alignment_rate,
  reference_genome,
  sequencing_center,
  processing_status,
  upload_timestamp
FROM ranked
WHERE rn = 1
""")

specimen_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/specimen_silver.csv"
)

# =============================
# sequencing_run_silver
# =============================
sequencing_run_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    srb.run_id AS run_id,
    srb.patient_id AS patient_id,
    srb.sample_id AS sample_id,
    TRIM(srb.sequencing_platform) AS sequencing_platform,
    CAST(srb.run_date AS date) AS run_date,
    TRIM(srb.technician_name) AS technician_name,
    CAST(srb.read_length AS int) AS read_length,
    CAST(srb.coverage_depth AS double) AS coverage_depth,
    CAST(srb.raw_data_size_gb AS double) AS raw_data_size_gb,
    CAST(srb.quality_score AS double) AS quality_score,
    CAST(srb.alignment_rate AS double) AS alignment_rate,
    TRIM(srb.reference_genome) AS reference_genome,
    TRIM(srb.sequencing_center) AS sequencing_center,
    UPPER(TRIM(srb.processing_status)) AS processing_status,
    CAST(srb.upload_timestamp AS timestamp) AS upload_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY srb.run_id
      ORDER BY srb.upload_timestamp DESC NULLS LAST
    ) AS rn
  FROM sequencing_run_bronze srb
)
SELECT
  run_id,
  patient_id,
  sample_id,
  sequencing_platform,
  run_date,
  technician_name,
  read_length,
  coverage_depth,
  raw_data_size_gb,
  quality_score,
  alignment_rate,
  reference_genome,
  sequencing_center,
  processing_status,
  upload_timestamp
FROM ranked
WHERE rn = 1
""")

sequencing_run_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sequencing_run_silver.csv"
)

# =============================
# lab_result_silver
# =============================
lab_result_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    lrb.result_id AS result_id,
    lrb.patient_id AS patient_id,
    lrb.sample_id AS sample_id,
    TRIM(lrb.test_name) AS test_name,
    TRIM(lrb.biomarker) AS biomarker,
    TRIM(lrb.test_result) AS test_result,
    TRIM(lrb.unit) AS unit,
    TRIM(lrb.reference_range) AS reference_range,
    UPPER(TRIM(lrb.interpretation)) AS interpretation,
    TRIM(lrb.performed_by) AS performed_by,
    TRIM(lrb.lab_name) AS lab_name,
    CAST(lrb.collection_date AS date) AS collection_date,
    CAST(lrb.result_date AS date) AS result_date,
    UPPER(TRIM(lrb.approval_status)) AS approval_status,
    TRIM(lrb.remarks) AS remarks,
    ROW_NUMBER() OVER (
      PARTITION BY lrb.result_id
      ORDER BY lrb.result_date DESC NULLS LAST
    ) AS rn
  FROM lab_result_bronze lrb
)
SELECT
  result_id,
  patient_id,
  sample_id,
  test_name,
  biomarker,
  test_result,
  unit,
  reference_range,
  interpretation,
  performed_by,
  lab_name,
  collection_date,
  result_date,
  approval_status,
  remarks
FROM ranked
WHERE rn = 1
""")

lab_result_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/lab_result_silver.csv"
)

# =============================
# genomic_variant_silver
# =============================
genomic_variant_silver_df = spark.sql("""
WITH ranked AS (
  SELECT
    gvb.variant_id AS variant_id,
    gvb.patient_id AS patient_id,
    gvb.run_id AS run_id,
    UPPER(TRIM(gvb.chromosome)) AS chromosome,
    TRIM(gvb.gene_name) AS gene_name,
    UPPER(TRIM(gvb.variant_type)) AS variant_type,
    TRIM(gvb.mutation) AS mutation,
    CAST(gvb.genomic_position AS int) AS genomic_position,
    UPPER(TRIM(gvb.reference_allele)) AS reference_allele,
    UPPER(TRIM(gvb.alternate_allele)) AS alternate_allele,
    UPPER(TRIM(gvb.clinical_significance)) AS clinical_significance,
    CAST(gvb.pathogenicity_score AS float) AS pathogenicity_score,
    CAST(gvb.detected_date AS date) AS detected_date,
    UPPER(TRIM(gvb.validation_status)) AS validation_status,
    TRIM(gvb.reporting_lab) AS reporting_lab,
    ROW_NUMBER() OVER (
      PARTITION BY gvb.variant_id
      ORDER BY gvb.detected_date DESC NULLS LAST
    ) AS rn
  FROM genomic_variant_bronze gvb
)
SELECT
  variant_id,
  patient_id,
  run_id,
  chromosome,
  gene_name,
  variant_type,
  mutation,
  genomic_position,
  reference_allele,
  alternate_allele,
  clinical_significance,
  pathogenicity_score,
  detected_date,
  validation_status,
  reporting_lab
FROM ranked
WHERE rn = 1
""")

genomic_variant_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/genomic_variant_silver.csv"
)

job.commit()