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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# =========================
# Read Source Tables (S3)
# =========================
patient_data_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_bronze.{FILE_FORMAT}/")
)

genomics_sequencing_runs_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_bronze.{FILE_FORMAT}/")
)

lab_test_results_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_bronze.{FILE_FORMAT}/")
)

genomic_variants_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_bronze.{FILE_FORMAT}/")
)

# =========================
# Create Temp Views
# =========================
patient_data_bronze_df.createOrReplaceTempView("patient_data_bronze")
genomics_sequencing_runs_bronze_df.createOrReplaceTempView(
    "genomics_sequencing_runs_bronze"
)
lab_test_results_bronze_df.createOrReplaceTempView("lab_test_results_bronze")
genomic_variants_bronze_df.createOrReplaceTempView("genomic_variants_bronze")

# =========================
# TARGET: patient_silver
# =========================
patient_silver_df = spark.sql(
    """
SELECT
  patient_id,
  NULLIF(TRIM(first_name),'') AS first_name,
  NULLIF(TRIM(last_name),'') AS last_name,
  NULLIF(UPPER(TRIM(gender)),'') AS gender,
  CAST(date_of_birth AS date) AS date_of_birth,
  NULLIF(UPPER(TRIM(blood_group)),'') AS blood_group,
  NULLIF(TRIM(ethnicity),'') AS ethnicity,
  NULLIF(TRIM(contact_number),'') AS contact_number,
  NULLIF(LOWER(TRIM(email)),'') AS email,
  NULLIF(TRIM(address),'') AS address,
  NULLIF(TRIM(city),'') AS city,
  NULLIF(TRIM(state),'') AS state,
  NULLIF(TRIM(country),'') AS country,
  NULLIF(TRIM(diagnosis),'') AS diagnosis,
  CAST(registration_date AS date) AS registration_date
FROM (
  SELECT
    pdb.*,
    ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY registration_date DESC, patient_id) AS rn
  FROM patient_data_bronze pdb
  WHERE patient_id IS NOT NULL
) x
WHERE rn = 1
"""
)

(
    patient_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_silver.csv")
)

# =========================
# TARGET: sequencing_run_silver
# =========================
sequencing_run_silver_df = spark.sql(
    """
SELECT
  run_id,
  patient_id,
  sample_id,
  NULLIF(TRIM(sequencing_platform),'') AS sequencing_platform,
  CAST(run_date AS date) AS run_date,
  NULLIF(TRIM(technician_name),'') AS technician_name,
  CAST(read_length AS int) AS read_length,
  CAST(coverage_depth AS double) AS coverage_depth,
  CAST(raw_data_size_gb AS double) AS raw_data_size_gb,
  CAST(quality_score AS double) AS quality_score,
  CAST(alignment_rate AS double) AS alignment_rate,
  NULLIF(TRIM(reference_genome),'') AS reference_genome,
  NULLIF(TRIM(sequencing_center),'') AS sequencing_center,
  NULLIF(UPPER(TRIM(processing_status)),'') AS processing_status,
  CAST(upload_timestamp AS timestamp) AS upload_timestamp
FROM (
  SELECT
    gsrb.*,
    ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY upload_timestamp DESC, run_date DESC, run_id) AS rn
  FROM genomics_sequencing_runs_bronze gsrb
  WHERE run_id IS NOT NULL
) x
WHERE rn = 1
"""
)

(
    sequencing_run_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sequencing_run_silver.csv")
)

# =========================
# TARGET: lab_test_result_silver
# =========================
lab_test_result_silver_df = spark.sql(
    """
SELECT
  result_id,
  patient_id,
  sample_id,
  NULLIF(TRIM(test_name),'') AS test_name,
  NULLIF(TRIM(biomarker),'') AS biomarker,
  NULLIF(TRIM(test_result),'') AS test_result,
  NULLIF(TRIM(unit),'') AS unit,
  NULLIF(TRIM(reference_range),'') AS reference_range,
  NULLIF(TRIM(interpretation),'') AS interpretation,
  NULLIF(TRIM(performed_by),'') AS performed_by,
  NULLIF(TRIM(lab_name),'') AS lab_name,
  CAST(collection_date AS date) AS collection_date,
  CAST(result_date AS date) AS result_date,
  NULLIF(UPPER(TRIM(approval_status)),'') AS approval_status,
  NULLIF(TRIM(remarks),'') AS remarks
FROM (
  SELECT
    ltrb.*,
    ROW_NUMBER() OVER (PARTITION BY result_id ORDER BY result_date DESC, collection_date DESC, result_id) AS rn
  FROM lab_test_results_bronze ltrb
  WHERE result_id IS NOT NULL
) x
WHERE rn = 1
"""
)

(
    lab_test_result_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_test_result_silver.csv")
)

# =========================
# TARGET: variant_observation_silver
# =========================
variant_observation_silver_df = spark.sql(
    """
SELECT
  variant_id,
  patient_id,
  run_id,
  NULLIF(TRIM(chromosome),'') AS chromosome,
  NULLIF(TRIM(gene_name),'') AS gene_name,
  NULLIF(TRIM(variant_type),'') AS variant_type,
  NULLIF(TRIM(mutation),'') AS mutation,
  CAST(genomic_position AS int) AS genomic_position,
  NULLIF(TRIM(reference_allele),'') AS reference_allele,
  NULLIF(TRIM(alternate_allele),'') AS alternate_allele,
  NULLIF(TRIM(clinical_significance),'') AS clinical_significance,
  CAST(pathogenicity_score AS float) AS pathogenicity_score,
  CAST(detected_date AS date) AS detected_date,
  NULLIF(UPPER(TRIM(validation_status)),'') AS validation_status,
  NULLIF(TRIM(reporting_lab),'') AS reporting_lab
FROM (
  SELECT
    gvb.*,
    ROW_NUMBER() OVER (PARTITION BY variant_id, patient_id, run_id ORDER BY detected_date DESC, variant_id) AS rn
  FROM genomic_variants_bronze gvb
  WHERE variant_id IS NOT NULL
) x
WHERE rn = 1
"""
)

(
    variant_observation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/variant_observation_silver.csv")
)

job.commit()
