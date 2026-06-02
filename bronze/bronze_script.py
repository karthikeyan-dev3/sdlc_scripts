import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# ---------------------------------------------------------------------------
# Source: patient_data_5000
# Target: patient_data_bronze
# ---------------------------------------------------------------------------
patient_data_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_5000.{FILE_FORMAT}/")
)
patient_data_5000_df.createOrReplaceTempView("patient_data_5000")

patient_data_bronze_df = spark.sql(
    """
    SELECT
        CAST(pdb.patient_id AS STRING) AS patient_id,
        CAST(pdb.first_name AS STRING) AS first_name,
        CAST(pdb.last_name AS STRING) AS last_name,
        CAST(pdb.gender AS STRING) AS gender,
        DATE(pdb.date_of_birth) AS date_of_birth,
        CAST(pdb.blood_group AS STRING) AS blood_group,
        CAST(pdb.ethnicity AS STRING) AS ethnicity,
        CAST(pdb.contact_number AS STRING) AS contact_number,
        CAST(pdb.email AS STRING) AS email,
        CAST(pdb.address AS STRING) AS address,
        CAST(pdb.city AS STRING) AS city,
        CAST(pdb.state AS STRING) AS state,
        CAST(pdb.country AS STRING) AS country,
        CAST(pdb.diagnosis AS STRING) AS diagnosis,
        DATE(pdb.registration_date) AS registration_date
    FROM patient_data_5000 pdb
    """
)

(
    patient_data_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_data_bronze.csv")
)

# ---------------------------------------------------------------------------
# Source: genomics_sequencing_runs_5000
# Target: genomics_sequencing_runs_bronze
# ---------------------------------------------------------------------------
genomics_sequencing_runs_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_5000.{FILE_FORMAT}/")
)
genomics_sequencing_runs_5000_df.createOrReplaceTempView("genomics_sequencing_runs_5000")

genomics_sequencing_runs_bronze_df = spark.sql(
    """
    SELECT
        CAST(gsrb.run_id AS STRING) AS run_id,
        CAST(gsrb.patient_id AS STRING) AS patient_id,
        CAST(gsrb.sample_id AS STRING) AS sample_id,
        CAST(gsrb.sequencing_platform AS STRING) AS sequencing_platform,
        DATE(gsrb.run_date) AS run_date,
        CAST(gsrb.technician_name AS STRING) AS technician_name,
        CAST(gsrb.read_length AS INT) AS read_length,
        CAST(gsrb.coverage_depth AS DOUBLE) AS coverage_depth,
        CAST(gsrb.raw_data_size_gb AS DOUBLE) AS raw_data_size_gb,
        CAST(gsrb.quality_score AS DOUBLE) AS quality_score,
        CAST(gsrb.alignment_rate AS DOUBLE) AS alignment_rate,
        CAST(gsrb.reference_genome AS STRING) AS reference_genome,
        CAST(gsrb.sequencing_center AS STRING) AS sequencing_center,
        CAST(gsrb.processing_status AS STRING) AS processing_status,
        CAST(gsrb.upload_timestamp AS TIMESTAMP) AS upload_timestamp
    FROM genomics_sequencing_runs_5000 gsrb
    """
)

(
    genomics_sequencing_runs_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/genomics_sequencing_runs_bronze.csv")
)

# ---------------------------------------------------------------------------
# Source: lab_test_results_5000
# Target: lab_test_results_bronze
# ---------------------------------------------------------------------------
lab_test_results_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_5000.{FILE_FORMAT}/")
)
lab_test_results_5000_df.createOrReplaceTempView("lab_test_results_5000")

lab_test_results_bronze_df = spark.sql(
    """
    SELECT
        CAST(ltrb.result_id AS STRING) AS result_id,
        CAST(ltrb.patient_id AS STRING) AS patient_id,
        CAST(ltrb.sample_id AS STRING) AS sample_id,
        CAST(ltrb.test_name AS STRING) AS test_name,
        CAST(ltrb.biomarker AS STRING) AS biomarker,
        CAST(ltrb.test_result AS STRING) AS test_result,
        CAST(ltrb.unit AS STRING) AS unit,
        CAST(ltrb.reference_range AS STRING) AS reference_range,
        CAST(ltrb.interpretation AS STRING) AS interpretation,
        CAST(ltrb.performed_by AS STRING) AS performed_by,
        CAST(ltrb.lab_name AS STRING) AS lab_name,
        DATE(ltrb.collection_date) AS collection_date,
        DATE(ltrb.result_date) AS result_date,
        CAST(ltrb.approval_status AS STRING) AS approval_status,
        CAST(ltrb.remarks AS STRING) AS remarks
    FROM lab_test_results_5000 ltrb
    """
)

(
    lab_test_results_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_test_results_bronze.csv")
)

# ---------------------------------------------------------------------------
# Source: genomic_variants_5000
# Target: genomic_variants_bronze
# ---------------------------------------------------------------------------
genomic_variants_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_5000.{FILE_FORMAT}/")
)
genomic_variants_5000_df.createOrReplaceTempView("genomic_variants_5000")

genomic_variants_bronze_df = spark.sql(
    """
    SELECT
        CAST(gvb.variant_id AS STRING) AS variant_id,
        CAST(gvb.patient_id AS STRING) AS patient_id,
        CAST(gvb.run_id AS STRING) AS run_id,
        CAST(gvb.chromosome AS STRING) AS chromosome,
        CAST(gvb.gene_name AS STRING) AS gene_name,
        CAST(gvb.variant_type AS STRING) AS variant_type,
        CAST(gvb.mutation AS STRING) AS mutation,
        CAST(gvb.genomic_position AS INT) AS genomic_position,
        CAST(gvb.reference_allele AS STRING) AS reference_allele,
        CAST(gvb.alternate_allele AS STRING) AS alternate_allele,
        CAST(gvb.clinical_significance AS STRING) AS clinical_significance,
        CAST(gvb.pathogenicity_score AS FLOAT) AS pathogenicity_score,
        DATE(gvb.detected_date) AS detected_date,
        CAST(gvb.validation_status AS STRING) AS validation_status,
        CAST(gvb.reporting_lab AS STRING) AS reporting_lab
    FROM genomic_variants_5000 gvb
    """
)

(
    genomic_variants_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/genomic_variants_bronze.csv")
)

job.commit()
