import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/genomics/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# Source Reads + Temp Views
# -----------------------------------------------------------------------------------

patient_data_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_data_5000.{FILE_FORMAT}/")
)
patient_data_5000_df.createOrReplaceTempView("patient_data_5000")

genomics_sequencing_runs_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomics_sequencing_runs_5000.{FILE_FORMAT}/")
)
genomics_sequencing_runs_5000_df.createOrReplaceTempView("genomics_sequencing_runs_5000")

lab_test_results_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/lab_test_results_5000.{FILE_FORMAT}/")
)
lab_test_results_5000_df.createOrReplaceTempView("lab_test_results_5000")

genomic_variants_5000_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/genomic_variants_5000.{FILE_FORMAT}/")
)
genomic_variants_5000_df.createOrReplaceTempView("genomic_variants_5000")

# -----------------------------------------------------------------------------------
# Target: bronze.patient_data_bronze
# -----------------------------------------------------------------------------------

patient_data_bronze_df = spark.sql(
    """
    SELECT
        CAST(pd.patient_id AS STRING) AS patient_id,
        CAST(pd.first_name AS STRING) AS first_name,
        CAST(pd.last_name AS STRING) AS last_name,
        CAST(pd.gender AS STRING) AS gender,
        DATE(pd.date_of_birth) AS date_of_birth,
        CAST(pd.blood_group AS STRING) AS blood_group,
        CAST(pd.ethnicity AS STRING) AS ethnicity,
        CAST(pd.contact_number AS STRING) AS contact_number,
        CAST(pd.email AS STRING) AS email,
        CAST(pd.address AS STRING) AS address,
        CAST(pd.city AS STRING) AS city,
        CAST(pd.state AS STRING) AS state,
        CAST(pd.country AS STRING) AS country,
        CAST(pd.diagnosis AS STRING) AS diagnosis,
        DATE(pd.registration_date) AS registration_date
    FROM patient_data_5000 pd
    """
)

(
    patient_data_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_data_bronze.csv")
)

# -----------------------------------------------------------------------------------
# Target: bronze.genomics_sequencing_runs_bronze
# -----------------------------------------------------------------------------------

genomics_sequencing_runs_bronze_df = spark.sql(
    """
    SELECT
        CAST(gsr.run_id AS STRING) AS run_id,
        CAST(gsr.patient_id AS STRING) AS patient_id,
        CAST(gsr.sample_id AS STRING) AS sample_id,
        CAST(gsr.sequencing_platform AS STRING) AS sequencing_platform,
        DATE(gsr.run_date) AS run_date,
        CAST(gsr.technician_name AS STRING) AS technician_name,
        CAST(gsr.read_length AS INT) AS read_length,
        CAST(gsr.coverage_depth AS DOUBLE) AS coverage_depth,
        CAST(gsr.raw_data_size_gb AS DOUBLE) AS raw_data_size_gb,
        CAST(gsr.quality_score AS DOUBLE) AS quality_score,
        CAST(gsr.alignment_rate AS DOUBLE) AS alignment_rate,
        CAST(gsr.reference_genome AS STRING) AS reference_genome,
        CAST(gsr.sequencing_center AS STRING) AS sequencing_center,
        CAST(gsr.processing_status AS STRING) AS processing_status,
        CAST(gsr.upload_timestamp AS TIMESTAMP) AS upload_timestamp
    FROM genomics_sequencing_runs_5000 gsr
    """
)

(
    genomics_sequencing_runs_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/genomics_sequencing_runs_bronze.csv")
)

# -----------------------------------------------------------------------------------
# Target: bronze.lab_test_results_bronze
# -----------------------------------------------------------------------------------

lab_test_results_bronze_df = spark.sql(
    """
    SELECT
        CAST(ltr.result_id AS STRING) AS result_id,
        CAST(ltr.patient_id AS STRING) AS patient_id,
        CAST(ltr.sample_id AS STRING) AS sample_id,
        CAST(ltr.test_name AS STRING) AS test_name,
        CAST(ltr.biomarker AS STRING) AS biomarker,
        CAST(ltr.test_result AS STRING) AS test_result,
        CAST(ltr.unit AS STRING) AS unit,
        CAST(ltr.reference_range AS STRING) AS reference_range,
        CAST(ltr.interpretation AS STRING) AS interpretation,
        CAST(ltr.performed_by AS STRING) AS performed_by,
        CAST(ltr.lab_name AS STRING) AS lab_name,
        DATE(ltr.collection_date) AS collection_date,
        DATE(ltr.result_date) AS result_date,
        CAST(ltr.approval_status AS STRING) AS approval_status,
        CAST(ltr.remarks AS STRING) AS remarks
    FROM lab_test_results_5000 ltr
    """
)

(
    lab_test_results_bronze_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/lab_test_results_bronze.csv")
)

# -----------------------------------------------------------------------------------
# Target: bronze.genomic_variants_bronze
# -----------------------------------------------------------------------------------

genomic_variants_bronze_df = spark.sql(
    """
    SELECT
        CAST(gv.variant_id AS STRING) AS variant_id,
        CAST(gv.patient_id AS STRING) AS patient_id,
        CAST(gv.run_id AS STRING) AS run_id,
        CAST(gv.chromosome AS STRING) AS chromosome,
        CAST(gv.gene_name AS STRING) AS gene_name,
        CAST(gv.variant_type AS STRING) AS variant_type,
        CAST(gv.mutation AS STRING) AS mutation,
        CAST(gv.genomic_position AS INT) AS genomic_position,
        CAST(gv.reference_allele AS STRING) AS reference_allele,
        CAST(gv.alternate_allele AS STRING) AS alternate_allele,
        CAST(gv.clinical_significance AS STRING) AS clinical_significance,
        CAST(gv.pathogenicity_score AS FLOAT) AS pathogenicity_score,
        DATE(gv.detected_date) AS detected_date,
        CAST(gv.validation_status AS STRING) AS validation_status,
        CAST(gv.reporting_lab AS STRING) AS reporting_lab
    FROM genomic_variants_5000 gv
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
